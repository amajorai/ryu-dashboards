//! HTTP API for Home dashboards (`/api/dashboards/*`).
//!
//! CRUD over dashboards and their widgets, a debounced layout-only update (the
//! drag/resize path), a force-refresh that resolves a widget's source on demand,
//! a small catalog endpoint (the allowed widget kinds + curated source names the
//! desktop builder UI offers), and an SSE event stream of live widget values.
//!
//! Widget *layout* (x/y/w/h) is a first-class persisted field here — the AI
//! builder arranges widgets, so positions round-trip through Core rather than
//! living in client localStorage.
//!
//! The router is built with its own state ([`DashboardsCtx`]) inside this crate so
//! it returns a state-less, mergeable `Router<()>`. The routes are declared
//! relative to `/api/dashboards` (Core nests this service at that prefix behind
//! the Dashboards-App gate), while the OpenAPI annotations keep the full external
//! paths.

use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    sources, CanvasLayout, Dashboard, DashboardEngine, GridLayout, Widget, WidgetKind, WidgetSource,
};

/// The Core-signed grant of the agent a request was forwarded for, if one is.
/// Core stamps it (and strips any caller copy), and it only ever narrows what a
/// widget may use: Core reads that agent's account access on every provider
/// call made with it.
fn agent_grant(headers: &HeaderMap) -> Option<String> {
    headers
        .get(ryu_app_events::HDR_AGENT_GRANT)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Whether a widget's cached result may be shown to the caller. A person's own
/// UI (no grant header) sees every widget. A limited agent (a grant header) sees
/// only the widgets that run for that same agent: a person's widget runs with
/// the person's accounts, so its cached value is theirs, not the agent's. A
/// failed lookup hides the value (fails closed).
async fn shows_cached_result(
    ctx: &DashboardsCtx,
    caller_grant: Option<&str>,
    widget_id: &str,
) -> bool {
    let Some(caller_grant) = caller_grant else {
        return true;
    };
    matches!(
        ctx.engine.store.widget_agent_grant(widget_id).await,
        Ok(Some(owner)) if owner == caller_grant
    )
}

/// Blank the cached `last_value` and `last_error` of widgets that do not run for
/// the calling agent (see [`shows_cached_result`]).
async fn scrub_for_caller(ctx: &DashboardsCtx, caller_grant: Option<&str>, widgets: &mut [Widget]) {
    for widget in widgets {
        if !shows_cached_result(ctx, caller_grant, &widget.id).await {
            widget.last_value = None;
            widget.last_error = None;
        }
    }
}

/// Router state for the dashboards HTTP surface: the [`DashboardEngine`] (which
/// owns the store, the shared HTTP client, and the inverted host).
#[derive(Clone)]
pub struct DashboardsCtx {
    pub engine: DashboardEngine,
}

impl DashboardsCtx {
    pub fn new(engine: DashboardEngine) -> Self {
        Self { engine }
    }
}

/// Build the `/api/dashboards/*` router with its own state baked in, returning a
/// state-less `Router<()>` the host nests at `/api/dashboards` behind the App
/// gate. Static segments (`events`, `catalog`) are registered before `:id` so
/// they match first.
pub fn routes(ctx: DashboardsCtx) -> Router<()> {
    Router::new()
        .route("/events", get(dashboard_events))
        .route("/catalog", get(catalog))
        // Capability routes: the `provides` rows in `manifest.json` point here. Core's
        // capability broker always POSTs a JSON body to a fixed route, so the by-id
        // and path-param operations get thin body-addressed twins. Registered before
        // `/:id` so the static segments win.
        .route("/changes", post(dashboard_changes))
        .route("/resolve", post(resolve_dashboard))
        .route("/rename", post(rename_dashboard))
        .route("/widgets/upsert", post(upsert_widget))
        .route("/widgets/remove", post(remove_widget))
        // Hardware device-dashboard surface (the `dashboards.device*` capabilities
        // that back Core's hardware display renderer and nudge loop).
        .route("/device/manifest", post(device_manifest))
        .route("/device/image", post(device_image))
        .route("/device/config", post(device_config).put(set_device_config))
        .route("/device/config/set", post(set_device_config))
        .route("/device/ensure", post(device_ensure))
        .route("/device/unbind", post(unbind_device))
        .route(
            "/device-bindings",
            get(device_bindings).post(device_bindings),
        )
        .route(
            "/device/:device_id",
            axum::routing::delete(delete_device_binding),
        )
        .route("/", get(list_dashboards).post(create_dashboard))
        .route(
            "/:id",
            get(get_dashboard)
                .put(update_dashboard)
                .delete(delete_dashboard),
        )
        .route("/:id/widgets", get(list_widgets).post(create_widget))
        .route(
            "/:id/widgets/:wid",
            axum::routing::put(update_widget).delete(delete_widget),
        )
        .route(
            "/:id/widgets/:wid/layout",
            axum::routing::put(update_widget_layout),
        )
        .route("/:id/widgets/:wid/refresh", post(refresh_widget))
        .with_state(ctx)
}

/// The OpenAPI sub-document for the dashboards surface, merged into Core's spec.
pub fn openapi() -> utoipa::openapi::OpenApi {
    <DashboardsApiDoc as utoipa::OpenApi>::openapi()
}

#[derive(utoipa::OpenApi)]
#[openapi(
    paths(
        catalog,
        create_dashboard,
        create_widget,
        dashboard_events,
        delete_dashboard,
        delete_widget,
        get_dashboard,
        list_dashboards,
        list_widgets,
        refresh_widget,
        update_dashboard,
        update_widget,
        update_widget_layout,
    ),
    // Every write route's body type plus the types they nest. utoipa 5 also
    // auto-collects what `paths(...)` reaches, so this is belt-and-braces — but it
    // is greppable, and it keeps the nested types registered even though the body
    // fields inline them (see `WidgetBody::source`).
    components(schemas(
        CanvasLayout,
        DashboardBody,
        DashboardUpdateBody,
        GridLayout,
        LayoutUpdateBody,
        WidgetBody,
        WidgetKind,
        WidgetSource,
    ))
)]
struct DashboardsApiDoc;

fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Query for the SSE `/events` stream: an internal subscriber (the hardware nudge
/// loop) passes `internal=1` so it does NOT register a UI viewer.
#[derive(Debug, Default, Deserialize)]
pub struct EventsQuery {
    #[serde(default, deserialize_with = "de_bool_flag")]
    pub internal: bool,
}

/// Deserialize a permissive boolean flag (`1`/`true`/`yes` ⇒ true) from the query
/// string, so `?internal=1` works the same as `?internal=true`.
fn de_bool_flag<'de, D: serde::Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    let s = String::deserialize(d)?;
    Ok(matches!(s.as_str(), "1" | "true" | "yes" | "on"))
}

// ── Dashboards ───────────────────────────────────────────────────────────────

/// `GET /api/dashboards` — list all dashboards.
#[utoipa::path(
    get,
    path = "/api/dashboards",
    tag = "Dashboards",
    summary = "list all dashboards.",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_dashboards(State(ctx): State<DashboardsCtx>) -> Json<Value> {
    match ctx.engine.store.list_dashboards().await {
        Ok(dashboards) => Json(json!({ "dashboards": dashboards })),
        Err(e) => Json(json!({ "dashboards": [], "error": e.to_string() })),
    }
}

/// Request body for creating a dashboard.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct DashboardBody {
    /// The dashboard's display name. Required and non-blank.
    pub name: String,
}

/// Request body for updating a dashboard: rename and/or switch the desktop render
/// mode. Both fields are optional and applied only when present, so the existing
/// rename client (`{ name }`) and the new view-toggle client (`{ view_mode }`)
/// coexist without either clobbering the other's field.
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
pub struct DashboardUpdateBody {
    /// A new display name. Omit to leave the name alone; a blank string is a 400.
    pub name: Option<String>,
    /// The desktop render mode: `grid` (v1) or `canvas` (v2). Omit to leave it.
    pub view_mode: Option<String>,
}

/// `POST /api/dashboards` — create a dashboard.
#[utoipa::path(
    post,
    path = "/api/dashboards",
    tag = "Dashboards",
    summary = "create a dashboard.",
    request_body = DashboardBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn create_dashboard(
    State(ctx): State<DashboardsCtx>,
    Json(body): Json<DashboardBody>,
) -> (StatusCode, Json<Value>) {
    let name = body.name.trim();
    if name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "name is required" })),
        );
    }
    let now = now();
    let dashboard = Dashboard {
        id: format!("dash_{}", uuid::Uuid::new_v4().simple()),
        name: name.to_string(),
        created_at: now.clone(),
        updated_at: now,
        view_mode: None,
    };
    if let Err(e) = ctx.engine.store.upsert_dashboard(&dashboard).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        );
    }
    (StatusCode::OK, Json(json!({ "dashboard": dashboard })))
}

/// `GET /api/dashboards/:id` — a dashboard with its widgets.
#[utoipa::path(
    get,
    path = "/api/dashboards/{id}",
    tag = "Dashboards",
    summary = "a dashboard with its widgets.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn get_dashboard(
    State(ctx): State<DashboardsCtx>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let dashboard = match ctx.engine.store.get_dashboard(&id).await {
        Ok(Some(d)) => d,
        Ok(None) => return (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };
    let mut widgets = ctx.engine.store.list_widgets(&id).await.unwrap_or_default();
    scrub_for_caller(&ctx, agent_grant(&headers).as_deref(), &mut widgets).await;
    (
        StatusCode::OK,
        Json(json!({
            "dashboard": dashboard,
            "widgets": widgets.iter().map(Widget::redacted).collect::<Vec<_>>(),
        })),
    )
}

/// `PUT /api/dashboards/:id` — rename a dashboard.
#[utoipa::path(
    put,
    path = "/api/dashboards/{id}",
    tag = "Dashboards",
    summary = "rename a dashboard.",
    params(("id" = String, Path)),
    request_body = DashboardUpdateBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn update_dashboard(
    State(ctx): State<DashboardsCtx>,
    Path(id): Path<String>,
    Json(body): Json<DashboardUpdateBody>,
) -> (StatusCode, Json<Value>) {
    let mut dashboard = match ctx.engine.store.get_dashboard(&id).await {
        Ok(Some(d)) => d,
        Ok(None) => return (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };
    // Rename when a (non-empty) name is supplied; a present-but-blank name is a bad
    // request (the historical contract), while an absent name is a view-only update.
    if let Some(name) = body.name.as_deref() {
        let name = name.trim();
        if name.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "name is required" })),
            );
        }
        dashboard.name = name.to_string();
    }
    if let Some(view_mode) = body.view_mode.as_deref() {
        if !matches!(view_mode, "grid" | "canvas") {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "view_mode must be 'grid' or 'canvas'" })),
            );
        }
        dashboard.view_mode = Some(view_mode.to_string());
    }
    dashboard.updated_at = now();
    if let Err(e) = ctx.engine.store.upsert_dashboard(&dashboard).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        );
    }
    (StatusCode::OK, Json(json!({ "dashboard": dashboard })))
}

/// `DELETE /api/dashboards/:id` — remove a dashboard and its widgets.
#[utoipa::path(
    delete,
    path = "/api/dashboards/{id}",
    tag = "Dashboards",
    summary = "remove a dashboard and its widgets.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn delete_dashboard(
    State(ctx): State<DashboardsCtx>,
    Path(id): Path<String>,
) -> (StatusCode, Json<Value>) {
    match ctx.engine.store.delete_dashboard(&id).await {
        Ok(true) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(false) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

// ── Widgets ──────────────────────────────────────────────────────────────────

/// `GET /api/dashboards/:id/widgets` — the widgets on a dashboard.
#[utoipa::path(
    get,
    path = "/api/dashboards/{id}/widgets",
    tag = "Dashboards",
    summary = "the widgets on a dashboard.",
    params(("id" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn list_widgets(
    State(ctx): State<DashboardsCtx>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Json<Value> {
    match ctx.engine.store.list_widgets(&id).await {
        Ok(mut widgets) => {
            scrub_for_caller(&ctx, agent_grant(&headers).as_deref(), &mut widgets).await;
            Json(json!({
                "widgets": widgets.iter().map(Widget::redacted).collect::<Vec<_>>(),
            }))
        }
        Err(e) => Json(json!({ "widgets": [], "error": e.to_string() })),
    }
}

/// Request body for creating/updating a widget. All optional except `kind` +
/// `source` on create; on update, missing fields keep their current value.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
pub struct WidgetBody {
    /// Optional caller-chosen widget id. When present + non-empty on `create`, the
    /// widget is upserted under THIS id (INSERT-OR-REPLACE) rather than a fresh
    /// generated one — so the `dashboard_builder` "pass an id to replace it" path
    /// round-trips through one create endpoint. Absent ⇒ a new `wgt_…` id.
    pub id: Option<String>,
    /// Which fixed component renders this widget. Required on create.
    // Inlined for the same reason as `source` below.
    #[schema(inline)]
    pub kind: Option<WidgetKind>,
    /// The heading shown above the widget.
    pub title: Option<String>,
    /// Free-form per-kind render options (axis keys, columns, units, …). Genuinely
    /// open-ended — it is stored and handed to the renderer, never interpreted here.
    pub config: Option<Value>,
    /// Where the widget's live data comes from. Defaults to empty static data.
    // `Option<WidgetSource>` renders as a nullable `oneOf`, which buries the
    // variant `$ref` one level deeper than Core's importer resolves — the model
    // would see an opaque pointer instead of the source kinds. `inline` splices
    // the real union in. The type stays registered in `components(schemas(...))`
    // so dropping this attribute later cannot dangle the reference.
    #[schema(inline)]
    pub source: Option<WidgetSource>,
    /// How often to re-resolve the source, as a duration like `30s` or `5m`.
    /// Clamped to a per-source-kind floor; omit to use that kind's default.
    pub refresh_interval: Option<String>,
    /// Position + size on the 12-column grid (the v1 view).
    #[schema(inline)]
    pub layout: Option<GridLayout>,
    /// Optional canvas (v2) position/size. Additive: old clients omit it and the
    /// widget derives its canvas placement from `layout` on demand.
    #[schema(inline)]
    pub canvas: Option<CanvasLayout>,
}

/// Reject a source this host cannot resolve, with the widget-facing explanation.
/// Enforced here (the store-owning process) so the desktop builder UI, the
/// `dashboard_builder` MCP tool, and the device config path get the same
/// guarantee: nothing is persisted that the refresh loop could only fail on.
pub(crate) fn ensure_source_supported(
    engine: &DashboardEngine,
    source: &WidgetSource,
) -> Result<(), String> {
    let source_type = source.source_type();
    if engine.host.supports_source(source_type) {
        return Ok(());
    }
    Err(format!(
        "source '{source_type}' is not available on this host. {}",
        sources::unsupported_source_message(source_type)
    ))
}

/// Whether `source` is exactly the source already stored on `widget_id` of
/// `dashboard_id`. A saved widget whose source this host cannot resolve (an old
/// seeded `core_endpoint` preset, say) must stay re-upsertable with that source
/// unchanged, so moving, resizing or retitling it — the builder resends the whole
/// widget to re-arrange a board — does not fail. Only a new or changed source goes
/// through [`ensure_source_supported`].
async fn is_stored_source(
    engine: &DashboardEngine,
    dashboard_id: &str,
    widget_id: Option<&str>,
    source: &WidgetSource,
) -> bool {
    let Some(widget_id) = widget_id.filter(|id| !id.trim().is_empty()) else {
        return false;
    };
    match engine
        .store
        .get_widget_for_dashboard(dashboard_id, widget_id)
        .await
    {
        Ok(Some(stored)) => same_source(&stored.source, source),
        _ => false,
    }
}

/// Structural equality of two sources (by their wire form, so field order and an
/// omitted-vs-null optional field do not matter).
fn same_source(a: &WidgetSource, b: &WidgetSource) -> bool {
    matches!(
        (serde_json::to_value(a), serde_json::to_value(b)),
        (Ok(a), Ok(b)) if a == b
    )
}

/// Reject a supported source whose config is unsafe. Other source kinds are
/// structurally validated by serde.
pub(crate) fn validate_widget_source(source: &WidgetSource) -> Result<(), String> {
    // The Composio `action` is forwarded as a provider tool id. Reject
    // path-traversal at write time too, so a bad config never reaches the store —
    // the resolve path re-checks regardless.
    if let WidgetSource::Composio { action, .. } = source {
        if !sources::id_segment_is_safe(action) {
            return Err(format!(
                "invalid composio action '{action}': must be a plain action id (no '/', '..', or query chars)"
            ));
        }
    }
    Ok(())
}

/// `POST /api/dashboards/:id/widgets` — add a widget.
#[utoipa::path(
    post,
    path = "/api/dashboards/{id}/widgets",
    tag = "Dashboards",
    summary = "add a widget.",
    params(("id" = String, Path)),
    request_body = WidgetBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn create_widget(
    State(ctx): State<DashboardsCtx>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<WidgetBody>,
) -> (StatusCode, Json<Value>) {
    if ctx
        .engine
        .store
        .get_dashboard(&id)
        .await
        .ok()
        .flatten()
        .is_none()
    {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "dashboard not found" })),
        );
    }
    let kind = match body.kind {
        Some(k) => k,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "kind is required" })),
            )
        }
    };
    let mut source = body
        .source
        .unwrap_or(WidgetSource::Static { data: Value::Null });
    // A caller id naming an existing widget replaces it: a header that echoes the
    // redacted placeholder keeps the stored value.
    let previous = match body.id.as_deref().filter(|s| !s.trim().is_empty()) {
        Some(existing_id) => ctx
            .engine
            .store
            .get_widget_for_dashboard(&id, existing_id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    if let Err(error) = source.keep_redacted_headers(previous.as_ref().map(|w| &w.source)) {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": error })));
    }
    let unchanged = is_stored_source(&ctx.engine, &id, body.id.as_deref(), &source).await;
    if !unchanged {
        if let Err(error) = ensure_source_supported(&ctx.engine, &source) {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": error })));
        }
    }
    if let Err(e) = validate_widget_source(&source) {
        return (StatusCode::BAD_REQUEST, Json(json!({ "error": e })));
    }
    let widget = Widget {
        id: body
            .id
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| format!("wgt_{}", uuid::Uuid::new_v4().simple())),
        dashboard_id: id,
        kind,
        title: body.title.unwrap_or_default(),
        config: body.config.unwrap_or(Value::Null),
        source,
        refresh_interval: body.refresh_interval.filter(|s| !s.trim().is_empty()),
        layout: body.layout.unwrap_or_default(),
        canvas: body.canvas,
        last_value: None,
        last_refresh_at: None,
        last_error: None,
    };
    // The writer decides who the widget runs for: an agent's grant, or none for
    // a person (which also replaces an agent's on a widget written over).
    if let Err(e) = ctx
        .engine
        .store
        .upsert_widget_for(&widget, agent_grant(&headers).as_deref())
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        );
    }
    (StatusCode::OK, Json(json!({ "widget": widget.redacted() })))
}

/// `PUT /api/dashboards/:id/widgets/:wid` — edit a widget (partial patch).
#[utoipa::path(
    put,
    path = "/api/dashboards/{id}/widgets/{wid}",
    tag = "Dashboards",
    summary = "edit a widget (partial patch).",
    params(("id" = String, Path)),
    params(("wid" = String, Path)),
    request_body = WidgetBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn update_widget(
    State(ctx): State<DashboardsCtx>,
    Path((id, wid)): Path<(String, String)>,
    headers: HeaderMap,
    Json(body): Json<WidgetBody>,
) -> (StatusCode, Json<Value>) {
    let mut widget = match ctx.engine.store.get_widget_for_dashboard(&id, &wid).await {
        Ok(Some(w)) => w,
        Ok(None) => return (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };
    let mut source_changed = false;
    if let Some(k) = body.kind {
        widget.kind = k;
    }
    if let Some(t) = body.title {
        widget.title = t;
    }
    if let Some(c) = body.config {
        widget.config = c;
    }
    if let Some(mut s) = body.source {
        if let Err(error) = s.keep_redacted_headers(Some(&widget.source)) {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": error })));
        }
        // Resending the stored source unchanged is not a source change, even when
        // this host cannot resolve that kind (see `is_stored_source`).
        if !same_source(&widget.source, &s) {
            if let Err(error) = ensure_source_supported(&ctx.engine, &s) {
                return (StatusCode::BAD_REQUEST, Json(json!({ "error": error })));
            }
        }
        if let Err(error) = validate_widget_source(&s) {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": error })));
        }
        widget.source = s;
        source_changed = true;
        // A new source invalidates the cached value.
        widget.last_value = None;
        widget.last_error = None;
        widget.last_refresh_at = None;
    }
    if let Some(i) = body.refresh_interval {
        widget.refresh_interval = Some(i).filter(|s| !s.trim().is_empty());
    }
    if let Some(l) = body.layout {
        widget.layout = l;
    }
    if let Some(c) = body.canvas {
        widget.canvas = Some(c);
    }
    // An agent's edit makes the widget run for that agent. A person's edit
    // hands it back to the person only when they choose its source; a title or
    // layout change leaves an agent's widget running for the agent.
    let grant = agent_grant(&headers);
    let written = if grant.is_some() || source_changed {
        ctx.engine
            .store
            .upsert_widget_for(&widget, grant.as_deref())
            .await
    } else {
        ctx.engine.store.upsert_widget(&widget).await
    };
    if let Err(e) = written {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        );
    }
    (StatusCode::OK, Json(json!({ "widget": widget.redacted() })))
}

/// `DELETE /api/dashboards/:id/widgets/:wid` — remove a widget.
#[utoipa::path(
    delete,
    path = "/api/dashboards/{id}/widgets/{wid}",
    tag = "Dashboards",
    summary = "remove a widget.",
    params(("id" = String, Path)),
    params(("wid" = String, Path)),
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn delete_widget(
    State(ctx): State<DashboardsCtx>,
    Path((id, wid)): Path<(String, String)>,
) -> (StatusCode, Json<Value>) {
    match ctx
        .engine
        .store
        .delete_widget_for_dashboard(&id, &wid)
        .await
    {
        Ok(true) => (StatusCode::OK, Json(json!({ "ok": true }))),
        Ok(false) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// Additive body for the layout PUT. The v1 grid client sends `{ x, y, w, h }`
/// (all four ⇒ a `GridLayout`); the v2 canvas client sends `{ canvas: {x,y,w,h} }`.
/// Both may be present. Every field is optional so old and new clients coexist and
/// a canvas drag never rewrites the grid arrangement (and vice-versa).
#[derive(Debug, Default, Deserialize, utoipa::ToSchema)]
pub struct LayoutUpdateBody {
    /// Grid column offset. The grid is only rewritten when all four of
    /// `x`/`y`/`w`/`h` are present.
    pub x: Option<u32>,
    /// Grid row offset. See `x`.
    pub y: Option<u32>,
    /// Width in grid columns. See `x`.
    pub w: Option<u32>,
    /// Height in grid rows. See `x`.
    pub h: Option<u32>,
    /// The canvas (v2) rect, applied independently of the grid fields above.
    // Inlined — see `WidgetBody::source` for why a nested `Option<T>` needs it.
    #[schema(inline)]
    pub canvas: Option<CanvasLayout>,
}

/// `PUT /api/dashboards/:id/widgets/:wid/layout` — persist drag/resize only.
///
/// Accepts a grid rect (`x`/`y`/`w`/`h`, the v1 path) and/or a `canvas` rect (v2),
/// applying only the fields present so the two views stay independent.
#[utoipa::path(
    put,
    path = "/api/dashboards/{id}/widgets/{wid}/layout",
    tag = "Dashboards",
    summary = "persist drag/resize only.",
    params(("id" = String, Path)),
    params(("wid" = String, Path)),
    request_body = LayoutUpdateBody,
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn update_widget_layout(
    State(ctx): State<DashboardsCtx>,
    Path((id, wid)): Path<(String, String)>,
    Json(body): Json<LayoutUpdateBody>,
) -> (StatusCode, Json<Value>) {
    // A grid update requires the full rect; a partial/absent rect leaves grid alone.
    let grid = match (body.x, body.y, body.w, body.h) {
        (Some(x), Some(y), Some(w), Some(h)) => Some(GridLayout { x, y, w, h }),
        _ => None,
    };
    match ctx
        .engine
        .store
        .update_widget_layout_fields_for_dashboard(&id, &wid, grid, body.canvas)
        .await
    {
        Ok(Some(w)) => (StatusCode::OK, Json(json!({ "widget": w.redacted() }))),
        Ok(None) => (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

/// `POST /api/dashboards/:id/widgets/:wid/refresh` — resolve the source now.
#[utoipa::path(
    post,
    path = "/api/dashboards/{id}/widgets/{wid}/refresh",
    tag = "Dashboards",
    summary = "resolve the source now.",
    params(("id" = String, Path)),
    params(("wid" = String, Path)),
    // No `request_body`: the handler takes only the two path ids. Declaring one
    // would give the derived LLM tool a phantom argument it can never fill.
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn refresh_widget(
    State(ctx): State<DashboardsCtx>,
    Path((id, wid)): Path<(String, String)>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let widget = match ctx.engine.store.get_widget_for_dashboard(&id, &wid).await {
        Ok(Some(w)) => w,
        Ok(None) => return (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))),
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
        }
    };
    // An agent asking runs the widget as itself; otherwise the widget runs for
    // the agent recorded on it, if any.
    let grant = match agent_grant(&headers) {
        Some(grant) => Some(grant),
        None => match ctx.engine.store.widget_agent_grant(&wid).await {
            Ok(grant) => grant,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": e.to_string() })),
                )
            }
        },
    };
    let result = sources::resolve(
        &ctx.engine.http,
        ctx.engine.host.as_ref(),
        &widget.source,
        &wid,
        grant.as_deref(),
    )
    .await
    .map_err(|e| e.to_string());
    let _ = ctx
        .engine
        .store
        .update_widget_value(&wid, result.clone())
        .await;
    match result {
        Ok(value) => (StatusCode::OK, Json(json!({ "value": value }))),
        Err(error) => (StatusCode::OK, Json(json!({ "error": error }))),
    }
}

/// `GET /api/dashboards/catalog` — the widget kinds + source types the builder UI
/// offers (the constrained catalog, surfaced for the desktop pickers). Only source
/// types this host can resolve are listed. `core_endpoints` stays in the shape for
/// older desktops that require it, and is empty while no host can read Core
/// endpoints (see [`crate::CORE_READ_SOURCE_TYPES`]).
#[utoipa::path(
    get,
    path = "/api/dashboards/catalog",
    tag = "Dashboards",
    summary = "the widget kinds + curated source names the",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn catalog(State(ctx): State<DashboardsCtx>) -> Json<Value> {
    Json(json!({
        "widget_kinds": [
            "stat", "line_chart", "bar_chart", "area_chart", "pie_chart",
            "table", "list", "text", "map", "agent_feed"
        ],
        "source_types": crate::SOURCE_TYPES.iter()
            .filter(|source_type| ctx.engine.host.supports_source(source_type)).collect::<Vec<_>>(),
        "core_endpoints": Vec::<&str>::new(),
    }))
}

/// `GET /api/dashboards/events` — SSE feed of live widget values + definition
/// changes. Mirrors `quests_api::quest_events`.
#[utoipa::path(
    get,
    path = "/api/dashboards/events",
    tag = "Dashboards",
    summary = "SSE feed of live widget values + definition",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn dashboard_events(
    State(ctx): State<DashboardsCtx>,
    axum::extract::Query(q): axum::extract::Query<EventsQuery>,
) -> axum::response::sse::Sse<
    impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    event_stream(&ctx, q.internal)
}

/// `POST /api/dashboards/changes` — the `dashboards.changes` streaming capability:
/// the same SSE feed as `/events`, always as an INTERNAL subscriber (no viewer
/// guard). Core's hardware nudge loop subscribes here through its capability
/// consumer and reconnects on its own policy when this sidecar restarts.
pub async fn dashboard_changes(
    State(ctx): State<DashboardsCtx>,
) -> axum::response::sse::Sse<
    impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    event_stream(&ctx, true)
}

/// The SSE feed behind `/events` and `/changes`.
fn event_stream(
    ctx: &DashboardsCtx,
    internal: bool,
) -> axum::response::sse::Sse<
    impl futures_util::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event, KeepAlive, Sse};
    use tokio::sync::broadcast::error::RecvError;

    let rx = ctx.engine.store.subscribe();
    // Hold a viewer guard for the life of the stream so the refresh loop knows a
    // human is watching (and runs expensive sources). Carried in the unfold state so
    // it drops exactly when the client disconnects. An INTERNAL subscriber (the
    // hardware nudge loop through `/changes`, or `/events?internal=1`) holds NO
    // guard, so it never fakes a UI viewer and defeats the refresh cost guard.
    let guard = if internal {
        None
    } else {
        Some(ctx.engine.store.viewer_guard())
    };
    // Seed the stream with an immediate SSE comment so the FIRST body byte lands at
    // connect, not only when the first dashboard event (or the 15s keep-alive) arrives.
    // Dashboards is frequently idle for long stretches (no source change), so without this
    // seed the stream stays byte-silent until the keep-alive — and any intermediary that
    // withholds the response head behind the first upstream body byte (the ext-proxy's
    // pre-streaming failure mode) reads that as a "no headers for ~15s" hang. A comment
    // line is ignored by `EventSource`, so this is invisible to real consumers. The `true`
    // in the unfold seed is the "emit the priming comment on first poll" flag.
    let stream =
        futures_util::stream::unfold((rx, guard, true), |(mut rx, guard, first)| async move {
            if first {
                return Some((Ok(Event::default().comment("ready")), (rx, guard, false)));
            }
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        let data = serde_json::to_string(&event).unwrap_or_default();
                        return Some((Ok(Event::default().data(data)), (rx, guard, false)));
                    }
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => return None,
                }
            }
        });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

// ── Capability routes (body-addressed twins) ─────────────────────────────────
//
// Core reaches this app only through the capabilities `manifest.json` `provides`.
// The broker always POSTs a JSON body to one fixed route, so operations whose REST
// form takes path ids (`GET /:id`, `PUT /:id`, `POST /:id/widgets`,
// `DELETE /:id/widgets/:wid`) get a twin that reads the ids from the body and
// delegates to the REST handler, so both forms share one implementation. They are
// declared in the manifest (the loader requires a provided route to be declared) but
// deliberately left out of the OpenAPI document: they would only duplicate the REST
// tools an agent already derives.

/// Body for `POST /api/dashboards/resolve` (`dashboards.get`).
#[derive(Debug, Deserialize)]
pub struct DashboardRefBody {
    /// The dashboard id.
    pub id: String,
}

/// `POST /api/dashboards/resolve` — `GET /:id` by body: `{ dashboard, widgets }`, or
/// a 404 for an unknown id.
pub async fn resolve_dashboard(
    State(ctx): State<DashboardsCtx>,
    headers: HeaderMap,
    Json(body): Json<DashboardRefBody>,
) -> (StatusCode, Json<Value>) {
    get_dashboard(State(ctx), Path(body.id), headers).await
}

/// Body for `POST /api/dashboards/rename` (`dashboards.rename`).
#[derive(Debug, Deserialize)]
pub struct DashboardRenameBody {
    /// The dashboard id.
    pub id: String,
    /// The new display name; blank is a 400.
    pub name: String,
}

/// `POST /api/dashboards/rename` — `PUT /:id { name }` by body.
pub async fn rename_dashboard(
    State(ctx): State<DashboardsCtx>,
    Json(body): Json<DashboardRenameBody>,
) -> (StatusCode, Json<Value>) {
    update_dashboard(
        State(ctx),
        Path(body.id),
        Json(DashboardUpdateBody {
            name: Some(body.name),
            view_mode: None,
        }),
    )
    .await
}

/// Body for `POST /api/dashboards/widgets/upsert` (`dashboards.upsertWidget`).
#[derive(Debug, Deserialize)]
pub struct WidgetUpsertBody {
    /// The dashboard the widget belongs to.
    pub dashboard_id: String,
    /// The widget; an `id` naming an existing widget replaces it.
    pub widget: WidgetBody,
}

/// `POST /api/dashboards/widgets/upsert` — `POST /:id/widgets` by body. A 4xx is a
/// rejection the caller can read (unknown dashboard, bad kind, disallowed source).
pub async fn upsert_widget(
    State(ctx): State<DashboardsCtx>,
    headers: HeaderMap,
    Json(body): Json<WidgetUpsertBody>,
) -> (StatusCode, Json<Value>) {
    create_widget(
        State(ctx),
        Path(body.dashboard_id),
        headers,
        Json(body.widget),
    )
    .await
}

/// Body for `POST /api/dashboards/widgets/remove` (`dashboards.deleteWidget`).
#[derive(Debug, Deserialize)]
pub struct WidgetRefBody {
    /// The dashboard the widget belongs to.
    pub dashboard_id: String,
    /// The widget id.
    pub widget_id: String,
}

/// `POST /api/dashboards/widgets/remove` — `DELETE /:id/widgets/:wid` by body.
pub async fn remove_widget(
    State(ctx): State<DashboardsCtx>,
    Json(body): Json<WidgetRefBody>,
) -> (StatusCode, Json<Value>) {
    delete_widget(State(ctx), Path((body.dashboard_id, body.widget_id))).await
}

// ── Hardware device-dashboard surface ────────────────────────────────────────
//
// These endpoints are the `dashboards.device*` capabilities behind Core's
// `ryu_hardware::DashboardFeed` adapter (the TRMNL display renderer and the nudge
// loop). Core reaches them through its capability consumer, bearer-gated like the
// rest of the sidecar. Each delegates to `crate::device::*`, the SAME render fns an
// in-process feed would use, so a decoupled node renders byte-identically. Like the
// capability twins above they are declared but not in the OpenAPI document: they
// are kernel plumbing, not agent tools.

/// Device metadata a render call needs: identity + panel class + saved prefs.
#[derive(Debug, Deserialize)]
pub struct DeviceRenderReq {
    pub device_id: String,
    #[serde(default)]
    pub device_name: String,
    #[serde(default)]
    pub device_type: String,
    #[serde(default)]
    pub prefs: Value,
    /// For the image endpoint: the `rev` the caller already holds (⇒ 304).
    #[serde(default)]
    pub known_rev: Option<String>,
}

/// `POST /api/dashboards/device/manifest` — the display manifest facts (renders
/// internally to compute the current `rev`).
pub async fn device_manifest(
    State(ctx): State<DashboardsCtx>,
    Json(req): Json<DeviceRenderReq>,
) -> Response {
    match crate::device::render_device(
        &ctx.engine,
        &req.device_id,
        &req.device_name,
        &req.device_type,
        &req.prefs,
    )
    .await
    {
        Ok((image, dd)) => Json(json!({
            "rev": image.rev(),
            "refresh_rate": dd.refresh_rate,
            "screen": crate::device::screen_json(&image.profile),
        }))
        .into_response(),
        Err(e) => internal_err(&e.to_string()),
    }
}

/// `POST /api/dashboards/device/image` — the rendered panel bytes, or `304` when
/// the caller's `known_rev` still matches the freshly-rendered content.
pub async fn device_image(
    State(ctx): State<DashboardsCtx>,
    Json(req): Json<DeviceRenderReq>,
) -> Response {
    match crate::device::render_device(
        &ctx.engine,
        &req.device_id,
        &req.device_name,
        &req.device_type,
        &req.prefs,
    )
    .await
    {
        Ok((image, _dd)) => {
            let rev = image.rev();
            if req.known_rev.as_deref() == Some(rev.as_str()) {
                return StatusCode::NOT_MODIFIED.into_response();
            }
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, image.content_type.to_string()),
                    (header::ETAG, format!("\"{rev}\"")),
                    (header::CACHE_CONTROL, "no-cache".to_string()),
                ],
                image.bytes,
            )
                .into_response()
        }
        Err(e) => internal_err(&e.to_string()),
    }
}

/// `POST /api/dashboards/device/config` — the device-dashboard config JSON
/// (binding + widgets + screen). POST (not GET) because it carries device metadata.
pub async fn device_config(
    State(ctx): State<DashboardsCtx>,
    headers: HeaderMap,
    Json(req): Json<DeviceRenderReq>,
) -> Response {
    match crate::device::device_config_json(
        &ctx.engine,
        &req.device_id,
        &req.device_name,
        &req.device_type,
        &req.prefs,
    )
    .await
    {
        Ok(mut config) => {
            // A limited agent does not see cached results of widgets that run
            // for someone else (see `shows_cached_result`).
            let grant = agent_grant(&headers);
            if let Some(widgets) = config.get_mut("widgets").and_then(Value::as_array_mut) {
                for widget in widgets {
                    let id = widget.get("id").and_then(Value::as_str).unwrap_or("");
                    if shows_cached_result(&ctx, grant.as_deref(), id).await {
                        continue;
                    }
                    if let Some(fields) = widget.as_object_mut() {
                        fields.remove("last_value");
                        fields.remove("last_error");
                    }
                }
            }
            Json(config).into_response()
        }
        Err(e) => internal_err(&e.to_string()),
    }
}

/// Body for `PUT /api/dashboards/device/config`.
#[derive(Debug, Deserialize)]
pub struct DeviceSetReq {
    pub device_id: String,
    #[serde(default)]
    pub device_name: String,
    #[serde(default)]
    pub refresh_rate: Option<u32>,
    #[serde(default)]
    pub widgets: Option<Value>,
}

/// `PUT /api/dashboards/device/config` — set the device's poll interval and/or
/// replace its widget selection.
pub async fn set_device_config(
    State(ctx): State<DashboardsCtx>,
    headers: HeaderMap,
    Json(req): Json<DeviceSetReq>,
) -> Response {
    match crate::device::set_device_config(
        &ctx.engine,
        &req.device_id,
        &req.device_name,
        req.refresh_rate,
        req.widgets,
        agent_grant(&headers).as_deref(),
    )
    .await
    {
        Ok((dashboard_id, refresh_rate)) => Json(json!({
            "ok": true,
            "dashboard_id": dashboard_id,
            "refresh_rate": refresh_rate,
        }))
        .into_response(),
        // A bad widget batch is a client error (the render fn validates sources).
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Body for `POST /api/dashboards/device/ensure`.
#[derive(Debug, Deserialize)]
pub struct DeviceEnsureReq {
    pub device_id: String,
    #[serde(default)]
    pub device_name: Option<String>,
}

/// `POST /api/dashboards/device/ensure` — ensure a device has a bound dashboard
/// (created on first use) and return its id. Backs the builder's device-target
/// path ("add a widget to my desk").
pub async fn device_ensure(
    State(ctx): State<DashboardsCtx>,
    Json(req): Json<DeviceEnsureReq>,
) -> Response {
    let name = req.device_name.unwrap_or_else(|| req.device_id.clone());
    match crate::device::ensure_device_dashboard(&ctx.engine, &req.device_id, &name).await {
        Ok(dd) => Json(json!({ "dashboard_id": dd.dashboard_id })).into_response(),
        Err(e) => internal_err(&e.to_string()),
    }
}

/// `GET /api/dashboards/device-bindings` — every device → dashboard binding (the
/// nudge loop's work list).
pub async fn device_bindings(State(ctx): State<DashboardsCtx>) -> Response {
    match ctx.engine.store.list_device_dashboards().await {
        Ok(rows) => {
            let items: Vec<Value> = rows
                .into_iter()
                .map(|dd| json!({ "device_id": dd.device_id, "dashboard_id": dd.dashboard_id }))
                .collect();
            Json(json!({ "bindings": items })).into_response()
        }
        Err(e) => internal_err(&e.to_string()),
    }
}

/// `DELETE /api/dashboards/device/:device_id` — drop a device's dashboard binding
/// (on device revoke). Best-effort; a missing binding is still `ok`.
pub async fn delete_device_binding(
    State(ctx): State<DashboardsCtx>,
    Path(device_id): Path<String>,
) -> Response {
    let _ = ctx.engine.store.delete_device_dashboard(&device_id).await;
    Json(json!({ "ok": true })).into_response()
}

/// Body for `POST /api/dashboards/device/unbind` (`dashboards.unbindDevice`).
#[derive(Debug, Deserialize)]
pub struct DeviceRefBody {
    pub device_id: String,
}

/// `POST /api/dashboards/device/unbind` — `DELETE /device/:device_id` by body.
pub async fn unbind_device(
    State(ctx): State<DashboardsCtx>,
    Json(body): Json<DeviceRefBody>,
) -> Response {
    delete_device_binding(State(ctx), Path(body.device_id)).await
}

/// A `500` JSON error body used by the device endpoints.
fn internal_err(msg: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": msg })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WidgetSource;

    #[test]
    fn validate_widget_source_ignores_free_form_kinds() {
        // Static/Http carry no id forwarded as a provider tool id, so serde's shape
        // check is all they need.
        assert!(validate_widget_source(&WidgetSource::Static { data: Value::Null }).is_ok());
        assert!(validate_widget_source(&WidgetSource::Http {
            url: "https://example.com/x.json".into(),
            selector: None,
            headers: Default::default(),
        })
        .is_ok());
    }

    #[test]
    fn validate_widget_source_rejects_path_traversal_ids() {
        // F6: the Composio action is forwarded as a provider tool id. A traversal
        // value must be rejected at write time (the resolve path re-checks).
        let bad_action = WidgetSource::Composio {
            action: "../../v1/config".into(),
            args: Value::Null,
        };
        assert!(
            validate_widget_source(&bad_action).is_err(),
            "traversal composio action must be rejected"
        );
        // An uppercase-underscore action still passes.
        assert!(validate_widget_source(&WidgetSource::Composio {
            action: "GITHUB_LIST".into(),
            args: Value::Null,
        })
        .is_ok());
    }

    #[test]
    fn ensure_source_supported_refuses_core_read_kinds_with_the_widget_message() {
        let engine = test_engine();
        let src = WidgetSource::Monitor {
            monitor_id: "m1".into(),
        };
        let err = ensure_source_supported(&engine, &src).expect_err("monitor is unsupported");
        assert!(err.contains("source 'monitor' is not available"), "{err}");
        assert!(err.contains(&sources::unsupported_source_message("monitor")));
        assert!(
            ensure_source_supported(&engine, &WidgetSource::Static { data: Value::Null }).is_ok()
        );
    }

    // ── Handler-level tests ──────────────────────────────────────────────────
    //
    // The axum handlers are exercised by calling them directly with constructed
    // extractors (`State`/`Path`/`Json`/`Query` are public tuple structs). This
    // reaches the full request→store→response logic with no live socket.

    use crate::testutil::test_engine;
    use axum::extract::Query;

    fn ctx() -> DashboardsCtx {
        DashboardsCtx::new(test_engine())
    }

    /// Deserialize a request body struct from a JSON literal (also exercises the
    /// body's serde contract).
    fn body<T: serde::de::DeserializeOwned>(v: Value) -> T {
        serde_json::from_value(v).expect("body deserializes")
    }

    /// Collect a `Response` into `(status, json-body)`.
    async fn read(resp: Response) -> (StatusCode, Value) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, v)
    }

    async fn seed_dashboard(ctx: &DashboardsCtx, id: &str) {
        let now = now();
        let d = Dashboard {
            id: id.into(),
            name: "Seed".into(),
            created_at: now.clone(),
            updated_at: now,
            view_mode: None,
        };
        ctx.engine.store.upsert_dashboard(&d).await.unwrap();
    }

    #[test]
    fn de_bool_flag_is_permissive() {
        for truthy in ["1", "true", "yes", "on"] {
            let q: EventsQuery = serde_json::from_value(json!({ "internal": truthy })).unwrap();
            assert!(q.internal, "{truthy} ⇒ true");
        }
        for falsy in ["0", "false", "no", "off", "banana"] {
            let q: EventsQuery = serde_json::from_value(json!({ "internal": falsy })).unwrap();
            assert!(!q.internal, "{falsy} ⇒ false");
        }
        // Absent ⇒ default false.
        let q: EventsQuery = EventsQuery::default();
        assert!(!q.internal);
    }

    #[test]
    fn routes_and_openapi_build() {
        // The router assembles (route registration order + state binding) and the
        // OpenAPI doc renders — both are otherwise never exercised by a unit test.
        let _router = routes(ctx());
        let doc = openapi();
        assert!(!doc.paths.paths.is_empty());
    }

    #[tokio::test]
    async fn catalog_lists_kinds_and_only_resolvable_sources() {
        let Json(v) = catalog(State(ctx())).await;
        assert!(v["widget_kinds"]
            .as_array()
            .unwrap()
            .contains(&json!("stat")));
        let source_types = v["source_types"].as_array().unwrap();
        for supported in ["static", "composio", "http"] {
            assert!(source_types.contains(&json!(supported)), "{supported}");
        }
        for unsupported in crate::CORE_READ_SOURCE_TYPES {
            assert!(
                !source_types.contains(&json!(unsupported)),
                "{unsupported} must not be offered"
            );
        }
        // Kept for older desktops whose catalog schema requires the array.
        assert_eq!(v["core_endpoints"], json!([]));
    }

    #[tokio::test]
    async fn create_dashboard_rejects_blank_name() {
        let ctx = ctx();
        let (status, v) =
            create_dashboard(State(ctx.clone()), Json(body(json!({ "name": "   " })))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["error"], json!("name is required"));
    }

    #[tokio::test]
    async fn create_then_list_and_get_dashboard() {
        let ctx = ctx();
        let (status, v) =
            create_dashboard(State(ctx.clone()), Json(body(json!({ "name": "Home" })))).await;
        assert_eq!(status, StatusCode::OK);
        let id = v["dashboard"]["id"].as_str().unwrap().to_string();
        assert!(id.starts_with("dash_"));

        let Json(listed) = list_dashboards(State(ctx.clone())).await;
        assert_eq!(listed["dashboards"].as_array().unwrap().len(), 1);

        let (status, got) =
            get_dashboard(State(ctx.clone()), Path(id.clone()), HeaderMap::new()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(got["dashboard"]["name"], json!("Home"));
        assert!(got["widgets"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_dashboard_missing_is_404() {
        let ctx = ctx();
        let (status, v) = get_dashboard(State(ctx), Path("nope".into()), HeaderMap::new()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(v["error"], json!("not found"));
    }

    #[tokio::test]
    async fn update_dashboard_rename_view_mode_and_validation() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;

        // Missing dashboard ⇒ 404.
        let (status, _) = update_dashboard(
            State(ctx.clone()),
            Path("ghost".into()),
            Json(body(json!({ "name": "x" }))),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Present-but-blank name ⇒ 400.
        let (status, _) = update_dashboard(
            State(ctx.clone()),
            Path("d1".into()),
            Json(body(json!({ "name": "  " }))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // Invalid view_mode ⇒ 400.
        let (status, v) = update_dashboard(
            State(ctx.clone()),
            Path("d1".into()),
            Json(body(json!({ "view_mode": "spiral" }))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"].as_str().unwrap().contains("view_mode"));

        // Valid rename + view_mode together.
        let (status, v) = update_dashboard(
            State(ctx.clone()),
            Path("d1".into()),
            Json(body(json!({ "name": "Renamed", "view_mode": "canvas" }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["dashboard"]["name"], json!("Renamed"));
        assert_eq!(v["dashboard"]["view_mode"], json!("canvas"));
    }

    #[tokio::test]
    async fn delete_dashboard_present_and_missing() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let (status, v) = delete_dashboard(State(ctx.clone()), Path("d1".into())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["ok"], json!(true));
        let (status, _) = delete_dashboard(State(ctx), Path("d1".into())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn create_widget_validation_paths() {
        let ctx = ctx();

        // Dashboard must exist first.
        let (status, v) = create_widget(
            State(ctx.clone()),
            Path("missing".into()),
            HeaderMap::new(),
            Json(body(json!({ "kind": "stat" }))),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(v["error"], json!("dashboard not found"));

        seed_dashboard(&ctx, "d1").await;

        // Missing kind ⇒ 400.
        let (status, v) = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({ "title": "no kind" }))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["error"], json!("kind is required"));

        // An unsupported Core-read source ⇒ 400 with the widget-facing reason.
        let (status, v) = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({
                "kind": "stat",
                "source": { "type": "core_endpoint", "endpoint": "system_status" }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"]
            .as_str()
            .unwrap()
            .contains("Core metric widgets are not available yet"));
        assert!(ctx
            .engine
            .store
            .list_widgets("d1")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn legacy_unsupported_widget_can_be_rearranged_with_its_source_unchanged() {
        // Old desktops seeded Home with `core_endpoint` presets. The builder moves a
        // widget by re-upserting it whole, so an unchanged legacy source must not be
        // refused — only a new or changed unsupported source is.
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let legacy = Widget {
            id: "w_clients".into(),
            dashboard_id: "d1".into(),
            kind: WidgetKind::Stat,
            title: "Connected clients".into(),
            config: Value::Null,
            source: WidgetSource::CoreEndpoint {
                endpoint: "connections".into(),
                selector: Some("clients".into()),
            },
            refresh_interval: None,
            layout: GridLayout::default(),
            canvas: None,
            last_value: None,
            last_refresh_at: None,
            last_error: None,
        };
        ctx.engine.store.upsert_widget(&legacy).await.unwrap();

        let (status, Json(v)) = upsert_widget(
            State(ctx.clone()),
            HeaderMap::new(),
            Json(body(json!({
                "dashboard_id": "d1",
                "widget": {
                    "id": "w_clients",
                    "kind": "stat",
                    "title": "Connected clients",
                    "source": { "type": "core_endpoint", "selector": "clients", "endpoint": "connections" },
                    "layout": { "x": 6, "y": 2, "w": 3, "h": 3 }
                }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let stored = ctx
            .engine
            .store
            .get_widget_for_dashboard("d1", "w_clients")
            .await
            .unwrap()
            .expect("widget kept");
        assert_eq!(stored.layout.x, 6);
        assert_eq!(stored.layout.y, 2);

        // The partial-patch path accepts the unchanged source too.
        let (status, Json(v)) = update_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w_clients".into())),
            HeaderMap::new(),
            Json(body(json!({
                "title": "Clients",
                "source": { "type": "core_endpoint", "endpoint": "connections", "selector": "clients" }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");

        // Changing the unsupported source, or adding it under a new id, is refused.
        for (widget_id, source) in [
            (
                "w_clients",
                json!({ "type": "core_endpoint", "endpoint": "quests" }),
            ),
            (
                "w_clients",
                json!({ "type": "monitor", "monitor_id": "m1" }),
            ),
            (
                "w_new",
                json!({ "type": "core_endpoint", "endpoint": "connections", "selector": "clients" }),
            ),
        ] {
            let (status, Json(v)) = upsert_widget(
                State(ctx.clone()),
                HeaderMap::new(),
                Json(body(json!({
                    "dashboard_id": "d1",
                    "widget": { "id": widget_id, "kind": "stat", "source": source }
                }))),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{widget_id}: {v}");
        }
        let (status, _) = update_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w_clients".into())),
            HeaderMap::new(),
            Json(body(
                json!({ "source": { "type": "monitor", "monitor_id": "m1" } }),
            )),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_widget_honors_caller_id_and_defaults_source() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        // No source ⇒ defaults to Static; caller id honored.
        let (status, v) = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({ "id": "mine", "kind": "text" }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["widget"]["id"], json!("mine"));
        assert_eq!(v["widget"]["source"]["type"], json!("static"));

        // A blank refresh_interval is dropped (stored as None).
        let (_, v) = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({ "kind": "stat", "refresh_interval": "  " }))),
        )
        .await;
        assert!(v["widget"].get("refresh_interval").is_none());
        // A generated id follows the wgt_ convention.
        assert!(v["widget"]["id"].as_str().unwrap().starts_with("wgt_"));
    }

    #[tokio::test]
    async fn update_widget_patches_and_clears_cache_on_source_change() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({ "id": "w1", "kind": "stat" }))),
        )
        .await;
        // Prime a cached value so we can prove a source change clears it.
        ctx.engine
            .store
            .update_widget_value("w1", Ok(json!(123)))
            .await
            .unwrap();

        // Missing widget ⇒ 404.
        let (status, _) = update_widget(
            State(ctx.clone()),
            Path(("d1".into(), "ghost".into())),
            HeaderMap::new(),
            Json(body(json!({ "title": "x" }))),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Patch title + change source ⇒ cached value cleared.
        let (status, v) = update_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            HeaderMap::new(),
            Json(body(json!({
                "title": "Updated",
                "source": { "type": "static", "data": 1 }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["widget"]["title"], json!("Updated"));
        assert!(
            v["widget"].get("last_value").is_none(),
            "source change clears cache"
        );
    }

    #[tokio::test]
    async fn update_widget_rejects_unsafe_source_without_changing_stored_widget() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let (create_status, _) = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({
                "id": "w1",
                "kind": "stat",
                "source": { "type": "static", "data": 7 }
            }))),
        )
        .await;
        assert_eq!(create_status, StatusCode::OK);

        let (status, response_body) = update_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            HeaderMap::new(),
            Json(body(json!({
                "source": {
                    "type": "composio",
                    "action": "../private",
                    "args": {}
                }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(response_body["error"]
            .as_str()
            .unwrap()
            .contains("invalid composio action"));

        let stored = ctx
            .engine
            .store
            .get_widget_for_dashboard("d1", "w1")
            .await
            .unwrap()
            .expect("the refused update must preserve the widget");
        assert_eq!(
            serde_json::to_value(stored.source).unwrap(),
            json!({
                "type": "static",
                "data": 7
            })
        );
    }

    #[tokio::test]
    async fn delete_widget_present_and_missing() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({ "id": "w1", "kind": "stat" }))),
        )
        .await;
        let (status, v) = delete_widget(State(ctx.clone()), Path(("d1".into(), "w1".into()))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["ok"], json!(true));
        let (status, _) = delete_widget(State(ctx), Path(("d1".into(), "w1".into()))).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn list_widgets_returns_dashboard_widgets() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({ "id": "w1", "kind": "stat" }))),
        )
        .await;
        let Json(v) = list_widgets(State(ctx), Path("d1".into()), HeaderMap::new()).await;
        assert_eq!(v["widgets"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn update_widget_layout_grid_canvas_and_missing() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({ "id": "w1", "kind": "stat" }))),
        )
        .await;

        // Full grid rect applies.
        let (status, v) = update_widget_layout(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            Json(body(json!({ "x": 1, "y": 2, "w": 3, "h": 4 }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["widget"]["layout"]["x"], json!(1));

        // Canvas-only update (partial grid ignored).
        let (status, v) = update_widget_layout(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            Json(body(
                json!({ "canvas": { "x": 9.0, "y": 8.0, "w": 7.0, "h": 6.0 } }),
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["widget"]["canvas"]["x"], json!(9.0));
        // Grid preserved from the previous update.
        assert_eq!(v["widget"]["layout"]["x"], json!(1));

        // Missing widget ⇒ 404.
        let (status, _) = update_widget_layout(
            State(ctx),
            Path(("d1".into(), "ghost".into())),
            Json(body(json!({ "x": 0, "y": 0, "w": 1, "h": 1 }))),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    /// A widget an agent writes runs for that agent: Core's grant from the write
    /// request is stored beside the widget (never in what the API returns) and
    /// handed back on every provider call, so the agent's account access applies
    /// even when a person or the refresh loop runs it later.
    #[tokio::test]
    async fn an_agents_widget_sends_its_grant_on_every_provider_call() {
        use crate::testutil::{engine_with, FakeHost};
        let host = std::sync::Arc::new(FakeHost::default());
        let ctx = DashboardsCtx::new(engine_with(host.clone()));
        seed_dashboard(&ctx, "d1").await;
        let headers = |grant: Option<&str>| {
            let mut headers = HeaderMap::new();
            if let Some(grant) = grant {
                headers.insert(ryu_app_events::HDR_AGENT_GRANT, grant.parse().unwrap());
            }
            headers
        };

        let (status, Json(created)) = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            headers(Some("grant.agent")),
            Json(body(json!({
                "id": "w1",
                "kind": "stat",
                "source": { "type": "composio", "action": "GMAIL_LIST", "args": {} }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            !created.to_string().contains("grant.agent"),
            "the grant never reaches an API response: {created}"
        );
        let stored = || async { ctx.engine.store.widget_agent_grant("w1").await.unwrap() };
        assert_eq!(stored().await.as_deref(), Some("grant.agent"));

        // A person's refresh runs the widget for the agent that wrote it (the
        // refresh loop does the same; see `refresh::tests`).
        let _ = refresh_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            headers(None),
        )
        .await;
        // A person's title edit keeps it running for the agent.
        let (status, _) = update_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            headers(None),
            Json(body(json!({ "title": "Inbox" }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stored().await.as_deref(), Some("grant.agent"));
        // Another agent asking runs it as that agent.
        let _ = refresh_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            headers(Some("grant.other")),
        )
        .await;
        // A person choosing a new source makes it theirs.
        let (status, _) = update_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            headers(None),
            Json(body(
                json!({ "source": { "type": "composio", "action": "SLACK_LIST", "args": {} } }),
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stored().await, None);
        let _ = refresh_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            headers(None),
        )
        .await;

        assert_eq!(
            *host.provider_grants.lock().unwrap(),
            vec![
                Some("grant.agent".to_owned()),
                Some("grant.other".to_owned()),
                None,
            ]
        );
    }

    fn grant_headers(grant: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ryu_app_events::HDR_AGENT_GRANT, grant.parse().unwrap());
        headers
    }

    /// The device-config write is an agent write like any other: the widgets it
    /// stores run for the agent that sent it, never with the person's accounts.
    #[tokio::test]
    async fn an_agents_device_config_widgets_run_for_that_agent() {
        use crate::testutil::{engine_with, FakeHost};
        let host = std::sync::Arc::new(FakeHost::default());
        let ctx = DashboardsCtx::new(engine_with(host.clone()));
        let resp = set_device_config(
            State(ctx.clone()),
            grant_headers("grant.agent"),
            Json(body(json!({
                "device_id": "rhw_1",
                "widgets": [{
                    "id": "dw1",
                    "kind": "stat",
                    "source": { "type": "composio", "action": "GMAIL_LIST", "args": {} }
                }]
            }))),
        )
        .await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let dashboard_id = v["dashboard_id"].as_str().unwrap().to_owned();
        assert_eq!(
            ctx.engine
                .store
                .widget_agent_grant("dw1")
                .await
                .unwrap()
                .as_deref(),
            Some("grant.agent")
        );
        // A later refresh with no caller grant hands the stored one to the host.
        let _ = refresh_widget(
            State(ctx.clone()),
            Path((dashboard_id.clone(), "dw1".into())),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(
            *host.provider_grants.lock().unwrap(),
            vec![Some("grant.agent".to_owned())]
        );

        // A person's replacement of the widgets hands them back to the person.
        let resp = set_device_config(
            State(ctx.clone()),
            HeaderMap::new(),
            Json(body(json!({
                "device_id": "rhw_1",
                "widgets": [{
                    "id": "dw1",
                    "kind": "stat",
                    "source": { "type": "composio", "action": "GMAIL_LIST", "args": {} }
                }]
            }))),
        )
        .await;
        let (status, _) = read(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            ctx.engine.store.widget_agent_grant("dw1").await.unwrap(),
            None
        );
    }

    /// A limited agent reading through the app's own routes does not get the
    /// cached results of a person's widgets (those ran with the person's
    /// accounts), only those of widgets that run for it. A person's own UI sees
    /// everything.
    #[tokio::test]
    async fn cached_results_of_a_persons_widgets_are_hidden_from_a_limited_agent() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        for (id, headers) in [
            ("person", HeaderMap::new()),
            ("mine", grant_headers("grant.agent")),
            ("theirs", grant_headers("grant.other")),
        ] {
            let (status, _) = create_widget(
                State(ctx.clone()),
                Path("d1".into()),
                headers,
                Json(body(json!({
                    "id": id,
                    "kind": "stat",
                    "source": { "type": "static", "data": 1 }
                }))),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            let mut widget = ctx.engine.store.get_widget(id).await.unwrap().unwrap();
            widget.last_value = Some(json!({ "cached": id }));
            widget.last_error = Some(format!("error of {id}"));
            ctx.engine.store.upsert_widget(&widget).await.unwrap();
        }
        let cached = |widgets: &Value| -> Vec<(String, bool)> {
            widgets
                .as_array()
                .unwrap()
                .iter()
                .map(|w| {
                    let shown = w.get("last_value").is_some() || w.get("last_error").is_some();
                    (w["id"].as_str().unwrap().to_owned(), shown)
                })
                .collect()
        };
        let sorted = |mut rows: Vec<(String, bool)>| {
            rows.sort();
            rows
        };
        let expected_for_agent = vec![
            ("mine".to_owned(), true),
            ("person".to_owned(), false),
            ("theirs".to_owned(), false),
        ];
        let everything = vec![
            ("mine".to_owned(), true),
            ("person".to_owned(), true),
            ("theirs".to_owned(), true),
        ];

        let (_, Json(got)) = resolve_dashboard(
            State(ctx.clone()),
            grant_headers("grant.agent"),
            Json(body(json!({ "id": "d1" }))),
        )
        .await;
        assert_eq!(sorted(cached(&got["widgets"])), expected_for_agent);
        let (_, Json(got)) = get_dashboard(
            State(ctx.clone()),
            Path("d1".into()),
            grant_headers("grant.agent"),
        )
        .await;
        assert_eq!(sorted(cached(&got["widgets"])), expected_for_agent);
        let Json(got) = list_widgets(
            State(ctx.clone()),
            Path("d1".into()),
            grant_headers("grant.agent"),
        )
        .await;
        assert_eq!(sorted(cached(&got["widgets"])), expected_for_agent);
        // The widget's own fields stay, only its cached result goes.
        assert!(got["widgets"]
            .as_array()
            .unwrap()
            .iter()
            .all(|w| w["kind"] == json!("stat")));

        // No grant header: a person's own UI, unchanged.
        let (_, Json(got)) = resolve_dashboard(
            State(ctx.clone()),
            HeaderMap::new(),
            Json(body(json!({ "id": "d1" }))),
        )
        .await;
        assert_eq!(sorted(cached(&got["widgets"])), everything);
    }

    #[tokio::test]
    async fn refresh_widget_static_returns_value_and_missing_is_404() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({
                "id": "w1",
                "kind": "stat",
                "source": { "type": "static", "data": { "n": 5 } }
            }))),
        )
        .await;
        // Static resolves instantly with no network.
        let (status, v) = refresh_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["value"], json!({ "n": 5 }));

        // Missing widget ⇒ 404.
        let (status, _) = refresh_widget(
            State(ctx),
            Path(("d1".into(), "ghost".into())),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn refresh_widget_returns_200_with_error_body_on_source_failure() {
        // Pinned behavior: a source error still returns 200 with an { error } body
        // (the widget shows the error, the request itself is not a failure).
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        // A Composio widget: the default FakeHost points gateway_url at a closed
        // loopback port, so resolving it fails fast (connection refused) with no
        // external traffic — a hermetic source-failure.
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            HeaderMap::new(),
            Json(body(json!({
                "id": "w1",
                "kind": "stat",
                "source": { "type": "composio", "action": "X", "args": {} }
            }))),
        )
        .await;
        let (status, v) = refresh_widget(
            State(ctx),
            Path(("d1".into(), "w1".into())),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            v.get("error").is_some(),
            "source failure surfaces as error body"
        );
    }

    // ── Device (internal hardware) handlers — return `Response` ───────────────

    #[tokio::test]
    async fn device_ensure_and_bindings_and_delete() {
        let ctx = ctx();
        let resp = device_ensure(
            State(ctx.clone()),
            Json(body(json!({ "device_id": "rhw_1", "device_name": "Desk" }))),
        )
        .await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert!(v["dashboard_id"].as_str().unwrap().starts_with("dash_"));

        // The binding now shows up in device-bindings.
        let resp = device_bindings(State(ctx.clone())).await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["bindings"].as_array().unwrap().len(), 1);
        assert_eq!(v["bindings"][0]["device_id"], json!("rhw_1"));

        // Delete is best-effort ok.
        let resp = delete_device_binding(State(ctx.clone()), Path("rhw_1".into())).await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["ok"], json!(true));
        let resp = device_bindings(State(ctx)).await;
        let (_, v) = read(resp).await;
        assert!(v["bindings"].as_array().unwrap().is_empty());
    }

    /// The body-addressed capability twins answer exactly like the REST handlers
    /// they delegate to.
    #[tokio::test]
    async fn capability_twins_delegate_to_the_rest_handlers() {
        let ctx = ctx();
        seed_dashboard(&ctx, "dash_twin").await;

        let (status, Json(v)) = rename_dashboard(
            State(ctx.clone()),
            Json(body(json!({ "id": "dash_twin", "name": "Renamed" }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["dashboard"]["name"], json!("Renamed"));
        let (status, _) = rename_dashboard(
            State(ctx.clone()),
            Json(body(json!({ "id": "dash_twin", "name": "  " }))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, Json(v)) = upsert_widget(
            State(ctx.clone()), HeaderMap::new(),
            Json(body(json!({
                "dashboard_id": "dash_twin",
                "widget": { "id": "w1", "kind": "text", "source": { "type": "static", "data": "hi" } }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{v}");
        let (status, _) = upsert_widget(
            State(ctx.clone()),
            HeaderMap::new(),
            Json(body(json!({
                "dashboard_id": "dash_missing",
                "widget": { "kind": "text" }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, Json(v)) = resolve_dashboard(
            State(ctx.clone()),
            HeaderMap::new(),
            Json(body(json!({ "id": "dash_twin" }))),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["dashboard"]["name"], json!("Renamed"));
        assert_eq!(v["widgets"][0]["id"], json!("w1"));

        let (status, _) = remove_widget(
            State(ctx.clone()),
            Json(body(
                json!({ "dashboard_id": "dash_twin", "widget_id": "w1" }),
            )),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = remove_widget(
            State(ctx.clone()),
            Json(body(
                json!({ "dashboard_id": "dash_twin", "widget_id": "w1" }),
            )),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _) = resolve_dashboard(
            State(ctx.clone()),
            HeaderMap::new(),
            Json(body(json!({ "id": "nope" }))),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let resp = device_ensure(
            State(ctx.clone()),
            Json(body(json!({ "device_id": "rhw_twin" }))),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = unbind_device(
            State(ctx.clone()),
            Json(body(json!({ "device_id": "rhw_twin" }))),
        )
        .await;
        assert_eq!(read(resp).await.1["ok"], json!(true));
        let (_, v) = read(device_bindings(State(ctx)).await).await;
        assert!(v["bindings"].as_array().unwrap().is_empty());
    }

    /// The `dashboards.changes` stream is an internal subscriber: it never counts as
    /// a UI viewer, while a plain `/events` subscription does.
    #[tokio::test]
    async fn the_changes_stream_holds_no_viewer_guard() {
        let ctx = ctx();
        let changes = dashboard_changes(State(ctx.clone())).await;
        assert_eq!(ctx.engine.store.viewer_count(), 0);
        let events = dashboard_events(State(ctx.clone()), Query(EventsQuery::default())).await;
        assert_eq!(ctx.engine.store.viewer_count(), 1);
        drop(events);
        drop(changes);
        assert_eq!(ctx.engine.store.viewer_count(), 0);
    }

    #[tokio::test]
    async fn device_ensure_defaults_name_to_id() {
        let ctx = ctx();
        // device_name omitted ⇒ falls back to device_id.
        let resp = device_ensure(
            State(ctx.clone()),
            Json(body(json!({ "device_id": "rhw_9" }))),
        )
        .await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::OK);
        let dash_id = v["dashboard_id"].as_str().unwrap();
        let dash = ctx
            .engine
            .store
            .get_dashboard(dash_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dash.name, "rhw_9 display");
    }

    #[tokio::test]
    async fn device_config_and_manifest_and_image() {
        let ctx = ctx();
        let req = json!({
            "device_id": "rhw_1",
            "device_name": "Desk",
            "device_type": "desk",
            "prefs": {}
        });

        // config → binding + widgets + screen.
        let resp = device_config(
            State(ctx.clone()),
            HeaderMap::new(),
            Json(body(req.clone())),
        )
        .await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["device_id"], json!("rhw_1"));
        assert_eq!(v["screen"]["w"], json!(800));

        // manifest → rev + refresh_rate + screen.
        let resp = device_manifest(State(ctx.clone()), Json(body(req.clone()))).await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::OK);
        let rev = v["rev"].as_str().unwrap().to_string();
        assert!(!rev.is_empty());

        // image → 200 bytes the first time.
        let resp = device_image(State(ctx.clone()), Json(body(req.clone()))).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // image with the matching known_rev ⇒ 304.
        let mut req2 = req.clone();
        req2["known_rev"] = json!(rev);
        let resp = device_image(State(ctx), Json(body(req2))).await;
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn set_device_config_handler_updates_and_rejects_bad_widgets() {
        let ctx = ctx();
        // Set a refresh rate (clamped) — returns ok + dashboard_id.
        let resp = set_device_config(
            State(ctx.clone()),
            HeaderMap::new(),
            Json(body(json!({ "device_id": "rhw_1", "refresh_rate": 5 }))),
        )
        .await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["ok"], json!(true));
        assert_eq!(v["refresh_rate"], json!(30)); // MIN_REFRESH_RATE floor.

        // A bad widget batch ⇒ 400.
        let resp = set_device_config(
            State(ctx),
            HeaderMap::new(),
            Json(body(json!({
                "device_id": "rhw_1",
                "widgets": [
                    { "kind": "stat", "source": { "type": "core_endpoint", "endpoint": "system_status" } }
                ]
            }))),
        )
        .await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"]
            .as_str()
            .unwrap()
            .contains("source 'core_endpoint' is not available"));
    }

    #[tokio::test]
    async fn events_query_default_holds_viewer_guard_semantics() {
        // The SSE handler is hard to poll in a unit test, but we can prove the
        // internal-flag parse the guard decision keys off of.
        let internal: EventsQuery = serde_json::from_value(json!({ "internal": "1" })).unwrap();
        assert!(internal.internal);
        let external: Query<EventsQuery> = Query(EventsQuery::default());
        assert!(!external.0.internal);
    }

    // ── OpenAPI document ───────────────────────────────────────────────────────

    /// This app's own manifest, read at compile time. The route contract lives there,
    /// so the invariants below compare the document against the real declaration
    /// rather than against a second list that could drift from it.
    fn openapi_manifest() -> serde_json::Value {
        serde_json::from_str(include_str!("../../manifest.json")).expect("valid JSON")
    }

    /// The manifest sidecar whose HTTP surface this router serves: the one that
    /// declares an `http.mount`. Selected BY mount rather than by index because an app
    /// may declare a second, mountless sidecar (finetune already does), and
    /// `sidecars[0]` would then quietly start asserting against the wrong process.
    fn mounted_sidecar() -> serde_json::Value {
        openapi_manifest()["sidecars"]
            .as_array()
            .expect("sidecars must be an array")
            .iter()
            .find(|s| s["http"]["mount"].is_string())
            .expect("one sidecar must declare an http.mount")
            .clone()
    }

    /// A manifest route (relative to the mount, in axum's `:param` form) rewritten
    /// into the form the OpenAPI document uses (absolute, in `{param}` form).
    ///
    /// The two forms differ ON PURPOSE — the router registers paths relative to the
    /// mount because Core nests it there, while the `#[utoipa::path]` annotations carry
    /// the absolute EXTERNAL path a caller actually hits. Normalise here; do not
    /// "align" either side.
    fn doc_path_for(mount: &str, route: &str) -> String {
        let joined = if route == "/" {
            mount.to_owned()
        } else {
            format!("{mount}{route}")
        };
        joined
            .split('/')
            .map(|seg| match seg.strip_prefix(':') {
                Some(name) => format!("{{{name}}}"),
                None => seg.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    #[test]
    fn openapi_doc_is_served_and_non_empty() {
        // The doc is no longer dead code: Core fetches it to derive tools.
        assert!(!super::openapi().paths.paths.is_empty());
    }

    /// Routes declared ONLY so a `provides` row can point at them: Core's capability
    /// consumer is their caller, not an agent. They stay out of the OpenAPI document
    /// on purpose (each would duplicate a REST tool, stream forever, or return panel
    /// bytes), and the test below pins both halves of that.
    const CAPABILITY_ONLY_ROUTES: &[&str] = &[
        "/changes",
        "/resolve",
        "/rename",
        "/widgets/upsert",
        "/widgets/remove",
        "/device/manifest",
        "/device/image",
        "/device/config",
        "/device/config/set",
        "/device/ensure",
        "/device/unbind",
        "/device-bindings",
    ];

    /// Every `provides` row points at a declared POST route on the mounted sidecar
    /// (Core's broker resolves `{mount}{route}` and always POSTs), every
    /// capability-only route backs a `provides` row and is absent from the OpenAPI
    /// document, and the set of capabilities is the one Core consumes.
    #[test]
    fn provided_capabilities_point_at_declared_post_routes() {
        let manifest = openapi_manifest();
        let sidecar = mounted_sidecar();
        let declared = sidecar["http"]["routes"].as_array().expect("routes");
        let provides = manifest["provides"]
            .as_array()
            .expect("the app provides its dashboard capabilities");
        let mut names: Vec<&str> = Vec::new();
        let mut provided_routes: Vec<&str> = Vec::new();
        for entry in provides {
            names.push(entry["capability"].as_str().expect("a capability name"));
            assert_eq!(entry["sidecar"], sidecar["name"], "{entry}");
            let route = entry["route"].as_str().expect("a sidecar route");
            provided_routes.push(route);
            assert!(
                declared
                    .iter()
                    .any(|r| r["path"] == route && r["method"] == "POST"),
                "'{route}' is not a declared POST route"
            );
            let grant = entry["grant"].as_str().expect("a grant");
            assert!(grant.starts_with("dashboards:"), "{grant}");
        }
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "dashboards.changes",
                "dashboards.create",
                "dashboards.deleteWidget",
                "dashboards.deviceBindings",
                "dashboards.deviceConfig",
                "dashboards.deviceImage",
                "dashboards.deviceManifest",
                "dashboards.ensureDeviceDashboard",
                "dashboards.get",
                "dashboards.rename",
                "dashboards.setDevice",
                "dashboards.unbindDevice",
                "dashboards.upsertWidget",
            ]
        );
        let doc = super::openapi();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        for route in CAPABILITY_ONLY_ROUTES {
            assert!(
                provided_routes.contains(route),
                "'{route}' backs no capability"
            );
            let path = doc_path_for(mount, route);
            assert!(
                !doc.paths.paths.contains_key(&path),
                "'{path}' is capability plumbing and must not become an agent tool"
            );
        }
    }

    #[test]
    fn every_declared_route_appears_in_the_openapi_doc() {
        // The direction that decides tool yield. Core's `ext_api::lower` keeps only the
        // document operations the manifest ALSO declares, so a declared route with no
        // `#[utoipa::path]` annotation is a tool that silently never exists — nothing
        // errors, the agent simply cannot call it. (The other direction is harmless: an
        // annotated path the manifest does not declare is dropped by the same filter.)
        let sidecar = mounted_sidecar();
        let mount = sidecar["http"]["mount"].as_str().expect("an http.mount");
        let doc = super::openapi();
        for route in sidecar["http"]["routes"]
            .as_array()
            .expect("routes must be an array")
        {
            let path = route["path"].as_str().expect("a route path");
            let expected = doc_path_for(mount, path);
            if CAPABILITY_ONLY_ROUTES.contains(&path) {
                continue;
            }
            assert!(
                doc.paths.paths.contains_key(&expected),
                "'{path}' is declared in manifest.json but the OpenAPI document has no \
                 '{expected}' operation — Core derives no tool for it"
            );
        }
    }

    // ── Request-body schemas (the arguments a derived tool actually gets) ──────
    //
    // Core builds each write tool's argument schema from `requestBody`. Every
    // annotation here used to say `request_body = serde_json::Value`, which
    // serialises to an untyped schema: `create_widget` reached the model with no
    // way to see that a widget even HAS a `kind` or a `source`. These tests pin the
    // fix, because the failure mode is silent — nothing errors, the agent just
    // cannot fill anything in.

    /// The whole document as JSON, which is the form Core imports it in.
    fn openapi_json() -> serde_json::Value {
        serde_json::to_value(super::openapi()).expect("the document must serialise")
    }

    /// The request-body schema node for one operation, or `None` when the
    /// operation declares no body. `/` is escaped as `~1` per RFC 6901.
    fn request_body_schema(doc: &serde_json::Value, path: &str, method: &str) -> Option<Value> {
        let pointer = format!(
            "/paths/{}/{method}/requestBody/content/application~1json/schema",
            path.replace('~', "~0").replace('/', "~1")
        );
        doc.pointer(&pointer).cloned()
    }

    #[test]
    fn post_routes_document_their_request_body() {
        let doc = openapi_json();
        for (path, method) in [
            ("/api/dashboards", "post"),
            ("/api/dashboards/{id}", "put"),
            ("/api/dashboards/{id}/widgets", "post"),
            ("/api/dashboards/{id}/widgets/{wid}", "put"),
            ("/api/dashboards/{id}/widgets/{wid}/layout", "put"),
        ] {
            let schema = request_body_schema(&doc, path, method)
                .unwrap_or_else(|| panic!("{method} {path} declares no JSON request body"));
            // A `$ref` is correct and expected: Core resolves it against
            // `components.schemas` on import. What must NEVER pass is an empty
            // object (the `serde_json::Value` shape) or a `oneOf` wrapper.
            assert!(
                schema.get("$ref").is_some() || schema.get("properties").is_some(),
                "a derived write tool for {method} {path} would have no arguments: {schema}"
            );
        }
    }

    #[test]
    fn every_request_body_ref_resolves_against_components() {
        // The assertion above is necessary but not sufficient: a `$ref` pointing at a
        // schema that was never registered looks identical in the operation and still
        // yields zero arguments in Core. Resolve every one of them here.
        let doc = openapi_json();
        let paths = doc["paths"].as_object().expect("paths object");
        for (path, item) in paths {
            let ops = item.as_object().expect("a path item object");
            for (method, op) in ops {
                let Some(schema) = op
                    .pointer("/requestBody/content/application~1json/schema")
                    .cloned()
                else {
                    continue;
                };
                let Some(reference) = schema.get("$ref").and_then(|r| r.as_str()) else {
                    assert!(
                        schema.get("properties").is_some(),
                        "{method} {path} has an inline body schema with no properties: {schema}"
                    );
                    continue;
                };
                let name = reference
                    .strip_prefix("#/components/schemas/")
                    .unwrap_or_else(|| panic!("{method} {path}: unexpected $ref '{reference}'"));
                let target = doc
                    .pointer(&format!("/components/schemas/{name}"))
                    .unwrap_or_else(|| {
                        panic!(
                            "{method} {path} references '{name}', which is not in \
                             components.schemas — add it to components(schemas(...))"
                        )
                    });
                assert!(
                    target.get("properties").is_some(),
                    "{method} {path} resolves to '{name}', which exposes no properties: {target}"
                );
            }
        }
    }

    #[test]
    fn a_nested_struct_argument_is_self_describing() {
        // `source` is an `Option<WidgetSource>`, which utoipa renders as a nullable
        // `oneOf`. Without `#[schema(inline)]` the variant `$ref` sits INSIDE that
        // wrapper, where Core's one-level resolution cannot reach it — the model
        // would be told a widget has a `source` but never which kinds exist.
        let doc = openapi_json();
        let source = doc
            .pointer("/components/schemas/WidgetBody/properties/source")
            .expect("WidgetBody must document `source`");
        let json = serde_json::to_string(source).expect("serialisable");
        for marker in ["core_endpoint", "endpoint", "workflow_id", "agent_id"] {
            assert!(
                json.contains(marker),
                "the widget source union is not self-describing (missing '{marker}'): {json}"
            );
        }
        // Same trap, one level simpler: the grid rect must show its four numbers.
        let layout = doc
            .pointer("/components/schemas/WidgetBody/properties/layout")
            .expect("WidgetBody must document `layout`");
        let json = serde_json::to_string(layout).expect("serialisable");
        assert!(
            json.contains("\"w\"") && json.contains("\"h\""),
            "the grid layout is not self-describing: {json}"
        );
    }

    #[test]
    fn body_field_docs_reach_the_schema_as_argument_descriptions() {
        // The payoff of the retrofit: a `///` on a field becomes the argument
        // description the model reads. Losing it is invisible at the type level.
        let doc = openapi_json();
        let description = doc
            .pointer("/components/schemas/WidgetBody/properties/refresh_interval/description")
            .and_then(|d| d.as_str())
            .expect("`refresh_interval` must carry a description");
        assert!(
            description.contains("30s"),
            "`refresh_interval` lost the duration format from its doc comment: {description}"
        );
        // The same must hold one level down, inside the inlined source union — that
        // is where the constraint the model most needs lives (which source kinds
        // are unavailable), and it only survives because the field is inlined.
        let source = serde_json::to_string(
            doc.pointer("/components/schemas/WidgetBody/properties/source")
                .expect("WidgetBody must document `source`"),
        )
        .expect("serialisable");
        assert!(
            source.contains("NOT AVAILABLE YET"),
            "the unavailable-source constraint did not reach the schema: {source}"
        );
    }

    #[test]
    fn body_less_routes_declare_no_request_body() {
        // The force-refresh takes only the two path ids. A `request_body` here would
        // hand the model a phantom argument; the ids must still arrive as parameters.
        let path = "/api/dashboards/{id}/widgets/{wid}/refresh";
        let doc = openapi_json();
        assert!(
            request_body_schema(&doc, path, "post").is_none(),
            "{path} declares a request body but its handler takes none"
        );
        let params = doc
            .pointer(&format!(
                "/paths/{}/post/parameters",
                path.replace('/', "~1")
            ))
            .and_then(|p| p.as_array())
            .unwrap_or_else(|| panic!("{path} must still document its path parameters"));
        for name in ["id", "wid"] {
            assert!(
                params.iter().any(|p| p["name"] == name),
                "{path} lost its `{name}` parameter: {params:?}"
            );
        }
    }
}
