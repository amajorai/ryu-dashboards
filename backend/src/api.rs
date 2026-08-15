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
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    sources, CanvasLayout, Dashboard, DashboardEngine, GridLayout, Widget, WidgetKind,
    WidgetSource, CORE_ENDPOINT_NAMES,
};

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
        // Internal hardware device-dashboard surface (Core's `dashboards_client`
        // reaches these over loopback; they are NOT desktop-facing / public_mount
        // routes). Registered before `/:id` so the static `device*` segments win.
        .route("/device/manifest", post(device_manifest))
        .route("/device/image", post(device_image))
        .route("/device/config", post(device_config).put(set_device_config))
        .route("/device/ensure", post(device_ensure))
        .route("/device-bindings", get(device_bindings))
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
    let widgets = ctx.engine.store.list_widgets(&id).await.unwrap_or_default();
    (
        StatusCode::OK,
        Json(json!({ "dashboard": dashboard, "widgets": widgets })),
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
pub async fn list_widgets(State(ctx): State<DashboardsCtx>, Path(id): Path<String>) -> Json<Value> {
    match ctx.engine.store.list_widgets(&id).await {
        Ok(widgets) => Json(json!({ "widgets": widgets })),
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

/// Reject a widget whose `core_endpoint` source names a non-allowlisted endpoint.
/// Enforced here (the store-owning process) so both the desktop builder UI and the
/// `dashboard_builder` MCP tool get the same curated-catalog guarantee. Other
/// source kinds are structurally validated by serde.
fn validate_widget_source(source: &WidgetSource) -> Result<(), String> {
    match source {
        WidgetSource::CoreEndpoint { endpoint, .. } => {
            if sources::core_endpoint_path(endpoint).is_none() {
                return Err(format!(
                    "'{endpoint}' is not an allowed core_endpoint. Allowed: {}",
                    CORE_ENDPOINT_NAMES.join(", ")
                ));
            }
        }
        // `workflow_id` / `action` are spliced raw into a privileged loopback URL
        // (see sources.rs). Reject path-traversal at write time too, so a bad
        // config never reaches the store — the resolve path re-checks regardless.
        WidgetSource::Workflow { workflow_id, .. } => {
            if !sources::id_segment_is_safe(workflow_id) {
                return Err(format!(
                    "invalid workflow_id '{workflow_id}': must be a plain id (no '/', '..', or query chars)"
                ));
            }
        }
        WidgetSource::Composio { action, .. } => {
            if !sources::id_segment_is_safe(action) {
                return Err(format!(
                    "invalid composio action '{action}': must be a plain action id (no '/', '..', or query chars)"
                ));
            }
        }
        _ => {}
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
    let source = body
        .source
        .unwrap_or(WidgetSource::Static { data: Value::Null });
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
    if let Err(e) = ctx.engine.store.upsert_widget(&widget).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        );
    }
    (StatusCode::OK, Json(json!({ "widget": widget })))
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
    if let Some(k) = body.kind {
        widget.kind = k;
    }
    if let Some(t) = body.title {
        widget.title = t;
    }
    if let Some(c) = body.config {
        widget.config = c;
    }
    if let Some(s) = body.source {
        widget.source = s;
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
    if let Err(e) = ctx.engine.store.upsert_widget(&widget).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        );
    }
    (StatusCode::OK, Json(json!({ "widget": widget })))
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
        Ok(Some(w)) => (StatusCode::OK, Json(json!({ "widget": w }))),
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
    let result = sources::resolve(
        &ctx.engine.http,
        ctx.engine.host.as_ref(),
        &widget.source,
        &wid,
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

/// `GET /api/dashboards/catalog` — the widget kinds + curated source names the
/// builder UI offers (the constrained catalog, surfaced for the desktop pickers).
#[utoipa::path(
    get,
    path = "/api/dashboards/catalog",
    tag = "Dashboards",
    summary = "the widget kinds + curated source names the",
    responses((status = 200, description = "OK", body = serde_json::Value))
)]
pub async fn catalog() -> Json<Value> {
    Json(json!({
        "widget_kinds": [
            "stat", "line_chart", "bar_chart", "area_chart", "pie_chart",
            "table", "list", "text", "map", "agent_feed"
        ],
        "source_types": [
            "static", "core_endpoint", "monitor", "workflow", "composio", "http", "agent"
        ],
        "core_endpoints": CORE_ENDPOINT_NAMES,
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
    use axum::response::sse::{Event, KeepAlive, Sse};
    use tokio::sync::broadcast::error::RecvError;

    let rx = ctx.engine.store.subscribe();
    // Hold a viewer guard for the life of the stream so the refresh loop knows a
    // human is watching (and runs expensive sources). Carried in the unfold state so
    // it drops exactly when the client disconnects. An INTERNAL subscriber (the
    // hardware nudge loop via `dashboards_client`) passes `?internal=1` and holds NO
    // guard, so it never fakes a UI viewer and defeats the refresh cost guard.
    let guard = if q.internal {
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

// ── Internal hardware device-dashboard surface ───────────────────────────────
//
// These endpoints back the `ryu_hardware::DashboardFeed` seam when dashboards runs
// out-of-process: Core's `dashboards_client` calls them over loopback (bearer-
// gated like the rest of the sidecar). They are NOT public_mount / desktop routes.
// Each delegates to `crate::device::*` — the SAME render fns Core's in-process feed
// uses — so a decoupled node renders byte-identically.

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
        Ok(config) => Json(config).into_response(),
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
    Json(req): Json<DeviceSetReq>,
) -> Response {
    match crate::device::set_device_config(
        &ctx.engine,
        &req.device_id,
        &req.device_name,
        req.refresh_rate,
        req.widgets,
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

    // The core_endpoint allowlist moved here (from the Core `dashboard_builder`
    // runnable) when dashboards went out-of-process: this crate owns the store, so
    // it owns the curated-catalog guarantee. `create_widget` calls this on every
    // add — both the desktop builder UI and the `dashboard_builder` MCP tool.
    #[test]
    fn validate_widget_source_allows_curated_core_endpoint() {
        let src = WidgetSource::CoreEndpoint {
            endpoint: "connections".into(),
            selector: Some("clients".into()),
        };
        assert!(validate_widget_source(&src).is_ok());
    }

    #[test]
    fn validate_widget_source_rejects_unknown_core_endpoint() {
        let src = WidgetSource::CoreEndpoint {
            endpoint: "secrets".into(),
            selector: None,
        };
        let err = validate_widget_source(&src).expect_err("bad endpoint must fail");
        assert!(err.contains("not an allowed core_endpoint"), "got: {err}");
    }

    #[test]
    fn validate_widget_source_ignores_free_form_kinds() {
        // Static/Monitor carry no id spliced into a privileged URL, so no allowlist
        // (serde already shape-validates them). Workflow/Composio ARE validated —
        // see validate_widget_source_rejects_path_traversal_ids below.
        assert!(validate_widget_source(&WidgetSource::Static { data: Value::Null }).is_ok());
        assert!(validate_widget_source(&WidgetSource::Monitor {
            monitor_id: "m1".into()
        })
        .is_ok());
    }

    #[test]
    fn validate_widget_source_rejects_path_traversal_ids() {
        // F6: workflow_id / Composio action are spliced raw into a privileged
        // loopback URL carrying the node token. A traversal value must be rejected
        // at write time (the resolve path re-checks regardless).
        let bad_wf = WidgetSource::Workflow {
            workflow_id: "../../api/agents/foo?".into(),
            input: Default::default(),
            output_key: None,
        };
        assert!(
            validate_widget_source(&bad_wf).is_err(),
            "traversal workflow_id must be rejected"
        );

        let bad_action = WidgetSource::Composio {
            action: "../../v1/config".into(),
            args: Value::Null,
        };
        assert!(
            validate_widget_source(&bad_action).is_err(),
            "traversal composio action must be rejected"
        );

        // A plain id / uppercase-underscore action still passes.
        assert!(validate_widget_source(&WidgetSource::Workflow {
            workflow_id: "wf_abc-123".into(),
            input: Default::default(),
            output_key: None,
        })
        .is_ok());
        assert!(validate_widget_source(&WidgetSource::Composio {
            action: "GITHUB_LIST".into(),
            args: Value::Null,
        })
        .is_ok());
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
    async fn catalog_lists_kinds_sources_and_endpoints() {
        let Json(v) = catalog().await;
        assert!(v["widget_kinds"]
            .as_array()
            .unwrap()
            .contains(&json!("stat")));
        assert!(v["source_types"]
            .as_array()
            .unwrap()
            .contains(&json!("core_endpoint")));
        assert!(v["core_endpoints"]
            .as_array()
            .unwrap()
            .contains(&json!("agents")));
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

        let (status, got) = get_dashboard(State(ctx.clone()), Path(id.clone())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(got["dashboard"]["name"], json!("Home"));
        assert!(got["widgets"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_dashboard_missing_is_404() {
        let ctx = ctx();
        let (status, v) = get_dashboard(State(ctx), Path("nope".into())).await;
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
            Json(body(json!({ "title": "no kind" }))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(v["error"], json!("kind is required"));

        // Bad core_endpoint source ⇒ 400 via validate_widget_source.
        let (status, v) = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            Json(body(json!({
                "kind": "stat",
                "source": { "type": "core_endpoint", "endpoint": "secrets" }
            }))),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"]
            .as_str()
            .unwrap()
            .contains("not an allowed core_endpoint"));
    }

    #[tokio::test]
    async fn create_widget_honors_caller_id_and_defaults_source() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        // No source ⇒ defaults to Static; caller id honored.
        let (status, v) = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
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
            Json(body(json!({ "title": "x" }))),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // Patch title + change source ⇒ cached value cleared.
        let (status, v) = update_widget(
            State(ctx.clone()),
            Path(("d1".into(), "w1".into())),
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
    async fn delete_widget_present_and_missing() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
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
            Json(body(json!({ "id": "w1", "kind": "stat" }))),
        )
        .await;
        let Json(v) = list_widgets(State(ctx), Path("d1".into())).await;
        assert_eq!(v["widgets"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn update_widget_layout_grid_canvas_and_missing() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
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

    #[tokio::test]
    async fn refresh_widget_static_returns_value_and_missing_is_404() {
        let ctx = ctx();
        seed_dashboard(&ctx, "d1").await;
        let _ = create_widget(
            State(ctx.clone()),
            Path("d1".into()),
            Json(body(json!({
                "id": "w1",
                "kind": "stat",
                "source": { "type": "static", "data": { "n": 5 } }
            }))),
        )
        .await;
        // Static resolves instantly with no network.
        let (status, v) =
            refresh_widget(State(ctx.clone()), Path(("d1".into(), "w1".into()))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(v["value"], json!({ "n": 5 }));

        // Missing widget ⇒ 404.
        let (status, _) = refresh_widget(State(ctx), Path(("d1".into(), "ghost".into()))).await;
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
            Json(body(json!({
                "id": "w1",
                "kind": "stat",
                "source": { "type": "composio", "action": "X", "args": {} }
            }))),
        )
        .await;
        let (status, v) = refresh_widget(State(ctx), Path(("d1".into(), "w1".into()))).await;
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
        let resp = device_config(State(ctx.clone()), Json(body(req.clone()))).await;
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
            Json(body(json!({
                "device_id": "rhw_1",
                "widgets": [
                    { "kind": "stat", "source": { "type": "core_endpoint", "endpoint": "secrets" } }
                ]
            }))),
        )
        .await;
        let (status, v) = read(resp).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(v["error"]
            .as_str()
            .unwrap()
            .contains("not an allowed core_endpoint"));
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
        // is where the constraint the model most needs lives (the core_endpoint
        // allowlist), and it only survives because the field is inlined.
        let source = serde_json::to_string(
            doc.pointer("/components/schemas/WidgetBody/properties/source")
                .expect("WidgetBody must document `source`"),
        )
        .expect("serialisable");
        assert!(
            source.contains("allowlist, not a URL"),
            "the core_endpoint constraint did not reach the schema: {source}"
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
