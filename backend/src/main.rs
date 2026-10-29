//! `ryu-dashboards` — the standalone, out-of-process Home-dashboards sidecar.
//!
//! The same "apps as microservices" pattern the `ryu-mail` tracer established: the
//! live widget-grid backend (store + engine + widget-source resolver + refresh loop
//! + `/api/dashboards/*` HTTP surface) runs here as a SEPARATE PROCESS that Core
//! spawns, health-checks, and proxies to. Core does NOT contain this loop when it
//! runs out-of-process — dashboards then scale and fail independently.
//!
//! Unlike `ryu-mail`, this package is BOTH a lib and a bin: Core still consumes the
//! `ryu_dashboards` LIB as an in-process path dependency (the `dashboard_builder`
//! MCP runnable and the hardware device-dashboard renderer reach its types in every
//! build), and this bin re-uses that same lib — it constructs the store, engine, and
//! router purely from the crate's PUBLIC API. The crate's router already binds its
//! own state ([`ryu_dashboards::DashboardsCtx`] / [`ryu_dashboards::DashboardEngine`]),
//! never Core's `ServerState`, so no re-parameterization is needed here.
//!
//! ## The `DashboardsHost` couplings
//!
//! The widget-source resolver inverts three cross-cutting host calls through the
//! [`ryu_dashboards::DashboardsHost`] trait. Core's `CoreDashboardsHost` wires them
//! to in-process facilities; this sidecar provides the smallest correct standalone
//! impl ([`SidecarDashboardsHost`]):
//!   - **Managed provider widgets** → sent through the authenticated Core → Gateway
//!     provider router. Gateway owns the provider credential and organization-wallet
//!     debit; the sidecar sends only the operation facts.
//!   - **Agent widgets** require delegated user execution authority, which refresh
//!     jobs do not carry. The catalog excludes this source and writes reject it.
//!   - **HTTP widgets** use the shared `ryu-egress` DNS-pinned primitive, preserving
//!     HTTPS-only, bounded responses, and disabled redirects.
//!
//!
//! The CoreEndpoint / Monitor / Workflow sources are **not available** here. Their
//! data sits behind Core's node-token-protected routes, and this process never holds
//! that token: Core spawns it with a scrubbed environment carrying only the minted
//! `RYU_EXT_TOKEN` and `RYU_CORE_PORT`, which authenticate the generic
//! `/api/host/*` surface. No generic host capability serves system status, quests,
//! monitor results, or workflow runs to an app sidecar today (`monitors.list` /
//! `quests.list` exist, but only as a hard `requires` edge plus a cross-app grant
//! the Gateway does not approve for this app). So the catalog omits these kinds,
//! every write rejects them, and a widget saved before this change shows a clear
//! "not available yet" error instead of failing an unauthenticated loopback call.
//!
//! ## Security
//!
//! Binds LOOPBACK ONLY (`127.0.0.1`) and gates EVERY route with the shared-secret
//! bearer Core injects at spawn (`RYU_EXT_TOKEN`). Core stays the auth front: it runs
//! `require_auth`, then re-stamps `Authorization: Bearer <RYU_EXT_TOKEN>` on the
//! loopback hop (and on its health probe), so a request that did NOT come through
//! Core is rejected with 401. FAIL-CLOSED: with no token configured every route
//! rejects. Dashboards has no public (tokenless) ingress, so — unlike mail — the
//! whole router is gated.
//!
//! Port: `RYU_DASHBOARDS_PORT` env (default `7997`). Data: the app's plugin data
//! dir (`RYU_PLUGIN_DATA_DIR`, injected by Core), so it opens the SAME
//! `plugin-data/@ryu+dashboards/dashboards.db` the node uses.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    Router,
};
use ryu_app_events::{ManagedProviderCall, ManagedProviderResponse, ProviderRouter};
use ryu_dashboards::{
    refresh, routes, set_global_engine, store::DashboardStore, DashboardEngine, DashboardsCtx,
    DashboardsHost,
};

mod paths;

/// Default loopback port (overridable via `RYU_DASHBOARDS_PORT`). Distinct from the
/// other local sidecars (mail 7996, browser 7993, gateway 7981).
const DEFAULT_PORT: u16 = 7997;

// ── DashboardsHost: the standalone concrete impl ─────────────────────────────

/// The sidecar's [`DashboardsHost`]. The provider router is built once so all
/// widget refreshes share one authenticated Core connection pool.
struct SidecarDashboardsHost {
    provider_router: ProviderRouter,
}

impl SidecarDashboardsHost {
    fn new() -> Self {
        Self {
            provider_router: ProviderRouter::from_env("@ryu/dashboards"),
        }
    }
}

#[async_trait]
impl DashboardsHost for SidecarDashboardsHost {
    fn supports_source(&self, source_type: &str) -> bool {
        source_type != "agent" && ryu_dashboards::source_is_resolvable(source_type)
    }

    async fn call_provider(
        &self,
        call: ManagedProviderCall,
        agent_grant: Option<&str>,
    ) -> Result<ManagedProviderResponse, String> {
        self.provider_router
            .call_with_agent_grant(call, agent_grant)
            .await
            .map_err(|error| error.to_string())
    }

    async fn agent_run(
        &self,
        _agent_id: &str,
        _conversation_id: &str,
        _prompt: &str,
    ) -> Result<String, String> {
        // Refresh jobs have no delegated user identity. A generic host RPC must
        // not bypass the agent's caller/ACL checks merely to refresh a widget.
        Err("Agent widget refresh requires delegated execution authority that this host does not provide.".to_owned())
    }

    async fn guarded_fetch(
        &self,
        url: &str,
        headers: &[(String, String)],
    ) -> Result<(u16, String), String> {
        let response = ryu_egress::guarded_request(
            ryu_egress::GuardedRequest {
                method: "GET".to_owned(),
                url: url.to_owned(),
                headers: headers.to_vec(),
                body: None,
            },
            ryu_egress::GuardedFetchPolicy {
                allow_http: false,
                max_redirect_hops: 0,
                max_body_bytes: 2 * 1024 * 1024,
                ..Default::default()
            },
        )
        .await?;
        let body = String::from_utf8(response.body)
            .map_err(|_| "HTTP widget returned non-UTF-8 data".to_owned())?;
        Ok((response.status, body))
    }
}

// ── Shared-secret gate ───────────────────────────────────────────────────────

/// Reject any request whose `Authorization: Bearer <token>` does not equal the
/// injected shared secret. FAIL-CLOSED: `expected == None` rejects everything.
async fn require_ext_token(req: Request, next: Next, expected: Option<String>) -> Response {
    let Some(expected) = expected.filter(|t| !t.is_empty()) else {
        return unauthorized();
    };
    let auth_header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if ryu_sidecar_runtime::bearer_ok(auth_header, Some(expected.as_str())) {
        next.run(req).await
    } else {
        unauthorized()
    }
}

fn unauthorized() -> Response {
    (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
}

fn main() -> anyhow::Result<()> {
    // Take the plugin sealing subkey Core delivered out of the environment
    // before any thread starts, so nothing this process spawns inherits it.
    ryu_crypto::capture_process_seal_key()?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = std::env::var("RYU_DASHBOARDS_PORT")
        .ok()
        .and_then(|p| p.trim().parse().ok())
        .unwrap_or(DEFAULT_PORT);

    // Shared-secret bearer Core injects when it spawns this sidecar via the generic
    // ext-proxy loader (mirrors ryu-mail). Fail-closed when absent.
    let token = std::env::var("RYU_EXT_TOKEN")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    if token.is_some() {
        tracing::info!("ryu-dashboards: routes require the injected shared-secret bearer");
    } else {
        tracing::warn!(
            "ryu-dashboards: no RYU_EXT_TOKEN set; all /api/dashboards/* routes are \
             FAIL-CLOSED (reject all). Core injects this token when it spawns the sidecar."
        );
    }

    // The sidecar OWNS the store (opens the SAME dashboards.db Core would in-process).
    // HTTP-widget header values are sealed with the `@ryu/dashboards` plugin
    // subkey Core delivers (or, standalone, one derived from RYU_MASTER_KEY).
    let sealer = match ryu_crypto::plugin_cipher_for_process("@ryu/dashboards") {
        Ok(cipher) => Some(cipher),
        Err(error) => {
            tracing::warn!(
                "ryu-dashboards: no sealing key ({error}); widget headers cannot be stored"
            );
            None
        }
    };
    let store = DashboardStore::open_with_sealer(paths::dashboards_db_path(), sealer)?;
    let engine = DashboardEngine::new(
        store,
        reqwest::Client::new(),
        Arc::new(SidecarDashboardsHost::new()),
    );
    // Publish the global engine (idempotent; mirrors Core startup) and drive the
    // refresh loop from this process so widgets update out-of-process.
    set_global_engine(engine.clone());
    refresh::spawn(engine.clone());

    // The crate router is declared RELATIVE to `/api/dashboards`; Core's ext proxy
    // forwards `<mount><sub_path>` (mount = `/api/dashboards`), so nest it here to
    // serve the full external paths the health probe + proxy hit.
    //
    // `/openapi.json` rides INSIDE the same bearer gate, at the SERVER ROOT. Core
    // fetches `http://127.0.0.1:<port>/openapi.json` on this sidecar's first Healthy
    // edge and lowers every operation it finds into searchable LLM tools, so routing
    // this one endpoint is what makes the whole `/api/dashboards` surface callable by
    // an agent (`ryu_dashboards::api::openapi()` was dead code until now — only tests
    // read it).
    //
    // Root, not under `/api/dashboards`: Core tries the root FIRST and only falls back
    // to the mount-prefixed form, and keeping the document off the mount keeps it out
    // of the manifest's declared `http.routes[]` — anything declared there is
    // reachable through the generic ext-proxy, and the schema is Core's to read, not
    // an app surface. Inside the gate: Core stamps the injected `RYU_EXT_TOKEN` on the
    // fetch (the Python sidecars already require the bearer for everything but
    // `/health`), so the gate costs the fetcher nothing — while un-gated it would
    // disclose this app's entire internal API surface to any other process on
    // loopback.
    let inner = routes(DashboardsCtx::new(engine));
    let app = Router::new()
        .nest("/api/dashboards", inner)
        .route(
            "/openapi.json",
            axum::routing::get(|| async { axum::Json(ryu_dashboards::api::openapi()) }),
        )
        .layer(from_fn(move |req: Request, next: Next| {
            let expected = token.clone();
            async move { require_ext_token(req, next, expected).await }
        }));

    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("ryu-dashboards sidecar listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sidecar_host_advertises_supported_sources_and_guards_http() {
        let host = SidecarDashboardsHost::new();
        assert!(!host.supports_source("agent"));
        for kind in ryu_dashboards::CORE_READ_SOURCE_TYPES {
            assert!(!host.supports_source(kind), "{kind}");
        }
        for kind in ["static", "composio", "http"] {
            assert!(host.supports_source(kind), "{kind}");
        }
        assert!(!host.supports_source("invented"));
        assert!(host.agent_run("a", "c", "p").await.is_err());
        assert!(host.guarded_fetch("http://example.com", &[]).await.is_err());
        assert!(host
            .guarded_fetch("https://127.0.0.1/private", &[])
            .await
            .is_err());
    }

    #[tokio::test]
    async fn standalone_catalog_and_writes_reject_agent_sources_without_mutating_saved_widgets() {
        use axum::extract::{Path, State};
        use axum::Json;
        use ryu_dashboards::api;
        use serde_json::json;
        let dir =
            std::env::temp_dir().join(format!("dashboard-capabilities-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let engine = DashboardEngine::new(
            DashboardStore::open_with_sealer(
                dir.join("test.db"),
                Some(ryu_dashboards::store::test_plugin_cipher(7)),
            )
            .unwrap(),
            reqwest::Client::new(),
            Arc::new(SidecarDashboardsHost::new()),
        );
        let ctx = DashboardsCtx::new(engine);
        let Json(catalog) = api::catalog(State(ctx.clone())).await;
        assert!(!catalog["source_types"]
            .as_array()
            .unwrap()
            .contains(&json!("agent")));
        assert!(catalog["source_types"]
            .as_array()
            .unwrap()
            .contains(&json!("http")));
        let (_, Json(created)) = api::create_dashboard(
            State(ctx.clone()),
            Json(serde_json::from_value(json!({"name": "Test"})).unwrap()),
        )
        .await;
        let dashboard_id = created["dashboard"]["id"].as_str().unwrap().to_owned();
        let (status, _) = api::create_widget(State(ctx.clone()), Path(dashboard_id.clone()), axum::http::HeaderMap::new(), Json(serde_json::from_value(json!({"kind": "stat", "source": {"type": "agent", "agent_id": "a", "prompt": "hello"}})).unwrap())).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(ctx
            .engine
            .store
            .list_widgets(&dashboard_id)
            .await
            .unwrap()
            .is_empty());
        let (_, Json(created)) = api::create_widget(
            State(ctx.clone()),
            Path(dashboard_id.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(
                    json!({"kind": "stat", "source": {"type": "static", "data": 42}}),
                )
                .unwrap(),
            ),
        )
        .await;
        let widget_id = created["widget"]["id"].as_str().unwrap().to_owned();
        let (status, _) = api::update_widget(
            State(ctx.clone()),
            Path((dashboard_id.clone(), widget_id.clone())),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(
                    json!({"source": {"type": "agent", "agent_id": "a", "prompt": "hello"}}),
                )
                .unwrap(),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            ctx.engine
                .store
                .get_widget_for_dashboard(&dashboard_id, &widget_id)
                .await
                .unwrap()
                .unwrap()
                .source
                .source_type(),
            "static"
        );
        let mut replacement = created["widget"].clone();
        replacement["source"] = json!({"type": "agent", "agent_id": "a", "prompt": "hello"});
        assert!(ryu_dashboards::replace_widgets(
            &ctx.engine,
            &dashboard_id,
            &json!([replacement]),
            None
        )
        .await
        .is_err());
        drop(ctx);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn standalone_host_refuses_core_read_sources_and_explains_saved_ones() {
        use axum::extract::{Path, State};
        use axum::Json;
        use ryu_dashboards::{api, GridLayout, Widget, WidgetKind, WidgetSource};
        use serde_json::json;
        let dir =
            std::env::temp_dir().join(format!("dashboard-core-read-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let engine = DashboardEngine::new(
            DashboardStore::open(dir.join("test.db")).unwrap(),
            reqwest::Client::new(),
            Arc::new(SidecarDashboardsHost::new()),
        );
        let ctx = DashboardsCtx::new(engine);

        let Json(catalog) = api::catalog(State(ctx.clone())).await;
        let offered = catalog["source_types"].as_array().unwrap();
        for kind in ryu_dashboards::CORE_READ_SOURCE_TYPES {
            assert!(
                !offered.contains(&json!(kind)),
                "{kind} must not be offered"
            );
        }
        assert_eq!(catalog["core_endpoints"], json!([]));

        let (_, Json(created)) = api::create_dashboard(
            State(ctx.clone()),
            Json(serde_json::from_value(json!({"name": "Home"})).unwrap()),
        )
        .await;
        let dashboard_id = created["dashboard"]["id"].as_str().unwrap().to_owned();
        for source in [
            json!({"type": "core_endpoint", "endpoint": "system_status"}),
            json!({"type": "monitor", "monitor_id": "m1"}),
            json!({"type": "workflow", "workflow_id": "wf1"}),
        ] {
            let (status, Json(body)) = api::create_widget(
                State(ctx.clone()),
                Path(dashboard_id.clone()),
                axum::http::HeaderMap::new(),
                Json(serde_json::from_value(json!({"kind": "stat", "source": source})).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert!(
                body["error"]
                    .as_str()
                    .unwrap()
                    .contains("not available yet"),
                "{body}"
            );
        }

        // A widget saved before these kinds were withdrawn: its refresh records a
        // clear, kind-specific error rather than an unauthenticated loopback 401.
        let legacy = Widget {
            id: "legacy".into(),
            dashboard_id: dashboard_id.clone(),
            kind: WidgetKind::Stat,
            title: "Status".into(),
            config: json!({}),
            source: WidgetSource::CoreEndpoint {
                endpoint: "system_status".into(),
                selector: None,
            },
            refresh_interval: None,
            layout: GridLayout::default(),
            canvas: None,
            last_value: None,
            last_refresh_at: None,
            last_error: None,
        };
        ctx.engine.store.upsert_widget(&legacy).await.unwrap();
        let (status, Json(body)) = api::refresh_widget(
            State(ctx.clone()),
            Path((dashboard_id.clone(), "legacy".into())),
            axum::http::HeaderMap::new(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let error = body["error"].as_str().unwrap();
        assert!(
            error.contains("Core metric widgets are not available yet"),
            "{error}"
        );
        let stored = ctx
            .engine
            .store
            .get_widget_for_dashboard(&dashboard_id, "legacy")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.last_error.as_deref(), Some(error));
        drop(ctx);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn require_ext_token_fail_closed_and_bearer_match() {
        use axum::body::Body;
        use axum::routing::get;
        use tower::ServiceExt; // oneshot

        // Build a tiny app guarded by require_ext_token with a known secret.
        let make_app = |expected: Option<String>| {
            Router::new()
                .route("/x", get(|| async { "ok" }))
                .layer(from_fn(move |req: Request, next: Next| {
                    let expected = expected.clone();
                    async move { require_ext_token(req, next, expected).await }
                }))
        };

        // Fail-closed: no expected token ⇒ 401 even with a bearer.
        let resp = make_app(None)
            .oneshot(
                Request::builder()
                    .uri("/x")
                    .header(AUTHORIZATION, "Bearer anything")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Wrong bearer ⇒ 401.
        let resp = make_app(Some("s3cret".into()))
            .oneshot(
                Request::builder()
                    .uri("/x")
                    .header(AUTHORIZATION, "Bearer nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Missing header ⇒ 401.
        let resp = make_app(Some("s3cret".into()))
            .oneshot(Request::builder().uri("/x").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // Correct bearer ⇒ pass through (200).
        let resp = make_app(Some("s3cret".into()))
            .oneshot(
                Request::builder()
                    .uri("/x")
                    .header(AUTHORIZATION, "Bearer s3cret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
