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
//!   - **Agent widgets** (`agent_run`) → require Core's in-process agent runner,
//!     which has NO loopback HTTP equivalent that returns a final reply. Refused with
//!     a clear error; Agent widgets degrade (show the error) until a host-broker hop
//!     lands. Documented broker-back.
//!   - **HTTP widgets** (`guarded_fetch`) → require Core's SSRF-guarded fetch. A
//!     hand-rolled SSRF guard would be strictly worse than refusing, so it is refused
//!     with a clear error. Documented broker-back.
//!
//! The curated CoreEndpoint / Monitor / Workflow sources carry NO host coupling — the
//! crate resolves them over plain loopback self-calls to Core (env-derived base URL +
//! the inherited `RYU_TOKEN`), so they work unchanged from this process.
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
//! Port: `RYU_DASHBOARDS_PORT` env (default `7997`). Data dir: `RYU_DIR`-env-first
//! (Core injects it), so it opens the SAME `dashboards.db` the node uses.

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
    async fn call_provider(
        &self,
        call: ManagedProviderCall,
    ) -> Result<ManagedProviderResponse, String> {
        self.provider_router
            .call(call)
            .await
            .map_err(|error| error.to_string())
    }

    async fn agent_run(
        &self,
        _agent_id: &str,
        _conversation_id: &str,
        _prompt: &str,
    ) -> Result<String, String> {
        // Needs Core's in-process agent runner; no loopback HTTP endpoint returns a
        // final agent reply. Refuse cleanly rather than fake it — Agent widgets
        // surface this as their error until a host-broker hop is added.
        Err(
            "agent widgets are not available in the standalone ryu-dashboards sidecar \
             (they require Core's in-process agent runner; brokering back to Core is a \
             later cut-over)"
                .to_owned(),
        )
    }

    async fn guarded_fetch(
        &self,
        _url: &str,
        _headers: &[(String, String)],
    ) -> Result<(u16, String), String> {
        // Needs Core's SSRF-guarded fetch. A hand-rolled guard would be strictly
        // worse than refusing, so HTTP widgets degrade out-of-process until a
        // host-broker hop is added.
        Err(
            "http widgets are not available in the standalone ryu-dashboards sidecar \
             (they require Core's SSRF-guarded fetch; brokering back to Core is a later \
             cut-over)"
                .to_owned(),
        )
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
    let store = DashboardStore::open(paths::dashboards_db_path())?;
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
    async fn sidecar_host_refuses_agent_and_http_widgets() {
        // The standalone sidecar cannot serve Agent/HTTP widgets (they need Core's
        // in-process runner / SSRF-guarded fetch); both refuse cleanly with a clear
        // broker-back message rather than faking a result.
        let host = SidecarDashboardsHost::new();
        let a = host.agent_run("a", "c", "p").await.unwrap_err();
        assert!(a.contains("not available in the standalone"));
        assert!(a.contains("agent"));
        let h = host.guarded_fetch("https://x", &[]).await.unwrap_err();
        assert!(h.contains("not available in the standalone"));
        assert!(h.contains("http"));
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
