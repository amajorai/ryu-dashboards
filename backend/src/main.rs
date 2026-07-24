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
//!   - **Composio widgets** (`gateway_url`/`gateway_token`) → resolved from the
//!     inherited `RYU_GATEWAY_URL` / `RYU_GATEWAY_TOKEN` env (Core runs the Gateway
//!     as a loopback sidecar). Fully functional out-of-process.
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
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    Router,
};
use ryu_dashboards::{
    refresh, routes, set_global_engine, store::DashboardStore, DashboardEngine, DashboardsCtx,
    DashboardsHost,
};

/// Default loopback port (overridable via `RYU_DASHBOARDS_PORT`). Distinct from the
/// other local sidecars (mail 7996, browser 7993, gateway 7981).
const DEFAULT_PORT: u16 = 7997;

/// Gateway default port when `RYU_GATEWAY_URL` is unset (mirrors Core's
/// `sidecar::gateway::DEFAULT_GATEWAY_URL` base + the dev profile port shift).
const GATEWAY_DEFAULT_PORT: u16 = 7981;
/// Port offset applied outside the release profile (mirrors Core's
/// `profile::DEV_PORT_OFFSET`), so a dev-profile node reaches the shifted Gateway.
const DEV_PORT_OFFSET: u16 = 1000;

// ── DashboardsHost: the standalone concrete impl ─────────────────────────────

/// The sidecar's [`DashboardsHost`]. Stateless — every coupling resolves from the
/// inherited env or is a documented broker-back refusal, so a unit struct suffices.
struct SidecarDashboardsHost;

#[async_trait]
impl DashboardsHost for SidecarDashboardsHost {
    fn gateway_url(&self) -> String {
        std::env::var("RYU_GATEWAY_URL")
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("http://127.0.0.1:{}", gateway_default_port()))
    }

    fn gateway_token(&self) -> Option<String> {
        std::env::var("RYU_GATEWAY_TOKEN")
            .ok()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
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

/// The Gateway port to target when `RYU_GATEWAY_URL` is unset: release default,
/// shifted by [`DEV_PORT_OFFSET`] outside the release profile.
fn gateway_default_port() -> u16 {
    let is_release = std::env::var("RYU_PROFILE")
        .ok()
        .map(|p| p.trim().to_owned())
        .map_or(true, |p| p.is_empty() || p == "release");
    if is_release {
        GATEWAY_DEFAULT_PORT
    } else {
        GATEWAY_DEFAULT_PORT.saturating_add(DEV_PORT_OFFSET)
    }
}

// ── Data-dir resolution (RYU_DIR-env-first) ──────────────────────────────────

/// The `dashboards.db` path under the node's data dir. Core injects `RYU_DIR` into
/// this child's spawn env (`inject_ext_env`), guaranteeing co-location with the node.
/// The `dirs`-based default + `RYU_PROFILE` suffix are replicated for a faithful
/// bare-run, but env-first is what actually guarantees the shared path.
fn dashboards_db_path() -> PathBuf {
    ryu_dir().join("dashboards.db")
}

fn ryu_dir() -> PathBuf {
    if let Some(v) = std::env::var_os("RYU_DIR") {
        let p = PathBuf::from(v);
        if !p.as_os_str().is_empty() {
            return p;
        }
    }
    let profile = std::env::var("RYU_PROFILE")
        .ok()
        .map(|p| p.trim().to_owned())
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "release".to_owned());
    let name = if profile == "release" {
        ".ryu".to_owned()
    } else {
        format!(".ryu-{profile}")
    };
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(name)
}

// ── Shared-secret gate ───────────────────────────────────────────────────────

/// Constant-time byte comparison so the bearer check does not leak length/prefix
/// timing.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Reject any request whose `Authorization: Bearer <token>` does not equal the
/// injected shared secret. FAIL-CLOSED: `expected == None` rejects everything.
async fn require_ext_token(req: Request, next: Next, expected: Option<String>) -> Response {
    let Some(expected) = expected.filter(|t| !t.is_empty()) else {
        return unauthorized();
    };
    let provided = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if ct_eq(provided.as_bytes(), expected.as_bytes()) {
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
    let store = DashboardStore::open(dashboards_db_path())?;
    let engine = DashboardEngine::new(
        store,
        reqwest::Client::new(),
        Arc::new(SidecarDashboardsHost),
    );
    // Publish the global engine (idempotent; mirrors Core startup) and drive the
    // refresh loop from this process so widgets update out-of-process.
    set_global_engine(engine.clone());
    refresh::spawn(engine.clone());

    // The crate router is declared RELATIVE to `/api/dashboards`; Core's ext proxy
    // forwards `<mount><sub_path>` (mount = `/api/dashboards`), so nest it here to
    // serve the full external paths the health probe + proxy hit.
    let inner = routes(DashboardsCtx::new(engine));
    let app = Router::new().nest("/api/dashboards", inner).layer(from_fn(
        move |req: Request, next: Next| {
            let expected = token.clone();
            async move { require_ext_token(req, next, expected).await }
        },
    ));

    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("ryu-dashboards sidecar listening on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ct_eq_is_length_and_content_sensitive() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secreT"));
        assert!(!ct_eq(b"secret", b"secret-longer"));
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
    }

    #[tokio::test]
    async fn sidecar_host_refuses_agent_and_http_widgets() {
        // The standalone sidecar cannot serve Agent/HTTP widgets (they need Core's
        // in-process runner / SSRF-guarded fetch); both refuse cleanly with a clear
        // broker-back message rather than faking a result.
        let host = SidecarDashboardsHost;
        let a = host.agent_run("a", "c", "p").await.unwrap_err();
        assert!(a.contains("not available in the standalone"));
        assert!(a.contains("agent"));
        let h = host.guarded_fetch("https://x", &[]).await.unwrap_err();
        assert!(h.contains("not available in the standalone"));
        assert!(h.contains("http"));
    }

    /// Env-reading helpers are exercised in a SINGLE test that mutates + restores
    /// process env sequentially, so no two threads race the same variable (the bin
    /// test binary would otherwise run these in parallel).
    #[test]
    fn env_derived_resolvers() {
        // Snapshot the vars we touch so the box's real env is restored afterwards.
        let saved: Vec<(&str, Option<String>)> = ["RYU_GATEWAY_URL", "RYU_GATEWAY_TOKEN", "RYU_PROFILE", "RYU_DIR"]
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect();
        let restore = || {
            for (k, v) in &saved {
                match v {
                    Some(val) => std::env::set_var(k, val),
                    None => std::env::remove_var(k),
                }
            }
        };

        // gateway_url: explicit env wins (trimmed).
        std::env::set_var("RYU_GATEWAY_URL", "  http://gw.local:9  ");
        assert_eq!(SidecarDashboardsHost.gateway_url(), "http://gw.local:9");
        // Empty env ⇒ default derived from the profile port.
        std::env::set_var("RYU_GATEWAY_URL", "   ");
        std::env::set_var("RYU_PROFILE", "release");
        assert_eq!(
            SidecarDashboardsHost.gateway_url(),
            format!("http://127.0.0.1:{GATEWAY_DEFAULT_PORT}")
        );

        // gateway_token: present + trimmed vs blank ⇒ None.
        std::env::set_var("RYU_GATEWAY_TOKEN", "  tok  ");
        assert_eq!(SidecarDashboardsHost.gateway_token().as_deref(), Some("tok"));
        std::env::set_var("RYU_GATEWAY_TOKEN", "   ");
        assert!(SidecarDashboardsHost.gateway_token().is_none());

        // gateway_default_port: release (or unset) ⇒ base; dev ⇒ base + offset.
        std::env::set_var("RYU_PROFILE", "release");
        assert_eq!(gateway_default_port(), GATEWAY_DEFAULT_PORT);
        std::env::remove_var("RYU_PROFILE");
        assert_eq!(gateway_default_port(), GATEWAY_DEFAULT_PORT);
        std::env::set_var("RYU_PROFILE", "dev");
        assert_eq!(
            gateway_default_port(),
            GATEWAY_DEFAULT_PORT + DEV_PORT_OFFSET
        );

        // ryu_dir: RYU_DIR env-first wins outright.
        std::env::set_var("RYU_DIR", "/tmp/ryu-dash-test-dir");
        assert_eq!(ryu_dir(), PathBuf::from("/tmp/ryu-dash-test-dir"));
        assert_eq!(
            dashboards_db_path(),
            PathBuf::from("/tmp/ryu-dash-test-dir").join("dashboards.db")
        );
        // No RYU_DIR + release profile ⇒ ~/.ryu; dev profile ⇒ ~/.ryu-dev.
        std::env::remove_var("RYU_DIR");
        std::env::set_var("RYU_PROFILE", "release");
        assert!(ryu_dir().ends_with(".ryu"));
        std::env::set_var("RYU_PROFILE", "dev");
        assert!(ryu_dir().ends_with(".ryu-dev"));

        restore();
    }

    #[tokio::test]
    async fn require_ext_token_fail_closed_and_bearer_match() {
        use axum::body::Body;
        use axum::routing::get;
        use tower::ServiceExt; // oneshot

        // Build a tiny app guarded by require_ext_token with a known secret.
        let make_app = |expected: Option<String>| {
            Router::new().route("/x", get(|| async { "ok" })).layer(from_fn(
                move |req: Request, next: Next| {
                    let expected = expected.clone();
                    async move { require_ext_token(req, next, expected).await }
                },
            ))
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
