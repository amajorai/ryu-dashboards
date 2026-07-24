//! Resolving a widget's [`WidgetSource`] into a JSON value.
//!
//! Each source kind has a dedicated path:
//!   - `Static`       — returns the literal inline data.
//!   - `CoreEndpoint` — a curated **allowlist** of internal Core routes, fetched
//!     over loopback (no arbitrary base URL). The allowlist is the security
//!     boundary: the AI/user can only name an endpoint we approved.
//!   - `Monitor`      — the named monitor's latest result (via `/api/monitors`).
//!   - `Workflow`     — runs a saved workflow and reads a key from its output.
//!   - `Composio`     — executes a Composio action through the Gateway.
//!   - `Http`         — an arbitrary external GET, **SSRF-guarded** (https only,
//!     no private/loopback/link-local/metadata targets).
//!   - `Agent`        — re-runs a configured agent and parses its JSON reply.
//!
//! An optional dotted `selector` ("a.b.0.c") narrows a JSON response to one field.

use anyhow::{anyhow, Result};
use serde_json::Value;
use std::time::Duration;

use super::{DashboardsHost, WidgetSource};

/// Curated allowlist: the only internal endpoints a `CoreEndpoint` widget may
/// read. Maps a stable short name (what the builder/UI uses) to its loopback
/// path. Arbitrary internal access is deliberately impossible.
pub fn core_endpoint_path(name: &str) -> Option<&'static str> {
    match name {
        "system_status" => Some("/api/system/status"),
        "sidecar_status" => Some("/api/sidecar/status"),
        "connections" => Some("/api/connections"),
        "quests" => Some("/api/quests"),
        "monitors" => Some("/api/monitors"),
        "engines" => Some("/api/engines"),
        "agents" => Some("/api/agents"),
        "workflows" => Some("/workflows"),
        "meetings" => Some("/api/meetings"),
        _ => None,
    }
}

/// The names a `CoreEndpoint` widget may use (surfaced to the builder + UI).
pub const CORE_ENDPOINT_NAMES: &[&str] = &[
    "system_status",
    "sidecar_status",
    "connections",
    "quests",
    "monitors",
    "engines",
    "agents",
    "workflows",
    "meetings",
];

/// Loopback base URL of this Core process, derived from the SAME bind chain
/// `main.rs` uses (`--bind=` arg → `RYU_BIND` env → `127.0.0.1:7980`), then forced
/// to the loopback host for self-calls. Replicating the full chain (not just
/// `RYU_BIND`) keeps CoreEndpoint widgets working under a headless `--bind=` too.
fn self_base() -> String {
    let bind = std::env::args()
        .skip(1)
        .find(|a| a.starts_with("--bind="))
        .and_then(|a| a.strip_prefix("--bind=").map(str::to_string))
        .or_else(|| std::env::var("RYU_BIND").ok())
        .unwrap_or_else(|| "127.0.0.1:7980".to_string());
    let port = bind.rsplit(':').next().unwrap_or("7980");
    format!("http://127.0.0.1:{port}")
}

/// The shared-secret token, if the node runs with one (self-calls must present it).
fn self_token() -> Option<String> {
    std::env::var("RYU_TOKEN").ok().filter(|s| !s.is_empty())
}

/// Is `id` safe to interpolate as a single path segment into a privileged
/// Core/Gateway loopback URL that carries the node token?
///
/// `workflow_id` and the Composio `action` are attacker-controlled widget config
/// spliced raw into a self-call URL. The `url` crate collapses `..` dot-segments
/// and a `?`/`#` terminates the path, so a value like `"../../api/agents/foo?"`
/// escapes the intended `/workflows/<id>/` prefix and reaches an ARBITRARY
/// internal route — defeating the curated `CoreEndpoint` allowlist. Restrict to a
/// conservative id charset and forbid the `..` sequence outright (mirrors clips'
/// `clip_id_is_safe`).
pub(crate) fn id_segment_is_safe(id: &str) -> bool {
    !id.is_empty()
        && !id.contains("..")
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// Walk a dotted path ("a.b.0.c") into a JSON value. Each segment is an object
/// key or, if numeric, an array index. Returns null when the path misses.
pub fn select(value: &Value, selector: Option<&str>) -> Value {
    let path = match selector.map(str::trim).filter(|s| !s.is_empty()) {
        Some(p) => p,
        None => return value.clone(),
    };
    let mut cur = value;
    for seg in path.split('.') {
        cur = match cur {
            Value::Object(map) => match map.get(seg) {
                Some(v) => v,
                None => return Value::Null,
            },
            Value::Array(arr) => match seg.parse::<usize>().ok().and_then(|i| arr.get(i)) {
                Some(v) => v,
                None => return Value::Null,
            },
            _ => return Value::Null,
        };
    }
    cur.clone()
}

/// Resolve a source to its current value. `widget_id` scopes the per-widget agent
/// conversation so two widgets on the same agent don't share (and bleed) context.
pub async fn resolve(
    http: &reqwest::Client,
    host: &dyn DashboardsHost,
    source: &WidgetSource,
    widget_id: &str,
) -> Result<Value> {
    match source {
        WidgetSource::Static { data } => Ok(data.clone()),
        WidgetSource::CoreEndpoint { endpoint, selector } => {
            let path = core_endpoint_path(endpoint)
                .ok_or_else(|| anyhow!("'{endpoint}' is not an allowed Core endpoint"))?;
            let body = loopback_get(http, path).await?;
            Ok(select(&body, selector.as_deref()))
        }
        WidgetSource::Monitor { monitor_id } => {
            let body = loopback_get(http, "/api/monitors").await?;
            let monitors = body.get("monitors").and_then(Value::as_array);
            let found = monitors.and_then(|arr| {
                arr.iter()
                    .find(|m| m.get("id").and_then(Value::as_str) == Some(monitor_id.as_str()))
                    .cloned()
            });
            found.ok_or_else(|| anyhow!("monitor '{monitor_id}' not found"))
        }
        WidgetSource::Workflow {
            workflow_id,
            input,
            output_key,
        } => resolve_workflow(http, workflow_id, input, output_key.as_deref()).await,
        WidgetSource::Composio { action, args } => resolve_composio(http, host, action, args).await,
        WidgetSource::Http {
            url,
            selector,
            headers,
        } => resolve_http(host, url, selector.as_deref(), headers).await,
        WidgetSource::Agent { agent_id, prompt } => {
            resolve_agent(host, agent_id, prompt, widget_id).await
        }
    }
}

/// GET an internal path over loopback, presenting the shared token when set.
async fn loopback_get(http: &reqwest::Client, path: &str) -> Result<Value> {
    let url = format!("{}{}", self_base(), path);
    let mut req = http.get(&url).timeout(Duration::from_secs(15));
    if let Some(t) = self_token() {
        req = req.bearer_auth(t);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| anyhow!("core self-call to {path} failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "core self-call to {path} returned {}",
            resp.status()
        ));
    }
    resp.json()
        .await
        .map_err(|e| anyhow!("core self-call to {path} was not JSON: {e}"))
}

async fn resolve_workflow(
    http: &reqwest::Client,
    workflow_id: &str,
    input: &std::collections::HashMap<String, String>,
    output_key: Option<&str>,
) -> Result<Value> {
    if !id_segment_is_safe(workflow_id) {
        return Err(anyhow!(
            "invalid workflow_id '{workflow_id}': must be a plain id (no '/', '..', or query chars)"
        ));
    }
    let url = format!("{}/workflows/{}/run", self_base(), workflow_id);
    let mut req = http
        .post(&url)
        .timeout(Duration::from_secs(120))
        .json(&serde_json::json!({ "input": input }));
    if let Some(t) = self_token() {
        req = req.bearer_auth(t);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| anyhow!("workflow run failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(anyhow!("workflow run returned {}", resp.status()));
    }
    let body: Value = resp
        .json()
        .await
        .map_err(|e| anyhow!("workflow run response was not JSON: {e}"))?;
    let output = body.get("run").and_then(|r| r.get("output"));
    match (output, output_key) {
        (Some(out), Some(key)) => Ok(out.get(key).cloned().unwrap_or(Value::Null)),
        (Some(out), None) => Ok(out.clone()),
        (None, _) => Ok(body),
    }
}

async fn resolve_composio(
    http: &reqwest::Client,
    host: &dyn DashboardsHost,
    action: &str,
    args: &Value,
) -> Result<Value> {
    if !id_segment_is_safe(action) {
        return Err(anyhow!(
            "invalid composio action '{action}': must be a plain action id (no '/', '..', or query chars)"
        ));
    }
    let url = format!("{}/tools/execute/{}", host.gateway_url(), action);
    let mut req = http
        .post(&url)
        .timeout(Duration::from_secs(60))
        .json(&serde_json::json!({ "arguments": args }));
    if let Some(t) = host.gateway_token() {
        req = req.bearer_auth(t);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| anyhow!("composio execute failed: {e}"))?;
    if !resp.status().is_success() {
        return Err(anyhow!("composio execute returned {}", resp.status()));
    }
    let body: Value = resp
        .json()
        .await
        .map_err(|e| anyhow!("composio response was not JSON: {e}"))?;
    // Composio wraps payloads in `data`; surface it when present.
    Ok(body.get("data").cloned().unwrap_or(body))
}

async fn resolve_agent(
    host: &dyn DashboardsHost,
    agent_id: &str,
    prompt: &str,
    widget_id: &str,
) -> Result<Value> {
    // One stable conversation PER WIDGET (not per agent) keeps each widget's
    // context across ticks without two widgets on the same agent bleeding context.
    let conv_id = format!("dashboard-widget-{widget_id}");
    let reply = host
        .agent_run(agent_id, &conv_id, prompt)
        .await
        .map_err(|e| anyhow!("agent run failed: {e}"))?;
    Ok(parse_agent_reply(&reply))
}

/// Best-effort: pull JSON out of an agent reply (a fenced ```json block or the
/// whole string), falling back to `{ "text": reply }` for prose.
fn parse_agent_reply(reply: &str) -> Value {
    let trimmed = reply.trim();
    if let Some(start) = trimmed.find("```") {
        let after = &trimmed[start + 3..];
        let after = after.strip_prefix("json").unwrap_or(after);
        if let Some(end) = after.find("```") {
            let inner = after[..end].trim();
            if let Ok(v) = serde_json::from_str::<Value>(inner) {
                return v;
            }
        }
    }
    if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
        return v;
    }
    serde_json::json!({ "text": reply })
}

/// Fetch an arbitrary external HTTPS endpoint with full SSRF protection:
///   - https only;
///   - the host is resolved ONCE, every resolved IP is validated as public, and
///     the connection is **pinned** to that validated IP (`Client::resolve`) so a
///     DNS-rebind between check and connect cannot retarget an internal address;
///   - **redirects are disabled** (`Policy::none()`) so a `30x → internal` hop
///     cannot bypass the guard. A redirect surfaces as a non-2xx error.
async fn resolve_http(
    host: &dyn DashboardsHost,
    url: &str,
    selector: Option<&str>,
    headers: &std::collections::HashMap<String, String>,
) -> Result<Value> {
    let header_pairs = headers
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    let (status, body) = host
        .guarded_fetch(url, &header_pairs)
        .await
        .map_err(|e| anyhow!("{e}"))?;
    if (300..400).contains(&status) {
        return Err(anyhow!("http source refused: redirects are not allowed"));
    }
    if !(200..300).contains(&status) {
        return Err(anyhow!("http GET returned {status}"));
    }
    let body: Value =
        serde_json::from_str(&body).map_err(|e| anyhow!("response was not JSON: {e}"))?;
    Ok(select(&body, selector.as_deref()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{engine_with, FakeHost};
    use crate::WidgetSource;
    use std::sync::Arc;

    #[test]
    fn selector_helpers_work() {
        let v = serde_json::json!({ "a": { "b": [1, 2, 3] } });
        assert_eq!(select(&v, Some("a.b.2")), serde_json::json!(3));
        assert_eq!(select(&v, Some("nope")), Value::Null);
    }

    #[test]
    fn select_edge_cases() {
        let v = serde_json::json!({ "a": [10, 20] });
        // Out-of-range array index ⇒ null.
        assert_eq!(select(&v, Some("a.5")), Value::Null);
        // Descending into a scalar ⇒ null.
        assert_eq!(select(&v, Some("a.0.deeper")), Value::Null);
        // Whitespace-only selector behaves like None (returns the whole value).
        assert_eq!(select(&v, Some("   ")), v);
        // Empty selector likewise.
        assert_eq!(select(&v, Some("")), v);
    }

    #[test]
    fn core_endpoint_allowlist_maps_known_and_rejects_unknown() {
        assert_eq!(
            core_endpoint_path("system_status"),
            Some("/api/system/status")
        );
        assert_eq!(core_endpoint_path("workflows"), Some("/workflows"));
        assert!(core_endpoint_path("secrets").is_none());
        assert!(core_endpoint_path("").is_none());
        // Every advertised name resolves to a path (catalog ↔ resolver agree).
        for name in CORE_ENDPOINT_NAMES {
            assert!(core_endpoint_path(name).is_some(), "{name} must map");
        }
    }

    #[test]
    fn id_segment_is_safe_rejects_traversal_and_separators() {
        // Ordinary ids (uuid/slug) pass.
        assert!(id_segment_is_safe("abc123"));
        assert!(id_segment_is_safe("wf-2026_07-16.v2"));
        assert!(id_segment_is_safe("550e8400-e29b-41d4-a716-446655440000"));
        // Empty collapses the path -> rejected.
        assert!(!id_segment_is_safe(""));
        // Traversal / route-smuggling attempts are rejected.
        assert!(!id_segment_is_safe(".."));
        assert!(!id_segment_is_safe("../../api/agents/foo"));
        assert!(!id_segment_is_safe("a..b"));
        assert!(!id_segment_is_safe("a/b")); // slash not in charset
        assert!(!id_segment_is_safe("../../api/agents/foo?")); // '?' + '/' + '..'
        assert!(!id_segment_is_safe("a b")); // space
    }

    #[test]
    fn parse_agent_reply_variants() {
        // A fenced ```json block is preferred.
        let v = parse_agent_reply("here you go:\n```json\n{\"n\": 7}\n```\nbye");
        assert_eq!(v, serde_json::json!({ "n": 7 }));
        // A bare fence without the json language tag still parses.
        let v = parse_agent_reply("```\n[1, 2, 3]\n```");
        assert_eq!(v, serde_json::json!([1, 2, 3]));
        // No fence, but the whole string is JSON.
        let v = parse_agent_reply("  {\"ok\": true}  ");
        assert_eq!(v, serde_json::json!({ "ok": true }));
        // Prose falls back to { text }.
        let v = parse_agent_reply("just some words");
        assert_eq!(v, serde_json::json!({ "text": "just some words" }));
        // A fence whose contents are not JSON falls through to the { text } wrap.
        let v = parse_agent_reply("```json\nnot json at all\n```");
        assert_eq!(v["text"], serde_json::json!("```json\nnot json at all\n```"));
    }

    #[tokio::test]
    async fn resolve_static_returns_inline_data() {
        let host = FakeHost::default();
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::Static {
            data: serde_json::json!({ "hi": 1 }),
        };
        let v = resolve(&engine.http, &host, &src, "w1").await.unwrap();
        assert_eq!(v, serde_json::json!({ "hi": 1 }));
    }

    #[tokio::test]
    async fn resolve_core_endpoint_rejects_unknown_without_network() {
        // An unknown endpoint fails at the allowlist BEFORE any loopback call — this
        // is hermetic even on a box where Core is up.
        let host = FakeHost::default();
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::CoreEndpoint {
            endpoint: "secrets".into(),
            selector: None,
        };
        let err = resolve(&engine.http, &host, &src, "w1").await.unwrap_err();
        assert!(err.to_string().contains("not an allowed Core endpoint"));
    }

    #[tokio::test]
    async fn resolve_http_success_applies_selector_and_records_call() {
        let host = FakeHost::default();
        *host.fetch_reply.lock().unwrap() = Ok((200, r#"{"data":{"count":5}}"#.into()));
        let engine = engine_with(Arc::new(FakeHost::default()));
        let mut headers = std::collections::HashMap::new();
        headers.insert("X-Api".to_string(), "key".to_string());
        let src = WidgetSource::Http {
            url: "https://api.example.com/x".into(),
            selector: Some("data.count".into()),
            headers,
        };
        let v = resolve(&engine.http, &host, &src, "w1").await.unwrap();
        assert_eq!(v, serde_json::json!(5));
        // The guarded fetch saw the url + header pair.
        let calls = host.fetch_calls.lock().unwrap();
        assert_eq!(calls[0].0, "https://api.example.com/x");
        assert_eq!(calls[0].1, vec![("X-Api".to_string(), "key".to_string())]);
    }

    #[tokio::test]
    async fn resolve_http_rejects_redirect_status() {
        let host = FakeHost::default();
        *host.fetch_reply.lock().unwrap() = Ok((302, String::new()));
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::Http {
            url: "https://x".into(),
            selector: None,
            headers: Default::default(),
        };
        let err = resolve(&engine.http, &host, &src, "w1").await.unwrap_err();
        assert!(err.to_string().contains("redirects are not allowed"));
    }

    #[tokio::test]
    async fn resolve_http_rejects_non_2xx_and_non_json() {
        let engine = engine_with(Arc::new(FakeHost::default()));
        // Non-2xx.
        let host = FakeHost::default();
        *host.fetch_reply.lock().unwrap() = Ok((500, "boom".into()));
        let src = WidgetSource::Http {
            url: "https://x".into(),
            selector: None,
            headers: Default::default(),
        };
        let err = resolve(&engine.http, &host, &src, "w1").await.unwrap_err();
        assert!(err.to_string().contains("http GET returned 500"));

        // 2xx but not JSON.
        let host2 = FakeHost::default();
        *host2.fetch_reply.lock().unwrap() = Ok((200, "<html>".into()));
        let err = resolve(&engine.http, &host2, &src, "w1").await.unwrap_err();
        assert!(err.to_string().contains("response was not JSON"));
    }

    #[tokio::test]
    async fn resolve_http_surfaces_guard_error() {
        let host = FakeHost::default();
        *host.fetch_reply.lock().unwrap() = Err("blocked: private address".into());
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::Http {
            url: "https://x".into(),
            selector: None,
            headers: Default::default(),
        };
        let err = resolve(&engine.http, &host, &src, "w1").await.unwrap_err();
        assert!(err.to_string().contains("blocked: private address"));
    }

    #[tokio::test]
    async fn resolve_agent_parses_reply_and_scopes_conversation() {
        let host = FakeHost::default();
        *host.agent_reply.lock().unwrap() = Ok("```json\n{\"v\": 42}\n```".into());
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::Agent {
            agent_id: "assistant".into(),
            prompt: "how many?".into(),
        };
        let v = resolve(&engine.http, &host, &src, "widget-7").await.unwrap();
        assert_eq!(v, serde_json::json!({ "v": 42 }));
        // The conversation id is per-widget (not per-agent), so two widgets don't bleed.
        let calls = host.agent_calls.lock().unwrap();
        assert_eq!(calls[0].0, "assistant");
        assert_eq!(calls[0].1, "dashboard-widget-widget-7");
        assert_eq!(calls[0].2, "how many?");
    }

    #[tokio::test]
    async fn resolve_agent_propagates_error() {
        let host = FakeHost::default();
        *host.agent_reply.lock().unwrap() = Err("runner offline".into());
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::Agent {
            agent_id: "a".into(),
            prompt: "p".into(),
        };
        let err = resolve(&engine.http, &host, &src, "w1").await.unwrap_err();
        assert!(err.to_string().contains("agent run failed"));
        assert!(err.to_string().contains("runner offline"));
    }

    #[tokio::test]
    async fn resolve_composio_errors_on_closed_gateway() {
        // The default FakeHost points gateway_url at a closed loopback port, so the
        // real reqwest POST fails fast (connection refused) — the Composio error
        // branch runs with no live server and no external traffic.
        let host = FakeHost::default();
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::Composio {
            action: "GITHUB_LIST".into(),
            args: serde_json::json!({ "x": 1 }),
        };
        let err = resolve(&engine.http, &host, &src, "w1").await.unwrap_err();
        assert!(err.to_string().contains("composio execute failed"), "{err}");
    }
}
