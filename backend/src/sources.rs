//! Resolving a widget's [`WidgetSource`] into a JSON value.
//!
//! Each source kind has a dedicated path:
//!   - `Static`       — returns the literal inline data.
//!   - `Composio`     — executes a Composio action through the managed provider
//!     router (Core → Gateway, authenticated with the sidecar's ext token).
//!   - `Http`         — an arbitrary external GET, **SSRF-guarded** (https only,
//!     no private/loopback/link-local/metadata targets).
//!   - `Agent`        — re-runs a configured agent and parses its JSON reply, when
//!     the host can run agents (the standalone sidecar cannot; see `main.rs`).
//!   - `CoreEndpoint` / `Monitor` / `Workflow` — **unsupported**. Their data sits
//!     behind Core's node-token-protected routes, which an app sidecar can never
//!     reach (it holds only its minted `RYU_EXT_TOKEN`), and no generic host
//!     capability serves those reads yet. They resolve to a clear widget error
//!     ([`unsupported_source_message`]) so a saved widget explains itself instead of
//!     failing a loopback call. See [`crate::CORE_READ_SOURCE_TYPES`].
//!
//! An optional dotted `selector` ("a.b.0.c") narrows a JSON response to one field.

use anyhow::{anyhow, Result};
use ryu_app_events::ManagedProviderCall;
use serde_json::Value;

use super::{DashboardsHost, WidgetSource};

/// The widget error for a source kind this host cannot resolve. Written for the
/// person looking at the broken widget: what is missing and what to use instead.
pub fn unsupported_source_message(source_type: &str) -> String {
    const ALTERNATIVES: &str = "Use an http, composio, or static source instead.";
    match source_type {
        "core_endpoint" => format!(
            "Core metric widgets are not available yet: app sidecars cannot read Core's \
             internal endpoints, and no host capability exposes them. {ALTERNATIVES}"
        ),
        "monitor" => format!(
            "Monitor widgets are not available yet: no host capability lets an app read \
             monitor results. {ALTERNATIVES}"
        ),
        "workflow" => format!(
            "Workflow widgets are not available yet: no host capability lets an app run a \
             saved workflow. {ALTERNATIVES}"
        ),
        "agent" => format!(
            "Agent widgets are not available on this node: refreshing them needs delegated \
             user execution authority that refresh jobs do not carry. {ALTERNATIVES}"
        ),
        other => format!("'{other}' widgets are not available on this node. {ALTERNATIVES}"),
    }
}

/// Is `id` safe to forward as a single path segment of a provider/tool id?
///
/// The Composio `action` is attacker-controlled widget config that ends up in a
/// provider route downstream. The `url` crate collapses `..` dot-segments and a
/// `?`/`#` terminates the path, so a value like `"../../v1/config?"` could escape
/// the intended prefix and reach an arbitrary route. Restrict to a conservative id
/// charset and forbid the `..` sequence outright (mirrors clips'
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
///
/// A source the host does not support (a widget saved before its kind was
/// withdrawn, or one written by another host) fails with
/// [`unsupported_source_message`] before any host call is made.
///
/// `agent_grant` is the Core-signed grant of the agent the widget runs for (the
/// one that wrote its source, or the one asking now), `None` for a person's
/// widget. Provider calls hand it back to Core, which applies that agent's
/// account access.
pub async fn resolve(
    http: &reqwest::Client,
    host: &dyn DashboardsHost,
    source: &WidgetSource,
    widget_id: &str,
    agent_grant: Option<&str>,
) -> Result<Value> {
    if !host.supports_source(source.source_type()) {
        return Err(anyhow!(unsupported_source_message(source.source_type())));
    }
    match source {
        WidgetSource::Static { data } => Ok(data.clone()),
        // No host can resolve these (see the module docs); `supports_source`
        // already refuses them for every well-behaved host, and this arm keeps a
        // host that over-claims from reaching a Core route it cannot authenticate to.
        WidgetSource::CoreEndpoint { .. }
        | WidgetSource::Monitor { .. }
        | WidgetSource::Workflow { .. } => {
            Err(anyhow!(unsupported_source_message(source.source_type())))
        }
        WidgetSource::Composio { action, args } => {
            resolve_composio(http, host, action, args, agent_grant).await
        }
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

async fn resolve_composio(
    _http: &reqwest::Client,
    host: &dyn DashboardsHost,
    action: &str,
    args: &Value,
    agent_grant: Option<&str>,
) -> Result<Value> {
    if !id_segment_is_safe(action) {
        return Err(anyhow!(
            "invalid composio action '{action}': must be a plain action id (no '/', '..', or query chars)"
        ));
    }
    let response = host
        .call_provider(
            ManagedProviderCall {
                provider: "composio".to_owned(),
                tool_id: action.to_owned(),
                operation: Some("execute".to_owned()),
                account_id: None,
                method: "POST".to_owned(),
                query: Vec::new(),
                body: Some(args.clone()),
                idempotency_key: None,
                request_id: format!("dashboards:composio:{action}:{}", uuid::Uuid::new_v4()),
                fallback_cost_micro_usd: None,
                task_label: Some("Dashboards Composio widget".to_owned()),
                timeout_ms: None,
                max_cost_micro_usd: None,
            },
            agent_grant,
        )
        .await
        .map_err(|error| anyhow!("composio execute failed: {error}"))?;
    if !response.ok {
        return Err(anyhow!(
            "composio execute returned HTTP {}",
            response.status
        ));
    }
    Ok(response.body)
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
    use serde_json::json;
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
        assert_eq!(
            v["text"],
            serde_json::json!("```json\nnot json at all\n```")
        );
    }

    #[tokio::test]
    async fn resolve_static_returns_inline_data() {
        let host = FakeHost::default();
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::Static {
            data: serde_json::json!({ "hi": 1 }),
        };
        let v = resolve(&engine.http, &host, &src, "w1", None)
            .await
            .unwrap();
        assert_eq!(v, serde_json::json!({ "hi": 1 }));
    }

    /// A host that claims every source kind, to prove the resolver itself never
    /// reaches Core for the Core-read kinds even when a host over-claims.
    struct OverclaimingHost(FakeHost);

    #[async_trait::async_trait]
    impl DashboardsHost for OverclaimingHost {
        fn supports_source(&self, _source_type: &str) -> bool {
            true
        }
        async fn call_provider(
            &self,
            call: ManagedProviderCall,
            agent_grant: Option<&str>,
        ) -> Result<ryu_app_events::ManagedProviderResponse, String> {
            self.0.call_provider(call, agent_grant).await
        }
        async fn agent_run(&self, a: &str, c: &str, p: &str) -> Result<String, String> {
            self.0.agent_run(a, c, p).await
        }
        async fn guarded_fetch(
            &self,
            url: &str,
            headers: &[(String, String)],
        ) -> Result<(u16, String), String> {
            self.0.guarded_fetch(url, headers).await
        }
    }

    fn core_read_sources() -> Vec<WidgetSource> {
        vec![
            WidgetSource::CoreEndpoint {
                endpoint: "system_status".into(),
                selector: None,
            },
            WidgetSource::Monitor {
                monitor_id: "m1".into(),
            },
            WidgetSource::Workflow {
                workflow_id: "wf1".into(),
                input: Default::default(),
                output_key: None,
            },
        ]
    }

    #[tokio::test]
    async fn core_read_sources_report_a_clear_unsupported_error() {
        // The default host refuses them up front; an over-claiming host still gets
        // the same error from the resolver arm. Neither makes a host call.
        let engine = engine_with(Arc::new(FakeHost::default()));
        let default_host = FakeHost::default();
        let overclaiming = OverclaimingHost(FakeHost::default());
        for src in core_read_sources() {
            for host in [&default_host as &dyn DashboardsHost, &overclaiming] {
                let err = resolve(&engine.http, host, &src, "w1", None)
                    .await
                    .unwrap_err();
                assert_eq!(
                    err.to_string(),
                    unsupported_source_message(src.source_type())
                );
                assert!(err.to_string().contains("not available yet"), "{err}");
            }
        }
        assert!(default_host.fetch_calls.lock().unwrap().is_empty());
        assert!(overclaiming.0.fetch_calls.lock().unwrap().is_empty());
        assert!(overclaiming.0.provider_calls.lock().unwrap().is_empty());
    }

    #[test]
    fn unsupported_messages_name_the_kind_and_an_alternative() {
        for (kind, needle) in [
            ("core_endpoint", "Core metric"),
            ("monitor", "Monitor"),
            ("workflow", "Workflow"),
            ("agent", "Agent"),
            ("invented", "'invented'"),
        ] {
            let msg = unsupported_source_message(kind);
            assert!(msg.contains(needle), "{kind}: {msg}");
            assert!(msg.contains("http, composio, or static"), "{kind}: {msg}");
        }
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
        let v = resolve(&engine.http, &host, &src, "w1", None)
            .await
            .unwrap();
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
        let err = resolve(&engine.http, &host, &src, "w1", None)
            .await
            .unwrap_err();
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
        let err = resolve(&engine.http, &host, &src, "w1", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("http GET returned 500"));

        // 2xx but not JSON.
        let host2 = FakeHost::default();
        *host2.fetch_reply.lock().unwrap() = Ok((200, "<html>".into()));
        let err = resolve(&engine.http, &host2, &src, "w1", None)
            .await
            .unwrap_err();
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
        let err = resolve(&engine.http, &host, &src, "w1", None)
            .await
            .unwrap_err();
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
        let v = resolve(&engine.http, &host, &src, "widget-7", None)
            .await
            .unwrap();
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
        let err = resolve(&engine.http, &host, &src, "w1", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("agent run failed"));
        assert!(err.to_string().contains("runner offline"));
    }

    #[tokio::test]
    async fn resolve_composio_errors_on_closed_gateway() {
        // The default FakeHost rejects the managed provider call, so the Composio
        // error branch runs with no live server and no external traffic.
        let host = FakeHost::default();
        let engine = engine_with(Arc::new(FakeHost::default()));
        let src = WidgetSource::Composio {
            action: "GITHUB_LIST".into(),
            args: serde_json::json!({ "x": 1 }),
        };
        let err = resolve(&engine.http, &host, &src, "w1", None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("composio execute failed"), "{err}");
        let calls = host.provider_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].provider, "composio");
        assert_eq!(calls[0].tool_id, "GITHUB_LIST");
        assert_eq!(
            calls[0].body.as_ref().expect("arguments"),
            &json!({ "x": 1 })
        );
    }
}
