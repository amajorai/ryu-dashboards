//! The dashboard refresh loop.
//!
//! A single background task ticks every few seconds, walks every widget, and
//! re-resolves the ones whose interval is due, caching + broadcasting the result.
//! This is intentionally a *dashboard-owned* loop, not a `JobTarget` on the
//! minute-granular scheduler — dashboards want second-to-minute cadence.
//!
//! Cost discipline (the money guard): expensive sources (agent / Composio /
//! workflow / arbitrary HTTP) are skipped entirely when no SSE client is watching
//! the dashboard, and every interval is clamped up to the source's per-kind floor
//! (see [`super::WidgetSource::min_interval_secs`]). Cheap internal reads tick
//! freely; the sources that cost tokens/quota do not run against an empty room.

use std::time::Duration;

use super::{sources, DashboardEngine};

/// How often the loop wakes to look for due widgets. The actual per-widget cadence
/// is governed by each widget's effective interval; this is just the resolution.
const TICK_SECS: u64 = 5;

/// Spawn the refresh loop for the given engine. Call once at startup.
pub fn spawn(engine: DashboardEngine) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(TICK_SECS));
        loop {
            tick.tick().await;
            if let Err(e) = run_once(&engine).await {
                tracing::warn!("dashboard refresh tick failed: {e:#}");
            }
        }
    });
}

/// One pass over all widgets: refresh each that is due.
async fn run_once(engine: &DashboardEngine) -> anyhow::Result<()> {
    let widgets = engine.store.list_all_widgets().await?;
    // Money guard keys off LIVE UI VIEWERS (open SSE streams), not the raw broadcast
    // receiver count — internal subscribers (the hardware nudge loop) must not be
    // mistaken for a human watcher and keep expensive sources running 24/7.
    let has_viewer = engine.store.viewer_count() > 0;
    let now = chrono::Utc::now();

    for widget in widgets {
        if !widget.source.is_refreshable() {
            continue;
        }
        // Money guard: don't run expensive sources when nobody is watching.
        if widget.source.is_expensive() && !has_viewer {
            continue;
        }
        if already_reported_unsupported(engine, &widget) {
            continue;
        }
        if !is_due(&widget, now) {
            continue;
        }
        // A widget an agent wrote runs for that agent: its grant goes with every
        // provider call. If it cannot be read, the widget does not run at all
        // rather than run with the user's accounts.
        let result = match engine.store.widget_agent_grant(&widget.id).await {
            Ok(agent_grant) => sources::resolve(
                &engine.http,
                engine.host.as_ref(),
                &widget.source,
                &widget.id,
                agent_grant.as_deref(),
            )
            .await
            .map_err(|e| e.to_string()),
            Err(error) => Err(format!("the widget's agent could not be read: {error:#}")),
        };
        if let Err(e) = engine.store.update_widget_value(&widget.id, result).await {
            tracing::warn!("dashboard: caching widget '{}' failed: {e:#}", widget.id);
        }
    }
    Ok(())
}

/// Whether `widget`'s source is one the host cannot resolve AND the widget already
/// shows that error. Such a widget can never recover by retrying (only an edit to
/// its source can), so after recording the error once the loop leaves it alone
/// instead of rewriting the same failure every interval.
fn already_reported_unsupported(engine: &DashboardEngine, widget: &super::Widget) -> bool {
    let source_type = widget.source.source_type();
    !engine.host.supports_source(source_type)
        && widget.last_error.as_deref()
            == Some(sources::unsupported_source_message(source_type).as_str())
}

/// Whether a widget's refresh interval has elapsed since its last refresh. A
/// widget that has never refreshed (no `last_refresh_at`) is always due.
fn is_due(widget: &super::Widget, now: chrono::DateTime<chrono::Utc>) -> bool {
    let interval = widget.effective_interval_secs();
    match widget
        .last_refresh_at
        .as_deref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
    {
        Some(last) => {
            let elapsed = now.signed_duration_since(last.with_timezone(&chrono::Utc));
            elapsed.num_seconds() >= interval as i64
        }
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{test_engine, FakeHost};
    use crate::{GridLayout, Widget, WidgetKind, WidgetSource};
    use serde_json::Value;
    use std::sync::Arc;

    fn widget(id: &str, source: WidgetSource, last_refresh_at: Option<String>) -> Widget {
        Widget {
            id: id.into(),
            dashboard_id: "d1".into(),
            kind: WidgetKind::Stat,
            title: "t".into(),
            config: Value::Null,
            source,
            refresh_interval: None,
            layout: GridLayout::default(),
            canvas: None,
            last_value: None,
            last_refresh_at,
            last_error: None,
        }
    }

    #[test]
    fn never_refreshed_widget_is_always_due() {
        let w = widget(
            "w",
            WidgetSource::CoreEndpoint {
                endpoint: "agents".into(),
                selector: None,
            },
            None,
        );
        assert!(is_due(&w, chrono::Utc::now()));
    }

    #[test]
    fn recently_refreshed_widget_is_not_due() {
        // Cheap source default interval is 15s; refreshed 1s ago ⇒ not due.
        let now = chrono::Utc::now();
        let last = (now - chrono::Duration::seconds(1)).to_rfc3339();
        let w = widget(
            "w",
            WidgetSource::CoreEndpoint {
                endpoint: "agents".into(),
                selector: None,
            },
            Some(last),
        );
        assert!(!is_due(&w, now));
    }

    #[test]
    fn elapsed_widget_is_due_again() {
        let now = chrono::Utc::now();
        let last = (now - chrono::Duration::seconds(999)).to_rfc3339();
        let w = widget(
            "w",
            WidgetSource::CoreEndpoint {
                endpoint: "agents".into(),
                selector: None,
            },
            Some(last),
        );
        assert!(is_due(&w, now));
    }

    #[test]
    fn unparseable_last_refresh_is_treated_as_never() {
        let w = widget(
            "w",
            WidgetSource::CoreEndpoint {
                endpoint: "agents".into(),
                selector: None,
            },
            Some("not-a-timestamp".into()),
        );
        assert!(is_due(&w, chrono::Utc::now()));
    }

    #[tokio::test]
    async fn run_once_reports_an_unsupported_source_once_then_leaves_it() {
        let engine = test_engine();
        let w = widget(
            "legacy",
            WidgetSource::Monitor {
                monitor_id: "m1".into(),
            },
            None,
        );
        engine.store.upsert_widget(&w).await.unwrap();
        run_once(&engine).await.unwrap();
        let first = engine.store.get_widget("legacy").await.unwrap().unwrap();
        assert_eq!(
            first.last_error.as_deref(),
            Some(sources::unsupported_source_message("monitor").as_str())
        );
        let stamped = first
            .last_refresh_at
            .clone()
            .expect("first pass records it");

        // Even once its interval has elapsed, the loop does not rewrite it.
        let mut aged = first.clone();
        aged.last_refresh_at =
            Some((chrono::Utc::now() - chrono::Duration::seconds(999)).to_rfc3339());
        engine.store.upsert_widget(&aged).await.unwrap();
        run_once(&engine).await.unwrap();
        let second = engine.store.get_widget("legacy").await.unwrap().unwrap();
        assert_eq!(second.last_refresh_at, aged.last_refresh_at);
        assert_ne!(second.last_refresh_at.as_deref(), Some(stamped.as_str()));
    }

    #[tokio::test]
    async fn run_once_skips_static_and_caches_nothing() {
        let engine = test_engine();
        let mut w = widget("w", WidgetSource::Static { data: Value::Null }, None);
        w.id = "w".into();
        engine.store.upsert_widget(&w).await.unwrap();
        run_once(&engine).await.unwrap();
        // Static is not refreshable ⇒ never resolved, no last_refresh stamped.
        let stored = engine.store.get_widget("w").await.unwrap().unwrap();
        assert!(stored.last_refresh_at.is_none());
    }

    #[tokio::test]
    async fn run_once_skips_expensive_source_without_viewer() {
        // An HTTP (expensive) widget must NOT run when nobody is watching (money guard).
        let engine = test_engine();
        let w = widget(
            "w",
            WidgetSource::Http {
                url: "https://x".into(),
                selector: None,
                headers: Default::default(),
            },
            None,
        );
        engine.store.upsert_widget(&w).await.unwrap();
        // No viewer_guard held ⇒ viewer_count == 0 ⇒ expensive source skipped.
        run_once(&engine).await.unwrap();
        let stored = engine.store.get_widget("w").await.unwrap().unwrap();
        assert!(
            stored.last_refresh_at.is_none(),
            "expensive source must be skipped with no viewer"
        );
    }

    #[tokio::test]
    async fn run_once_resolves_expensive_source_with_viewer() {
        let host = Arc::new(FakeHost::default());
        *host.fetch_reply.lock().unwrap() = Ok((200, r#"{"n":3}"#.into()));
        let engine = crate::testutil::engine_with(host);
        let w = widget(
            "w",
            WidgetSource::Http {
                url: "https://x".into(),
                selector: Some("n".into()),
                headers: Default::default(),
            },
            None,
        );
        engine.store.upsert_widget(&w).await.unwrap();
        // Hold a viewer guard so the money guard lets the expensive source run.
        let _guard = engine.store.viewer_guard();
        run_once(&engine).await.unwrap();
        let stored = engine.store.get_widget("w").await.unwrap().unwrap();
        assert_eq!(stored.last_value, Some(serde_json::json!(3)));
        assert!(stored.last_refresh_at.is_some());
        assert!(stored.last_error.is_none());
    }

    /// The refresh loop runs an agent's widget for that agent: the grant stored
    /// with the widget goes with its provider call. A person's widget sends none.
    #[tokio::test]
    async fn run_once_sends_the_widget_agents_grant() {
        let host = Arc::new(FakeHost::default());
        let engine = crate::testutil::engine_with(host.clone());
        let source = || WidgetSource::Composio {
            action: "GMAIL_LIST".into(),
            args: serde_json::Value::Null,
        };
        engine
            .store
            .upsert_widget_for(&widget("agents", source(), None), Some("grant.agent"))
            .await
            .unwrap();
        engine
            .store
            .upsert_widget(&widget("persons", source(), None))
            .await
            .unwrap();
        let _guard = engine.store.viewer_guard();
        run_once(&engine).await.unwrap();

        let mut grants = host.provider_grants.lock().unwrap().clone();
        grants.sort();
        assert_eq!(grants, vec![None, Some("grant.agent".to_owned())]);
    }

    #[tokio::test]
    async fn run_once_caches_error_from_failed_source() {
        let host = Arc::new(FakeHost::default());
        *host.fetch_reply.lock().unwrap() = Err("guard blocked".into());
        let engine = crate::testutil::engine_with(host);
        let w = widget(
            "w",
            WidgetSource::Http {
                url: "https://x".into(),
                selector: None,
                headers: Default::default(),
            },
            None,
        );
        engine.store.upsert_widget(&w).await.unwrap();
        let _guard = engine.store.viewer_guard();
        run_once(&engine).await.unwrap();
        let stored = engine.store.get_widget("w").await.unwrap().unwrap();
        assert!(stored
            .last_error
            .as_deref()
            .unwrap()
            .contains("guard blocked"));
        assert!(stored.last_value.is_none());
    }
}
