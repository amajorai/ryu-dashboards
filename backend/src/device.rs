//! Device-dashboard rendering: the single source of truth for the hardware
//! display surface (TRMNL model, `apps/hardware/DASHBOARD.md`).
//!
//! This module used to live inside `ryu_hardware::api` (welding the kernel
//! hardware crate to `ryu_dashboards`). It moved here so BOTH consumers share one
//! implementation with zero drift:
//!
//! - **Core, in-process** (`DashboardEngine` on hand) — the transitional
//!   `InProcDashboardFeed` calls these `fn`s directly.
//! - **The `ryu-dashboards` sidecar** — its internal `/api/dashboards/device/*`
//!   endpoints call these same `fn`s, so a decoupled node renders identically.
//!
//! The device *auth* (per-device Bearer verification against the hardware
//! registry) stays Core-side in `ryu_hardware` — only the dashboard *data +
//! render* live here. The panel geometry is derived from the device class +
//! saved prefs (`device_type` + `prefs`), passed as plain values so this module
//! never depends on the hardware crate's `DeviceType`/`DeviceRecord` types.

use serde_json::{json, Value};

use crate::render::{self, DeviceProfile, Palette};
use crate::{Dashboard, DashboardEngine, DeviceDashboard};

/// Refresh-rate floor (seconds) so a device can't be told to hammer the node.
/// Byte-identical to the old `ryu_hardware::api::MIN_REFRESH_RATE`.
const MIN_REFRESH_RATE: u32 = 30;
/// Default device dashboard poll interval (seconds) when none is set.
const DEFAULT_REFRESH_RATE: u32 = 300;

/// Resolve a device's [`DeviceProfile`] from its class + saved prefs. The class
/// picks a sensible default panel (desk = 800×480 1-bit e-ink; watch = 240×240
/// colour LCD; necklace has no display); `prefs.screen` may override any field so
/// a different panel revision works without a code change. Nothing is hardcoded
/// past the class default.
///
/// `device_type` is the wire string the hardware protocol uses (`"desk"` /
/// `"watch"` / `"necklace"`); an unknown value falls back to the e-ink default so
/// the endpoint still produces something deterministic.
pub fn profile_for_device(device_type: &str, prefs: &Value) -> DeviceProfile {
    let mut profile = match device_type {
        "watch" => DeviceProfile::watch_lcd(),
        // Desk + no-display necklace both fall back to the e-ink default.
        _ => DeviceProfile::desk_eink(),
    };
    if let Some(screen) = prefs.get("screen").filter(|v| v.is_object()) {
        if let Some(w) = screen.get("w").and_then(Value::as_u64) {
            profile.w = w as u32;
        }
        if let Some(h) = screen.get("h").and_then(Value::as_u64) {
            profile.h = h as u32;
        }
        if let Some(bd) = screen.get("bit_depth").and_then(Value::as_u64) {
            profile.bit_depth = bd as u8;
        }
        if let Some(rot) = screen.get("rotation").and_then(Value::as_u64) {
            profile.rotation = rot as u16;
        }
        if let Some(p) = screen.get("palette").and_then(Value::as_str) {
            profile.palette = match p {
                "rgb565" => Palette::Rgb565,
                "rgba" => Palette::Rgba,
                _ => Palette::Mono,
            };
        }
    }
    profile
}

/// The `screen` object echoed in the display manifest + device config, kept
/// byte-identical across both callers.
pub fn screen_json(profile: &DeviceProfile) -> Value {
    json!({
        "w": profile.w,
        "h": profile.h,
        "bit_depth": profile.bit_depth,
        "palette": profile.palette.as_str(),
        "rotation": profile.rotation,
    })
}

/// Ensure the device has a bound dashboard, creating an empty one on first use so
/// every device always has a real, builder-editable surface. Returns the binding.
pub async fn ensure_device_dashboard(
    engine: &DashboardEngine,
    device_id: &str,
    device_name: &str,
) -> anyhow::Result<DeviceDashboard> {
    if let Some(dd) = engine.store.get_device_dashboard(device_id).await? {
        // The bound dashboard could have been deleted out from under us; recreate.
        if engine
            .store
            .get_dashboard(&dd.dashboard_id)
            .await?
            .is_some()
        {
            return Ok(dd);
        }
    }
    let now = chrono::Utc::now().to_rfc3339();
    let dashboard = Dashboard {
        id: format!("dash_{}", uuid::Uuid::new_v4().simple()),
        name: format!("{device_name} display"),
        created_at: now.clone(),
        updated_at: now.clone(),
        view_mode: None,
    };
    engine.store.upsert_dashboard(&dashboard).await?;
    let dd = DeviceDashboard {
        device_id: device_id.to_string(),
        dashboard_id: dashboard.id,
        refresh_rate: DEFAULT_REFRESH_RATE,
        created_at: now.clone(),
        updated_at: now,
    };
    engine.store.upsert_device_dashboard(&dd).await?;
    Ok(dd)
}

/// Render a device's current dashboard to its panel encoding.
pub async fn render_device(
    engine: &DashboardEngine,
    device_id: &str,
    device_name: &str,
    device_type: &str,
    prefs: &Value,
) -> anyhow::Result<(render::RenderedImage, DeviceDashboard)> {
    let dd = ensure_device_dashboard(engine, device_id, device_name).await?;
    let widgets = engine
        .store
        .list_widgets(&dd.dashboard_id)
        .await
        .unwrap_or_default();
    let profile = profile_for_device(device_type, prefs);
    let image = render::render(&widgets, profile)?;
    Ok((image, dd))
}

/// Build the device-dashboard config JSON (the binding + the bound dashboard's
/// widgets + the screen geometry). Byte-identical to the old
/// `ryu_hardware::api::get_device_dashboard` body.
pub async fn device_config_json(
    engine: &DashboardEngine,
    device_id: &str,
    device_name: &str,
    device_type: &str,
    prefs: &Value,
) -> anyhow::Result<Value> {
    let dd = ensure_device_dashboard(engine, device_id, device_name).await?;
    let widgets = engine
        .store
        .list_widgets(&dd.dashboard_id)
        .await
        .unwrap_or_default();
    let profile = profile_for_device(device_type, prefs);
    Ok(json!({
        "device_id": dd.device_id,
        "dashboard_id": dd.dashboard_id,
        "refresh_rate": dd.refresh_rate,
        "screen": screen_json(&profile),
        "widgets": widgets,
    }))
}

/// Apply a device-dashboard update: set the poll interval and/or replace the
/// widget selection. Returns `(dashboard_id, effective_refresh_rate)`. Mirrors the
/// old `ryu_hardware::api::set_device_dashboard` mutation (the caller owns the
/// live-device nudge, which stays hardware-side).
pub async fn set_device_config(
    engine: &DashboardEngine,
    device_id: &str,
    device_name: &str,
    refresh_rate: Option<u32>,
    widgets: Option<Value>,
) -> anyhow::Result<(String, u32)> {
    let mut dd = ensure_device_dashboard(engine, device_id, device_name).await?;

    if let Some(rate) = refresh_rate {
        dd.refresh_rate = rate.max(MIN_REFRESH_RATE);
        dd.updated_at = chrono::Utc::now().to_rfc3339();
        engine.store.upsert_device_dashboard(&dd).await?;
    }

    if let Some(widgets) = widgets {
        crate::replace_widgets(engine, &dd.dashboard_id, &widgets).await?;
    }

    Ok((dd.dashboard_id, dd.refresh_rate))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::Palette;
    use crate::testutil::test_engine;

    #[test]
    fn profile_watch_vs_desk_defaults() {
        let watch = profile_for_device("watch", &Value::Null);
        assert_eq!((watch.w, watch.h), (240, 240));
        assert_eq!(watch.palette, Palette::Rgba);
        // Desk + unknown both fall back to the e-ink default.
        for ty in ["desk", "necklace", "totally-unknown", ""] {
            let p = profile_for_device(ty, &Value::Null);
            assert_eq!((p.w, p.h, p.bit_depth), (800, 480, 1), "type {ty}");
            assert_eq!(p.palette, Palette::Mono);
        }
    }

    #[test]
    fn profile_prefs_override_every_screen_field() {
        let prefs = json!({
            "screen": {
                "w": 128,
                "h": 64,
                "bit_depth": 16,
                "rotation": 90,
                "palette": "rgb565"
            }
        });
        let p = profile_for_device("desk", &prefs);
        assert_eq!((p.w, p.h, p.bit_depth, p.rotation), (128, 64, 16, 90));
        assert_eq!(p.palette, Palette::Rgb565);

        // "rgba" maps to Rgba; anything else maps to Mono.
        let p2 = profile_for_device("desk", &json!({ "screen": { "palette": "rgba" } }));
        assert_eq!(p2.palette, Palette::Rgba);
        let p3 = profile_for_device("desk", &json!({ "screen": { "palette": "weird" } }));
        assert_eq!(p3.palette, Palette::Mono);
    }

    #[test]
    fn profile_ignores_non_object_screen() {
        // A non-object `screen` is ignored (the class default stands).
        let p = profile_for_device("watch", &json!({ "screen": "nope" }));
        assert_eq!((p.w, p.h), (240, 240));
    }

    #[test]
    fn screen_json_echoes_profile() {
        let p = DeviceProfile::watch_lcd();
        let j = screen_json(&p);
        assert_eq!(j["w"], json!(240));
        assert_eq!(j["h"], json!(240));
        assert_eq!(j["palette"], json!("rgba"));
        assert_eq!(j["bit_depth"], json!(24));
        assert_eq!(j["rotation"], json!(0));
    }

    #[tokio::test]
    async fn ensure_creates_then_returns_same_binding() {
        let engine = test_engine();
        let dd = ensure_device_dashboard(&engine, "rhw_1", "Desk")
            .await
            .unwrap();
        assert_eq!(dd.device_id, "rhw_1");
        assert_eq!(dd.refresh_rate, DEFAULT_REFRESH_RATE);
        // The bound dashboard was actually created + named after the device.
        let dash = engine
            .store
            .get_dashboard(&dd.dashboard_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dash.name, "Desk display");
        // A second call returns the SAME binding (idempotent).
        let dd2 = ensure_device_dashboard(&engine, "rhw_1", "Desk")
            .await
            .unwrap();
        assert_eq!(dd2.dashboard_id, dd.dashboard_id);
    }

    #[tokio::test]
    async fn ensure_recreates_when_bound_dashboard_deleted() {
        let engine = test_engine();
        let dd = ensure_device_dashboard(&engine, "rhw_1", "Desk")
            .await
            .unwrap();
        // Delete the bound dashboard out from under the device.
        engine.store.delete_dashboard(&dd.dashboard_id).await.unwrap();
        // Ensure must rebuild a fresh dashboard rather than return a dangling id.
        let dd2 = ensure_device_dashboard(&engine, "rhw_1", "Desk")
            .await
            .unwrap();
        assert_ne!(dd2.dashboard_id, dd.dashboard_id);
        assert!(engine
            .store
            .get_dashboard(&dd2.dashboard_id)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn device_config_json_has_binding_widgets_and_screen() {
        let engine = test_engine();
        let cfg = device_config_json(&engine, "rhw_1", "Desk", "desk", &Value::Null)
            .await
            .unwrap();
        assert_eq!(cfg["device_id"], json!("rhw_1"));
        assert!(cfg["dashboard_id"].is_string());
        assert_eq!(cfg["refresh_rate"], json!(DEFAULT_REFRESH_RATE));
        assert_eq!(cfg["screen"]["w"], json!(800));
        assert!(cfg["widgets"].is_array());
    }

    #[tokio::test]
    async fn render_device_produces_eink_bytes() {
        let engine = test_engine();
        let (image, dd) = render_device(&engine, "rhw_1", "Desk", "desk", &Value::Null)
            .await
            .unwrap();
        assert_eq!(image.content_type, "application/octet-stream");
        assert_eq!(image.bytes.len(), 100 * 480); // 800×480 mono packed.
        assert_eq!(dd.device_id, "rhw_1");
    }

    #[tokio::test]
    async fn set_device_config_clamps_refresh_rate_to_floor() {
        let engine = test_engine();
        // Ask for a hammering 5s rate; it clamps up to MIN_REFRESH_RATE (30s).
        let (dashboard_id, rate) =
            set_device_config(&engine, "rhw_1", "Desk", Some(5), None)
                .await
                .unwrap();
        assert_eq!(rate, MIN_REFRESH_RATE);
        let dd = engine
            .store
            .get_device_dashboard("rhw_1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(dd.refresh_rate, MIN_REFRESH_RATE);
        assert_eq!(dd.dashboard_id, dashboard_id);
    }

    #[tokio::test]
    async fn set_device_config_replaces_widgets() {
        let engine = test_engine();
        let widgets = json!([
            { "kind": "stat", "source": { "type": "static", "data": 1 } },
            { "kind": "text", "source": { "type": "static", "data": "hi" } }
        ]);
        let (dashboard_id, _rate) =
            set_device_config(&engine, "rhw_1", "Desk", None, Some(widgets))
                .await
                .unwrap();
        let stored = engine.store.list_widgets(&dashboard_id).await.unwrap();
        assert_eq!(stored.len(), 2);
    }

    #[tokio::test]
    async fn set_device_config_rejects_bad_widget_batch() {
        let engine = test_engine();
        let bad = json!([
            { "kind": "stat", "source": { "type": "core_endpoint", "endpoint": "secrets" } }
        ]);
        let err = set_device_config(&engine, "rhw_1", "Desk", None, Some(bad))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not an allowed core_endpoint"));
    }
}
