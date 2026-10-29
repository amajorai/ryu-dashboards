//! SQLite-backed persistence for dashboards + widgets.
//!
//! Two tables live in `<data dir>/plugin-data/@ryu+dashboards/dashboards.db`:
//!   - `dashboards` — the dashboard identities (id + name).
//!   - `widgets`    — the widgets, keyed by `dashboard_id`, each stored as JSON
//!     (kind, config, source, layout, and the cached last value/refresh/error).
//!
//! HTTP-widget header VALUES (API keys, bearer tokens) are sealed inside
//! `widgets.json` (`encp:v1:`, ChaCha20-Poly1305 under the `@ryu/dashboards`
//! plugin subkey Core delivers at spawn). The store seals on every write and
//! opens on every read, so the refresh loop gets plaintext headers in memory and
//! nothing else changes. Rows from an older version are resealed when the store
//! opens with a key; without a key, storing a header value fails closed.
//!
//! A broadcast channel fans freshly-changed widgets out to SSE subscribers (the
//! desktop Home grid), mirroring `ryu_quests`. The refresh loop calls
//! [`DashboardStore::update_widget_value`] to cache + broadcast a new value, and
//! gates expensive refreshes on [`DashboardStore::receiver_count`] (no live
//! viewer ⇒ skip).

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use ryu_app_events::EventEmitter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

use ryu_crypto::{PluginCipher, SecretSealer as _};

use super::{
    CanvasLayout, Dashboard, DashboardEvent, DeviceDashboard, GridLayout, Widget, WidgetSource,
    DASHBOARDS_PLUGIN_ID, EVENT_WIDGET_FAILED, EVENT_WIDGET_RECOVERED,
};

/// RAII guard counting one **live UI viewer** (an open SSE stream). The refresh
/// loop's money guard keys off [`DashboardStore::viewer_count`], NOT the broadcast
/// `receiver_count`, so that internal subscribers (the hardware nudge loop) don't
/// trip the guard into thinking a human is watching and burn quota 24/7. Held by
/// the SSE handler for the life of the stream; decrements on drop.
pub struct ViewerGuard {
    viewers: Arc<AtomicUsize>,
}

impl Drop for ViewerGuard {
    fn drop(&mut self) {
        self.viewers.fetch_sub(1, Ordering::SeqCst);
    }
}

/// SQLite-backed dashboard store. Cheap to clone (wraps `Arc`s).
#[derive(Clone)]
pub struct DashboardStore {
    conn: Arc<Mutex<Connection>>,
    tx: broadcast::Sender<DashboardEvent>,
    /// Count of live UI viewers (open SSE streams). Distinct from the broadcast
    /// receiver count so an internal subscriber (the nudge loop) can listen without
    /// faking a viewer and defeating the refresh loop's cost guard.
    viewers: Arc<AtomicUsize>,
    /// Raises the health-transition app events. NOT a constructor parameter: the
    /// emitter reads Core's address + this sidecar's token from the environment and
    /// is inert when neither is present, so threading it through [`Self::open`]'s
    /// callers would buy nothing. The broadcast `tx` above reaches *this node's own
    /// UI*; this reaches other plugins and workflows.
    events: EventEmitter,
    /// Seals HTTP-widget header values and agent grants. `None` when the process
    /// has no key.
    sealer: Option<PluginCipher>,
}

/// A fixed-key cipher for tests (never resolves a process key).
#[doc(hidden)]
pub fn test_plugin_cipher(byte: u8) -> PluginCipher {
    use base64::Engine as _;
    let subkey = base64::engine::general_purpose::STANDARD.encode([byte; 32]);
    PluginCipher::from_subkey_b64(DASHBOARDS_PLUGIN_ID, &subkey).expect("test subkey")
}

impl DashboardStore {
    /// Open (or create) the store at a specific path and run migrations. The
    /// sidecar passes `paths::dashboards_db_path()` (the plugin data dir is the
    /// host's, not this crate's, concern).
    pub fn open(path: PathBuf) -> Result<Self> {
        Self::open_with_sealer(path, None)
    }

    /// [`Self::open`] with the header sealer. With a key, plaintext header
    /// values an older version stored are resealed here.
    pub fn open_with_sealer(path: PathBuf, sealer: Option<PluginCipher>) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating db dir {}", parent.display()))?;
        }
        let conn = Connection::open(&path)
            .with_context(|| format!("opening dashboards db {}", path.display()))?;
        Self::init_schema(&conn)?;
        if let Some(sealer) = &sealer {
            Self::reseal_plaintext_headers(&conn, sealer)?;
            Self::reseal_plaintext_grants(&conn, sealer)?;
        }
        let (tx, _rx) = broadcast::channel(256);
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            tx,
            viewers: Arc::new(AtomicUsize::new(0)),
            events: EventEmitter::from_env(DASHBOARDS_PLUGIN_ID),
            sealer,
        })
    }

    /// Seal every plaintext header value left by an older version, in one
    /// transaction (idempotent).
    fn reseal_plaintext_headers(conn: &Connection, sealer: &PluginCipher) -> Result<()> {
        let rows = {
            let mut stmt = conn.prepare("SELECT id, json FROM widgets")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        let tx = conn.unchecked_transaction()?;
        for (id, json) in rows {
            let Ok(mut widget) = serde_json::from_str::<Widget>(&json) else {
                continue;
            };
            let WidgetSource::Http { headers, .. } = &mut widget.source else {
                continue;
            };
            let mut changed = false;
            for value in headers.values_mut() {
                if let Some(sealed) = sealer.reseal_if_plaintext(value)? {
                    *value = sealed;
                    changed = true;
                }
            }
            if changed {
                let json = serde_json::to_string(&widget).context("serializing widget")?;
                tx.execute(
                    "UPDATE widgets SET json = ?2 WHERE id = ?1",
                    params![id, json],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Seal every plaintext agent grant left by an older version, in one
    /// transaction (idempotent).
    fn reseal_plaintext_grants(conn: &Connection, sealer: &PluginCipher) -> Result<()> {
        let rows = {
            let mut stmt =
                conn.prepare("SELECT id, agent_grant FROM widgets WHERE agent_grant IS NOT NULL")?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };
        let tx = conn.unchecked_transaction()?;
        for (id, grant) in rows {
            if let Some(sealed) = sealer.reseal_if_plaintext(&grant)? {
                tx.execute(
                    "UPDATE widgets SET agent_grant = ?2 WHERE id = ?1",
                    params![id, sealed],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// The stored JSON of a widget, header values sealed. Fails closed when a
    /// header value is set and there is no key.
    fn encode_widget(&self, widget: &Widget) -> Result<String> {
        let WidgetSource::Http { headers, .. } = &widget.source else {
            return serde_json::to_string(widget).context("serializing widget");
        };
        if headers.is_empty() {
            return serde_json::to_string(widget).context("serializing widget");
        }
        let mut stored = widget.clone();
        if let WidgetSource::Http { headers, .. } = &mut stored.source {
            for value in headers.values_mut() {
                if value.is_empty() || PluginCipher::is_sealed(value) {
                    continue;
                }
                let sealer = self.sealer.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "ryu-dashboards has no sealing key, so it cannot store a widget header \
                         value; run it from Ryu Core or set RYU_MASTER_KEY"
                    )
                })?;
                *value = sealer.seal(value)?;
            }
        }
        serde_json::to_string(&stored).context("serializing widget")
    }

    /// Parse a stored widget and open its sealed header values. A sealed value
    /// without a key (or with another key) is an error, never an empty header.
    fn decode_widget(&self, json: &str) -> Result<Widget> {
        let mut widget: Widget = serde_json::from_str(json).context("deserializing widget")?;
        if let WidgetSource::Http { headers, .. } = &mut widget.source {
            for value in headers.values_mut() {
                if !ryu_crypto::looks_sealed(value) {
                    continue;
                }
                let sealer = self.sealer.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "a stored widget header is sealed but ryu-dashboards has no sealing key"
                    )
                })?;
                *value = sealer.open(value)?;
            }
        }
        Ok(widget)
    }

    /// Decode a listed row: a row that does not parse is skipped (as before); a
    /// sealed header that cannot be opened is an error.
    fn decode_listed(&self, json: &str) -> Result<Option<Widget>> {
        if serde_json::from_str::<Widget>(json).is_err() {
            return Ok(None);
        }
        self.decode_widget(json).map(Some)
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA secure_delete = ON;
             CREATE TABLE IF NOT EXISTS dashboards (
                 id          TEXT PRIMARY KEY,
                 json        TEXT NOT NULL,
                 created_at  TEXT NOT NULL,
                 updated_at  TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS widgets (
                 id           TEXT PRIMARY KEY,
                 dashboard_id TEXT NOT NULL,
                 json         TEXT NOT NULL,
                 created_at   TEXT NOT NULL,
                 updated_at   TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_widgets_dashboard
                 ON widgets(dashboard_id);
             CREATE TABLE IF NOT EXISTS hardware_dashboards (
                 device_id    TEXT PRIMARY KEY,
                 dashboard_id TEXT NOT NULL,
                 refresh_rate INTEGER NOT NULL DEFAULT 300,
                 created_at   TEXT NOT NULL,
                 updated_at   TEXT NOT NULL
             );",
        )
        .context("initializing dashboards schema")?;
        // The Core-signed grant of the agent that last wrote a widget's source,
        // kept beside (never inside) the widget JSON so no API response carries
        // it. Added in place, so an existing database keeps its widgets.
        let has_agent_grant = conn
            .prepare("SELECT 1 FROM pragma_table_info('widgets') WHERE name = 'agent_grant'")?
            .exists([])?;
        if !has_agent_grant {
            conn.execute_batch("ALTER TABLE widgets ADD COLUMN agent_grant TEXT;")
                .context("adding the widget agent grant column")?;
        }
        Ok(())
    }

    // ---- device dashboards (hardware-scoped) ------------------------------

    /// Fetch the per-device dashboard binding, if one exists.
    pub async fn get_device_dashboard(&self, device_id: &str) -> Result<Option<DeviceDashboard>> {
        let conn = self.conn.lock().await;
        conn.query_row(
            "SELECT device_id, dashboard_id, refresh_rate, created_at, updated_at
             FROM hardware_dashboards WHERE device_id = ?1",
            params![device_id],
            |row| {
                Ok(DeviceDashboard {
                    device_id: row.get(0)?,
                    dashboard_id: row.get(1)?,
                    refresh_rate: row.get::<_, i64>(2)? as u32,
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                })
            },
        )
        .optional()
        .context("reading device dashboard")
    }

    /// Insert or replace a device's dashboard binding.
    pub async fn upsert_device_dashboard(&self, dd: &DeviceDashboard) -> Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO hardware_dashboards (device_id, dashboard_id, refresh_rate, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(device_id) DO UPDATE SET
                 dashboard_id = ?2, refresh_rate = ?3, updated_at = ?5",
            params![
                dd.device_id,
                dd.dashboard_id,
                dd.refresh_rate as i64,
                dd.created_at,
                dd.updated_at,
            ],
        )
        .context("upserting device dashboard")?;
        Ok(())
    }

    /// List every device→dashboard binding (the nudge loop's work list).
    pub async fn list_device_dashboards(&self) -> Result<Vec<DeviceDashboard>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT device_id, dashboard_id, refresh_rate, created_at, updated_at
             FROM hardware_dashboards",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(DeviceDashboard {
                device_id: row.get(0)?,
                dashboard_id: row.get(1)?,
                refresh_rate: row.get::<_, i64>(2)? as u32,
                created_at: row.get(3)?,
                updated_at: row.get(4)?,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Remove a device's dashboard binding (on device revoke). Returns true when
    /// a row was removed.
    pub async fn delete_device_dashboard(&self, device_id: &str) -> Result<bool> {
        let conn = self.conn.lock().await;
        let n = conn.execute(
            "DELETE FROM hardware_dashboards WHERE device_id = ?1",
            params![device_id],
        )?;
        Ok(n > 0)
    }

    // ---- dashboards -------------------------------------------------------

    /// Insert or replace a dashboard, then broadcast a `DashboardUpdated` event.
    pub async fn upsert_dashboard(&self, dashboard: &Dashboard) -> Result<()> {
        let json = serde_json::to_string(dashboard).context("serializing dashboard")?;
        {
            let conn = self.conn.lock().await;
            conn.execute(
                "INSERT INTO dashboards (id, json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(id) DO UPDATE SET json = ?2, updated_at = ?4",
                params![
                    dashboard.id,
                    json,
                    dashboard.created_at,
                    dashboard.updated_at
                ],
            )
            .context("upserting dashboard")?;
        }
        self.broadcast(DashboardEvent::DashboardUpdated {
            dashboard_id: dashboard.id.clone(),
        });
        Ok(())
    }

    /// Fetch a dashboard by id.
    pub async fn get_dashboard(&self, id: &str) -> Result<Option<Dashboard>> {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT json FROM dashboards WHERE id = ?1",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading dashboard")?;
        match json {
            Some(j) => Ok(Some(
                serde_json::from_str(&j).context("deserializing dashboard")?,
            )),
            None => Ok(None),
        }
    }

    /// List all dashboards, oldest first (creation order is the tab order).
    pub async fn list_dashboards(&self) -> Result<Vec<Dashboard>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT json FROM dashboards ORDER BY created_at ASC")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            if let Ok(d) = serde_json::from_str::<Dashboard>(&row?) {
                out.push(d);
            }
        }
        Ok(out)
    }

    /// Delete a dashboard and all its widgets. Returns true when removed.
    pub async fn delete_dashboard(&self, id: &str) -> Result<bool> {
        let removed = {
            let conn = self.conn.lock().await;
            let n = conn.execute("DELETE FROM dashboards WHERE id = ?1", params![id])?;
            conn.execute("DELETE FROM widgets WHERE dashboard_id = ?1", params![id])?;
            n > 0
        };
        if removed {
            self.broadcast(DashboardEvent::DashboardUpdated {
                dashboard_id: id.to_string(),
            });
        }
        Ok(removed)
    }

    // ---- widgets ----------------------------------------------------------

    /// Insert or replace a widget, then broadcast a `WidgetUpdated` event. The
    /// widget's recorded agent (see [`Self::upsert_widget_for`]) is kept; a new
    /// widget has none.
    pub async fn upsert_widget(&self, widget: &Widget) -> Result<()> {
        self.write_widget(widget, None).await
    }

    /// [`Self::upsert_widget`] that also records who the widget's source runs
    /// for: `Some(grant)` is the Core-signed grant of the agent writing it,
    /// `None` a person. A refresh hands it back to Core on every provider call,
    /// so the agent's current account access applies to the widget.
    pub async fn upsert_widget_for(
        &self,
        widget: &Widget,
        agent_grant: Option<&str>,
    ) -> Result<()> {
        self.write_widget(widget, Some(agent_grant)).await
    }

    /// The grant of the agent recorded for a widget, if one is.
    pub async fn widget_agent_grant(&self, id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().await;
        let grant = conn
            .query_row(
                "SELECT agent_grant FROM widgets WHERE id = ?1",
                params![id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .context("reading widget agent grant")?;
        grant.flatten().map(|g| self.open_grant(&g)).transpose()
    }

    /// Seal a widget's agent grant (a Core-signed credential) for storage. Fails
    /// closed without a key, like a header value.
    fn seal_grant(&self, grant: &str) -> Result<String> {
        let sealer = self.sealer.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "ryu-dashboards has no sealing key, so it cannot store an agent's grant; \
                 run it from Ryu Core or set RYU_MASTER_KEY"
            )
        })?;
        sealer.seal(grant)
    }

    /// Open a stored agent grant. A sealed grant without a key (or with another
    /// key) is an error, never "no agent" (which would widen what the widget may use).
    fn open_grant(&self, stored: &str) -> Result<String> {
        if !ryu_crypto::looks_sealed(stored) {
            return Ok(stored.to_owned());
        }
        let sealer = self.sealer.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "a stored widget agent grant is sealed but ryu-dashboards has no sealing key"
            )
        })?;
        sealer.open(stored)
    }

    /// `agent_grant`: `None` keeps the recorded agent, `Some(value)` replaces it.
    async fn write_widget(&self, widget: &Widget, agent_grant: Option<Option<&str>>) -> Result<()> {
        let json = self.encode_widget(widget)?;
        let now = chrono::Utc::now().to_rfc3339();
        {
            let conn = self.conn.lock().await;
            let existing_dashboard_id: Option<String> = conn
                .query_row(
                    "SELECT dashboard_id FROM widgets WHERE id = ?1",
                    params![widget.id],
                    |row| row.get(0),
                )
                .optional()
                .context("checking widget dashboard")?;
            if let Some(existing_dashboard_id) = existing_dashboard_id {
                if existing_dashboard_id != widget.dashboard_id {
                    anyhow::bail!(
                        "widget '{}' already belongs to dashboard '{}'",
                        widget.id,
                        existing_dashboard_id
                    );
                }
            }
            let replace_grant = agent_grant.is_some();
            let grant = agent_grant
                .flatten()
                .map(str::trim)
                .filter(|grant| !grant.is_empty())
                .map(|grant| self.seal_grant(grant))
                .transpose()?;
            conn.execute(
                "INSERT INTO widgets (id, dashboard_id, json, created_at, updated_at, agent_grant)
                 VALUES (?1, ?2, ?3, ?4, ?4, ?6)
                 ON CONFLICT(id) DO UPDATE SET dashboard_id = ?2, json = ?3, updated_at = ?4,
                     agent_grant = CASE WHEN ?5 THEN ?6 ELSE agent_grant END",
                params![
                    widget.id,
                    widget.dashboard_id,
                    json,
                    now,
                    replace_grant,
                    grant
                ],
            )
            .context("upserting widget")?;
        }
        self.broadcast(DashboardEvent::WidgetUpdated {
            dashboard_id: widget.dashboard_id.clone(),
            widget: widget.redacted(),
        });
        Ok(())
    }

    /// Fetch a widget by id.
    pub async fn get_widget(&self, id: &str) -> Result<Option<Widget>> {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT json FROM widgets WHERE id = ?1",
                params![id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading widget")?;
        json.map(|j| self.decode_widget(&j)).transpose()
    }

    /// Fetch a widget only when it belongs to the supplied dashboard.
    pub async fn get_widget_for_dashboard(
        &self,
        dashboard_id: &str,
        id: &str,
    ) -> Result<Option<Widget>> {
        let conn = self.conn.lock().await;
        let json = conn
            .query_row(
                "SELECT json FROM widgets WHERE dashboard_id = ?1 AND id = ?2",
                params![dashboard_id, id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .context("reading dashboard widget")?;
        json.map(|j| self.decode_widget(&j)).transpose()
    }

    /// List the widgets on a dashboard.
    pub async fn list_widgets(&self, dashboard_id: &str) -> Result<Vec<Widget>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn
            .prepare("SELECT json FROM widgets WHERE dashboard_id = ?1 ORDER BY created_at ASC")?;
        let rows = stmt.query_map(params![dashboard_id], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(w) = self.decode_listed(&row?)? {
                out.push(w);
            }
        }
        Ok(out)
    }

    /// Every widget across every dashboard (the refresh loop's work list).
    pub async fn list_all_widgets(&self) -> Result<Vec<Widget>> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare("SELECT json FROM widgets")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut out = Vec::new();
        for row in rows {
            if let Some(w) = self.decode_listed(&row?)? {
                out.push(w);
            }
        }
        Ok(out)
    }

    /// Delete a widget. Returns true when removed.
    pub async fn delete_widget(&self, id: &str) -> Result<bool> {
        let (removed, dashboard_id) = {
            let conn = self.conn.lock().await;
            let dashboard_id: Option<String> = conn
                .query_row(
                    "SELECT dashboard_id FROM widgets WHERE id = ?1",
                    params![id],
                    |row| row.get(0),
                )
                .optional()?;
            let n = conn.execute("DELETE FROM widgets WHERE id = ?1", params![id])?;
            (n > 0, dashboard_id)
        };
        if let (true, Some(did)) = (removed, dashboard_id) {
            self.broadcast(DashboardEvent::WidgetDeleted {
                dashboard_id: did,
                widget_id: id.to_string(),
            });
        }
        Ok(removed)
    }

    /// Delete a widget only when it belongs to the supplied dashboard.
    pub async fn delete_widget_for_dashboard(&self, dashboard_id: &str, id: &str) -> Result<bool> {
        let removed = {
            let conn = self.conn.lock().await;
            let n = conn.execute(
                "DELETE FROM widgets WHERE dashboard_id = ?1 AND id = ?2",
                params![dashboard_id, id],
            )?;
            n > 0
        };
        if removed {
            self.broadcast(DashboardEvent::WidgetDeleted {
                dashboard_id: dashboard_id.to_string(),
                widget_id: id.to_string(),
            });
        }
        Ok(removed)
    }

    /// Persist only a widget's grid layout (the debounced drag/resize path).
    /// Returns the updated widget, or None if it does not exist.
    pub async fn update_widget_layout(
        &self,
        id: &str,
        layout: GridLayout,
    ) -> Result<Option<Widget>> {
        let mut widget = match self.get_widget(id).await? {
            Some(w) => w,
            None => return Ok(None),
        };
        widget.layout = layout;
        self.upsert_widget(&widget).await?;
        Ok(Some(widget))
    }

    /// Persist only a widget's grid layout when it belongs to the supplied dashboard.
    pub async fn update_widget_layout_for_dashboard(
        &self,
        dashboard_id: &str,
        id: &str,
        layout: GridLayout,
    ) -> Result<Option<Widget>> {
        self.update_widget_layout_fields_for_dashboard(dashboard_id, id, Some(layout), None)
            .await
    }

    /// Persist a widget's positional fields — grid layout and/or canvas layout —
    /// applying only the ones supplied. The two views are independent: a canvas
    /// drag passes `grid = None` so it never disturbs the v1 grid arrangement, and a
    /// grid drag passes `canvas = None`. A no-op call (both `None`) still returns the
    /// widget unchanged. Scoped to the dashboard so a widget can't be moved via the
    /// wrong dashboard id.
    pub async fn update_widget_layout_fields_for_dashboard(
        &self,
        dashboard_id: &str,
        id: &str,
        grid: Option<GridLayout>,
        canvas: Option<CanvasLayout>,
    ) -> Result<Option<Widget>> {
        let mut widget = match self.get_widget_for_dashboard(dashboard_id, id).await? {
            Some(w) => w,
            None => return Ok(None),
        };
        if let Some(layout) = grid {
            widget.layout = layout;
        }
        if let Some(c) = canvas {
            widget.canvas = Some(c);
        }
        self.upsert_widget(&widget).await?;
        Ok(Some(widget))
    }

    /// Cache a freshly-resolved value (or error) on a widget and broadcast it.
    /// Writes through to SQLite so a reload shows the last value immediately.
    pub async fn update_widget_value(
        &self,
        id: &str,
        value: Result<serde_json::Value, String>,
    ) -> Result<()> {
        let mut widget = match self.get_widget(id).await? {
            Some(w) => w,
            None => return Ok(()),
        };
        let now = chrono::Utc::now().to_rfc3339();
        widget.last_refresh_at = Some(now.clone());
        // The value being replaced is what decides whether this resolve crosses a
        // health boundary; read it before the branches below overwrite it.
        let was_failing = widget.last_error.is_some();
        // The event branches move `now`; the outward emit below needs it too.
        let at = now.clone();
        let event = match &value {
            Ok(v) => {
                widget.last_value = Some(v.clone());
                widget.last_error = None;
                DashboardEvent::WidgetData {
                    dashboard_id: widget.dashboard_id.clone(),
                    widget_id: widget.id.clone(),
                    value: v.clone(),
                    at: now,
                }
            }
            Err(e) => {
                widget.last_error = Some(e.clone());
                DashboardEvent::WidgetError {
                    dashboard_id: widget.dashboard_id.clone(),
                    widget_id: widget.id.clone(),
                    error: e.clone(),
                    at: now,
                }
            }
        };
        // Persist the cached value WITHOUT emitting a (noisy) WidgetUpdated; emit
        // the precise data/error event instead.
        let json = self.encode_widget(&widget)?;
        let updated_at = chrono::Utc::now().to_rfc3339();
        {
            let conn = self.conn.lock().await;
            conn.execute(
                "UPDATE widgets SET json = ?2, updated_at = ?3 WHERE id = ?1",
                params![widget.id, json, updated_at],
            )
            .context("caching widget value")?;
        }
        self.broadcast(event);

        // The same transition, raised OUTWARD as an app event. `broadcast` reaches
        // this node's own grid, which is watching anyway and shows the error inline;
        // this reaches plugins and workflows that are not watching at all, which is
        // the only way a broken widget ever gets acted on — a dashboard nobody has
        // open is exactly where a dead data source hides.
        //
        // Edge-triggered, deliberately: a broken widget re-resolves every interval,
        // so emitting per failure would turn one dead endpoint into a subscribed
        // workflow firing every 15s forever. The `last_error` this call is replacing
        // IS the dedupe — only a crossing between healthy and broken leaves here.
        match (&value, was_failing) {
            (Err(error), false) => {
                self.events
                    .emit_with_notify(
                        EVENT_WIDGET_FAILED,
                        serde_json::json!({
                            "dashboard_id": widget.dashboard_id,
                            "widget_id": widget.id,
                            "title": widget.title,
                            "kind": widget.kind,
                            // Which resolver broke, so a consumer can tell an
                            // unreachable site from a refusing agent without
                            // pattern-matching on the message text.
                            "source_type": source_type(&widget.source),
                            "error": error,
                            "at": at,
                        }),
                        // A widget breaking is the exact "something you rely on is
                        // down" alert; edge-triggered above, so it fires once per
                        // broken spell, not every resolve.
                        Some(
                            ryu_app_events::NotifyHint::info(
                                format!("Widget “{}” broke", widget.title),
                                Some(error.to_string()),
                            )
                            .with_level("warning"),
                        ),
                    )
                    .await;
            }
            (Ok(_), true) => {
                self.events
                    .emit_with_notify(
                        EVENT_WIDGET_RECOVERED,
                        serde_json::json!({
                            "dashboard_id": widget.dashboard_id,
                            "widget_id": widget.id,
                            "title": widget.title,
                            "kind": widget.kind,
                            "source_type": source_type(&widget.source),
                            "at": at,
                        }),
                        Some(
                            ryu_app_events::NotifyHint::info(
                                format!("Widget “{}” recovered", widget.title),
                                None,
                            )
                            .with_level("success"),
                        ),
                    )
                    .await;
            }
            // Healthy → healthy and broken → broken are the steady states; neither
            // is news.
            _ => {}
        }
        Ok(())
    }

    /// Broadcast a dashboard event to SSE subscribers (no live subscriber ⇒ noop).
    pub fn broadcast(&self, event: DashboardEvent) {
        let _ = self.tx.send(event);
    }

    /// Subscribe to live dashboard events (the SSE endpoint + internal listeners
    /// such as the hardware nudge loop). Subscribing does NOT register a UI viewer —
    /// SSE handlers separately hold a [`ViewerGuard`] so internal subscribers don't
    /// trip the refresh loop's cost guard.
    pub fn subscribe(&self) -> broadcast::Receiver<DashboardEvent> {
        self.tx.subscribe()
    }

    /// Register one live UI viewer for the duration of the returned guard. The SSE
    /// handler holds this while a client stream is open; the refresh loop reads
    /// [`Self::viewer_count`] to decide whether to run expensive sources.
    pub fn viewer_guard(&self) -> ViewerGuard {
        self.viewers.fetch_add(1, Ordering::SeqCst);
        ViewerGuard {
            viewers: Arc::clone(&self.viewers),
        }
    }

    /// Number of live UI viewers (open SSE streams). The refresh loop skips
    /// expensive sources when this is zero (no human is watching ⇒ do not burn
    /// tokens/quota). This is deliberately NOT the broadcast `receiver_count`, so an
    /// internal subscriber (the nudge loop) cannot fake a viewer.
    pub fn viewer_count(&self) -> usize {
        self.viewers.load(Ordering::SeqCst)
    }

    /// Number of live broadcast subscribers (UI streams + internal listeners).
    /// Retained for diagnostics; the cost guard uses [`Self::viewer_count`].
    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// A source's wire tag (`core_endpoint`, `monitor`, `http`, `agent`, …) for the
/// failure payload. Read off the serialized form rather than hand-written per
/// variant so it can never drift from the `#[serde(tag = "type")]` names the API
/// and the builder tool already speak.
fn source_type(source: &WidgetSource) -> String {
    serde_json::to_value(source)
        .ok()
        .and_then(|v| {
            v.get("type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{WidgetKind, WidgetSource};

    fn temp_store() -> DashboardStore {
        let dir = std::env::temp_dir().join(format!("ryu-dash-test-{}", uuid::Uuid::new_v4()));
        DashboardStore::open_with_sealer(dir.join("dashboards.db"), Some(test_plugin_cipher(7)))
            .expect("open")
    }

    fn test_widget(id: &str, dashboard_id: &str) -> Widget {
        Widget {
            id: id.to_string(),
            dashboard_id: dashboard_id.to_string(),
            kind: WidgetKind::Stat,
            title: "Test".to_string(),
            config: serde_json::Value::Null,
            source: WidgetSource::Static {
                data: serde_json::Value::Null,
            },
            refresh_interval: None,
            layout: GridLayout::default(),
            canvas: None,
            last_value: None,
            last_refresh_at: None,
            last_error: None,
        }
    }

    fn http_widget(id: &str) -> Widget {
        let mut widget = test_widget(id, "dash");
        widget.source = WidgetSource::Http {
            url: "https://api.example.com/data".into(),
            selector: None,
            headers: [(
                "Authorization".to_string(),
                "Bearer HEADER-SECRET-1".to_string(),
            )]
            .into_iter()
            .collect(),
        };
        widget
    }

    fn raw_widget_json(path: &std::path::Path, id: &str) -> String {
        Connection::open(path)
            .unwrap()
            .query_row("SELECT json FROM widgets WHERE id = ?1", [id], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn seal_path() -> PathBuf {
        std::env::temp_dir()
            .join(format!("ryu-dash-seal-{}", uuid::Uuid::new_v4()))
            .join("dashboards.db")
    }

    fn header_of(widget: &Widget) -> String {
        match &widget.source {
            WidgetSource::Http { headers, .. } => headers["Authorization"].clone(),
            _ => panic!("not an http widget"),
        }
    }

    #[tokio::test]
    async fn header_values_are_sealed_at_rest_and_open_for_the_refresh_loop() {
        let path = seal_path();
        let store =
            DashboardStore::open_with_sealer(path.clone(), Some(test_plugin_cipher(7))).unwrap();
        store.upsert_widget(&http_widget("w1")).await.unwrap();
        let raw = raw_widget_json(&path, "w1");
        assert!(!raw.contains("HEADER-SECRET-1"), "{raw}");
        assert!(raw.contains("encp:v1:") && raw.contains("Authorization"));
        let got = store.get_widget("w1").await.unwrap().unwrap();
        assert_eq!(header_of(&got), "Bearer HEADER-SECRET-1");
        // The value cache rewrite keeps the header sealed.
        store
            .update_widget_value("w1", Ok(serde_json::json!(1)))
            .await
            .unwrap();
        assert!(!raw_widget_json(&path, "w1").contains("HEADER-SECRET-1"));
        assert_eq!(
            header_of(&store.list_all_widgets().await.unwrap()[0]),
            "Bearer HEADER-SECRET-1"
        );
        // The HTTP view never carries the value.
        let view = serde_json::to_string(&got.redacted()).unwrap();
        assert!(!view.contains("HEADER-SECRET-1"), "{view}");
        drop(store);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);
        let bytes = std::fs::read(&path).unwrap();
        assert!(!String::from_utf8_lossy(&bytes).contains("HEADER-SECRET-1"));
    }

    #[tokio::test]
    async fn an_agent_widget_write_is_refused_without_a_sealing_key() {
        let path = seal_path();
        let keyless = DashboardStore::open(path.clone()).unwrap();
        let widget = test_widget("w1", "dash");
        // A person's widget needs no key; an agent's grant is a credential and does.
        keyless.upsert_widget_for(&widget, None).await.unwrap();
        assert!(keyless
            .upsert_widget_for(&widget, Some("grant.agent"))
            .await
            .is_err());
        assert_eq!(keyless.widget_agent_grant("w1").await.unwrap(), None);
        drop(keyless);
        let keyed =
            DashboardStore::open_with_sealer(path.clone(), Some(test_plugin_cipher(7))).unwrap();
        keyed
            .upsert_widget_for(&widget, Some("grant.agent"))
            .await
            .unwrap();
        assert_eq!(
            keyed.widget_agent_grant("w1").await.unwrap().as_deref(),
            Some("grant.agent")
        );
    }

    #[tokio::test]
    async fn legacy_plaintext_headers_are_resealed_and_keys_fail_closed() {
        let path = seal_path();
        let legacy = DashboardStore::open(path.clone()).unwrap();
        // Without a key a header value cannot be stored...
        assert!(legacy.upsert_widget(&http_widget("w1")).await.is_err());
        // ...but a row an older version wrote in plaintext still reads.
        let widget = http_widget("w1");
        legacy
            .conn
            .lock()
            .await
            .execute(
                "INSERT INTO widgets (id, dashboard_id, json, created_at, updated_at)
                 VALUES (?1, 'dash', ?2, 'a', 'a')",
                params![widget.id, serde_json::to_string(&widget).unwrap()],
            )
            .unwrap();
        assert_eq!(
            header_of(&legacy.get_widget("w1").await.unwrap().unwrap()),
            "Bearer HEADER-SECRET-1"
        );
        drop(legacy);
        let store =
            DashboardStore::open_with_sealer(path.clone(), Some(test_plugin_cipher(7))).unwrap();
        assert!(!raw_widget_json(&path, "w1").contains("HEADER-SECRET-1"));
        assert_eq!(
            header_of(&store.get_widget("w1").await.unwrap().unwrap()),
            "Bearer HEADER-SECRET-1"
        );
        drop(store);
        let keyless = DashboardStore::open(path.clone()).unwrap();
        assert!(keyless.get_widget("w1").await.is_err());
        let other =
            DashboardStore::open_with_sealer(path.clone(), Some(test_plugin_cipher(8))).unwrap();
        assert!(other.list_all_widgets().await.is_err());
    }

    #[test]
    fn a_placeholder_header_keeps_the_stored_value() {
        let stored = http_widget("w").source;
        let mut incoming = http_widget("w").redacted().source;
        incoming.keep_redacted_headers(Some(&stored)).unwrap();
        assert_eq!(
            serde_json::to_value(&incoming).unwrap(),
            serde_json::to_value(&stored).unwrap()
        );
        let mut orphan = http_widget("w").redacted().source;
        assert!(orphan.keep_redacted_headers(None).is_err());
    }

    fn with_url(mut source: WidgetSource, new_url: &str) -> WidgetSource {
        if let WidgetSource::Http { url, .. } = &mut source {
            *url = new_url.to_string();
        }
        source
    }

    fn header_value(source: &WidgetSource) -> String {
        match source {
            WidgetSource::Http { headers, .. } => headers["Authorization"].clone(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn a_placeholder_header_is_refused_when_the_origin_changes() {
        let stored = http_widget("w").source;
        for other in [
            "https://attacker.example/data",
            "http://api.example.com/data",
            "https://api.example.com:8443/data",
            "https://sub.api.example.com/data",
            "not a url",
        ] {
            let mut incoming = with_url(http_widget("w").redacted().source, other);
            let error = incoming.keep_redacted_headers(Some(&stored)).unwrap_err();
            assert!(error.contains("different origin"), "{other}: {error}");
            assert!(!header_value(&incoming).contains("HEADER-SECRET-1"));
        }
        // Same origin, different path/query: the stored value is kept.
        let mut moved = with_url(
            http_widget("w").redacted().source,
            "https://api.example.com:443/other?q=1",
        );
        moved.keep_redacted_headers(Some(&stored)).unwrap();
        assert_eq!(header_value(&moved), "Bearer HEADER-SECRET-1");
        // A freshly entered value is accepted for any origin.
        let mut fresh = with_url(http_widget("w").source, "https://elsewhere.example/");
        fresh.keep_redacted_headers(Some(&stored)).unwrap();
    }

    #[test]
    fn source_type_is_the_serde_wire_tag() {
        // The failure event's `source_type` must be the SAME string the API speaks,
        // so a consumer can branch on it without a second vocabulary to learn.
        assert_eq!(
            source_type(&WidgetSource::CoreEndpoint {
                endpoint: "agents".into(),
                selector: None,
            }),
            "core_endpoint"
        );
        assert_eq!(
            source_type(&WidgetSource::Agent {
                agent_id: "a".into(),
                prompt: "p".into(),
            }),
            "agent"
        );
    }

    #[tokio::test]
    async fn viewer_count_is_decoupled_from_subscribe() {
        let store = temp_store();
        // An internal subscriber (the nudge loop) must NOT register as a UI viewer,
        // or it would defeat the refresh loop's cost guard (expensive sources would
        // run 24/7). subscribe() bumps receiver_count but NOT viewer_count.
        let _internal_rx = store.subscribe();
        assert_eq!(
            store.viewer_count(),
            0,
            "subscribe alone is not a UI viewer"
        );
        assert!(store.receiver_count() >= 1);

        // A UI stream holds a viewer guard for its lifetime.
        let guard = store.viewer_guard();
        assert_eq!(store.viewer_count(), 1);
        drop(guard);
        assert_eq!(
            store.viewer_count(),
            0,
            "dropping the stream clears the viewer"
        );
    }

    #[tokio::test]
    async fn device_dashboard_round_trips() {
        let store = temp_store();
        let now = chrono::Utc::now().to_rfc3339();
        let dd = DeviceDashboard {
            device_id: "rhw_1".into(),
            dashboard_id: "dash_1".into(),
            refresh_rate: 300,
            created_at: now.clone(),
            updated_at: now,
        };
        store.upsert_device_dashboard(&dd).await.unwrap();
        let got = store.get_device_dashboard("rhw_1").await.unwrap().unwrap();
        assert_eq!(got.dashboard_id, "dash_1");
        assert_eq!(got.refresh_rate, 300);
        assert_eq!(store.list_device_dashboards().await.unwrap().len(), 1);
        assert!(store.delete_device_dashboard("rhw_1").await.unwrap());
        assert!(store.get_device_dashboard("rhw_1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn widget_id_cannot_move_between_dashboards() {
        let store = temp_store();
        store
            .upsert_widget(&test_widget("w1", "dash_a"))
            .await
            .unwrap();

        let err = store
            .upsert_widget(&test_widget("w1", "dash_b"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("already belongs to dashboard"));

        let widget = store.get_widget("w1").await.unwrap().unwrap();
        assert_eq!(widget.dashboard_id, "dash_a");
    }

    #[tokio::test]
    async fn scoped_widget_delete_requires_matching_dashboard() {
        let store = temp_store();
        store
            .upsert_widget(&test_widget("w1", "dash_a"))
            .await
            .unwrap();

        assert!(!store
            .delete_widget_for_dashboard("dash_b", "w1")
            .await
            .unwrap());
        assert!(store.get_widget("w1").await.unwrap().is_some());

        assert!(store
            .delete_widget_for_dashboard("dash_a", "w1")
            .await
            .unwrap());
        assert!(store.get_widget("w1").await.unwrap().is_none());
    }

    fn test_dashboard(id: &str, name: &str) -> Dashboard {
        let now = chrono::Utc::now().to_rfc3339();
        Dashboard {
            id: id.into(),
            name: name.into(),
            created_at: now.clone(),
            updated_at: now,
            view_mode: None,
        }
    }

    #[tokio::test]
    async fn dashboard_crud_round_trips() {
        let store = temp_store();
        assert!(store.get_dashboard("d1").await.unwrap().is_none());
        assert!(store.list_dashboards().await.unwrap().is_empty());

        store
            .upsert_dashboard(&test_dashboard("d1", "Home"))
            .await
            .unwrap();
        store
            .upsert_dashboard(&test_dashboard("d2", "Ops"))
            .await
            .unwrap();

        let got = store.get_dashboard("d1").await.unwrap().unwrap();
        assert_eq!(got.name, "Home");
        let all = store.list_dashboards().await.unwrap();
        assert_eq!(all.len(), 2);

        // Upsert on the same id updates in place (no duplicate row).
        let mut renamed = test_dashboard("d1", "Renamed");
        renamed.view_mode = Some("canvas".into());
        store.upsert_dashboard(&renamed).await.unwrap();
        let got = store.get_dashboard("d1").await.unwrap().unwrap();
        assert_eq!(got.name, "Renamed");
        assert_eq!(got.view_mode.as_deref(), Some("canvas"));
        assert_eq!(store.list_dashboards().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn delete_dashboard_cascades_widgets() {
        let store = temp_store();
        store
            .upsert_dashboard(&test_dashboard("d1", "Home"))
            .await
            .unwrap();
        store.upsert_widget(&test_widget("w1", "d1")).await.unwrap();
        store.upsert_widget(&test_widget("w2", "d1")).await.unwrap();
        assert_eq!(store.list_widgets("d1").await.unwrap().len(), 2);

        assert!(store.delete_dashboard("d1").await.unwrap());
        assert!(store.get_dashboard("d1").await.unwrap().is_none());
        // Widgets are cascade-deleted with their dashboard.
        assert!(store.list_widgets("d1").await.unwrap().is_empty());
        assert!(store.get_widget("w1").await.unwrap().is_none());
        // Deleting a missing dashboard reports false.
        assert!(!store.delete_dashboard("nope").await.unwrap());
    }

    #[tokio::test]
    async fn widget_get_scoped_and_list_all() {
        let store = temp_store();
        store.upsert_widget(&test_widget("w1", "d1")).await.unwrap();
        store.upsert_widget(&test_widget("w2", "d2")).await.unwrap();

        // get_widget_for_dashboard only returns a match on the right dashboard.
        assert!(store
            .get_widget_for_dashboard("d1", "w1")
            .await
            .unwrap()
            .is_some());
        assert!(store
            .get_widget_for_dashboard("d2", "w1")
            .await
            .unwrap()
            .is_none());
        // list_all_widgets spans dashboards (the refresh loop's work list).
        assert_eq!(store.list_all_widgets().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn update_layout_grid_only_and_canvas_only_are_independent() {
        let store = temp_store();
        store.upsert_widget(&test_widget("w1", "d1")).await.unwrap();

        // Grid-only update leaves canvas untouched (None).
        let updated = store
            .update_widget_layout(
                "w1",
                GridLayout {
                    x: 2,
                    y: 3,
                    w: 6,
                    h: 5,
                },
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!((updated.layout.x, updated.layout.h), (2, 5));
        assert!(updated.canvas.is_none());

        // Canvas-only update (grid = None) must not disturb the grid.
        let canvas = CanvasLayout {
            x: 11.0,
            y: 22.0,
            w: 33.0,
            h: 44.0,
        };
        let updated = store
            .update_widget_layout_fields_for_dashboard("d1", "w1", None, Some(canvas))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (updated.layout.x, updated.layout.h),
            (2, 5),
            "grid preserved"
        );
        assert_eq!(updated.canvas.unwrap().x, 11.0);

        // A no-op call (both None) returns the widget unchanged.
        let same = store
            .update_widget_layout_fields_for_dashboard("d1", "w1", None, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(same.layout.x, 2);
    }

    #[tokio::test]
    async fn update_layout_missing_or_wrong_dashboard_returns_none() {
        let store = temp_store();
        store.upsert_widget(&test_widget("w1", "d1")).await.unwrap();
        // Unknown widget.
        assert!(store
            .update_widget_layout("ghost", GridLayout::default())
            .await
            .unwrap()
            .is_none());
        // Right widget, wrong dashboard scope ⇒ None (can't move via wrong id).
        assert!(store
            .update_widget_layout_for_dashboard("d2", "w1", GridLayout::default())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn update_widget_value_caches_ok_then_error() {
        let store = temp_store();
        store.upsert_widget(&test_widget("w1", "d1")).await.unwrap();

        // Ok value is cached and clears any prior error.
        store
            .update_widget_value("w1", Ok(serde_json::json!({ "n": 1 })))
            .await
            .unwrap();
        let w = store.get_widget("w1").await.unwrap().unwrap();
        assert_eq!(w.last_value, Some(serde_json::json!({ "n": 1 })));
        assert!(w.last_error.is_none());
        assert!(w.last_refresh_at.is_some());

        // An error stamps last_error (value from the ok path is retained here since
        // the error branch does not clear it) and updates the refresh timestamp.
        store
            .update_widget_value("w1", Err("boom".to_string()))
            .await
            .unwrap();
        let w = store.get_widget("w1").await.unwrap().unwrap();
        assert_eq!(w.last_error.as_deref(), Some("boom"));

        // A value update for a missing widget is a silent no-op (Ok).
        store
            .update_widget_value("ghost", Ok(serde_json::Value::Null))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn upsert_widget_broadcasts_to_subscribers() {
        let store = temp_store();
        let mut rx = store.subscribe();
        store.upsert_widget(&test_widget("w1", "d1")).await.unwrap();
        let event = rx.recv().await.unwrap();
        match event {
            DashboardEvent::WidgetUpdated {
                dashboard_id,
                widget,
            } => {
                assert_eq!(dashboard_id, "d1");
                assert_eq!(widget.id, "w1");
            }
            other => panic!("expected WidgetUpdated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn update_widget_value_broadcasts_data_event() {
        let store = temp_store();
        store.upsert_widget(&test_widget("w1", "d1")).await.unwrap();
        let mut rx = store.subscribe();
        store
            .update_widget_value("w1", Ok(serde_json::json!(7)))
            .await
            .unwrap();
        let event = rx.recv().await.unwrap();
        match event {
            DashboardEvent::WidgetData {
                widget_id, value, ..
            } => {
                assert_eq!(widget_id, "w1");
                assert_eq!(value, serde_json::json!(7));
            }
            other => panic!("expected WidgetData, got {other:?}"),
        }
    }
}
