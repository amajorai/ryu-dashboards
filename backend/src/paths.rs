//! Shared Ryu sidecar data-directory seam.

use std::path::PathBuf;

/// `<data dir>/plugin-data/@ryu+dashboards/dashboards.db`.
pub fn dashboards_db_path() -> PathBuf {
    ryu_sidecar_runtime::plugin_data_dir("@ryu/dashboards").join("dashboards.db")
}
