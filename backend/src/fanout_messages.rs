//! Inbox, toast and push copy as localizable messages.
//!
//! Core renders a notify hint's `i18n` text per recipient (the inbox row and
//! toast in the node's `ui-language`, each push in the locale its device
//! registered with); the English rendering still goes out as the plain
//! `title`/`body`, which an older Core delivers as is.
//!
//! Each `*_MESSAGE` is `format!` with one named argument per placeholder
//! (`name = arg("name")`) so the i18n extractor catalogs the ICU source;
//! `tools/generate-core-i18n.mjs` copies the translations into Core's catalog.
//! Every message is a whole sentence. Core's renderer has no plural selector,
//! so counts are written as `label: {count}`.
#![allow(clippy::useless_format)]

use ryu_app_events::{NotifyHint, NotifyMessage, NotifyTemplate as Template};

pub(crate) const WIDGET_BROKE_MESSAGE: Template =
    |arg| format!("Widget “{title}” broke", title = arg("title"));
pub(crate) const WIDGET_RECOVERED_MESSAGE: Template =
    |arg| format!("Widget “{title}” recovered", title = arg("title"));

/// A widget's source started failing; the body relays the resolver error.
pub(crate) fn widget_broke(title: &str, error: &str) -> NotifyHint {
    NotifyHint::localized(
        NotifyMessage::new(WIDGET_BROKE_MESSAGE).text("title", title),
        vec![NotifyMessage::relayed(error)],
    )
    .with_level("warning")
}

/// A broken widget resolves again.
pub(crate) fn widget_recovered(title: &str) -> NotifyHint {
    NotifyHint::localized(
        NotifyMessage::new(WIDGET_RECOVERED_MESSAGE).text("title", title),
        Vec::new(),
    )
    .with_level("success")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_broken_widget_relays_the_resolver_error_for_core_to_translate() {
        let error = crate::sources::http_status_message(500);
        let hint = widget_broke("Sales", &error);
        assert_eq!(
            hint.body.as_deref(),
            Some("The web source returned HTTP 500.")
        );
        let body = &serde_json::to_value(&hint).unwrap()["i18n"]["body"][0];
        assert_eq!(body["relay"], true);
    }
}
