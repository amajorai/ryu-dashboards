//! The widget errors the source resolvers raise. Each is a whole sentence
//! returned by a `*_message` helper, so the i18n extractor catalogs it and
//! Core can translate it when the widget-broke notification relays it
//! (`tools/core-i18n-relayed-sources.mjs`); the dynamic part (a status code, an
//! underlying error) is a placeholder Core keeps verbatim.

/// A Composio widget's action call failed before it returned.
pub fn composio_failed_message(error: &str) -> String {
    format!("The Composio action failed: {error}")
}

/// A Composio widget's action returned a non-success HTTP status.
pub fn composio_status_message(status: impl std::fmt::Display) -> String {
    format!("The Composio action returned HTTP {status}.")
}

/// An agent widget's run failed.
pub fn agent_failed_message(error: &str) -> String {
    format!("The widget's agent run failed: {error}")
}

/// A web widget's source answered with a redirect, which the SSRF guard refuses.
pub fn redirect_refused_message() -> String {
    "The web source redirected, and redirects are not allowed.".to_owned()
}

/// A web widget's source answered with a non-success HTTP status.
pub fn http_status_message(status: impl std::fmt::Display) -> String {
    format!("The web source returned HTTP {status}.")
}

/// A web widget's source answered with a body that is not JSON.
pub fn not_json_message(error: &str) -> String {
    format!("The web source sent a reply Ryu could not read: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolver_errors_are_whole_sentences_with_their_detail() {
        assert_eq!(
            http_status_message(502),
            "The web source returned HTTP 502."
        );
        assert_eq!(
            composio_status_message(429),
            "The Composio action returned HTTP 429."
        );
        assert_eq!(
            not_json_message("expected value at line 1"),
            "The web source sent a reply Ryu could not read: expected value at line 1"
        );
        assert!(agent_failed_message("offline").ends_with("failed: offline"));
        assert!(composio_failed_message("boom").ends_with("failed: boom"));
        assert!(redirect_refused_message().contains("redirects are not allowed"));
    }
}
