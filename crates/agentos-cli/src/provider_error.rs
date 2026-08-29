//! Turning a provider failure into something a person can act on.
//!
//! Both binaries surface the same failures — the TUI prints them, the gateway
//! sends them back over the channel — so both need the same wording and the
//! same request id. They carried byte-identical private copies of this logic,
//! which is one edit away from two deployments explaining the same outage
//! differently.

/// A message for the person who hit this error, not for the log.
///
/// Quota exhaustion gets its own text because it is the one provider failure an
/// operator can actually fix, and it is not obvious from the raw error which
/// budget ran out. Everything else points at the gateway log rather than
/// guessing.
pub fn user_facing_error_message(error: &str) -> String {
    let mut message = if error.contains("insufficient_quota") {
        "AgentOS reached OpenAI, but OpenAI returned insufficient_quota for the configured API \
         project or organization. Check OpenAI Platform billing, project budget, org usage \
         limits, and prepaid API credits."
            .to_owned()
    } else {
        "AgentOS could not complete this request. See the gateway log for details.".to_owned()
    };
    if let Some(request_id) = extract_openai_request_id(error) {
        message.push_str("\nOpenAI request id: ");
        message.push_str(&request_id);
    }
    message
}

/// Pull `x-request-id=...` out of a formatted provider error, so a report can
/// name the exact request OpenAI support will ask for.
pub fn extract_openai_request_id(error: &str) -> Option<String> {
    let (_, rest) = error.split_once("x-request-id=")?;
    let request_id = rest
        .split(|ch: char| ch == ',' || ch == ';' || ch.is_whitespace())
        .next()?
        .trim();
    if request_id.is_empty() {
        None
    } else {
        Some(request_id.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_exhaustion_is_named_and_carries_its_request_id() {
        let message = user_facing_error_message(
            "provider error: insufficient_quota (x-request-id=req_abc123, status=429)",
        );
        assert!(message.contains("insufficient_quota"));
        assert!(message.ends_with("\nOpenAI request id: req_abc123"));
    }

    #[test]
    fn an_unrecognised_failure_points_at_the_log() {
        let message = user_facing_error_message("connection reset by peer");
        assert!(message.contains("See the gateway log"));
        assert!(!message.contains("request id"));
    }

    #[test]
    fn a_request_id_is_read_up_to_its_delimiter_and_never_empty() {
        assert_eq!(
            extract_openai_request_id("x-request-id=req_1 trailing"),
            Some("req_1".to_owned())
        );
        assert_eq!(
            extract_openai_request_id("x-request-id=req_2;next"),
            Some("req_2".to_owned())
        );
        assert_eq!(extract_openai_request_id("x-request-id="), None);
        assert_eq!(extract_openai_request_id("no id here"), None);
    }
}
