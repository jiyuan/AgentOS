//! Source ratchets for process boundaries that are intentionally absent.

/// AF-039: Telegram no longer has a child process whose environment, group,
/// and output must be controlled; every API response uses the shared cap.
#[test]
fn telegram_transport_has_no_child_and_uses_bounded_http() {
    let receive = include_str!("../src/channels/telegram/mod.rs");
    let egress = include_str!("../src/channels/telegram/egress.rs");

    for source in [receive, egress] {
        assert!(!source.contains("Command::new"));
        assert!(!source.contains("std::process::Command"));
        assert!(!source.contains("tokio::process::Command"));
    }
    assert!(receive.contains("bounded_response::json"));
    assert!(egress.contains("telegram_json(response).await"));
}
