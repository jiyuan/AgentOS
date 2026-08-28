//! AF-034: protocol-controlled lengths are bounded before allocation.

#![allow(dead_code)]

#[path = "../src/channels/bounded_response.rs"]
mod bounded_response;
#[path = "../src/channels/feishu/fragments.rs"]
mod fragments;
#[path = "../src/channels/feishu/limits.rs"]
mod limits;
#[path = "../src/channels/feishu/proto.rs"]
mod proto;

use fragments::FragmentBuffer;
use limits::{MAX_EVENT_BYTES, MAX_FRAME_BYTES};
use proto::FeishuFrame;

#[tokio::test]
async fn all_protocol_bodies_are_bounded_before_allocation() {
    let oversized_frame = vec![0; MAX_FRAME_BYTES + 1];
    assert!(
        FeishuFrame::decode(&oversized_frame).is_err(),
        "the protobuf parser independently ratchets the WebSocket frame ceiling"
    );

    let encoded = FeishuFrame {
        payload: vec![b'x'; MAX_EVENT_BYTES + 1],
        ..FeishuFrame::default()
    };
    assert!(
        FeishuFrame::decode(&encoded.encode()).is_err(),
        "a declared protobuf payload length is checked before it is cloned"
    );

    let mut fragments = FragmentBuffer::default();
    assert!(fragments
        .accept(None, None, None, vec![0; MAX_EVENT_BYTES + 1])
        .is_err());
    assert!(fragments
        .accept(Some("not-a-number"), None, None, vec![0])
        .is_err());
    assert!(fragments
        .accept(None, Some("0"), Some("message"), vec![0])
        .is_err());

    let half = vec![0; MAX_EVENT_BYTES / 2 + 1];
    assert_eq!(
        fragments
            .accept(Some("2"), Some("0"), Some("aggregate"), half.clone())
            .expect("the first bounded fragment is retained"),
        None
    );
    assert!(fragments
        .accept(Some("2"), Some("1"), Some("aggregate"), half)
        .is_err());

    let response: reqwest::Response = tokio_tungstenite::tungstenite::http::Response::builder()
        .body(r#"{"body":"0123456789abcdef"}"#.as_bytes().to_vec())
        .expect("synthetic response builds")
        .into();
    assert!(
        bounded_response::json(response, "test channel", 16)
            .await
            .is_err(),
        "a length-less HTTP response is stopped while streaming"
    );
}
