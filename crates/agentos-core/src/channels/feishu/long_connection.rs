use super::event::{feishu_drop_reason, parse_event, ParsedFeishuEvent};
use super::fragments::FragmentBuffer;
use super::proto::{header_value, pong_frame, success_frame, FeishuFrame};
use super::websocket::WebSocketConnection;
use crate::channels::admission::AdmissionPolicy;
use agentos_interfaces::ChannelError;
use agentos_proto::ChannelId;
use serde_json::Value;
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FeishuEndpoint {
    pub(super) url: String,
}

pub(super) struct FeishuLongConnection {
    socket: WebSocketConnection,
    fragments: FragmentBuffer,
}

impl FeishuLongConnection {
    pub(super) async fn connect(endpoint: &FeishuEndpoint) -> Result<Self, ChannelError> {
        Ok(Self {
            socket: WebSocketConnection::connect(&endpoint.url).await?,
            fragments: FragmentBuffer::default(),
        })
    }

    pub(super) async fn receive_next_event(
        &mut self,
        channel_id: &ChannelId,
        admission: &AdmissionPolicy,
        receive_id_type: &str,
        log_receive_errors: bool,
    ) -> Result<Option<(ParsedFeishuEvent, Arc<[u8]>)>, ChannelError> {
        loop {
            let payload = self.socket.read_frame().await?;
            let frame = FeishuFrame::decode(&payload)
                .map_err(|err| ChannelError::Backend(Arc::from(err)))?;
            if frame.method == 0 {
                if header_value(&frame.headers, "type") == Some("ping") {
                    self.socket
                        .write_frame(&pong_frame(&frame).encode())
                        .await?;
                }
                continue;
            }
            if frame.method != 1 {
                continue;
            }

            let frame_type = header_value(&frame.headers, "type");
            if frame_type != Some("event") {
                continue;
            }
            let payload = self.event_payload(&frame)?;
            let Some(payload) = payload else {
                continue;
            };
            let payload: Value = serde_json::from_slice(&payload)
                .map_err(|err| {
                    ChannelError::Backend(Arc::from(format!(
                        "Feishu event payload JSON parse failed: {err}; payload_encoding={}, payload_type={}",
                        frame.payload_encoding, frame.payload_type
                    )))
                })?;
            if let Some(parsed) = parse_event(&payload, channel_id, admission, receive_id_type) {
                let acknowledgement = Arc::<[u8]>::from(success_frame(&frame, 0).encode());
                return Ok(Some((parsed, acknowledgement)));
            }
            if log_receive_errors {
                if let Some(reason) = feishu_drop_reason(&payload, admission) {
                    eprintln!("feishu event dropped: {reason}");
                }
            }
        }
    }

    pub(super) async fn acknowledge(&mut self, token: &[u8]) -> Result<(), ChannelError> {
        self.socket.write_frame(token).await
    }

    fn event_payload(&mut self, frame: &FeishuFrame) -> Result<Option<Vec<u8>>, ChannelError> {
        if frame.payload.is_empty() {
            return Ok(None);
        }
        self.fragments.accept(
            header_value(&frame.headers, "sum"),
            header_value(&frame.headers, "seq"),
            header_value(&frame.headers, "message_id"),
            &frame.payload,
        )
    }
}
