use super::event::{feishu_drop_reason, parse_event, ParsedFeishuEvent};
use super::proto::{header_value, pong_frame, success_frame, FeishuFrame};
use super::websocket::WebSocketConnection;
use crate::channels::auth::RemoteIngressPolicy;
use agentos_interfaces::ChannelError;
use agentos_proto::ChannelId;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FeishuEndpoint {
    pub(super) url: String,
}

pub(super) struct FeishuLongConnection {
    socket: WebSocketConnection,
    fragments: HashMap<String, Vec<Option<Vec<u8>>>>,
}

impl FeishuLongConnection {
    pub(super) async fn connect(endpoint: &FeishuEndpoint) -> Result<Self, ChannelError> {
        Ok(Self {
            socket: WebSocketConnection::connect(&endpoint.url).await?,
            fragments: HashMap::new(),
        })
    }

    pub(super) async fn receive_next_event(
        &mut self,
        channel_id: &ChannelId,
        ingress_policy: &RemoteIngressPolicy,
        receive_id_type: &str,
        log_receive_errors: bool,
    ) -> Result<Option<ParsedFeishuEvent>, ChannelError> {
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
            let started = Instant::now();
            self.ack_event(&frame, started).await?;
            if let Some(parsed) = parse_event(&payload, channel_id, ingress_policy, receive_id_type)
            {
                return Ok(Some(parsed));
            }
            if log_receive_errors {
                if let Some(reason) = feishu_drop_reason(&payload, ingress_policy) {
                    eprintln!("feishu event dropped: {reason}");
                }
            }
        }
    }

    async fn ack_event(
        &mut self,
        frame: &FeishuFrame,
        started: Instant,
    ) -> Result<(), ChannelError> {
        let ack = success_frame(frame, started.elapsed().as_millis() as u64);
        self.socket.write_frame(&ack.encode()).await
    }

    fn event_payload(&mut self, frame: &FeishuFrame) -> Result<Option<Vec<u8>>, ChannelError> {
        if frame.payload.is_empty() {
            return Ok(None);
        }
        let sum = header_value(&frame.headers, "sum")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1);
        if sum <= 1 {
            return Ok(Some(frame.payload.clone()));
        }
        let seq = header_value(&frame.headers, "seq")
            .and_then(|value| value.parse::<usize>().ok())
            .ok_or_else(|| {
                ChannelError::Backend(Arc::from("Feishu fragmented event is missing seq header"))
            })?;
        let message_id = header_value(&frame.headers, "message_id")
            .ok_or_else(|| {
                ChannelError::Backend(Arc::from(
                    "Feishu fragmented event is missing message_id header",
                ))
            })?
            .to_owned();
        if seq >= sum {
            return Err(ChannelError::Backend(Arc::from(format!(
                "Feishu fragmented event has invalid seq={seq}, sum={sum}",
            ))));
        }
        let entry = self
            .fragments
            .entry(message_id.clone())
            .or_insert_with(|| vec![None; sum]);
        if entry.len() != sum {
            *entry = vec![None; sum];
        }
        entry[seq] = Some(frame.payload.clone());
        if entry.iter().any(Option::is_none) {
            return Ok(None);
        }
        let chunks = self
            .fragments
            .remove(&message_id)
            .expect("fragment entry should exist");
        let total = chunks
            .iter()
            .map(|chunk| chunk.as_ref().map_or(0, Vec::len))
            .sum();
        let mut combined = Vec::with_capacity(total);
        for chunk in chunks.into_iter().flatten() {
            combined.extend_from_slice(&chunk);
        }
        Ok(Some(combined))
    }
}
