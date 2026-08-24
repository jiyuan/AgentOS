use serde::{Deserialize, Serialize};
use std::sync::Arc;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Arc<str>);

        impl $name {
            pub fn new(value: impl Into<Arc<str>>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

id_type!(AgentId);
id_type!(ApprovalInstanceId);
id_type!(ChannelId);
id_type!(ConversationId);
id_type!(InterruptionId);
id_type!(Namespace);
id_type!(RecordId);
id_type!(RunId);
id_type!(SpanId);
id_type!(TaskId);
id_type!(ToolCallId);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SchemaVersion(pub u16);

impl Default for SchemaVersion {
    fn default() -> Self {
        Self(1)
    }
}
