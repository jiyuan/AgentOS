use crate::{AgentId, PrincipalKey, RunId, SchemaVersion, ToolCall};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub const DELEGATION_GRANT_SCOPES_KEY: &str = "delegation_grant_scopes";
pub const DELEGATION_GRANT_TTL_KEY: &str = "delegation_grant_ttl_secs";

/// One exact tool-call region covered by an explicitly authorized delegation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DelegationGrantScope {
    pub tool: Arc<str>,
    /// Every listed top-level argument must equal the configured value.
    /// Empty constraints are rejected by the runtime when grants are loaded.
    pub arg_equals: BTreeMap<Arc<str>, Value>,
}

impl DelegationGrantScope {
    pub fn covers(&self, call: &ToolCall) -> bool {
        if self.tool != call.name {
            return false;
        }
        let Ok(Value::Object(arguments)) = serde_json::from_str::<Value>(call.args.get()) else {
            return false;
        };
        self.arg_equals
            .iter()
            .all(|(key, expected)| arguments.get(key.as_ref()) == Some(expected))
    }
}

/// Versioned, persisted evidence that a principal authorized bounded child
/// authority outside the ordinary parent/child policy lattice.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DelegationGrant {
    pub version: SchemaVersion,
    pub grant_id: Arc<str>,
    pub authorized_by: PrincipalKey,
    pub parent_run_id: RunId,
    pub delegatee: AgentId,
    pub policy_id: Arc<str>,
    pub issued_at_unix: u64,
    pub expires_at_unix: u64,
    /// Grants are deliberately non-transitive. This persisted field makes the
    /// default explicit and gives a future schema a fail-closed migration path.
    pub transitive: bool,
    pub scopes: Vec<DelegationGrantScope>,
}

impl DelegationGrant {
    pub fn covers(&self, agent_id: &AgentId, policy_id: &str, call: &ToolCall) -> bool {
        !self.transitive
            && self.delegatee == *agent_id
            && self.policy_id.as_ref() == policy_id
            && self.scopes.iter().any(|scope| scope.covers(call))
    }

    pub fn is_active_at(&self, now_unix: u64) -> bool {
        self.issued_at_unix <= now_unix && now_unix < self.expires_at_unix
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChannelId, ConversationId, SenderIdentity, ToolCallId};
    use serde_json::{json, value::RawValue};

    fn grant() -> DelegationGrant {
        DelegationGrant {
            version: SchemaVersion::default(),
            grant_id: Arc::from("grant-1"),
            authorized_by: PrincipalKey::v1(
                AgentId::new("parent"),
                ChannelId::new("cli"),
                ConversationId::new("conversation"),
                SenderIdentity::identified("operator"),
            ),
            parent_run_id: RunId::new("parent-run"),
            delegatee: AgentId::new("child"),
            policy_id: Arc::from("child-policy"),
            issued_at_unix: 10,
            expires_at_unix: 20,
            transitive: false,
            scopes: vec![DelegationGrantScope {
                tool: Arc::from("file"),
                arg_equals: BTreeMap::from([(Arc::from("operation"), json!("read"))]),
            }],
        }
    }

    #[test]
    fn scope_is_exact_and_grant_is_time_bounded_and_non_transitive() {
        let read = ToolCall {
            id: ToolCallId::new("read"),
            name: Arc::from("file"),
            args: RawValue::from_string(json!({"operation": "read", "path": "a"}).to_string())
                .expect("valid args"),
        };
        let write = ToolCall {
            id: ToolCallId::new("write"),
            name: Arc::from("file"),
            args: RawValue::from_string(json!({"operation": "write", "path": "a"}).to_string())
                .expect("valid args"),
        };
        let grant = grant();

        assert!(grant.covers(&AgentId::new("child"), "child-policy", &read));
        assert!(!grant.covers(&AgentId::new("child"), "child-policy", &write));
        assert!(!grant.covers(&AgentId::new("grandchild"), "child-policy", &read));
        assert!(grant.is_active_at(10));
        assert!(!grant.is_active_at(20));
    }

    #[test]
    fn grant_round_trips_as_a_versioned_persisted_type() {
        let expected = grant();
        let encoded = serde_json::to_string(&expected).expect("grant serializes");
        let decoded: DelegationGrant = serde_json::from_str(&encoded).expect("grant deserializes");
        assert_eq!(decoded, expected);
    }
}
