//! Bounded delegation-grant templates and principal-bound runtime instances.

use super::{
    decision_permissiveness, permissiveness, Policy, PolicyAction, PolicyDecision, PolicyRule,
    PolicyVerb,
};
use agentos_interfaces::orchestrator::Plan;
use agentos_proto::{base64url, ActorPrincipal, AgentId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

/// Maximum lifetime of a runtime delegation grant.
///
/// Grant templates name a shorter lifetime when appropriate. A longer-lived
/// or standing authority needs a replacement ADR: it must not be smuggled in
/// by removing the expiry from this object.
pub const MAX_DELEGATION_GRANT_LIFETIME_SECS: u64 = 3_600;

/// Configuration-time authority, not yet usable by any actor.
///
/// A template deliberately has no principal, delegatee, generation, grant ID,
/// or absolute expiry. Those facts exist only once a particular delegation is
/// started, at which point [`DelegatedAuthority::issue`] binds them.
#[derive(Clone, Debug, PartialEq)]
pub struct DelegationGrantTemplate {
    pub action: PolicyAction,
    pub decision: PolicyVerb,
    pub arg_equals: BTreeMap<Arc<str>, Value>,
    pub reason: Arc<str>,
    pub lifetime_secs: u64,
}

/// One generation of one parent actor delegating to one registered child.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DelegationScope {
    initiating_actor: ActorPrincipal,
    delegatee: AgentId,
    policy_id: Arc<str>,
    generation_id: Arc<str>,
    issued_at: u64,
}

impl DelegationScope {
    /// Mint a fresh delegation generation at `issued_at`.
    pub fn mint(
        initiating_actor: ActorPrincipal,
        delegatee: AgentId,
        policy_id: impl Into<Arc<str>>,
        issued_at: u64,
    ) -> Result<Self, DelegationGrantError> {
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| DelegationGrantError::EntropyUnavailable)?;
        Ok(Self {
            initiating_actor,
            delegatee,
            policy_id: policy_id.into(),
            generation_id: Arc::from(format!("delegation.v1.{}", base64url(&nonce))),
            issued_at,
        })
    }

    /// Reconstruct a previously minted generation, for example when resuming
    /// a paused child. This creates scope, not authority; a runtime grant must
    /// still have been issued for exactly the same value.
    pub fn for_generation(
        initiating_actor: ActorPrincipal,
        delegatee: AgentId,
        policy_id: impl Into<Arc<str>>,
        generation_id: impl Into<Arc<str>>,
        issued_at: u64,
    ) -> Result<Self, DelegationGrantError> {
        let scope = Self {
            initiating_actor,
            delegatee,
            policy_id: policy_id.into(),
            generation_id: generation_id.into(),
            issued_at,
        };
        if !scope.generation_id.starts_with("delegation.v1.") {
            return Err(DelegationGrantError::InvalidGeneration);
        }
        Ok(scope)
    }

    pub(crate) fn validate_binding(
        &self,
        initiating_actor: &ActorPrincipal,
        delegatee: &AgentId,
        policy_id: &str,
    ) -> Result<(), DelegationGrantError> {
        if !self.generation_id.starts_with("delegation.v1.") {
            return Err(DelegationGrantError::InvalidGeneration);
        }
        if &self.initiating_actor != initiating_actor
            || &self.delegatee != delegatee
            || self.policy_id.as_ref() != policy_id
        {
            return Err(DelegationGrantError::BindingMismatch);
        }
        Ok(())
    }

    pub fn initiating_actor(&self) -> &ActorPrincipal {
        &self.initiating_actor
    }

    pub fn delegatee(&self) -> &AgentId {
        &self.delegatee
    }

    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    pub fn generation_id(&self) -> &str {
        &self.generation_id
    }

    pub fn issued_at(&self) -> u64 {
        self.issued_at
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum DelegationGrantError {
    #[error("delegation grant lifetime must be at least one second")]
    ZeroLifetime,
    #[error("delegation grants must name one non-empty tool")]
    InvalidAction,
    #[error("delegation grants may widen only to allow or ask_user")]
    InvalidDecision,
    #[error("delegation grants require a non-empty operator reason")]
    EmptyReason,
    #[error("delegation grant lifetime {requested_secs}s exceeds the maximum {max_secs}s")]
    LifetimeTooLong { requested_secs: u64, max_secs: u64 },
    #[error("delegation grant expiry overflows the Unix timestamp")]
    ExpiryOverflow,
    #[error("secure entropy is unavailable for a delegation generation")]
    EntropyUnavailable,
    #[error("stored delegation generation has an invalid identifier")]
    InvalidGeneration,
    #[error("delegation grant is bound to another actor, delegatee, policy, or generation")]
    BindingMismatch,
}

/// Runtime authority bound to one actor, delegatee, and delegation generation.
#[derive(Clone, Debug, PartialEq)]
pub struct DelegatedAuthority {
    scope: DelegationScope,
    parent_policy: Policy,
    grants: Vec<DelegationGrant>,
}

impl DelegatedAuthority {
    pub fn issue(
        templates: &[DelegationGrantTemplate],
        parent_policy: &Policy,
        scope: DelegationScope,
    ) -> Result<Self, DelegationGrantError> {
        let grants = templates
            .iter()
            .enumerate()
            .map(|(index, template)| DelegationGrant::issue(template, &scope, index))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            scope,
            parent_policy: parent_policy.clone(),
            grants,
        })
    }

    pub fn scope(&self) -> &DelegationScope {
        &self.scope
    }

    pub fn grants(&self) -> &[DelegationGrant] {
        &self.grants
    }

    pub(crate) fn restricted(mut self, grants: Vec<DelegationGrant>) -> Self {
        self.grants = grants;
        self
    }

    /// The grant required by an allowed child action, if the parent would not
    /// itself have allowed that exact action.
    pub fn grant_for_action(
        &self,
        plan: &Plan,
        now: u64,
    ) -> Result<Option<&DelegationGrant>, Arc<str>> {
        if matches!(self.parent_policy.decide(plan), PolicyDecision::Allow) {
            return Ok(None);
        }
        self.grants
            .iter()
            .find(|grant| {
                grant.permits(
                    plan,
                    &PolicyDecision::Allow,
                    &self.parent_policy,
                    &self.scope,
                    now,
                )
            })
            .map(Some)
            .ok_or_else(|| {
                Arc::from(
                    "delegation grant is absent, expired, clock-invalid, or bound to another actor or generation",
                )
            })
    }
}

/// One runtime grant issued for exactly one delegation scope.
///
/// The explicit form of what the old tool-name escape hatch did implicitly.
/// A grant names exactly one tool, may pin exact argument values, and elevates
/// only against the immediate parent. Its mandatory expiry and stable ID are
/// created from a bounded template for one delegation generation.
#[derive(Clone, Debug, PartialEq)]
pub struct DelegationGrant {
    id: Arc<str>,
    scope: DelegationScope,
    action: PolicyAction,
    decision: PolicyVerb,
    arg_equals: BTreeMap<Arc<str>, Value>,
    reason: Arc<str>,
    expires_at: u64,
}

impl DelegationGrant {
    fn issue(
        template: &DelegationGrantTemplate,
        scope: &DelegationScope,
        template_index: usize,
    ) -> Result<Self, DelegationGrantError> {
        if !matches!(&template.action, PolicyAction::Tool(tool) if !tool.trim().is_empty()) {
            return Err(DelegationGrantError::InvalidAction);
        }
        if matches!(template.decision, PolicyVerb::Deny) {
            return Err(DelegationGrantError::InvalidDecision);
        }
        if template.reason.trim().is_empty() {
            return Err(DelegationGrantError::EmptyReason);
        }
        if template.lifetime_secs == 0 {
            return Err(DelegationGrantError::ZeroLifetime);
        }
        if template.lifetime_secs > MAX_DELEGATION_GRANT_LIFETIME_SECS {
            return Err(DelegationGrantError::LifetimeTooLong {
                requested_secs: template.lifetime_secs,
                max_secs: MAX_DELEGATION_GRANT_LIFETIME_SECS,
            });
        }
        let expires_at = scope
            .issued_at
            .checked_add(template.lifetime_secs)
            .ok_or(DelegationGrantError::ExpiryOverflow)?;
        let mut hasher = Sha256::new();
        hasher.update(b"agentos.delegation-grant.v1");
        hasher.update(
            serde_json::to_vec(scope)
                .expect("a delegation scope contains only serializable wire values"),
        );
        hasher.update(template_index.to_be_bytes());
        hasher.update(
            serde_json::to_vec(&template.action)
                .expect("a policy action contains only serializable wire values"),
        );
        hasher.update(
            serde_json::to_vec(&template.decision)
                .expect("a policy decision contains only serializable wire values"),
        );
        hasher.update(
            serde_json::to_vec(&template.arg_equals)
                .expect("grant arguments are already validated JSON values"),
        );
        hasher.update(expires_at.to_be_bytes());
        let id = Arc::from(format!("grant.v1.{}", base64url(&hasher.finalize())));
        Ok(Self {
            id,
            scope: scope.clone(),
            action: template.action.clone(),
            decision: template.decision.clone(),
            arg_equals: template.arg_equals.clone(),
            reason: Arc::clone(&template.reason),
            expires_at,
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }

    pub fn scope(&self) -> &DelegationScope {
        &self.scope
    }

    pub(super) fn permits(
        &self,
        call: &Plan,
        decision: &PolicyDecision,
        parent: &Policy,
        scope: &DelegationScope,
        now: u64,
    ) -> bool {
        if &self.scope != scope || !self.is_live(now) {
            return false;
        }
        if decision_permissiveness(decision) > permissiveness(&self.decision) {
            return false;
        }
        let tool_args = match call {
            Plan::CallTool(tool_call) => serde_json::from_str::<Value>(tool_call.args.get()).ok(),
            _ => None,
        };
        if !self.as_rule().matches(call, tool_args.as_ref()) {
            return false;
        }
        !matches!(parent.decide(call), PolicyDecision::Deny { .. })
    }

    /// Whether the grant covers `plan` for this exact scope and wall clock.
    pub fn covers_at(&self, plan: &Plan, scope: &DelegationScope, now: u64) -> bool {
        if &self.scope != scope || !self.is_live(now) {
            return false;
        }
        let tool_args = match plan {
            Plan::CallTool(tool_call) => serde_json::from_str::<Value>(tool_call.args.get()).ok(),
            _ => None,
        };
        self.as_rule().matches(plan, tool_args.as_ref())
    }

    fn is_live(&self, now: u64) -> bool {
        // Rollback before issuance and forward movement to expiry both fail
        // closed. Returning to the interval can make the in-process grant live
        // again; monotonic anti-rollback persistence is not claimed here.
        now >= self.scope.issued_at && now < self.expires_at
    }

    fn as_rule(&self) -> PolicyRule {
        PolicyRule {
            action: self.action.clone(),
            decision: self.decision.clone(),
            reason: None,
            arg_equals: self.arg_equals.clone(),
        }
    }
}
