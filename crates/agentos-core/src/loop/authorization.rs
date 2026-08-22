//! Proof that a plan crossed `Approve` (M6 / `STATE-001`, deliverable 8).
//!
//! `RunLoopState` is a public enum over context structs with public fields, so
//! the *variants* are compiler-checked but the *ordering* was not: a caller
//! could hand-build an `ActCtx` carrying a tool call and hand it to `step`,
//! and `Act` executed it without a policy decision ever having been made.
//! `AGENTS.md` said review enforces the ordering, which is another way of
//! saying nothing does.
//!
//! Full typestate would cost several hundred lines and still need an
//! unchecked constructor for resuming a `Paused` run, which is the case that
//! matters. This closes the same hole with a witness:
//!
//! - [`Authorization`] has **no public constructor**, so `ActCtx` cannot be
//!   built by a literal outside this crate at all. That is the fabrication
//!   half.
//! - It carries a fingerprint of the plan it was issued for, so a caller
//!   holding a legitimate `ActCtx` cannot assign a different plan over the
//!   approved one. That is the substitution half — and it is the half a plain
//!   marker type would miss.
//! - `Act` re-decides the plan against the live policy before running it, so a
//!   denial that appeared between `Approve` and `Act` still stops the call.
//!   `ask_user` passes only when a human actually answered, which is exactly
//!   what resuming an approved pause means.

use crate::approve::{Policy, PolicyDecision};
use agentos_interfaces::orchestrator::Plan;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Why a plan in `ActCtx` may run.
///
/// Minted only by `Approve` and by the resume path. See the module docs for
/// why the fingerprint is part of it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authorization {
    plan: Arc<str>,
    /// Set when a human answered an `ask_user` pause. `Act` then accepts a
    /// policy that still says `ask_user`, which it must — being approved is
    /// precisely the state of having been asked about and permitted.
    human_approved: bool,
}

/// Why `Act` refused to run a plan it was handed.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Unauthorized {
    /// The plan is not the one `Approve` decided.
    #[error("the plan reaching Act is not the one Approve decided")]
    PlanSubstituted,
    /// The policy denies it now, whatever it said before.
    #[error("policy denies this now: {reason}")]
    Denied { reason: Arc<str> },
    /// The policy wants a human and no human has answered.
    #[error("policy requires approval and none was given: {reason}")]
    Unanswered { reason: Arc<str> },
}

impl Authorization {
    /// Issued by the `Approve` state for a plan the policy allowed.
    pub(super) fn allowed(plan: &Plan) -> Self {
        Self {
            plan: fingerprint(plan),
            human_approved: false,
        }
    }

    /// Issued when a paused run resumes because somebody said yes.
    pub(super) fn approved_by_human(plan: &Plan) -> Self {
        Self {
            plan: fingerprint(plan),
            human_approved: true,
        }
    }

    /// Whether `plan` may run under this authorization and `policy`.
    ///
    /// Cheap enough for the hot path: one SHA-256 over a short canonical
    /// string, plus the policy decision the loop would make anyway.
    pub(super) fn admits(&self, plan: &Plan, policy: &Policy) -> Result<(), Unauthorized> {
        if self.plan != fingerprint(plan) {
            return Err(Unauthorized::PlanSubstituted);
        }
        match policy.decide(plan) {
            PolicyDecision::Allow => Ok(()),
            PolicyDecision::Deny { reason } => Err(Unauthorized::Denied { reason }),
            PolicyDecision::AskUser { reason } => {
                if self.human_approved {
                    Ok(())
                } else {
                    Err(Unauthorized::Unanswered { reason })
                }
            }
        }
    }
}

/// What a plan is *about*, for the `subject` of a safety event: the tool a
/// call names, the agent a handoff targets, the sub-agent a delegation runs.
pub(super) fn plan_subject(plan: &Plan) -> Arc<str> {
    match plan {
        Plan::CallTool(call) => Arc::clone(&call.name),
        Plan::CallTools(_) => Arc::from("tool_batch"),
        Plan::Delegate(spec) => Arc::from(spec.agent_id.as_str()),
        Plan::Escalate(spec) => Arc::clone(&spec.template.name),
        Plan::Handoff(agent_id, _) => Arc::from(agent_id.as_str()),
        Plan::ResumeSubAgent { spec, .. } => Arc::from(spec.agent_id.as_str()),
        Plan::Reply(_) => Arc::from("reply"),
    }
}

/// A canonical name for what a plan would do, hashed.
///
/// Hashed rather than kept verbatim because a tool call's arguments are
/// model-supplied and unbounded, and this value is held in memory for the
/// whole `Act` state. Two plans agree here exactly when they would ask the
/// policy engine the same question — which is the property the check needs,
/// and the reason a `Reply` (which the policy always allows) is distinguished
/// only by its variant.
fn fingerprint(plan: &Plan) -> Arc<str> {
    let mut hasher = Sha256::new();
    match plan {
        Plan::Reply(_) => hasher.update(b"reply"),
        Plan::CallTool(call) => {
            hasher.update(b"tool\0");
            hasher.update(call.name.as_bytes());
            hasher.update(b"\0");
            hasher.update(call.args.get().as_bytes());
        }
        Plan::CallTools(calls) => {
            hasher.update(b"tools\0");
            for call in calls {
                hasher.update(call.name.as_bytes());
                hasher.update(b"\0");
                hasher.update(call.args.get().as_bytes());
                hasher.update(b"\0");
            }
        }
        Plan::Delegate(spec) => {
            hasher.update(b"delegate\0");
            hasher.update(spec.agent_id.as_str().as_bytes());
            hasher.update(b"\0");
            hasher.update(spec.policy_id.as_bytes());
        }
        Plan::Escalate(spec) => {
            hasher.update(b"escalate\0");
            hasher.update(spec.template.name.as_bytes());
            hasher.update(b"\0");
            hasher.update(spec.task_id.as_str().as_bytes());
        }
        Plan::Handoff(agent_id, _) => {
            hasher.update(b"handoff\0");
            hasher.update(agent_id.as_str().as_bytes());
        }
        Plan::ResumeSubAgent { spec, .. } => {
            hasher.update(b"resume\0");
            hasher.update(spec.agent_id.as_str().as_bytes());
            hasher.update(b"\0");
            hasher.update(spec.policy_id.as_bytes());
        }
    }
    let mut rendered = String::with_capacity(64);
    for byte in hasher.finalize() {
        rendered.push_str(&format!("{byte:02x}"));
    }
    Arc::from(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approve::PolicyVerb;
    use agentos_proto::{AgentId, ToolCall, ToolCallId};
    use serde_json::value::RawValue;

    fn call(name: &str, args: &str) -> Plan {
        Plan::CallTool(ToolCall {
            id: ToolCallId::new("c"),
            name: Arc::from(name),
            args: RawValue::from_string(args.to_owned()).expect("valid JSON"),
        })
    }

    #[test]
    fn the_same_call_fingerprints_the_same_and_a_different_one_does_not() {
        assert_eq!(
            fingerprint(&call("shell", r#"{"command":"ls"}"#)),
            fingerprint(&call("shell", r#"{"command":"ls"}"#))
        );
        // Arguments are part of it: a policy constrained on `arg_equals` would
        // decide these two differently.
        assert_ne!(
            fingerprint(&call("shell", r#"{"command":"ls"}"#)),
            fingerprint(&call("shell", r#"{"command":"rm"}"#))
        );
        assert_ne!(
            fingerprint(&call("shell", "{}")),
            fingerprint(&call("file", "{}"))
        );
        // A tool named to collide with the variant tag cannot forge another
        // variant's fingerprint, because the tag is separated by a NUL the
        // name cannot contain in any shipped tool and is hashed positionally.
        assert_ne!(
            fingerprint(&Plan::Handoff(AgentId::new("x"), None)),
            fingerprint(&call("handoff", "\"x\""))
        );
    }

    #[test]
    fn a_swapped_plan_is_refused_even_though_the_policy_allows_it() {
        // The substitution half. Both calls are allowed, so a re-check against
        // the policy alone would pass this; the authorization was issued for
        // one of them.
        let policy = Policy::allow_tools(["shell", "file"]);
        let authorization = Authorization::allowed(&call("shell", "{}"));
        assert_eq!(authorization.admits(&call("shell", "{}"), &policy), Ok(()));
        assert_eq!(
            authorization.admits(&call("file", "{}"), &policy),
            Err(Unauthorized::PlanSubstituted)
        );
    }

    #[test]
    fn an_approval_that_nobody_answered_does_not_admit_the_call() {
        let policy = Policy::ask_user_tools(["shell"]);
        let plan = call("shell", "{}");
        assert!(matches!(
            Authorization::allowed(&plan).admits(&plan, &policy),
            Err(Unauthorized::Unanswered { .. })
        ));
        // The same plan after a human said yes. Without this the resume path
        // would re-pause forever.
        assert_eq!(
            Authorization::approved_by_human(&plan).admits(&plan, &policy),
            Ok(())
        );
    }

    #[test]
    fn a_denial_that_appeared_after_approve_still_stops_the_call() {
        let plan = call("shell", "{}");
        let authorization = Authorization::approved_by_human(&plan);
        let denying = Policy {
            rules: Vec::new(),
            default_decision: PolicyVerb::Deny,
        };
        assert!(matches!(
            authorization.admits(&plan, &denying),
            Err(Unauthorized::Denied { .. })
        ));
    }
}
