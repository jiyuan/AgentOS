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
//! - [`AuthorizedPlan`] owns the plan and the witness together, and both are
//!   private inside `ActCtx`. A caller can neither fabricate an act context nor
//!   replace the plan or the denied/executable disposition after approval.
//! - [`Authorization`] carries a canonical commitment to the complete
//!   serialized [`Plan`]. It is not a hand-maintained selection of fields, so
//!   tool ids, routing payloads, child state, metadata, and future serialized
//!   fields all participate automatically.
//! - `Act` re-decides the plan against the live policy before running it, so a
//!   denial that appeared between `Approve` and `Act` still stops the call.
//!   `ask_user` passes only when a human actually answered, which is exactly
//!   what resuming an approved pause means.

use crate::approve::{Policy, PolicyDecision};
use agentos_interfaces::orchestrator::Plan;
use agentos_proto::ToolCall;
use sha2::{Digest, Sha256};
use std::io::{self, Write};
use std::sync::Arc;

const PLAN_COMMITMENT_DOMAIN: &[u8] = b"agentos.plan-authorization";
const PLAN_COMMITMENT_VERSION: u8 = 1;

/// A domain-separated SHA-256 commitment to one complete serialized plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PlanCommitment([u8; 32]);

/// Why a plan in `ActCtx` may run.
///
/// Minted only by `Approve` and by the resume path. See the module docs for
/// why the complete plan commitment is part of it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authorization {
    plan: PlanCommitment,
    /// Set when a human answered an `ask_user` pause. `Act` then accepts a
    /// policy that still says `ask_user`, which it must — being approved is
    /// precisely the state of having been asked about and permitted.
    human_approved: bool,
}

/// A plan whose approval witness and execution disposition cannot be changed
/// independently after `Approve` constructs it.
#[derive(Debug)]
pub(super) struct AuthorizedPlan {
    plan: Plan,
    authorization: Authorization,
    denied_tool: Option<Arc<str>>,
}

/// The only two dispositions `Act` can receive from an approved plan.
pub(super) enum ActPlan {
    Execute(Plan),
    DeniedTool { call: ToolCall, reason: Arc<str> },
}

/// A plan refused by its commitment or the live policy.
pub(super) struct AuthorizationFailure {
    pub plan: Box<Plan>,
    pub error: Unauthorized,
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
            plan: commitment(plan),
            human_approved: false,
        }
    }

    /// Issued when a paused run resumes because somebody said yes.
    pub(super) fn approved_by_human(plan: &Plan) -> Self {
        Self {
            plan: commitment(plan),
            human_approved: true,
        }
    }

    /// Whether `plan` may run under this authorization and `policy`.
    ///
    /// One streaming SHA-256 over canonical JSON, plus the live policy
    /// decision the loop would make anyway. No plan-sized allocation is made.
    pub(super) fn admits(&self, plan: &Plan, policy: &Policy) -> Result<(), Unauthorized> {
        if self.plan != commitment(plan) {
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

impl AuthorizedPlan {
    pub(super) fn allowed(plan: Plan) -> Self {
        let authorization = Authorization::allowed(&plan);
        Self {
            plan,
            authorization,
            denied_tool: None,
        }
    }

    pub(super) fn approved_by_human(plan: Plan) -> Self {
        let authorization = Authorization::approved_by_human(&plan);
        Self {
            plan,
            authorization,
            denied_tool: None,
        }
    }

    pub(super) fn denied_tool(plan: Plan, reason: Arc<str>) -> Self {
        let authorization = Authorization::allowed(&plan);
        Self {
            plan,
            authorization,
            denied_tool: Some(reason),
        }
    }

    /// Consume the inseparable plan/witness pair for `Act`.
    ///
    /// A denied disposition is valid only for a single tool call. Checking it
    /// here prevents a future constructor mistake from using the denial path
    /// to skip the live-policy decision for a structural action.
    pub(super) fn into_act(self, policy: &Policy) -> Result<ActPlan, AuthorizationFailure> {
        if self.authorization.plan != commitment(&self.plan) {
            return Err(AuthorizationFailure {
                plan: Box::new(self.plan),
                error: Unauthorized::PlanSubstituted,
            });
        }
        if let Some(reason) = self.denied_tool {
            return match self.plan {
                Plan::CallTool(call) => Ok(ActPlan::DeniedTool { call, reason }),
                plan => Err(AuthorizationFailure {
                    plan: Box::new(plan),
                    error: Unauthorized::PlanSubstituted,
                }),
            };
        }
        match self.authorization.admits(&self.plan, policy) {
            Ok(()) => Ok(ActPlan::Execute(self.plan)),
            Err(error) => Err(AuthorizationFailure {
                plan: Box::new(self.plan),
                error,
            }),
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

/// Commit to the complete serde representation of `Plan` (`AUTH-005`).
///
/// JSON is deterministic here: every struct has declaration order, metadata
/// uses `BTreeMap`, and this workspace does not enable serde_json's
/// insertion-order map feature. Streaming into the digest avoids retaining a
/// second, attacker-sized copy of tool arguments or resumed child state.
fn commitment(plan: &Plan) -> PlanCommitment {
    let mut hasher = Sha256::new();
    hasher.update(PLAN_COMMITMENT_DOMAIN);
    hasher.update([PLAN_COMMITMENT_VERSION]);
    serde_json::to_writer(DigestWriter(&mut hasher), plan)
        .expect("Plan serialization contains no fallible map keys or non-finite numbers");
    PlanCommitment(hasher.finalize().into())
}

struct DigestWriter<'a>(&'a mut Sha256);

impl Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approve::PolicyVerb;
    use agentos_interfaces::orchestrator::{
        OrchestratorTemplate, Stage, SubAgentSpec, SubOrchSpec,
    };
    use agentos_interfaces::RunState;
    use agentos_proto::{
        AgentId, ChannelId, ConversationId, Message, MessageRole, RunId, TaskId, ToolCall,
        ToolCallId,
    };
    use serde_json::value::RawValue;
    use serde_json::Value;
    use std::collections::BTreeMap;

    fn call_with(id: &str, name: &str, args: &str) -> Plan {
        Plan::CallTool(ToolCall {
            id: ToolCallId::new(id),
            name: Arc::from(name),
            args: RawValue::from_string(args.to_owned()).expect("valid JSON"),
        })
    }

    fn call(name: &str, args: &str) -> Plan {
        call_with("c", name, args)
    }

    fn metadata(value: &str) -> BTreeMap<Arc<str>, Value> {
        BTreeMap::from([(Arc::from("scope"), Value::String(value.to_owned()))])
    }

    fn child(agent: &str, policy: &str, value: &str) -> SubAgentSpec {
        SubAgentSpec {
            agent_id: AgentId::new(agent),
            policy_id: Arc::from(policy),
            metadata: metadata(value),
        }
    }

    fn escalation() -> Plan {
        Plan::Escalate(SubOrchSpec {
            template: OrchestratorTemplate {
                name: Arc::from("pipeline"),
                stages: vec![Stage {
                    name: Arc::from("review"),
                    agent: child("reviewer", "restricted", "stage"),
                    depends_on: vec![Arc::from("prepare")],
                }],
            },
            task_id: TaskId::new("task-1"),
            policy_id: Arc::from("pipeline-policy"),
            metadata: metadata("escalate"),
        })
    }

    fn resumed() -> Plan {
        let mut state = RunState::new(RunId::new("child-run"), AgentId::new("worker"));
        state.task_id = Some(TaskId::new("child-task"));
        Plan::ResumeSubAgent {
            spec: child("worker", "restricted", "resume"),
            child_channel_id: ChannelId::new("subagent:worker"),
            child_conversation_id: ConversationId::new("child.v1.identity"),
            child_state: Box::new(state),
        }
    }

    fn clone_plan(plan: &Plan) -> Plan {
        serde_json::from_value(
            serde_json::to_value(plan).expect("fixture plan serializes without loss"),
        )
        .expect("fixture plan round trips")
    }

    fn assert_substitutions_rejected(original: Plan, substitutions: Vec<(&str, Plan)>) {
        let policy = Policy {
            rules: Vec::new(),
            default_decision: PolicyVerb::Allow,
        };
        let authorization = Authorization::allowed(&original);
        assert_eq!(authorization.admits(&original, &policy), Ok(()));
        for (field, substitution) in substitutions {
            assert_eq!(
                authorization.admits(&substitution, &policy),
                Err(Unauthorized::PlanSubstituted),
                "mutating {field} preserved the authorization commitment"
            );
        }
    }

    #[test]
    fn the_same_plan_commits_the_same_and_a_different_one_does_not() {
        assert_eq!(
            commitment(&call("shell", r#"{"command":"ls"}"#)),
            commitment(&call("shell", r#"{"command":"ls"}"#))
        );
        assert_ne!(
            commitment(&call("shell", r#"{"command":"ls"}"#)),
            commitment(&call("shell", r#"{"command":"rm"}"#))
        );
        assert_ne!(
            commitment(&call("shell", "{}")),
            commitment(&call("file", "{}"))
        );
        assert_ne!(
            commitment(&Plan::Handoff(AgentId::new("x"), None)),
            commitment(&call("handoff", "\"x\""))
        );
    }

    /// AF-009: every field omitted by the old hand-maintained fingerprint is
    /// independently changed while the original authorization is retained.
    #[test]
    fn mutating_any_plan_field_invalidates_authorization() {
        assert_substitutions_rejected(
            call_with("call-1", "shell", r#"{"command":"ls"}"#),
            vec![
                (
                    "CallTool.id",
                    call_with("call-2", "shell", r#"{"command":"ls"}"#),
                ),
                (
                    "CallTool.name",
                    call_with("call-1", "file", r#"{"command":"ls"}"#),
                ),
                (
                    "CallTool.args",
                    call_with("call-1", "shell", r#"{"command":"rm"}"#),
                ),
            ],
        );

        let delegate = Plan::Delegate(child("worker", "restricted", "one"));
        let mut delegate_agent = clone_plan(&delegate);
        let Plan::Delegate(spec) = &mut delegate_agent else {
            unreachable!("fixture variant")
        };
        spec.agent_id = AgentId::new("other");
        let mut delegate_policy = clone_plan(&delegate);
        let Plan::Delegate(spec) = &mut delegate_policy else {
            unreachable!("fixture variant")
        };
        spec.policy_id = Arc::from("other-policy");
        let mut delegate_metadata = clone_plan(&delegate);
        let Plan::Delegate(spec) = &mut delegate_metadata else {
            unreachable!("fixture variant")
        };
        spec.metadata = metadata("two");
        assert_substitutions_rejected(
            delegate,
            vec![
                ("Delegate.agent_id", delegate_agent),
                ("Delegate.policy_id", delegate_policy),
                ("Delegate.metadata", delegate_metadata),
            ],
        );

        assert_substitutions_rejected(
            Plan::Handoff(AgentId::new("target"), Some(Value::from("payload"))),
            vec![
                (
                    "Handoff.agent_id",
                    Plan::Handoff(AgentId::new("other"), Some(Value::from("payload"))),
                ),
                (
                    "Handoff.payload",
                    Plan::Handoff(AgentId::new("target"), None),
                ),
            ],
        );

        let escalate = escalation();
        let mut template_name = clone_plan(&escalate);
        let Plan::Escalate(spec) = &mut template_name else {
            unreachable!("fixture variant")
        };
        spec.template.name = Arc::from("other-pipeline");
        let mut stages = clone_plan(&escalate);
        let Plan::Escalate(spec) = &mut stages else {
            unreachable!("fixture variant")
        };
        spec.template.stages[0].depends_on.push(Arc::from("extra"));
        let mut task_id = clone_plan(&escalate);
        let Plan::Escalate(spec) = &mut task_id else {
            unreachable!("fixture variant")
        };
        spec.task_id = TaskId::new("task-2");
        let mut policy_id = clone_plan(&escalate);
        let Plan::Escalate(spec) = &mut policy_id else {
            unreachable!("fixture variant")
        };
        spec.policy_id = Arc::from("other-policy");
        let mut escalate_metadata = clone_plan(&escalate);
        let Plan::Escalate(spec) = &mut escalate_metadata else {
            unreachable!("fixture variant")
        };
        spec.metadata = metadata("other");
        assert_substitutions_rejected(
            escalate,
            vec![
                ("Escalate.template.name", template_name),
                ("Escalate.template.stages", stages),
                ("Escalate.task_id", task_id),
                ("Escalate.policy_id", policy_id),
                ("Escalate.metadata", escalate_metadata),
            ],
        );

        let resume = resumed();
        let mut resume_spec = clone_plan(&resume);
        let Plan::ResumeSubAgent { spec, .. } = &mut resume_spec else {
            unreachable!("fixture variant")
        };
        spec.metadata = metadata("other");
        let mut resume_channel = clone_plan(&resume);
        let Plan::ResumeSubAgent {
            child_channel_id, ..
        } = &mut resume_channel
        else {
            unreachable!("fixture variant")
        };
        *child_channel_id = ChannelId::new("subagent:other");
        let mut resume_conversation = clone_plan(&resume);
        let Plan::ResumeSubAgent {
            child_conversation_id,
            ..
        } = &mut resume_conversation
        else {
            unreachable!("fixture variant")
        };
        *child_conversation_id = ConversationId::new("child.v1.other");
        let mut resume_state = clone_plan(&resume);
        let Plan::ResumeSubAgent { child_state, .. } = &mut resume_state else {
            unreachable!("fixture variant")
        };
        child_state.run_id = RunId::new("other-run");
        assert_substitutions_rejected(
            resume,
            vec![
                ("ResumeSubAgent.spec", resume_spec),
                ("ResumeSubAgent.child_channel_id", resume_channel),
                ("ResumeSubAgent.child_conversation_id", resume_conversation),
                ("ResumeSubAgent.child_state", resume_state),
            ],
        );

        assert_substitutions_rejected(
            Plan::Reply(Message::text(MessageRole::Assistant, "one")),
            vec![(
                "Reply.message",
                Plan::Reply(Message::text(MessageRole::Assistant, "two")),
            )],
        );
        assert_substitutions_rejected(
            Plan::CallTools(vec![ToolCall {
                id: ToolCallId::new("batch-1"),
                name: Arc::from("shell"),
                args: RawValue::from_string("{}".to_owned()).expect("valid JSON"),
            }]),
            vec![("CallTools.calls", Plan::CallTools(Vec::new()))],
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
