use crate::approve::{DelegationGrant, Policy, PolicyAction, PolicyRule, PolicyVerb};
use crate::config::{LimitsConfig, SubAgentConfig, WorkspaceConfig};
use crate::jobs::JobRegistry;
use crate::memory::MemoryManager;
use crate::tools::{
    CronCreatorTool, CronListTool, CronRemoveTool, FileTool, HttpTool, JobKillTool, JobOutputTool,
    JobStatusTool, MemoryTool, ShellTool, SkillValidateTool, ToolRegistry,
};
use agentos_interfaces::tool::ToolSpec;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) fn build_parent_tools(
    config: &WorkspaceConfig,
    memory_manager: Arc<MemoryManager>,
    jobs: Arc<JobRegistry>,
) -> Result<ToolRegistry, String> {
    let mut tools = ToolRegistry::new();
    for tool in &config.resources.tools.enabled {
        match tool.as_ref() {
            // These need a runtime-owned handle rather than just a name, so
            // they cannot go through `register_builtin_tool`. Kept in step with
            // `RUNTIME_TOOL_NAMES`.
            "memory" => tools.register(MemoryTool::with_manager(memory_manager.clone())),
            "job_status" => tools.register(JobStatusTool::new(jobs.clone())),
            "job_output" => tools.register(JobOutputTool::new(jobs.clone())),
            "job_kill" => tools.register(JobKillTool::new(jobs.clone())),
            _ => register_builtin_tool(
                &mut tools,
                tool,
                &config.limits,
                &config.isolation.env_passthrough,
            )?,
        }
    }
    Ok(tools
        .with_timeouts(
            config.limits.tool_timeout(),
            config.limits.tool_timeout_overrides(),
        )
        .with_output_limit(config.limits.tool_output_bytes)
        .with_jobs(jobs, config.jobs.promotable.iter().cloned()))
}

pub fn phase5_policy(config: &WorkspaceConfig, mcp_specs: &[ToolSpec]) -> Policy {
    let mut policy = Policy::default();
    policy.default_decision = policy_default_decision(&config.policy.default);
    let allowlist = &config.policy.allowlist;
    for tool in &config.resources.tools.enabled {
        if is_allowlisted(allowlist, tool) {
            allowlist_tool(&mut policy, Arc::clone(tool));
        } else {
            add_builtin_tool_policy(&mut policy, tool);
        }
    }
    if !config.subagents.is_empty() {
        policy.rules.push(PolicyRule {
            action: PolicyAction::Delegate,
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        });
    }
    if !config.orchestrator_templates.is_empty() {
        policy.rules.push(PolicyRule {
            action: PolicyAction::Escalate,
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        });
    }
    for spec in mcp_specs {
        allow_tool_once(&mut policy, Arc::clone(&spec.name));
    }
    policy
}

fn is_allowlisted(allowlist: &[Arc<str>], tool: &Arc<str>) -> bool {
    allowlist.iter().any(|entry| entry == tool)
}

// Bypass any `AskUser` gating for a tool the operator has explicitly
// allowlisted. Unlike `allow_tool_once`, this does not exempt `memory` —
// the operator's intent in naming the tool is to suppress the prompts.
fn allowlist_tool(policy: &mut Policy, tool: Arc<str>) {
    if policy.rules.iter().any(|rule| {
        rule.action == PolicyAction::Tool(Arc::clone(&tool))
            && rule.decision == PolicyVerb::Allow
            && rule.arg_equals.is_empty()
    }) {
        return;
    }
    policy.rules.push(PolicyRule {
        action: PolicyAction::Tool(tool),
        decision: PolicyVerb::Allow,
        reason: None,
        arg_equals: BTreeMap::new(),
    });
}

fn policy_default_decision(input: &str) -> PolicyVerb {
    match input {
        "allow" => PolicyVerb::Allow,
        "ask_user" => PolicyVerb::AskUser,
        _ => PolicyVerb::Deny,
    }
}

/// The policy a sub-agent runs under: the parent's rules, restricted to the
/// tools the sub-agent lists.
///
/// Previously every listed tool became a blanket `Allow`, on the reasoning
/// that "naming a tool in the sub-agent allowlist is an explicit grant". That
/// is what made `file` reachable for `write` by a sub-agent whose parent only
/// held `read`, and `shell` unattended for a sub-agent whose parent asks about
/// every call — the `AUTH-002` widening, produced here and then waved through
/// by `parent_exposes_tool`.
///
/// Inheriting the parent's rules verbatim is both narrower and simpler: the
/// sub-agent's list decides *which* tools it can reach, and the parent's rules
/// keep deciding *under what conditions*. A tool the parent has no rule for
/// yields no rule here either, so it falls to the child's `Deny` default.
///
/// A sub-agent that genuinely needs more than its parent says so in
/// `[[subagents.delegation_grants]]`, where the elevation is visible.
pub(super) fn subagent_policy(
    subagent: &SubAgentConfig,
    parent: &Policy,
) -> Result<Policy, String> {
    let grants = subagent_delegation_grants(subagent)?;
    let mut policy = Policy::default();
    // Grants first, because `decide` is first-match-wins: a grant governs the
    // calls it covers, and the inherited rules below govern everything else.
    // An unconstrained grant therefore replaces the inherited rule outright,
    // while one pinned to `{operation: "read"}` leaves `write` inherited.
    for grant in &grants {
        policy.rules.push(PolicyRule {
            action: grant.action.clone(),
            decision: grant.decision.clone(),
            reason: Some(Arc::clone(&grant.reason)),
            arg_equals: grant.arg_equals.clone(),
        });
    }
    for tool in subagent
        .tools
        .iter()
        .filter(|tool| tool.as_ref() != "memory")
    {
        policy
            .rules
            .extend(parent_rules_for_tool(parent, tool).cloned());
    }
    if subagent_memory_tool_enabled(subagent) {
        for operation in subagent_memory_operations(subagent)? {
            if !matches!(operation.as_ref(), "read" | "write" | "forget") {
                return Err(format!(
                    "unknown subagent memory operation '{operation}'; expected read, write, or forget"
                ));
            }
            // The parent's rule for exactly this operation, or nothing. A
            // sub-agent listing `write` when the parent gates writes behind
            // `ask_user` inherits the gate rather than escaping it.
            policy.rules.extend(
                parent_rules_for_tool(parent, "memory")
                    .filter(|rule| {
                        rule.arg_equals.get("operation")
                            == Some(&Value::String(operation.to_string()))
                    })
                    .cloned(),
            );
        }
    }
    Ok(policy)
}

fn parent_rules_for_tool<'a>(
    parent: &'a Policy,
    tool: &'a str,
) -> impl Iterator<Item = &'a PolicyRule> {
    parent.rules.iter().filter(
        move |rule| matches!(&rule.action, PolicyAction::Tool(name) if name.as_ref() == tool),
    )
}

/// The grants declared for one sub-agent, validated.
///
/// Kept next to `subagent_policy` because the two are read together: the
/// policy states what the sub-agent asks for, and these state which parts of
/// that the operator has authorised it to hold beyond the parent.
pub(super) fn subagent_delegation_grants(
    subagent: &SubAgentConfig,
) -> Result<Vec<DelegationGrant>, String> {
    subagent
        .delegation_grants
        .iter()
        .map(|grant| {
            let decision = match grant.decision.as_ref() {
                "allow" => PolicyVerb::Allow,
                "ask_user" => PolicyVerb::AskUser,
                "deny" => {
                    return Err(format!(
                        "sub-agent '{}' grants tool '{}' the decision 'deny'; a grant widens \
                         authority, and narrowing needs no grant",
                        subagent.id, grant.tool
                    ));
                }
                other => {
                    return Err(format!(
                        "sub-agent '{}' grants tool '{}' the unknown decision '{other}'; \
                         expected allow or ask_user",
                        subagent.id, grant.tool
                    ));
                }
            };
            if grant.reason.trim().is_empty() {
                return Err(format!(
                    "sub-agent '{}' grants tool '{}' with an empty reason; state why this \
                     sub-agent needs authority its parent withheld",
                    subagent.id, grant.tool
                ));
            }
            if !subagent
                .tools
                .iter()
                .any(|tool| tool.as_ref() == grant.tool.as_ref())
            {
                return Err(format!(
                    "sub-agent '{}' grants tool '{}', which it does not list in `tools`; the \
                     grant would have no effect",
                    subagent.id, grant.tool
                ));
            }
            Ok(DelegationGrant {
                action: PolicyAction::Tool(Arc::clone(&grant.tool)),
                decision,
                arg_equals: grant.arg_equals.clone(),
                reason: Arc::clone(&grant.reason),
                expires_at: grant.expires_at,
            })
        })
        .collect()
}

pub(super) fn subagent_memory_tool_enabled(subagent: &SubAgentConfig) -> bool {
    subagent.tools.iter().any(|tool| tool.as_ref() == "memory") || !subagent.memory_tools.is_empty()
}

fn subagent_memory_operations(subagent: &SubAgentConfig) -> Result<Vec<Arc<str>>, String> {
    if subagent.memory_tools.is_empty() {
        return Ok(vec![Arc::from("read"), Arc::from("write")]);
    }
    subagent
        .memory_tools
        .iter()
        .map(|operation| match operation.as_ref() {
            "read" | "write" | "forget" => Ok(Arc::clone(operation)),
            other => Err(format!(
                "unknown subagent memory operation '{other}'; expected read, write, or forget"
            )),
        })
        .collect()
}

/// Every built-in tool this build offers.
///
/// One list so the tool catalog (roadmap X4) cannot describe a different set
/// from the one `register_builtin_tool` will actually build — a catalog that
/// silently omitted a tool would be worse than none, since the omission reads
/// as "this deployment does not have it".
pub const BUILTIN_TOOL_NAMES: &[&str] = &[
    "shell",
    "http",
    "file",
    "skill_validate",
    "cron_create",
    "cron_list",
    "cron_remove",
];

/// Built-in tools that [`register_builtin_tool`] cannot construct, because they
/// need a runtime-owned handle — the memory manager or the job registry — and
/// not just a name. `build_parent_tools` registers them directly.
///
/// Named rather than left implicit in that `match` so anything enumerating the
/// tool surface sees all of it: `docs/TOOL_CATALOG.md` describes these four in
/// prose for the same reason, and `tests/capability_matrix.rs` checks the
/// matrix against both lists.
pub const RUNTIME_TOOL_NAMES: &[&str] = &["memory", "job_status", "job_output", "job_kill"];

/// Register a built-in tool by name, bounded by the deployment's `[limits]`.
///
/// The limits are passed rather than read from a global so a sub-agent's
/// registry is bounded the same way the parent's is, visibly, at the one place
/// both go through.
pub fn register_builtin_tool(
    tools: &mut ToolRegistry,
    name: &str,
    limits: &LimitsConfig,
    env_passthrough: &[String],
) -> Result<(), String> {
    match name {
        "shell" => tools.register(
            ShellTool::with_output_limit(limits.tool_output_bytes)
                .with_env_passthrough(env_passthrough.iter().cloned()),
        ),
        "http" => tools.register(HttpTool::with_response_limit(limits.http_response_bytes)),
        "file" => tools.register(FileTool::with_limits(
            limits.directory_list_entries,
            limits.file_read_bytes,
            limits.file_read_max_bytes,
        )),
        "skill_validate" => tools.register(SkillValidateTool),
        "cron_create" => tools.register(CronCreatorTool),
        "cron_list" => tools.register(CronListTool),
        "cron_remove" => tools.register(CronRemoveTool),
        _ => return Err(format!("unknown built-in tool '{name}'")),
    }
    Ok(())
}

fn add_builtin_tool_policy(policy: &mut Policy, tool: &str) {
    match tool {
        "shell" => ask_tool_once(
            policy,
            Arc::from("shell"),
            None,
            "shell tool requires user approval",
        ),
        // Reading and stopping *this conversation's own* jobs. The registry
        // fences every lookup by conversation, so there is nothing here to
        // gate that the fence does not already deny.
        "http" | "skill_validate" | "job_status" | "job_output" | "job_kill" => {
            allow_tool_once(policy, Arc::from(tool))
        }
        "file" => {
            allow_tool_operation(policy, "file", "read");
            ask_tool_operation(policy, "file", "write", "file write requires user approval");
        }
        "memory" => {
            allow_tool_operation(policy, "memory", "read");
            ask_tool_operation(
                policy,
                "memory",
                "write",
                "memory write requires user approval",
            );
            ask_tool_operation(
                policy,
                "memory",
                "forget",
                "memory forget requires user approval",
            );
        }
        "cron_list" => allow_tool_once(policy, Arc::from("cron_list")),
        "cron_create" => ask_tool_once(
            policy,
            Arc::from("cron_create"),
            None,
            "cron creation requires user approval",
        ),
        "cron_remove" => ask_tool_once(
            policy,
            Arc::from("cron_remove"),
            None,
            "cron removal requires user approval",
        ),
        _ => {}
    }
}

fn allow_tool_once(policy: &mut Policy, tool: Arc<str>) {
    if tool.as_ref() == "memory" {
        return;
    }
    if !policy.rules.iter().any(|rule| {
        rule.action == PolicyAction::Tool(Arc::clone(&tool))
            && rule.decision == PolicyVerb::Allow
            && rule.arg_equals.is_empty()
    }) {
        policy.rules.push(PolicyRule {
            action: PolicyAction::Tool(tool),
            decision: PolicyVerb::Allow,
            reason: None,
            arg_equals: BTreeMap::new(),
        });
    }
}

fn allow_tool_operation(policy: &mut Policy, tool: &str, operation: &str) {
    policy.rules.push(PolicyRule {
        action: PolicyAction::Tool(Arc::from(tool)),
        decision: PolicyVerb::Allow,
        reason: None,
        arg_equals: BTreeMap::from([(Arc::from("operation"), Value::from(operation))]),
    });
}

fn ask_tool_operation(policy: &mut Policy, tool: &str, operation: &str, reason: &str) {
    ask_tool_once(
        policy,
        Arc::from(tool),
        Some(BTreeMap::from([(
            Arc::from("operation"),
            Value::from(operation),
        )])),
        reason,
    );
}

fn ask_tool_once(
    policy: &mut Policy,
    tool: Arc<str>,
    arg_equals: Option<BTreeMap<Arc<str>, Value>>,
    reason: &str,
) {
    let arg_equals = arg_equals.unwrap_or_default();
    if policy.rules.iter().any(|rule| {
        rule.action == PolicyAction::Tool(Arc::clone(&tool))
            && rule.decision == PolicyVerb::AskUser
            && rule.arg_equals == arg_equals
    }) {
        return;
    }
    policy.rules.push(PolicyRule {
        action: PolicyAction::Tool(tool),
        decision: PolicyVerb::AskUser,
        reason: Some(Arc::from(reason)),
        arg_equals,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::approve::PolicyDecision;
    use crate::config::DelegationGrantConfig;
    use crate::memory::InMemoryMemory;
    use agentos_interfaces::orchestrator::Plan;
    use agentos_proto::{ToolCall, ToolCallId};
    use serde_json::{json, value::RawValue};

    fn tool_plan(name: &str, args: serde_json::Value) -> Plan {
        Plan::CallTool(ToolCall {
            id: ToolCallId::new(format!("{name}-test")),
            name: Arc::from(name),
            args: RawValue::from_string(args.to_string()).expect("test args are valid JSON"),
        })
    }

    fn config_with_parent_tools(tools: &[&str]) -> WorkspaceConfig {
        let mut config = WorkspaceConfig::default();
        config.resources.tools.enabled = tools.iter().map(|tool| Arc::from(*tool)).collect();
        config
    }

    #[test]
    fn parent_policy_does_not_inherit_subagent_tool_permissions() {
        let mut config = config_with_parent_tools(&["file"]);
        config.subagents.push(SubAgentConfig {
            tools: vec![Arc::from("http")],
            ..SubAgentConfig::default()
        });

        let policy = phase5_policy(&config, &[]);
        let decision = policy.decide(&tool_plan("http", json!({ "url": "https://example.com" })));

        assert!(matches!(decision, PolicyDecision::Deny { .. }));
    }

    #[test]
    fn parent_file_write_requires_approval_even_when_child_declares_file() {
        let mut config = config_with_parent_tools(&["file"]);
        config.subagents.push(SubAgentConfig {
            tools: vec![Arc::from("file")],
            ..SubAgentConfig::default()
        });

        let policy = phase5_policy(&config, &[]);
        assert_eq!(
            policy.decide(&tool_plan(
                "file",
                json!({ "operation": "read", "path": "README.md" })
            )),
            PolicyDecision::Allow
        );
        assert!(matches!(
            policy.decide(&tool_plan(
                "file",
                json!({ "operation": "write", "path": "README.md", "content": "changed" })
            )),
            PolicyDecision::AskUser { .. }
        ));
    }

    #[test]
    fn a_subagent_inherits_the_parents_rules_for_the_tools_it_lists() {
        let config = config_with_parent_tools(&["file"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("file")],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config, &parent).expect("child policy builds");

        Policy::narrow(&parent, &child).expect("inherited rules narrow by construction");
        // The parent's split survives into the sub-agent: read without asking,
        // write with. This used to be a blanket Allow covering both.
        assert_eq!(
            child.decide(&tool_plan(
                "file",
                json!({ "operation": "read", "path": "README.md" })
            )),
            PolicyDecision::Allow
        );
        assert!(matches!(
            child.decide(&tool_plan(
                "file",
                json!({ "operation": "write", "path": "README.md", "content": "changed" })
            )),
            PolicyDecision::AskUser { .. }
        ));
    }

    /// The behaviour `AUTH-002` removed, stated as what now happens instead.
    ///
    /// Listing a tool decides *which* tools a sub-agent can reach. It does not
    /// decide under what conditions — the parent's rules still do. An
    /// operation the parent never granted is denied rather than inherited as
    /// an unconstrained allow.
    #[test]
    fn listing_a_tool_does_not_elevate_what_the_parent_gated() {
        let config = config_with_parent_tools(&["file", "shell"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("file"), Arc::from("shell")],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config, &parent).expect("child policy builds");
        let effective = Policy::narrow(&parent, &child).expect("listed tools narrow cleanly");

        assert!(matches!(
            effective.decide(&tool_plan(
                "file",
                json!({ "operation": "write", "path": "x", "content": "y" })
            )),
            PolicyDecision::AskUser { .. }
        ));
        // An operation the parent has no rule for reaches neither of them.
        assert!(matches!(
            effective.decide(&tool_plan(
                "file",
                json!({ "operation": "delete", "path": "x" })
            )),
            PolicyDecision::Deny { .. }
        ));
        assert!(matches!(
            effective.decide(&tool_plan("shell", json!({ "command": "ls" }))),
            PolicyDecision::AskUser { .. }
        ));
    }

    /// The replacement for the escape hatch: an operator who wants a sub-agent
    /// to run a gated tool unattended says so, once, with a reason.
    #[test]
    fn a_delegation_grant_elevates_exactly_what_it_names() {
        let config = config_with_parent_tools(&["file", "shell"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("file"), Arc::from("shell")],
            delegation_grants: vec![DelegationGrantConfig {
                tool: Arc::from("shell"),
                decision: Arc::from("allow"),
                arg_equals: BTreeMap::new(),
                reason: Arc::from("unattended nightly maintenance"),
                expires_at: None,
            }],
            ..SubAgentConfig::default()
        };
        let grants = subagent_delegation_grants(&child_config).expect("the grant is valid");

        // `subagent_policy` emits the granted rule ahead of the inherited one.
        let child = subagent_policy(&child_config, &parent).expect("child policy builds");

        let effective =
            Policy::narrow_with_grants(&parent, &child, &grants).expect("the granted tool narrows");
        assert_eq!(
            effective.decide(&tool_plan("shell", json!({ "command": "ls" }))),
            PolicyDecision::Allow
        );

        // Only what it names: `file` write is untouched by a `shell` grant.
        assert!(matches!(
            effective.decide(&tool_plan(
                "file",
                json!({ "operation": "write", "path": "x", "content": "y" })
            )),
            PolicyDecision::AskUser { .. }
        ));

        // And the same policy without the grant backing it is a widening.
        assert!(Policy::narrow(&parent, &child).is_err());
    }

    #[test]
    fn a_grant_for_a_tool_the_subagent_does_not_list_is_rejected() {
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("file")],
            delegation_grants: vec![DelegationGrantConfig {
                tool: Arc::from("shell"),
                decision: Arc::from("allow"),
                arg_equals: BTreeMap::new(),
                reason: Arc::from("would have no effect"),
                expires_at: None,
            }],
            ..SubAgentConfig::default()
        };
        let error = subagent_delegation_grants(&child_config).expect_err("the grant is inert");
        assert!(error.contains("does not list in `tools`"), "{error}");
    }

    #[test]
    fn a_grant_needs_a_reason_and_a_widening_decision() {
        let with_empty_reason = SubAgentConfig {
            tools: vec![Arc::from("shell")],
            delegation_grants: vec![DelegationGrantConfig {
                tool: Arc::from("shell"),
                decision: Arc::from("allow"),
                arg_equals: BTreeMap::new(),
                reason: Arc::from("   "),
                expires_at: None,
            }],
            ..SubAgentConfig::default()
        };
        assert!(subagent_delegation_grants(&with_empty_reason)
            .expect_err("an unexplained grant is rejected")
            .contains("empty reason"));

        let denying = SubAgentConfig {
            tools: vec![Arc::from("shell")],
            delegation_grants: vec![DelegationGrantConfig {
                tool: Arc::from("shell"),
                decision: Arc::from("deny"),
                arg_equals: BTreeMap::new(),
                reason: Arc::from("narrowing needs no grant"),
                expires_at: None,
            }],
            ..SubAgentConfig::default()
        };
        assert!(subagent_delegation_grants(&denying)
            .expect_err("a denying grant is rejected")
            .contains("a grant widens authority"));
    }

    #[test]
    fn an_expired_grant_does_not_elevate() {
        let config = config_with_parent_tools(&["shell"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("shell")],
            delegation_grants: vec![DelegationGrantConfig {
                tool: Arc::from("shell"),
                decision: Arc::from("allow"),
                arg_equals: BTreeMap::new(),
                reason: Arc::from("expired last century"),
                expires_at: Some(1),
            }],
            ..SubAgentConfig::default()
        };
        let grants = subagent_delegation_grants(&child_config).expect("the grant parses");
        let child = subagent_policy(&child_config, &parent).expect("child policy builds");

        assert!(Policy::narrow_with_grants(&parent, &child, &grants).is_err());
    }

    /// The same safety property, reached differently. It used to be a widening
    /// *error*, because the child synthesised a blanket `Allow` for any tool it
    /// listed. Now there is nothing to widen: the parent has no rule for the
    /// tool, so the child inherits none and the call falls to its `Deny`
    /// default. The sub-agent still cannot reach it, and the refusal names the
    /// tool at the point of use.
    #[test]
    fn a_subagent_cannot_reach_a_tool_the_parent_never_grants() {
        let config = config_with_parent_tools(&["http"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("shell")],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config, &parent).expect("child policy builds");

        Policy::narrow(&parent, &child).expect("an empty child policy widens nothing");
        assert!(matches!(
            child.decide(&tool_plan("shell", json!({ "command": "ls" }))),
            PolicyDecision::Deny { .. }
        ));
    }

    /// And a grant cannot conjure authority the parent never had: the grant is
    /// only consulted for a rule the child actually holds, and the child holds
    /// the granted rule only against a parent that is not denying it outright.
    #[test]
    fn a_grant_cannot_reach_past_an_explicit_parent_deny() {
        let config = config_with_parent_tools(&["shell"]);
        let mut parent = phase5_policy(&config, &[]);
        parent.rules.insert(
            0,
            PolicyRule {
                action: PolicyAction::Tool(Arc::from("shell")),
                decision: PolicyVerb::Deny,
                reason: Some(Arc::from("shell is forbidden here")),
                arg_equals: BTreeMap::new(),
            },
        );
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("shell")],
            delegation_grants: vec![DelegationGrantConfig {
                tool: Arc::from("shell"),
                decision: Arc::from("allow"),
                arg_equals: BTreeMap::new(),
                reason: Arc::from("should not defeat an explicit deny"),
                expires_at: None,
            }],
            ..SubAgentConfig::default()
        };
        let grants = subagent_delegation_grants(&child_config).expect("the grant parses");
        let child = subagent_policy(&child_config, &parent).expect("child policy builds");

        assert!(Policy::narrow_with_grants(&parent, &child, &grants).is_err());
    }

    /// A tool the *parent* allows outright stays allowed for the sub-agent;
    /// one the parent gates stays gated. The sub-agent's list selects, it does
    /// not elevate.
    #[test]
    fn a_subagent_tool_list_selects_rather_than_elevates() {
        let mut config = config_with_parent_tools(&["shell", "cron_create", "cron_remove"]);
        config.policy.allowlist = vec![Arc::from("cron_create"), Arc::from("cron_remove")];
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![
                Arc::from("shell"),
                Arc::from("cron_create"),
                Arc::from("cron_remove"),
            ],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config, &parent).expect("child policy builds");

        // Gated for the parent, so gated here. Previously a blanket Allow.
        assert!(matches!(
            child.decide(&tool_plan("shell", json!({ "command": "ls" }))),
            PolicyDecision::AskUser { .. }
        ));
        assert_eq!(
            child.decide(&tool_plan(
                "cron_create",
                json!({ "id": "daily", "channel_id": "telegram", "conversation_id": "1", "prompt": "hi", "interval_minutes": 60 })
            )),
            PolicyDecision::Allow
        );
        assert_eq!(
            child.decide(&tool_plan("cron_remove", json!({ "id": "daily" }))),
            PolicyDecision::Allow
        );
    }

    /// `memory_tools` selects which operations the sub-agent can reach at all;
    /// the parent's rule for each still decides whether it asks. An operation
    /// left off the list is denied, which is what it always was.
    #[test]
    fn subagent_memory_operations_are_selected_then_inherited() {
        let config = config_with_parent_tools(&["memory"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("memory")],
            memory_tools: vec![Arc::from("read"), Arc::from("write")],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config, &parent).expect("child policy builds");

        assert_eq!(
            child.decide(&tool_plan("memory", json!({ "operation": "read" }))),
            PolicyDecision::Allow
        );
        // The parent asks about memory writes, so the sub-agent does too.
        assert!(matches!(
            child.decide(&tool_plan(
                "memory",
                json!({ "operation": "write", "body": { "fact": "x" } })
            )),
            PolicyDecision::AskUser { .. }
        ));
        assert!(matches!(
            child.decide(&tool_plan("memory", json!({ "operation": "forget" }))),
            PolicyDecision::Deny { .. }
        ));
    }

    #[test]
    fn parent_tools_follow_resources_tools_enabled() {
        let memory = Arc::new(MemoryManager::new(Arc::new(InMemoryMemory::default())));
        let tools = build_parent_tools(
            &config_with_parent_tools(&["http"]),
            memory,
            Arc::new(JobRegistry::default()),
        )
        .expect("configured tools build");

        assert!(tools.contains("http"));
        assert!(!tools.contains("file"));
        assert!(!tools.contains("shell"));
    }

    #[test]
    fn allowlisted_tool_bypasses_ask_user() {
        let mut config = config_with_parent_tools(&["shell", "file"]);
        config.policy.allowlist = vec![Arc::from("shell"), Arc::from("file")];

        let policy = phase5_policy(&config, &[]);

        assert_eq!(
            policy.decide(&tool_plan("shell", json!({ "command": "ls" }))),
            PolicyDecision::Allow
        );
        assert_eq!(
            policy.decide(&tool_plan(
                "file",
                json!({ "operation": "write", "path": "x", "content": "y" })
            )),
            PolicyDecision::Allow
        );
    }

    #[test]
    fn non_allowlisted_tool_still_asks_user() {
        let mut config = config_with_parent_tools(&["shell", "file"]);
        config.policy.allowlist = vec![Arc::from("file")];

        let policy = phase5_policy(&config, &[]);

        assert!(matches!(
            policy.decide(&tool_plan("shell", json!({ "command": "ls" }))),
            PolicyDecision::AskUser { .. }
        ));
        assert_eq!(
            policy.decide(&tool_plan(
                "file",
                json!({ "operation": "write", "path": "x", "content": "y" })
            )),
            PolicyDecision::Allow
        );
    }

    #[test]
    fn parent_policy_default_follows_config_policy() {
        let mut config = config_with_parent_tools(&[]);
        config.policy.default = Arc::from("ask_user");

        let policy = phase5_policy(&config, &[]);
        let decision = policy.decide(&tool_plan("unknown_tool", json!({})));

        assert!(matches!(decision, PolicyDecision::AskUser { .. }));
    }

    #[test]
    fn repository_subagent_policies_narrow_parent_policy() {
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let config = WorkspaceConfig::load(&repo_root.join("workspace/agent.toml"))
            .expect("workspace config loads");
        let parent = phase5_policy(&config, &[]);

        for subagent in &config.subagents {
            let child = subagent_policy(subagent, &parent).expect("subagent policy builds");
            Policy::narrow(&parent, &child).unwrap_or_else(|err| {
                panic!(
                    "subagent '{}' policy '{}' should narrow parent policy: {err}",
                    subagent.id, subagent.policy_id
                )
            });
        }
    }
    /// `[limits]` reaches the tool the registry built, not just the struct that
    /// parsed it. Driven through `ToolRegistry::call` so the assertion is about
    /// what a run would see, and with a control at a larger cap so a listing
    /// that was short anyway cannot pass for a truncated one.
    #[tokio::test]
    async fn configured_file_limits_reach_the_registered_tool() {
        async fn list_entries(limit: usize) -> usize {
            let mut config = config_with_parent_tools(&["file"]);
            config.limits.directory_list_entries = limit;
            let memory = Arc::new(MemoryManager::new(Arc::new(InMemoryMemory::default())));
            let jobs = Arc::new(JobRegistry::default());
            let tools = build_parent_tools(&config, memory, jobs).expect("tools build");

            // `read` on a directory is how the tool lists. `src` is relative
            // to the workspace root, which in a unit test is the crate
            // directory — comfortably more entries than the tight cap below.
            let call = ToolCall {
                id: ToolCallId::new("list"),
                name: Arc::from("file"),
                args: RawValue::from_string(
                    json!({ "operation": "read", "path": "src" }).to_string(),
                )
                .expect("test args are valid JSON"),
            };
            let result = tools.call(&call).await.expect("the file tool answers");
            result.content.lines().count()
        }

        let tight = list_entries(2).await;
        let loose = list_entries(500).await;
        // Two entries plus the truncation marker.
        assert_eq!(tight, 3, "the configured cap must be what truncates");
        assert!(
            loose > tight,
            "the control must return more than the capped listing, got {loose}"
        );
    }
    /// The catalog's list and the registration match must not drift: a name in
    /// the list that does not register would put a tool in the docs that
    /// nobody can enable.
    #[test]
    fn every_named_builtin_registers() {
        let limits = LimitsConfig::default();
        for name in BUILTIN_TOOL_NAMES {
            let mut registry = ToolRegistry::new();
            register_builtin_tool(&mut registry, name, &limits, &[])
                .unwrap_or_else(|err| panic!("{name} should register: {err}"));
            assert_eq!(registry.specs().len(), 1, "{name} registered nothing");
        }
        let mut registry = ToolRegistry::new();
        assert!(register_builtin_tool(&mut registry, "not_a_tool", &limits, &[]).is_err());
    }
}
