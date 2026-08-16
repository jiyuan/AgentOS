use crate::approve::{Policy, PolicyAction, PolicyRule, PolicyVerb};
use crate::config::{SubAgentConfig, WorkspaceConfig};
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
            // Both of these need a runtime-owned handle rather than just a
            // name, so they cannot go through `register_builtin_tool`.
            "memory" => tools.register(MemoryTool::with_manager(memory_manager.clone())),
            "job_status" => tools.register(JobStatusTool::new(jobs.clone())),
            "job_output" => tools.register(JobOutputTool::new(jobs.clone())),
            "job_kill" => tools.register(JobKillTool::new(jobs.clone())),
            _ => register_builtin_tool(&mut tools, tool)?,
        }
    }
    Ok(tools
        .with_timeouts(
            config.limits.tool_timeout(),
            config.limits.tool_timeout_overrides(),
        )
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

pub(super) fn subagent_policy(subagent: &SubAgentConfig) -> Result<Policy, String> {
    let mut policy = Policy::default();
    for tool in subagent
        .tools
        .iter()
        .filter(|tool| tool.as_ref() != "memory")
    {
        add_subagent_tool_allow_policy(&mut policy, tool);
    }
    if subagent_memory_tool_enabled(subagent) {
        for operation in subagent_memory_operations(subagent)? {
            match operation.as_ref() {
                "read" => allow_tool_operation(&mut policy, "memory", "read"),
                "write" => allow_tool_operation(&mut policy, "memory", "write"),
                "forget" => allow_tool_operation(&mut policy, "memory", "forget"),
                other => {
                    return Err(format!(
                        "unknown subagent memory operation '{other}'; expected read, write, or forget"
                    ));
                }
            }
        }
    }
    Ok(policy)
}

fn add_subagent_tool_allow_policy(policy: &mut Policy, tool: &str) {
    // Naming a tool in the sub-agent allowlist is an explicit grant: allow
    // every operation of that tool unconditionally so the sub-agent never
    // re-prompts for approval mid-task (e.g. `file` write, not just read).
    // `memory` is excluded here and handled per-operation by the caller so
    // shared-domain writes/forgets keep their own gating.
    allow_tool_once(policy, Arc::from(tool));
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

pub fn register_builtin_tool(tools: &mut ToolRegistry, name: &str) -> Result<(), String> {
    match name {
        "shell" => tools.register(ShellTool),
        "http" => tools.register(HttpTool),
        "file" => tools.register(FileTool),
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
    fn subagent_file_policy_narrows_parent_file_policy() {
        let config = config_with_parent_tools(&["file"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("file")],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config).expect("child policy builds");

        Policy::narrow(&parent, &child).expect("file child policy should narrow parent policy");
        assert_eq!(
            child.decide(&tool_plan(
                "file",
                json!({ "operation": "write", "path": "README.md", "content": "changed" })
            )),
            PolicyDecision::Allow
        );
    }

    #[test]
    fn narrowed_subagent_listed_tool_never_asks_for_approval() {
        // Parent gates `file` write behind AskUser for itself. A sub-agent
        // that explicitly lists `file` must get the *narrowed* (effective)
        // policy that allows every file operation without approval — the
        // parent's AskUser must not leak into the delegatee.
        let config = config_with_parent_tools(&["file", "shell"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("file"), Arc::from("shell")],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config).expect("child policy builds");
        let effective =
            Policy::narrow(&parent, &child).expect("listed tools should narrow cleanly");

        assert_eq!(
            effective.decide(&tool_plan(
                "file",
                json!({ "operation": "write", "path": "x", "content": "y" })
            )),
            PolicyDecision::Allow
        );
        assert_eq!(
            effective.decide(&tool_plan(
                "file",
                json!({ "operation": "delete", "path": "x" })
            )),
            PolicyDecision::Allow
        );
        assert_eq!(
            effective.decide(&tool_plan("shell", json!({ "command": "ls" }))),
            PolicyDecision::Allow
        );
    }

    #[test]
    fn subagent_cannot_allowlist_tool_parent_never_grants() {
        // Safety invariant preserved: listing a tool the parent does not
        // expose at all is still a widening error, not a silent grant.
        let config = config_with_parent_tools(&["http"]);
        let parent = phase5_policy(&config, &[]);
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("shell")],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config).expect("child policy builds");

        assert!(Policy::narrow(&parent, &child).is_err());
    }

    #[test]
    fn subagent_explicit_tool_allowlist_avoids_tool_approval() {
        let child_config = SubAgentConfig {
            tools: vec![
                Arc::from("shell"),
                Arc::from("cron_create"),
                Arc::from("cron_remove"),
            ],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config).expect("child policy builds");

        assert_eq!(
            child.decide(&tool_plan("shell", json!({ "command": "ls" }))),
            PolicyDecision::Allow
        );
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

    #[test]
    fn subagent_memory_tool_operations_avoid_approval_when_declared() {
        let child_config = SubAgentConfig {
            tools: vec![Arc::from("memory")],
            memory_tools: vec![Arc::from("read"), Arc::from("write")],
            ..SubAgentConfig::default()
        };
        let child = subagent_policy(&child_config).expect("child policy builds");

        assert_eq!(
            child.decide(&tool_plan(
                "memory",
                json!({ "operation": "write", "body": { "fact": "x" } })
            )),
            PolicyDecision::Allow
        );
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
            let child = subagent_policy(subagent).expect("subagent policy builds");
            Policy::narrow(&parent, &child).unwrap_or_else(|err| {
                panic!(
                    "subagent '{}' policy '{}' should narrow parent policy: {err}",
                    subagent.id, subagent.policy_id
                )
            });
        }
    }
}
