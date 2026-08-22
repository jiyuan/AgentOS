//! How a built [`AgentRuntime`] hands a run its dependencies.
//!
//! Split out of `runtime/mod.rs` to keep it under the module ceiling. The
//! interesting thing here is what a runtime *supplies by default* rather than
//! how it is assembled: the trace sink, the safety log, the cancellation
//! token, and — deliberately — nothing for steering or granted authority,
//! because a top-level run is nobody's delegatee and only a caller that
//! multiplexes conversations has anywhere for mid-run input to arrive from.

use super::{main_max_turns, AgentRuntime};
use crate::memory::MemoryManager;
use crate::r#loop::{InputGuardrailEntry, OutputGuardrailEntry, ToolGuardrailEntry};
use crate::runner::RunnerDeps;
use std::sync::Arc;

pub struct RuntimeDepsScope<'a> {
    pub(super) runtime: &'a AgentRuntime,
}

impl<'a> RuntimeDepsScope<'a> {
    pub fn deps(&'a self) -> RunnerDeps<'a> {
        RunnerDeps {
            orchestrator: &self.runtime.orchestrator,
            session: self.runtime.session.as_ref(),
            memory_manager: self.episode_memory_manager(),
            hooks: None,
            max_turns: main_max_turns(&self.runtime.workspace_config),
            active_agent: self.runtime.active_agent.clone(),
            tools: Some(&self.runtime.tools),
            trace_sink: Some(self.runtime.trace_sink.as_ref()),
            task_workspace: Some(self.runtime.task_workspace.as_ref()),
            policy: &self.runtime.policy,
            subagents: self.runtime.subagents.as_ref(),
            input_guardrails: &[],
            output_guardrails: &[],
            tool_guardrails: &[],
            stream_sink: None,
            content_limits: self.runtime.content_limits(),
            compaction: self.runtime.compaction(),
            cancel: self.runtime.cancel.child_token(),
            // The caller decides whether this run can be steered: only a
            // gateway that multiplexes conversations has anywhere for
            // mid-run input to arrive from.
            steering: None,
            safety_log: Some(self.runtime.session.as_ref()),
            // The top-level run is nobody's delegatee, so it holds no granted
            // authority. Only a sub-agent's `RunnerDeps` carries any.
            granted_authority: &[],
        }
    }

    pub fn input_guardrails(&'a self) -> [InputGuardrailEntry<'a>; 1] {
        [InputGuardrailEntry {
            name: Arc::from("PiiFilter"),
            guardrail: &self.runtime.pii_filter,
        }]
    }

    pub fn output_guardrails(&'a self) -> [OutputGuardrailEntry<'a>; 1] {
        [OutputGuardrailEntry {
            name: Arc::from("MaxOutputLength"),
            guardrail: &self.runtime.max_output_length,
        }]
    }

    pub fn tool_guardrails(&'a self) -> [ToolGuardrailEntry<'a>; 1] {
        [ToolGuardrailEntry {
            name: Arc::from("ShellCommandAllowlist"),
            guardrail: &self.runtime.shell_allowlist,
        }]
    }

    pub fn deps_with_guardrails(
        &'a self,
        input_guardrails: &'a [InputGuardrailEntry<'a>],
        output_guardrails: &'a [OutputGuardrailEntry<'a>],
        tool_guardrails: &'a [ToolGuardrailEntry<'a>],
    ) -> RunnerDeps<'a> {
        RunnerDeps {
            orchestrator: &self.runtime.orchestrator,
            session: self.runtime.session.as_ref(),
            memory_manager: self.episode_memory_manager(),
            hooks: None,
            max_turns: main_max_turns(&self.runtime.workspace_config),
            active_agent: self.runtime.active_agent.clone(),
            tools: Some(&self.runtime.tools),
            trace_sink: Some(self.runtime.trace_sink.as_ref()),
            task_workspace: Some(self.runtime.task_workspace.as_ref()),
            policy: &self.runtime.policy,
            subagents: self.runtime.subagents.as_ref(),
            input_guardrails,
            output_guardrails,
            tool_guardrails,
            stream_sink: None,
            content_limits: self.runtime.content_limits(),
            compaction: self.runtime.compaction(),
            cancel: self.runtime.cancel.child_token(),
            // The caller decides whether this run can be steered: only a
            // gateway that multiplexes conversations has anywhere for
            // mid-run input to arrive from.
            steering: None,
            safety_log: Some(self.runtime.session.as_ref()),
            // The top-level run is nobody's delegatee, so it holds no granted
            // authority. Only a sub-agent's `RunnerDeps` carries any.
            granted_authority: &[],
        }
    }

    fn episode_memory_manager(&'a self) -> Option<&'a MemoryManager> {
        self.runtime
            .workspace_config
            .memory
            .episode_recording_enabled
            .then_some(self.runtime.memory_manager.as_ref())
    }
}
