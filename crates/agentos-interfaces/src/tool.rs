use crate::orchestrator::RunContext;
use agentos_proto::{ToolCall, ToolResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ToolError {
    #[error("tool failed: {0}")]
    Failed(Arc<str>),
}

/// What a tool may do to the filesystem.
///
/// Ordered from most to least restricted. Anything other than
/// [`SandboxMode::FullAccess`] means the tool runs in a child process under a
/// kernel sandbox — Landlock on Linux, Seatbelt on macOS — so the restriction
/// is enforced by the kernel rather than by the tool's own good behaviour.
///
/// **What this does not cover:** a tool that does its work in-process cannot be
/// restricted this way, because the only sandbox available would restrict the
/// whole agent. Those tools declare [`SandboxMode::FullAccess`] and are bounded
/// by the guardrails and the policy engine instead. Declaring a narrower mode
/// on a tool that never spawns a child would be a claim the kernel is not
/// making.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxMode {
    /// May read the filesystem; may not write to it anywhere.
    ReadOnly,
    /// May write beneath the workspace root and the temporary directory;
    /// may not write anywhere else.
    WorkspaceWrite,
    /// Unrestricted, and not run under a sandbox at all.
    ///
    /// The default, because it is exactly what the old `requires_isolation:
    /// false` meant and most built-in tools do their work in-process where
    /// there is nothing for a sandbox to restrict. It is a claim about
    /// enforcement, not a grant of privilege: an unsandboxed tool is still
    /// bounded by its guardrails and by `Approve`.
    #[default]
    FullAccess,
}

impl SandboxMode {
    /// Whether this mode is enforced by running the tool in a sandboxed child
    /// process.
    pub fn is_sandboxed(self) -> bool {
        !matches!(self, Self::FullAccess)
    }

    /// Stable name for traces, logs, and `/tools`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::WorkspaceWrite => "workspace_write",
            Self::FullAccess => "full_access",
        }
    }
}

/// Whether invoking a tool can change state outside the current call.
///
/// This is authorization metadata, not a promise about filesystem isolation.
/// A tool that only reads files may still need a sandbox; a tool that mutates
/// a database in-process may declare [`SandboxMode::FullAccess`] while still
/// carrying [`ToolSideEffect::PersistentMutation`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSideEffect {
    /// The tool did not declare its behavior. Policy treats this as unsafe for
    /// blanket authorization so an older or third-party specification cannot
    /// gain authority merely by omitting new metadata.
    #[default]
    Unspecified,
    /// Reads or computes without changing external state.
    ReadOnly,
    /// Changes process-lifetime state that does not survive a restart.
    TransientMutation,
    /// Changes state that survives the call or process.
    PersistentMutation,
}

impl ToolSideEffect {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::ReadOnly => "read_only",
            Self::TransientMutation => "transient_mutation",
            Self::PersistentMutation => "persistent_mutation",
        }
    }

    pub fn is_mutation(self) -> bool {
        matches!(self, Self::TransientMutation | Self::PersistentMutation)
    }
}

/// The widest durable or shared state an invocation can address.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolPersistenceScope {
    /// The tool did not declare its scope. Like an unspecified side effect,
    /// this fails closed for blanket authorization.
    #[default]
    Unspecified,
    /// No durable or shared state is addressed.
    None,
    /// State is fenced to the initiating conversation.
    Conversation,
    /// State belongs to the agent workspace and can affect later runs.
    Workspace,
    /// One call can address state owned by another conversation.
    CrossConversation,
}

impl ToolPersistenceScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::None => "none",
            Self::Conversation => "conversation",
            Self::Workspace => "workspace",
            Self::CrossConversation => "cross_conversation",
        }
    }
}

/// Authorization-relevant behavior declared by a tool.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSafety {
    #[serde(default)]
    pub side_effect: ToolSideEffect,
    #[serde(default)]
    pub persistence_scope: ToolPersistenceScope,
}

impl ToolSafety {
    pub const fn new(side_effect: ToolSideEffect, persistence_scope: ToolPersistenceScope) -> Self {
        Self {
            side_effect,
            persistence_scope,
        }
    }

    /// Whether a blanket `Allow` would cover a persistent or
    /// cross-conversation mutation, or metadata too incomplete to prove it
    /// would not.
    pub fn rejects_blanket_allow(self) -> bool {
        matches!(
            self.side_effect,
            ToolSideEffect::Unspecified | ToolSideEffect::PersistentMutation
        ) || matches!(self.persistence_scope, ToolPersistenceScope::Unspecified)
            || (self.side_effect.is_mutation()
                && self.persistence_scope == ToolPersistenceScope::CrossConversation)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: Arc<str>,
    pub description: Arc<str>,
    pub input_schema: Value,
    /// Side effects and persistence scope used by policy construction. An
    /// omitted declaration deserializes to `unspecified`, which prevents
    /// blanket authorization rather than granting it.
    #[serde(default)]
    pub safety: ToolSafety,
    /// What this tool is allowed to do to the filesystem, and therefore how it
    /// is executed (roadmap item X2).
    ///
    /// Replaces the old `requires_isolation: bool`, which said only *whether*
    /// to spend a process on a tool and nothing about what that process could
    /// then do — it ran as the same user with the same filesystem, network and
    /// environment as the agent.
    #[serde(default)]
    pub sandbox: SandboxMode,
    /// How long this tool may run before the registry gives up on it, in
    /// milliseconds (roadmap item D2).
    ///
    /// The tool's *own* declaration: set it when a tool knows it is slow (a
    /// build, a long fetch) and should not be held to the deployment's default.
    /// `None` accepts that default. A deployment can override either through
    /// `[limits].tool_timeout_overrides`, which wins over both.
    ///
    /// A deadline that expires produces a **failed `ToolResult`**, never an
    /// error: the run loop already recovers from a failed result by letting the
    /// model read it and replan, whereas an error would kill the run and every
    /// parent above it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// How a tool's declared [`SandboxMode`] is actually enforced.
///
/// [`SandboxMode`] says *what* the restriction is. This says *who applies it*,
/// and the two are not the same question: a mode is only real if some process
/// boundary carries it. Before this existed the runtime assumed the registry
/// would always be that boundary, and when no isolated executor was configured
/// it ran the tool in-process with the mode silently discarded — the tool
/// claimed `read_only` and had full access. See
/// `docs/adr/0002-FAIL_CLOSED_ISOLATION.md`.
///
/// The default is [`Isolation::RequiresExecutor`] so that an implementation
/// which has not thought about the question fails closed rather than open.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Isolation {
    /// The runtime must run this tool inside a compatible isolated executor.
    /// Where none exists, the call fails before the tool's body is reached.
    ///
    /// Correct for every tool that does its work in-process, and for every
    /// tool the runtime did not write — an MCP tool cannot vouch for its own
    /// containment.
    #[default]
    RequiresExecutor,
    /// The tool spawns its own child process and applies the sandbox to it
    /// before the spawn, treating a failure to do so as fatal for the call.
    ///
    /// A claim about the implementation, not a waiver: `ShellTool` builds a
    /// [`SandboxMode`]-matching sandbox and hands it to the exec helper, where
    /// hardening runs before the child exists and an error aborts the call.
    /// A tool that returns this without doing that has lied about the kernel,
    /// which is the one thing the mode is supposed to prevent.
    SelfHardened,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ToolMetadata {
    pub duration_ms: u64,
    pub bytes_out: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<Arc<str>, Value>,
}

#[async_trait]
pub trait Tool: Send + Sync {
    /// Return the static schema and safety metadata for this tool.
    fn spec(&self) -> ToolSpec;

    /// Who enforces the [`SandboxMode`] this tool declares.
    ///
    /// Only consulted when the declared mode is not
    /// [`SandboxMode::FullAccess`]; an unsandboxed tool has nothing to
    /// enforce. Override it only if the implementation genuinely hardens its
    /// own child process — the default refuses to run without an executor,
    /// which is the safe answer for anything else.
    fn isolation(&self) -> Isolation {
        Isolation::RequiresExecutor
    }

    /// Execute a tool call after guardrails and approval pass.
    ///
    /// Implementations receive raw JSON arguments and should parse them only
    /// when execution requires it.
    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError>;

    /// Execute a tool call with the active run context.
    ///
    /// Tools that need authenticated caller metadata should override this.
    /// Most tools can use the default and parse only their raw arguments.
    async fn call_with_context(
        &self,
        call: &ToolCall,
        args: &RawValue,
        _ctx: &RunContext<'_>,
    ) -> Result<ToolResult, ToolError> {
        self.call(call, args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_safety_metadata_deserializes_fail_closed() {
        let spec: ToolSpec = serde_json::from_value(serde_json::json!({
            "name": "legacy",
            "description": "legacy spec",
            "input_schema": { "type": "object" },
            "sandbox": "full_access"
        }))
        .expect("legacy tool specs remain readable");

        assert_eq!(spec.safety, ToolSafety::default());
        assert!(spec.safety.rejects_blanket_allow());
    }

    #[test]
    fn read_only_cross_conversation_inspection_is_not_a_mutation() {
        let safety = ToolSafety::new(
            ToolSideEffect::ReadOnly,
            ToolPersistenceScope::CrossConversation,
        );

        assert!(!safety.rejects_blanket_allow());
    }

    #[test]
    fn persistent_and_cross_conversation_mutations_reject_blanket_allow() {
        for safety in [
            ToolSafety::new(
                ToolSideEffect::PersistentMutation,
                ToolPersistenceScope::Workspace,
            ),
            ToolSafety::new(
                ToolSideEffect::TransientMutation,
                ToolPersistenceScope::CrossConversation,
            ),
        ] {
            assert!(safety.rejects_blanket_allow());
        }
    }
}
