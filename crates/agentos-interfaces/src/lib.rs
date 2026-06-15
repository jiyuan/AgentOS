//! Public extension interfaces for Agent OS.
//!
//! This crate is the ABI surface for orchestrators, memory backends, sessions,
//! channels, tools, skills, MCP clients, and guardrails. It must not depend on
//! `agentos-core`, workspace-owned content, or extension implementations.

pub mod channel;
pub mod guardrail;
pub mod mcp;
pub mod memory;
pub mod orchestrator;
pub mod paths;
pub mod run_state;
pub mod session;
pub mod skill;
pub mod tool;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use channel::{Channel, ChannelError, StreamEgress};
pub use guardrail::{
    GuardrailError, GuardrailOutcome, Input, InputGuardrail, OutputGuardrail, ToolGuardrail,
};
pub use mcp::{McpClient, McpError, McpServer};
pub use memory::{Memory, MemoryError, Query, Record, Selector};
pub use orchestrator::{
    DispatchPriority, DispatchTarget, MemoryFragment, Orchestrator, OrchestratorError,
    OrchestratorTemplate, Plan, ResourceEntry, ResourceIndex, ResourceKind, RoutingRule,
    RoutingTable, RunContext, Stage, SubAgentSpec, SubOrchSpec, SystemContext, TaskDomain,
};
pub use paths::agentos_home;
pub use run_state::{ApprovalStatus, Interruption, InterruptionAction, RunState};
pub use session::{Item, Session, SessionError, Transcript};
pub use skill::{Skill, SkillError, SkillInvocation};
pub use tool::{Tool, ToolError, ToolMetadata, ToolSpec};
