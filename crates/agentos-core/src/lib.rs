//! Core Agent OS runtime.
//!
//! The core owns the run loop, approval engine, gateway, hooks, and runtime
//! errors. It must not depend on workspace-owned content.

pub mod approve;
pub mod channels;
pub mod config;
pub mod crons;
pub mod egress;
pub mod gateway;
pub mod guardrails;
pub mod hooks;
pub(crate) mod http;
/// Debug-build assertions over the runtime's load-bearing relationships
/// (roadmap X5). Compiled away entirely in release builds.
#[cfg(debug_assertions)]
pub(crate) mod invariants;
pub mod jobs;
pub mod r#loop;
pub mod memory;
pub mod orchestrator;
pub mod paths;
pub mod prompt;
pub mod runner;
pub mod runtime;
pub mod sandbox;
pub mod skills;
pub mod spill;
pub mod subagents;
pub mod task_workspace;
pub mod tools;
mod trace;
