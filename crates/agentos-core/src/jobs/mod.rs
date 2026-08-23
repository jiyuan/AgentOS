//! Long work that outlives the turn that started it.
//!
//! Roadmap item D3 in `docs/TRANSFER_ROADMAP.md`. D2 gave every tool call a
//! deadline, which stops a slow tool from wedging a conversation — but it does
//! so by *throwing the work away*. A build that needs ten minutes is now
//! reliably killed at sixty seconds, over and over, and the model has no way to
//! ask for it any differently.
//!
//! A job is the escape hatch: work that runs past the turn, reports progress
//! while it runs, and can be killed on demand.
//!
//! # Owner fencing
//!
//! Every job belongs to exactly one [`ConversationId`], and every registry
//! operation takes the conversation asking. A job started in one conversation
//! is invisible to every other — not merely unlisted but unreadable and
//! unkillable, since the lookup itself is fenced. This is the same boundary the
//! memory tool draws, and for the same reason: a model that can name an id must
//! not be able to reach across conversations by guessing one.
//!
//! # Where the handles live, and why that is temporary
//!
//! Jobs outlive a run, so their handles cannot live in `RunState`. They belong
//! to a conversation actor — roadmap item G1, which does not exist yet — so for
//! now the registry is owned by [`crate::runtime::AgentRuntime`] and keyed by
//! conversation, which is the fallback the roadmap sanctions.
//!
//! One consequence is visible in the API: [`JobRegistry::dispose_conversation`]
//! exists and is tested, but nothing calls it, because nothing in the runtime
//! yet knows when a conversation ends. G1 is what will call it.

mod registry;

pub use registry::{
    JobError, JobId, JobRegistry, JobSink, JobSnapshot, JobSpec, JobState,
    DEFAULT_COMPLETED_JOB_SECS, DEFAULT_JOB_OUTPUT_BYTES, DEFAULT_MAX_CONCURRENT_JOBS,
};
