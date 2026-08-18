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
//! Every job belongs to exactly one [`agentos_proto::SessionKey`], and every
//! registry operation takes the complete session principal asking. A job
//! started for one agent, channel, conversation, sender, and epoch is invisible
//! to every other principal — not merely unlisted but unreadable and unkillable,
//! since the lookup itself is fenced. This is the same boundary the memory tool
//! draws, and for the same reason: a model that can name an id must not be able
//! to reach across trust boundaries by guessing one.
//!
//! # Where the handles live, and why that is temporary
//!
//! Jobs outlive a run, so their handles cannot live in `RunState`. The registry
//! is owned by [`crate::runtime::AgentRuntime`] and each handle is keyed by its
//! typed session principal.
//!
//! One consequence is visible in the API: [`JobRegistry::dispose_session`]
//! cancels and forgets exactly that principal's jobs when the gateway handles
//! `/clear`.

mod registry;

pub use registry::{
    JobError, JobId, JobRegistry, JobSink, JobSnapshot, JobSpec, JobState,
    DEFAULT_JOB_OUTPUT_BYTES, DEFAULT_MAX_CONCURRENT_JOBS,
};
