//! Kernel-enforced limits on what a tool's child process may write.
//!
//! Roadmap item X2. Before this, `requires_isolation` bought a subprocess and
//! nothing else: same user, same filesystem, same network, same environment.
//! `DESIGN.md` called that safety ring 4, which was generous — a process
//! boundary stops a crash from taking the runtime down, and stops nothing an
//! attacker would want to do.
//!
//! A [`SandboxMode`] on a tool's spec now means the kernel refuses the writes
//! the mode does not allow:
//!
//! - **Linux** — Landlock. A ruleset is built in the parent, and applied in the
//!   child between `fork` and `exec`, so only the child is restricted. Landlock
//!   is irreversible for the thread that applies it and is inherited by every
//!   descendant, which is exactly the shape wanted here and exactly why it must
//!   not be applied to the agent's own threads.
//! - **macOS** — Seatbelt, by exec'ing the command through `sandbox-exec` with
//!   a generated profile.
//! - **Anything else** — nothing, reported honestly by [`availability`] rather
//!   than pretended.
//!
//! # What it does not restrict
//!
//! Only child processes. A tool that does its work in-process cannot be
//! sandboxed this way: the only restriction available would apply to the agent
//! itself, permanently. Those tools declare [`SandboxMode::FullAccess`] and are
//! bounded by their guardrails and by `Approve` — which is a real boundary, but
//! a different one, and `DESIGN.md` says so rather than implying the kernel is
//! involved.
//!
//! Reads are also unrestricted. The mode names filesystem *writes* because that
//! is what the enforcement covers on both platforms without a per-deployment
//! read allowlist nobody would maintain correctly. A tool that must not read
//! something is a policy question, not a sandbox one.

use agentos_interfaces::tool::SandboxMode;
use std::path::PathBuf;
use thiserror::Error;

#[cfg(target_os = "linux")]
mod linux;
// Not gated on the target: the Seatbelt backend is string building and one
// `Path::exists`, so compiling and unit-testing it everywhere is the only
// verification available for a platform the CI machine is not. Only its use is
// conditional.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
mod macos;

#[derive(Debug, Error)]
pub enum SandboxError {
    /// The mode asked for enforcement this platform cannot provide.
    ///
    /// A hard error, not a warning: running a tool that declared `read_only`
    /// without the sandbox would run it with full access, which is the one
    /// outcome nobody asked for.
    #[error("{mode} cannot be enforced here: {reason}")]
    Unavailable {
        mode: &'static str,
        reason: &'static str,
    },
    #[error("failed to build the sandbox: {0}")]
    Setup(String),
}

/// Whether this build, on this machine, can enforce a sandbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Availability {
    /// Enforced, by the named mechanism.
    Enforced(&'static str),
    /// Not enforced, and why. A test that needs enforcement should skip with
    /// this reason rather than pass without it.
    Unavailable(&'static str),
}

impl Availability {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Enforced(mechanism) => mechanism,
            Self::Unavailable(reason) => reason,
        }
    }
}

/// What this machine can enforce, checked against the running kernel rather
/// than assumed from the target triple: a Linux build on a kernel without
/// Landlock reports unavailable.
pub fn availability() -> Availability {
    #[cfg(target_os = "linux")]
    {
        linux::availability()
    }
    #[cfg(target_os = "macos")]
    {
        macos::availability()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Availability::Unavailable("no sandbox backend for this platform")
    }
}

/// The restriction to apply to one child process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Sandbox {
    mode: SandboxMode,
    /// Directories the child may write beneath, empty for [`SandboxMode::ReadOnly`].
    writable: Vec<PathBuf>,
}

impl Sandbox {
    /// The sandbox for `mode`, with `workspace_root` as the writable root when
    /// the mode allows writing.
    ///
    /// The temporary directory is writable alongside the workspace, because
    /// enough ordinary commands — compilers, package managers, anything using
    /// `mkstemp` — write there that refusing it would make `workspace_write`
    /// mean "cannot run a build", and an operator would reach for
    /// `full_access` instead. Granting a directory the machine already treats
    /// as scratch buys back far more than it costs.
    pub fn new(mode: SandboxMode, workspace_root: PathBuf) -> Self {
        let writable = match mode {
            SandboxMode::ReadOnly | SandboxMode::FullAccess => Vec::new(),
            SandboxMode::WorkspaceWrite => vec![workspace_root, std::env::temp_dir()],
        };
        Self { mode, writable }
    }

    /// No restriction — for a caller with nothing to sandbox.
    pub fn unrestricted() -> Self {
        Self {
            mode: SandboxMode::FullAccess,
            writable: Vec::new(),
        }
    }

    pub fn mode(&self) -> SandboxMode {
        self.mode
    }

    /// Directories the child may write beneath.
    pub fn writable(&self) -> &[PathBuf] {
        &self.writable
    }

    /// Rewrite the command so the platform's sandbox wraps it.
    ///
    /// Identity everywhere the sandbox is applied to the process rather than
    /// around it (Linux), and everywhere it is not applied at all.
    pub(crate) fn wrap(&self, program: &str, args: &[String]) -> (String, Vec<String>) {
        if !self.mode.is_sandboxed() {
            return (program.to_owned(), args.to_vec());
        }
        #[cfg(target_os = "macos")]
        {
            return macos::wrap(self, program, args);
        }
        #[allow(unreachable_code)]
        (program.to_owned(), args.to_vec())
    }

    /// Arrange for the spawned child to be restricted.
    ///
    /// Called after the command is built and before it is spawned. Errors are
    /// fatal for the call: a tool that asked for a sandbox and did not get one
    /// must not run.
    pub(crate) fn harden(&self, command: &mut tokio::process::Command) -> Result<(), SandboxError> {
        if !self.mode.is_sandboxed() {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        {
            return linux::harden(self, command);
        }
        #[cfg(target_os = "macos")]
        {
            // `wrap` already put the command inside `sandbox-exec`.
            let _ = command;
            return Ok(());
        }
        #[allow(unreachable_code)]
        Err(SandboxError::Unavailable {
            mode: self.mode.as_str(),
            reason: "no sandbox backend for this platform",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_grants_nothing_writable() {
        let sandbox = Sandbox::new(SandboxMode::ReadOnly, PathBuf::from("/workspace"));
        assert!(sandbox.writable().is_empty());
        assert!(sandbox.mode().is_sandboxed());
    }

    /// The workspace *and* the temp dir — see the note on [`Sandbox::new`].
    #[test]
    fn workspace_write_grants_the_workspace_and_scratch() {
        let sandbox = Sandbox::new(SandboxMode::WorkspaceWrite, PathBuf::from("/workspace"));
        assert!(sandbox.writable().contains(&PathBuf::from("/workspace")));
        assert!(sandbox.writable().contains(&std::env::temp_dir()));
    }

    /// `full_access` is not a sandbox with everything granted; it is no sandbox
    /// at all, so nothing has to be enforceable for it to work.
    #[test]
    fn full_access_needs_no_backend() {
        let sandbox = Sandbox::new(SandboxMode::FullAccess, PathBuf::from("/workspace"));
        assert!(!sandbox.mode().is_sandboxed());
        let mut command = tokio::process::Command::new("true");
        assert!(sandbox.harden(&mut command).is_ok());
        assert_eq!(
            sandbox.wrap("echo", &["hi".to_owned()]),
            ("echo".to_owned(), vec!["hi".to_owned()])
        );
    }

    #[test]
    fn availability_answers_for_this_machine() {
        // Whatever it says, it says something specific: a caller that has to
        // skip a test needs a reason to print.
        assert!(!availability().describe().is_empty());
    }
}
