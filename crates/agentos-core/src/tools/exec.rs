//! Running a child process without betting the runtime on it.
//!
//! Roadmap item D2 in `docs/TRANSFER_ROADMAP.md`. Before this, `ShellTool` and
//! the isolation worker both called `std::process::Command::output()` from
//! inside an `async fn`. Three things follow from that, and all three are bad:
//!
//! - **It blocks a Tokio worker.** Not the task — the *thread*. On a
//!   `current_thread` runtime, which is what a run uses, that is the whole
//!   executor: every other conversation on that thread stops until the child
//!   exits.
//! - **It cannot be cancelled.** D1 races tool calls against the run's token,
//!   but a `select!` can only observe cancellation when the work yields, and
//!   blocking work never does.
//! - **It has no deadline and no output bound.** A child that hangs hangs the
//!   process; a child that prints forever is read into memory forever.
//!
//! [`run`] fixes all three: `tokio::process` throughout, a hard deadline, and a
//! byte cap on what is captured.
//!
//! # How the child is reaped
//!
//! `kill_on_drop(true)` for the direct child. Both ways one can outlive its
//! usefulness end in the same place: the deadline drops the future that owns
//! the `Child`, and D1's cancellation drops the whole call. Tokio signals the
//! child on that drop and its orphan reaper collects it, so there is no path
//! where a caller must remember to clean up — which is exactly why this is not
//! an explicit `kill().await` somewhere in [`run`]. `a_killed_child_leaves_no_survivor`
//! checks the pid is actually gone rather than trusting the mechanism.
//!
//! Since M4 / `PROC-001` it reaps the *process group*, not only the direct
//! child. Every child is spawned into a new group of its own, and a deadline
//! or a cancellation signals the group — so a shell that forks before it is
//! killed no longer leaves a grandchild reparented to init and still running.
//! `a_killed_child_leaves_no_grandchild` is the test that says so; it was red
//! on purpose until this landed. The group is signalled only on those two
//! paths: a command that *finishes* may have started something on purpose,
//! and killing it afterwards would make `sh -c 'daemon &'` mean something
//! different here than everywhere else.
//!
//! # The child does not inherit the runtime's environment
//!
//! `env_clear`, then an allowlist ([`crate::tools::child_env`]). Before this
//! every subprocess the model could reach — including the isolation worker —
//! saw every provider and channel credential the gateway holds, and could
//! print them with `env`.
//!
//! # Capping is not the same as not reading
//!
//! [`capped`] keeps the first `max_output_bytes` and then **keeps draining and
//! discarding** to EOF. Simply stopping at the cap looks equivalent and is not:
//! the child's pipe fills, it blocks in `write`, and it never exits — so a
//! chatty-but-healthy tool would come back as a deadline failure instead of a
//! truncated success. The first version of this module had that bug and its
//! test caught it.

use crate::sandbox::Sandbox;
use crate::tools::child_env;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::time::timeout;

/// Bytes captured from each of stdout and stderr before the rest is dropped.
///
/// Generous next to `[limits].tool_result_inline_bytes` (64 KiB by default),
/// because C2 spills what does not fit inline and can only spill what was
/// read. The cap exists to bound memory against a runaway process, not to
/// shape what the model sees.
pub const DEFAULT_MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("failed to start {program}: {source}")]
    Spawn {
        program: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed while running {program}: {source}")]
    Io {
        program: String,
        #[source]
        source: std::io::Error,
    },
    /// The child outlived its deadline and was killed.
    ///
    /// A distinct variant because the caller turns this into a *failed
    /// `ToolResult`* the model can read and work around, never a run error.
    #[error("{program} exceeded its {timeout_ms} ms deadline and was terminated")]
    TimedOut { program: String, timeout_ms: u64 },
    /// The sandbox the tool asked for could not be applied.
    ///
    /// Fatal for the call rather than a warning: the alternative to a sandbox
    /// that failed to build is running the tool with everything it asked not
    /// to have.
    #[error("cannot sandbox {program}: {source}")]
    Sandbox {
        program: String,
        #[source]
        source: crate::sandbox::SandboxError,
    },
}

/// What to run, and the bounds it runs under.
pub struct Exec<'a> {
    pub program: &'a str,
    pub args: &'a [String],
    /// What this child may write (roadmap item X2). Every subprocess the
    /// runtime starts goes through here, so this is the one place a sandbox
    /// has to be applied to cover all of them.
    pub sandbox: &'a Sandbox,
    pub cwd: Option<PathBuf>,
    /// Written to the child's stdin, which is then closed. `None` closes it
    /// immediately, so a child that reads stdin sees EOF rather than hanging.
    pub stdin: Option<&'a [u8]>,
    pub timeout: Duration,
    pub max_output_bytes: usize,
    /// Variable names to pass through on top of the allowlist in
    /// [`crate::tools::child_env`] (`[isolation].env_passthrough`).
    ///
    /// Names, not values: what a deployment declares is which of *its own*
    /// variables a tool may see, never a value this code could invent.
    pub extra_env: &'a [String],
}

/// One finished child process.
#[derive(Debug)]
pub struct ExecOutput {
    /// `None` when the child was terminated by a signal.
    pub status: Option<i32>,
    pub success: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// Whether either stream hit [`Exec::max_output_bytes`].
    pub truncated: bool,
}

/// Run a child process to completion, under a deadline.
pub async fn run(exec: Exec<'_>) -> Result<ExecOutput, ExecError> {
    let program = exec.program.to_owned();
    let timeout_ms = exec.timeout.as_millis() as u64;

    // On a platform that sandboxes by wrapping (macOS), this is where the
    // wrapper takes over as the program being run; elsewhere it is the tool's
    // own command, restricted below.
    let (spawned, args) = exec.sandbox.wrap(exec.program, exec.args);
    let mut command = Command::new(&spawned);
    command
        .args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Covers the dropped-future path — D1 cancellation — where no code of
        // ours runs to clean up.
        .kill_on_drop(true);
    // M4 / `PROC-001`: the child starts from nothing and is given back only
    // what it needs, so a credential the gateway holds is not one `env` away
    // from the model.
    command.env_clear();
    command.envs(child_env::minimal(exec.extra_env));
    if let Some(cwd) = &exec.cwd {
        command.current_dir(cwd);
    }
    // Its own process group, so the deadline below can end everything the
    // command started rather than only the command.
    #[cfg(unix)]
    command.process_group(0);
    // Before the spawn, and fatal if it fails: a tool that asked to be
    // sandboxed must not run unsandboxed instead.
    exec.sandbox
        .harden(&mut command)
        .map_err(|source| ExecError::Sandbox {
            program: program.clone(),
            source,
        })?;

    let mut child = command.spawn().map_err(|source| ExecError::Spawn {
        program: program.clone(),
        source,
    })?;
    // `process_group(0)` makes the child's pid its own pgid, so this is the
    // handle to everything it goes on to start. Taken before `child` is moved
    // into the future below, which is the last point it can be read.
    let mut group = child.id().map(ProcessGroup::new);

    // Taken up front: `wait` needs the pipes closed or a child that fills one
    // never exits, and holding them past this point would deadlock the join
    // below on a child that writes more than it reads.
    let stdin = child.stdin.take();
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();

    let payload = exec.stdin.unwrap_or(&[]).to_vec();
    let cap = exec.max_output_bytes;
    let gathered = timeout(exec.timeout, async {
        // Feed stdin and drain both pipes concurrently. Doing stdin first and
        // sequentially would deadlock on a child that writes more than one
        // pipe buffer before reading its input.
        let write = async {
            if let Some(mut stdin) = stdin {
                stdin.write_all(&payload).await?;
                stdin.shutdown().await?;
            }
            Ok::<(), std::io::Error>(())
        };
        let read_out = capped(&mut stdout, cap);
        let read_err = capped(&mut stderr, cap);
        let (write, out, err) = tokio::join!(write, read_out, read_err);
        write?;
        let (stdout, out_truncated) = out?;
        let (stderr, err_truncated) = err?;
        let status = child.wait().await?;
        Ok::<_, std::io::Error>(ExecOutput {
            status: status.code(),
            success: status.success(),
            stdout,
            stderr,
            truncated: out_truncated || err_truncated,
        })
    })
    .await;

    match gathered {
        // The command finished on its own terms. Anything it deliberately
        // left running is its business, so the group is not signalled.
        Ok(Ok(output)) => {
            if let Some(group) = group.as_mut() {
                group.leave_running();
            }
            Ok(output)
        }
        Ok(Err(source)) => Err(ExecError::Io { program, source }),
        Err(_elapsed) => {
            // `child` was moved into the future above, which the timeout has
            // now dropped — `kill_on_drop` has already signalled the direct
            // child. This signals the rest of its group, which is where a
            // forked grandchild lives.
            if let Some(group) = group.as_mut() {
                group.terminate();
            }
            Err(ExecError::TimedOut {
                program,
                timeout_ms,
            })
        }
    }
}

/// A spawned child's process group, and the ability to end it.
///
/// Exists as a type rather than a call so the cancellation path is covered
/// too: D1 drops the whole `run` future, and no code of ours runs at the end
/// of the function — but `Drop` does. `kill_on_drop` handles the direct child
/// on that path and nothing below it, which is exactly the gap
/// `a_killed_child_leaves_no_grandchild` was written against.
struct ProcessGroup {
    pgid: u32,
    /// Cleared once the command has finished on its own terms, so a normal
    /// exit does not sweep up whatever the command started on purpose.
    signal_on_drop: bool,
}

impl ProcessGroup {
    fn new(pgid: u32) -> Self {
        Self {
            pgid,
            signal_on_drop: true,
        }
    }

    /// The command exited by itself. Leave its descendants alone.
    fn leave_running(&mut self) {
        self.signal_on_drop = false;
    }

    /// End every process in the group, now.
    ///
    /// `SIGKILL` rather than `SIGTERM`: this runs only after a deadline has
    /// already expired or a run has been cancelled, so the process has had
    /// whatever time it was going to get. A graceful signal here would mean a
    /// second deadline to enforce, for a case where the caller has already
    /// stopped waiting.
    fn terminate(&mut self) {
        self.signal_on_drop = false;
        #[cfg(unix)]
        // SAFETY: `kill` with a negative pid signals a process group and
        // touches no memory. A pgid that no longer exists returns `ESRCH`,
        // which is ignored.
        unsafe {
            libc::kill(-(self.pgid as i32), libc::SIGKILL);
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if self.signal_on_drop {
            self.terminate();
        }
    }
}

/// Keep the first `cap` bytes, discard the rest, and report whether there was
/// a rest. Always reads to EOF — see the module docs on why stopping early
/// would hang the child.
async fn capped<R>(reader: &mut Option<R>, cap: usize) -> std::io::Result<(Vec<u8>, bool)>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let Some(reader) = reader.as_mut() else {
        return Ok((Vec::new(), false));
    };
    let mut kept = Vec::new();
    // One byte past the cap, so hitting it is distinguishable from exactly
    // filling it.
    reader.take(cap as u64 + 1).read_to_end(&mut kept).await?;
    if kept.len() <= cap {
        return Ok((kept, false));
    }
    kept.truncate(cap);

    // Drain the overflow so the child can finish writing and exit.
    let mut scratch = [0u8; 8 * 1024];
    while reader.read(&mut scratch).await? > 0 {}
    Ok((kept, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// These cover the child-process mechanics, so they run unsandboxed; the
    /// sandbox has its own suite in `tests/sandbox.rs`.
    static UNRESTRICTED: std::sync::LazyLock<Sandbox> =
        std::sync::LazyLock::new(Sandbox::unrestricted);

    /// A per-test scratch path for a child to record a pid in.
    ///
    /// Hashed rather than interpolating the ids: a `ThreadId` renders with
    /// parentheses, which the shell commands below would then choke on.
    fn probe_path(label: &str) -> PathBuf {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(
            &(std::process::id(), std::thread::current().id(), label),
            &mut hasher,
        );
        std::env::temp_dir().join(format!(
            "{label}-{:016x}",
            std::hash::Hasher::finish(&hasher)
        ))
    }

    /// Whether `pid` is still around after a couple of seconds of polling.
    ///
    /// Polled rather than slept once: a kill is asynchronous, so the only
    /// thing worth asserting is that it happens promptly, not instantly. Kills
    /// whatever survives, so one test's orphan is not the next one's.
    async fn stays_alive(pid: &str) -> bool {
        let mut alive = true;
        for _ in 0..20 {
            let probe = std::process::Command::new("kill")
                .args(["-0", pid])
                .output()
                .expect("kill -0 runs");
            if !probe.status.success() {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if alive {
            let _ = std::process::Command::new("kill")
                .args(["-9", pid])
                .output();
        }
        alive
    }

    fn exec<'a>(program: &'a str, args: &'a [String]) -> Exec<'a> {
        Exec {
            program,
            args,
            cwd: None,
            stdin: None,
            timeout: Duration::from_secs(10),
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            sandbox: &UNRESTRICTED,
            extra_env: &[],
        }
    }

    #[tokio::test]
    async fn a_successful_command_returns_its_output() {
        let args = vec!["hello".to_owned()];
        let output = run(exec("echo", &args)).await.expect("echo runs");
        assert!(output.success);
        assert_eq!(output.status, Some(0));
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "hello");
        assert!(!output.truncated);
    }

    #[tokio::test]
    async fn a_failing_command_reports_its_exit_code_rather_than_erroring() {
        // A non-zero exit is the tool's business, not a runtime failure.
        let args = vec!["-c".to_owned(), "exit 3".to_owned()];
        let output = run(exec("sh", &args)).await.expect("sh runs");
        assert!(!output.success);
        assert_eq!(output.status, Some(3));
    }

    #[tokio::test]
    async fn stdin_is_written_and_closed() {
        let args = vec![];
        let mut spec = exec("cat", &args);
        spec.stdin = Some(b"piped input");
        let output = run(spec).await.expect("cat runs");
        // `cat` only exits when stdin closes, so a returned result proves the
        // close happened as well as the write.
        assert_eq!(String::from_utf8_lossy(&output.stdout), "piped input");
    }

    #[tokio::test]
    async fn a_child_that_reads_nothing_does_not_deadlock_on_a_large_stdin() {
        // The failure mode of writing stdin before draining stdout: `true`
        // never reads, so a large write fills the pipe and blocks forever
        // unless the reads run concurrently.
        let args = vec![];
        let payload = vec![b'x'; 1024 * 1024];
        let mut spec = exec("true", &args);
        spec.stdin = Some(&payload);
        spec.timeout = Duration::from_secs(5);
        // Either it completes, or stdin errored with EPIPE because the child
        // exited first — both are fine, and neither is a hang.
        let outcome = run(spec).await;
        assert!(
            !matches!(outcome, Err(ExecError::TimedOut { .. })),
            "writing stdin must not deadlock against an unread pipe"
        );
    }

    #[tokio::test]
    async fn a_command_past_its_deadline_is_killed_rather_than_awaited() {
        let args = vec!["30".to_owned()];
        let mut spec = exec("sleep", &args);
        spec.timeout = Duration::from_millis(150);

        let started = std::time::Instant::now();
        let error = run(spec).await.expect_err("sleep 30 must not complete");

        assert!(matches!(error, ExecError::TimedOut { .. }), "{error:?}");
        // The point of the whole module: the deadline is honoured in real
        // time, not after the child decides to finish.
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "returned only after {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_killed_child_leaves_no_survivor() {
        // The Verify's "no orphan process", checked by pid rather than by
        // grepping `ps` output. An argv grep cannot work here: the checking
        // command necessarily contains whatever pattern it is looking for, so
        // it matches itself, and a bare `grep sleep` would be an assertion
        // about the whole machine rather than about this test's child.
        //
        // `exec` matters — without it the shell forks `sleep` as a grandchild
        // that outlives the shell we kill, which is a real orphan and exactly
        // what this asserts against.
        // Hashed rather than interpolating the ids: a `ThreadId` renders with
        // parentheses, which the shell command below would then choke on.
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(
            &(std::process::id(), std::thread::current().id()),
            &mut hasher,
        );
        let pid_file = std::env::temp_dir().join(format!(
            "agentos-orphan-probe-{:016x}",
            std::hash::Hasher::finish(&hasher)
        ));
        let _ = std::fs::remove_file(&pid_file);
        let args = vec![
            "-c".to_owned(),
            format!("echo $$ > {}; exec sleep 30", pid_file.display()),
        ];
        let mut spec = exec("sh", &args);
        spec.timeout = Duration::from_millis(300);
        let error = run(spec).await.expect_err("the sleep must not complete");
        assert!(matches!(error, ExecError::TimedOut { .. }));

        let pid = std::fs::read_to_string(&pid_file)
            .expect("the child recorded its pid")
            .trim()
            .to_owned();
        let _ = std::fs::remove_file(&pid_file);

        // Poll rather than sleep once: the kill is asynchronous, so the only
        // thing worth asserting is that it happens promptly, not instantly.
        let mut alive = true;
        for _ in 0..20 {
            let probe = std::process::Command::new("kill")
                .args(["-0", &pid])
                .output()
                .expect("kill -0 runs");
            if !probe.status.success() {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(!alive, "pid {pid} was left running after its deadline");
    }

    /// The case the test above sidesteps with `exec`.
    ///
    /// `kill_on_drop` signals the direct child and nothing else, so a shell
    /// that forks before it is killed used to leave a grandchild reparented to
    /// init and still running past the deadline the caller asked for. That is
    /// the orphan `a_killed_child_leaves_no_survivor`'s own comment names and
    /// then avoids, which made "no orphan process" a claim no test checked.
    ///
    /// Green since M4 / `PROC-001`: the child is spawned into its own process
    /// group and the deadline signals the group, so the grandchild goes with
    /// its parent.
    #[tokio::test]
    async fn a_killed_child_leaves_no_grandchild() {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(
            &(std::process::id(), std::thread::current().id()),
            &mut hasher,
        );
        let pid_file = std::env::temp_dir().join(format!(
            "agentos-grandchild-probe-{:016x}",
            std::hash::Hasher::finish(&hasher)
        ));
        let _ = std::fs::remove_file(&pid_file);

        // `sleep 10` rather than 30: long enough to outlast the 300ms deadline
        // and the poll window below by a wide margin, short enough that a
        // surviving orphan cleans itself up soon after the test reports.
        let args = vec![
            "-c".to_owned(),
            format!("sleep 10 & echo $! > {}; sleep 10", pid_file.display()),
        ];
        let mut spec = exec("sh", &args);
        spec.timeout = Duration::from_millis(300);
        let error = run(spec).await.expect_err("the shell must not complete");
        assert!(matches!(error, ExecError::TimedOut { .. }));

        let pid = std::fs::read_to_string(&pid_file)
            .expect("the shell recorded its grandchild's pid")
            .trim()
            .to_owned();
        let _ = std::fs::remove_file(&pid_file);

        let mut alive = true;
        for _ in 0..20 {
            let probe = std::process::Command::new("kill")
                .args(["-0", &pid])
                .output()
                .expect("kill -0 runs");
            if !probe.status.success() {
                alive = false;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Best effort: do not leave the orphan behind for the next test.
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid])
            .output();
        assert!(
            !alive,
            "grandchild pid {pid} outlived the deadline its parent was killed for"
        );
    }

    /// Two directions of the same rule, so "kill the group" does not quietly
    /// become "kill anything the command started".
    ///
    /// A command that *finishes* may have started something deliberately —
    /// `sh -c 'daemon &'` is a normal thing to run — and sweeping it up
    /// afterwards would make this runtime's shell mean something different
    /// from every other one. Only a deadline or a cancellation signals the
    /// group.
    #[tokio::test]
    async fn a_command_that_finishes_leaves_its_deliberate_background_child_alone() {
        let pid_file = probe_path("agentos-deliberate-child");
        let _ = std::fs::remove_file(&pid_file);

        // Redirected, because a background child that keeps the shell's
        // stdout pipe open holds `run`'s reads open with it — the shell would
        // exit and the call would still wait for the `sleep`, which is a
        // different thing from what this test is about.
        let args = vec![
            "-c".to_owned(),
            format!("sleep 5 >/dev/null 2>&1 & echo $! > {}", pid_file.display()),
        ];
        let mut spec = exec("sh", &args);
        spec.timeout = Duration::from_secs(10);
        let output = run(spec).await.expect("the shell finishes promptly");
        assert!(output.success);

        let pid = std::fs::read_to_string(&pid_file)
            .expect("the shell recorded its background child's pid")
            .trim()
            .to_owned();
        let _ = std::fs::remove_file(&pid_file);

        // Immediately: the sleep has 5 seconds left, so anything that ended it
        // was this module.
        let probe = std::process::Command::new("kill")
            .args(["-0", &pid])
            .output()
            .expect("kill -0 runs");
        let alive = probe.status.success();
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid])
            .output();
        assert!(
            alive,
            "pid {pid} was started deliberately by a command that then exited cleanly, \
             and must not have been swept up with it"
        );
    }

    /// The cancellation path, which no code of ours runs at the end of: D1
    /// drops the whole `run` future, and the process group is ended by the
    /// guard's `Drop` rather than by a line in [`run`].
    #[tokio::test]
    async fn a_cancelled_call_takes_the_whole_group_with_it() {
        let pid_file = probe_path("agentos-cancelled-group");
        let _ = std::fs::remove_file(&pid_file);

        let args = vec![
            "-c".to_owned(),
            format!("sleep 10 & echo $! > {}; sleep 10", pid_file.display()),
        ];
        let mut spec = exec("sh", &args);
        // Far longer than the outer timeout: what ends this call must be the
        // caller giving up on it, not its own deadline. Losing a `timeout`
        // drops the future, which is exactly what `unless_cancelled` does to a
        // tool call when a run is cancelled.
        spec.timeout = Duration::from_secs(30);
        let abandoned = tokio::time::timeout(Duration::from_millis(300), run(spec)).await;
        assert!(abandoned.is_err(), "the call must not have finished");

        let pid = std::fs::read_to_string(&pid_file)
            .expect("the shell recorded its grandchild's pid")
            .trim()
            .to_owned();
        let _ = std::fs::remove_file(&pid_file);

        assert!(
            !stays_alive(&pid).await,
            "grandchild pid {pid} outlived the cancelled call that started it"
        );
    }

    #[tokio::test]
    async fn output_past_the_cap_is_truncated_rather_than_buffered() {
        let args = vec![
            "-c".to_owned(),
            "head -c 100000 /dev/zero | tr '\\0' 'x'".to_owned(),
        ];
        let mut spec = exec("sh", &args);
        spec.max_output_bytes = 1_024;
        let output = run(spec).await.expect("sh runs");
        assert_eq!(output.stdout.len(), 1_024);
        assert!(output.truncated);
    }

    #[tokio::test]
    async fn output_exactly_at_the_cap_is_not_reported_truncated() {
        // The off-by-one the `cap + 1` read exists to get right.
        let args = vec!["-c".to_owned(), "printf 'abcd'".to_owned()];
        let mut spec = exec("sh", &args);
        spec.max_output_bytes = 4;
        let output = run(spec).await.expect("sh runs");
        assert_eq!(output.stdout, b"abcd");
        assert!(!output.truncated);
    }

    #[tokio::test]
    async fn a_missing_program_is_a_spawn_error_not_a_panic() {
        let args = vec![];
        let error = run(exec("agentos-no-such-program", &args))
            .await
            .expect_err("a missing program cannot run");
        assert!(matches!(error, ExecError::Spawn { .. }), "{error:?}");
    }
}
