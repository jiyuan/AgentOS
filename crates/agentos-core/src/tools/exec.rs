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
//! `kill_on_drop(true)`, and nothing else. Both ways a child can outlive its
//! usefulness end in the same place: the deadline drops the future that owns
//! the `Child`, and D1's cancellation drops the whole call. Tokio signals the
//! child on that drop and its orphan reaper collects it, so there is no path
//! where a caller must remember to clean up — which is exactly why this is not
//! an explicit `kill().await` somewhere in [`run`]. `a_killed_child_leaves_no_survivor`
//! checks the pid is actually gone rather than trusting the mechanism.
//!
//! It reaps the *direct child only*. A child that forks before it is killed
//! leaves a grandchild reparented to init and still running — see the ignored
//! `a_killed_child_leaves_no_grandchild`, which is red on purpose until M4 in
//! `docs/AUDIT_REMEDIATION_PLAN.md` terminates the process group. Read the
//! deadline as a bound on the process this module started, not on everything
//! that process went on to start.
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
    if let Some(cwd) = &exec.cwd {
        command.current_dir(cwd);
    }
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
        Ok(Ok(output)) => Ok(output),
        Ok(Err(source)) => Err(ExecError::Io { program, source }),
        Err(_elapsed) => {
            // `child` was moved into the future above, which the timeout has
            // now dropped — `kill_on_drop` has already signalled it. Nothing
            // is left to await here, so the reap is the runtime's, and the
            // caller gets a deadline error rather than a hang.
            Err(ExecError::TimedOut {
                program,
                timeout_ms,
            })
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

    fn exec<'a>(program: &'a str, args: &'a [String]) -> Exec<'a> {
        Exec {
            program,
            args,
            cwd: None,
            stdin: None,
            timeout: Duration::from_secs(10),
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            sandbox: &UNRESTRICTED,
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
    /// that forks before it is killed leaves a grandchild reparented to init
    /// and still running past the deadline the caller asked for. That is the
    /// orphan `a_killed_child_leaves_no_survivor`'s own comment names and then
    /// avoids, which made "no orphan process" a claim no test checked.
    ///
    /// Ignored because it is red on purpose: M4 in
    /// `docs/AUDIT_REMEDIATION_PLAN.md` terminates the process group rather
    /// than the direct child, and this is the test that will show it worked.
    /// Run with `cargo test -p agentos-core -- --ignored a_killed_child_leaves_no_grandchild`.
    #[tokio::test]
    #[ignore = "red until M4 terminates the process group; see AUDIT_REMEDIATION_PLAN.md"]
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
