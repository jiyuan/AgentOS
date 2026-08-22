//! One live stdio MCP server: how it is started, how it is spoken to, and
//! every way it is bounded (M8 / `MCP-001`, deliverables 6, 7 and 8).
//!
//! # What was there before
//!
//! A `std::process` child with `Stdio::null()` on its stderr, a dedicated
//! thread doing one `write_all` and one `read_line` per call, and an
//! **unbounded** `std::sync::mpsc` in front of it. `read_line` had no ceiling,
//! so a server that emitted a gigabyte on one line was a gigabyte of
//! allocation. A timeout killed the child. There was no correlation between
//! request and reply — whatever line came back was the answer — so one stray
//! line desynchronized every later call. And nothing ever restarted: a server
//! that died once was gone until the process was.
//!
//! # The shape now
//!
//! One reader task per connection owns stdout and dispatches each line by its
//! JSON-RPC id to the caller waiting on it. That single change is what makes
//! everything else possible: concurrent calls, out-of-order replies, a timeout
//! that cancels one request rather than killing the server, and
//! server-initiated notifications having somewhere to go.
//!
//! # Isolation
//!
//! The server child is spawned through [`Sandbox`] directly (deliverable 7).
//! MCP calls used to be routed through the shell-only isolation worker, which
//! could not run them — so in practice an MCP server ran unsandboxed with the
//! declared mode as decoration. Now the mode restricts *the server process*,
//! which is the process that touches the filesystem. Where no backend exists,
//! [`Sandbox::harden`] fails and the server does not start: the same
//! fail-closed rule as every other sandboxed child.

use super::protocol::{
    self, Message, ProtocolError, Request, RequestId, ServerHandshake, ToolPage,
};
use crate::sandbox::Sandbox;
use agentos_interfaces::mcp::McpError;
use agentos_interfaces::tool::{SandboxMode, ToolSpec};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::oneshot;

/// The largest single JSON-RPC message this client will read.
///
/// One line, one message, so this is also the ceiling on how much a server can
/// make this process allocate in one go. Four megabytes is far past any
/// plausible tool result — a bigger one belongs in a resource, which is a URI
/// — and far short of a memory problem.
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// Requests that may be in flight on one connection at once.
///
/// The pending map is the queue, and an unbounded one is an unbounded
/// allocation driven by how fast the model asks. A caller past the bound is
/// refused rather than queued: the tool deadline is already the answer to "the
/// server is slow", and stacking more work behind it makes that worse.
pub const MAX_PENDING_REQUESTS: usize = 64;

/// Bytes of a server's stderr kept for diagnostics.
///
/// Kept at all, which is the change — it used to be `Stdio::null()`, so a
/// server that explained exactly why it was failing explained it to nothing.
/// Bounded, and it is the *tail* that is kept: the last thing a dying process
/// says is the useful part.
pub const MAX_STDERR_BYTES: usize = 16 * 1024;

/// Pages of `tools/list` this client will walk.
///
/// A cursor is server-supplied and a server that returns one forever is a
/// loop. Bounded, and a repeated cursor stops the walk immediately.
pub const MAX_TOOL_PAGES: usize = 32;

/// Tools one server may offer. Past this the catalog stops being something a
/// model can choose from, and every one of them costs tokens in every request.
pub const MAX_TOOLS_PER_SERVER: usize = 256;

/// Restarts allowed inside [`RESTART_WINDOW`] before the connection gives up.
///
/// A server that crashes on every call would otherwise be restarted on every
/// call, turning a broken deployment into a fork bomb with extra steps.
pub const MAX_RESTARTS_PER_WINDOW: u32 = 5;
pub const RESTART_WINDOW: Duration = Duration::from_secs(60);

/// How long a graceful shutdown waits at each step: stdin closed, then
/// `SIGTERM`, then `SIGKILL`.
const SHUTDOWN_STEP: Duration = Duration::from_secs(2);

/// Requests waiting for a reply, keyed by the id they were sent with. The
/// reader task is the only thing that removes an entry on the happy path.
type Pending = Arc<Mutex<HashMap<RequestId, oneshot::Sender<Result<Value, ProtocolError>>>>>;

/// A started, initialized MCP server.
pub struct Connection {
    /// Shared with the reader task, which answers a server-initiated request
    /// with a structured `method not found` rather than leaving the server
    /// waiting on a reply that will never come.
    stdin: Arc<tokio::sync::Mutex<ChildStdin>>,
    child: tokio::sync::Mutex<Child>,
    pending: Pending,
    next_id: AtomicU64,
    stderr: Arc<Mutex<StderrTail>>,
    handshake: ServerHandshake,
    reader: tokio::task::JoinHandle<()>,
}

/// The last [`MAX_STDERR_BYTES`] a server wrote to stderr.
#[derive(Default)]
pub struct StderrTail {
    bytes: Vec<u8>,
}

impl StderrTail {
    fn push(&mut self, line: &str) {
        self.bytes.extend_from_slice(line.as_bytes());
        self.bytes.push(b'\n');
        if self.bytes.len() > MAX_STDERR_BYTES {
            // Drop from the front: the tail is what says why it died.
            let excess = self.bytes.len() - MAX_STDERR_BYTES;
            self.bytes.drain(..excess);
        }
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }
}

fn failed(message: impl Into<String>) -> McpError {
    McpError::Failed(Arc::from(message.into()))
}

impl Connection {
    /// Spawn `program` with `args` under `sandbox`, complete the MCP
    /// handshake, and return the live connection.
    ///
    /// The handshake happens here rather than lazily: a server that answers
    /// with a protocol version this build cannot read, or that offers no
    /// tools capability, is a deployment error, and it should surface at
    /// startup rather than at whatever moment the model first reaches for it.
    pub async fn open(
        program: &str,
        args: &[String],
        env: std::collections::BTreeMap<std::ffi::OsString, std::ffi::OsString>,
        sandbox: &Sandbox,
        timeout: Duration,
    ) -> Result<Self, McpError> {
        let (program, args) = sandbox.wrap(program, args);
        let mut command = Command::new(&program);
        command.args(&args);
        // M4 / `PROC-001`: an MCP server is third-party code the deployment
        // chose to run, so it gets the neutral allowlist and nothing else.
        command.env_clear();
        command.envs(env);
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        // Fail-closed: a mode the kernel cannot enforce is not a mode.
        sandbox
            .harden(&mut command)
            .map_err(|err| failed(format!("MCP server '{program}' cannot be sandboxed: {err}")))?;

        let mut child = command
            .spawn()
            .map_err(|err| failed(format!("MCP server '{program}' failed to start: {err}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| failed("MCP server stdin unavailable"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| failed("MCP server stdout unavailable"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| failed("MCP server stderr unavailable"))?;

        let pending: Pending = Arc::default();
        let tail = Arc::new(Mutex::new(StderrTail::default()));
        let stdin = Arc::new(tokio::sync::Mutex::new(stdin));
        tokio::spawn(read_stderr(stderr, Arc::clone(&tail)));
        let reader = tokio::spawn(read_stdout(
            stdout,
            Arc::clone(&pending),
            Arc::clone(&stdin),
        ));

        let connection = Self {
            stdin,
            child: tokio::sync::Mutex::new(child),
            pending,
            next_id: AtomicU64::new(1),
            stderr: tail,
            // Replaced below. Constructing it first lets the handshake go
            // through the ordinary request path rather than a second one.
            handshake: ServerHandshake {
                protocol_version: Arc::from(""),
                server_name: Arc::from(""),
                server_version: Arc::from(""),
                tools_list_changed: false,
            },
            reader,
        };

        let result = connection
            .request("initialize", protocol::initialize_params(), timeout)
            .await?;
        let handshake = protocol::parse_initialize(&result).map_err(|err| {
            failed(format!(
                "MCP server '{program}' handshake failed: {err}{}",
                connection.stderr_context()
            ))
        })?;
        // MCP requires this before any other request; a server may legitimately
        // refuse everything until it arrives.
        connection
            .notify("notifications/initialized", serde_json::json!({}))
            .await?;

        Ok(Self {
            handshake,
            ..connection
        })
    }

    pub fn handshake(&self) -> &ServerHandshake {
        &self.handshake
    }

    /// The tail of the server's stderr, rendered for an error message.
    pub fn stderr_context(&self) -> String {
        let tail = self
            .stderr
            .lock()
            .map(|tail| tail.text())
            .unwrap_or_default();
        let trimmed = tail.trim();
        if trimmed.is_empty() {
            return String::new();
        }
        format!("; server stderr: {trimmed}")
    }

    /// Whether the reader task is still running. A finished reader means
    /// stdout closed, which means the server is gone.
    pub fn is_live(&self) -> bool {
        !self.reader.is_finished()
    }

    /// Every tool the server offers, following `nextCursor` to the end.
    pub async fn list_tools(
        &self,
        sandbox: SandboxMode,
        timeout: Duration,
    ) -> Result<Vec<ToolSpec>, McpError> {
        let mut tools = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen_cursors: Vec<String> = Vec::new();
        for _ in 0..MAX_TOOL_PAGES {
            let params = match &cursor {
                Some(cursor) => serde_json::json!({ "cursor": cursor }),
                None => serde_json::json!({}),
            };
            let result = self.request("tools/list", params, timeout).await?;
            let ToolPage {
                tools: page,
                next_cursor,
            } = protocol::parse_tool_page(&result, sandbox)
                .map_err(|err| failed(format!("MCP tools/list failed: {err}")))?;
            tools.extend(page);
            if tools.len() > MAX_TOOLS_PER_SERVER {
                return Err(failed(format!(
                    "MCP server offered more than {MAX_TOOLS_PER_SERVER} tools"
                )));
            }
            let Some(next) = next_cursor else {
                return Ok(tools);
            };
            // A server that returns the cursor it was just given is a loop
            // that the page budget alone would take 32 round trips to notice.
            if seen_cursors.contains(&next) {
                return Err(failed("MCP server repeated a pagination cursor"));
            }
            seen_cursors.push(next.clone());
            cursor = Some(next);
        }
        Err(failed(format!(
            "MCP tools/list did not finish within {MAX_TOOL_PAGES} pages"
        )))
    }

    /// Call one tool.
    pub async fn call_tool(
        &self,
        name: &str,
        arguments: &Value,
        timeout: Duration,
    ) -> Result<Value, McpError> {
        self.request(
            "tools/call",
            protocol::call_params(name, arguments),
            timeout,
        )
        .await
    }

    /// Send a request and wait for the reply with that id.
    async fn request(
        &self,
        method: &'static str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, McpError> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self
                .pending
                .lock()
                .map_err(|_| failed("MCP pending-request map poisoned"))?;
            if pending.len() >= MAX_PENDING_REQUESTS {
                return Err(failed(format!(
                    "MCP server already has {MAX_PENDING_REQUESTS} requests in flight"
                )));
            }
            pending.insert(id, tx);
        }

        if let Err(err) = self.write(&Request::call(id, method, params)).await {
            self.forget(id);
            return Err(err);
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(Ok(result))) => Ok(result),
            Ok(Ok(Err(err))) => Err(failed(format!(
                "MCP {method} failed: {err}{}",
                self.stderr_context()
            ))),
            // The reader dropped the sender, which it only does when stdout
            // closed. The server is gone.
            Ok(Err(_)) => Err(failed(format!(
                "MCP server closed while answering {method}{}",
                self.stderr_context()
            ))),
            Err(_) => {
                self.forget(id);
                // Tell the server to stop working on it, rather than killing
                // the server. A timeout is one slow call; the old code took
                // the whole connection down with it, so every *other*
                // conversation using that server lost its tools too.
                let _ = self
                    .write(&Request::notify(
                        "notifications/cancelled",
                        protocol::cancelled_params(id, "client deadline exceeded"),
                    ))
                    .await;
                Err(failed(format!(
                    "MCP {method} timed out after {} ms",
                    timeout.as_millis()
                )))
            }
        }
    }

    async fn notify(&self, method: &'static str, params: Value) -> Result<(), McpError> {
        self.write(&Request::notify(method, params)).await
    }

    async fn write(&self, request: &Request) -> Result<(), McpError> {
        let line = request
            .encode()
            .map_err(|err| failed(format!("MCP request could not be encoded: {err}")))?;
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(line.as_bytes())
            .await
            .map_err(|err| failed(format!("MCP server stdin write failed: {err}")))?;
        stdin
            .flush()
            .await
            .map_err(|err| failed(format!("MCP server stdin flush failed: {err}")))
    }

    fn forget(&self, id: RequestId) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&id);
        }
    }

    /// Close the server down: stdin, then `SIGTERM`, then `SIGKILL`, each with
    /// its own deadline.
    ///
    /// MCP's stdio transport has no `shutdown` method — closing stdin *is* the
    /// request to exit, and a well-behaved server exits on EOF. The signals
    /// are for the ones that do not.
    pub async fn shutdown(self) {
        self.reader.abort();
        drop(self.stdin);
        let mut child = self.child.lock().await;
        if wait_briefly(&mut child).await {
            return;
        }
        #[cfg(unix)]
        if let Some(pid) = child.id() {
            // SAFETY: `kill` with a pid and a signal number. The pid is this
            // child's and the child has not been reaped, so it is not reused.
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
        }
        if wait_briefly(&mut child).await {
            return;
        }
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

async fn wait_briefly(child: &mut Child) -> bool {
    tokio::time::timeout(SHUTDOWN_STEP, child.wait())
        .await
        .is_ok()
}

/// Own stdout and hand each reply to whoever is waiting for it.
///
/// The correlation is the point of this task. Without it a reply is matched to
/// a request by *arrival order*, so one unsolicited line — a log message, a
/// notification, a duplicate — shifts every later answer onto the wrong
/// question, permanently and silently.
async fn read_stdout(
    stdout: tokio::process::ChildStdout,
    pending: Pending,
    stdin: Arc<tokio::sync::Mutex<ChildStdin>>,
) {
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    loop {
        line.clear();
        // `take` caps the read, so a server that never emits a newline costs
        // the cap rather than all of memory. Rebuilt per line because the
        // limit is per message.
        let mut limited = (&mut reader).take(MAX_MESSAGE_BYTES as u64 + 1);
        let read = match limited.read_line(&mut line).await {
            Ok(0) => break,
            Ok(read) => read,
            // Invalid UTF-8 or a closed pipe. Either way this connection is
            // over; the pending map is drained below.
            Err(_) => break,
        };
        if read > MAX_MESSAGE_BYTES {
            tracing::warn!(
                bytes = read,
                limit = MAX_MESSAGE_BYTES,
                "MCP server sent an oversized message; dropping the connection"
            );
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match protocol::parse(trimmed) {
            Ok(Message::Response { id, outcome }) => {
                let waiting = pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&id));
                match waiting {
                    Some(sender) => {
                        let _ = sender.send(outcome);
                    }
                    // A reply to a request that timed out, or to one nobody
                    // sent. Dropped, and — this is the change — it disturbs
                    // nothing, because the next real reply is found by its own
                    // id rather than by being next in line.
                    None => tracing::debug!(id, "MCP server replied to no pending request"),
                }
            }
            Ok(Message::Notification { method, params }) => {
                tracing::debug!(method = %method, params = %params, "MCP server notification");
            }
            Ok(Message::ServerRequest { id, method }) => {
                // This client claims no capabilities in `initialize`, so a
                // `roots/list` or `sampling/createMessage` is a server asking
                // for something it was told it would not get. Answered with a
                // structured error rather than ignored: a server blocked on a
                // reply that never comes is a server that stops answering
                // *us*.
                tracing::warn!(method = %method, "MCP server called an unimplemented client method");
                let reply = protocol::method_not_found(&id, &method);
                let mut stdin = stdin.lock().await;
                let _ = stdin.write_all(reply.as_bytes()).await;
                let _ = stdin.flush().await;
            }
            Err(err) => {
                // One bad line is not a broken connection any more. It used to
                // be the *answer*.
                tracing::warn!(error = %err, "MCP server sent an unreadable message; ignoring it");
            }
        }
    }

    // Stdout is closed. Everyone still waiting gets an answer rather than
    // their deadline.
    if let Ok(mut pending) = pending.lock() {
        pending.clear();
    }
}

async fn read_stderr(stderr: tokio::process::ChildStderr, tail: Arc<Mutex<StderrTail>>) {
    let mut lines = BufReader::new(stderr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        tracing::debug!(line = %line, "MCP server stderr");
        if let Ok(mut tail) = tail.lock() {
            tail.push(&line);
        }
    }
}

/// How many times a connection has been restarted lately.
///
/// A server that crashes on every call must not be restarted on every call.
pub struct RestartBudget {
    window_started: Instant,
    restarts: u32,
}

impl Default for RestartBudget {
    fn default() -> Self {
        Self {
            window_started: Instant::now(),
            restarts: 0,
        }
    }
}

impl RestartBudget {
    /// Take one restart, or say the budget is spent.
    pub fn take(&mut self) -> Result<u32, McpError> {
        let now = Instant::now();
        if now.duration_since(self.window_started) >= RESTART_WINDOW {
            self.window_started = now;
            self.restarts = 0;
        }
        if self.restarts >= MAX_RESTARTS_PER_WINDOW {
            return Err(failed(format!(
                "MCP server restarted {MAX_RESTARTS_PER_WINDOW} times in {}s; not restarting again",
                RESTART_WINDOW.as_secs()
            )));
        }
        self.restarts += 1;
        Ok(self.restarts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stderr_tail_keeps_the_end_and_stays_bounded() {
        let mut tail = StderrTail::default();
        for index in 0..10_000 {
            tail.push(&format!("line {index}"));
        }
        let text = tail.text();
        assert!(
            text.len() <= MAX_STDERR_BYTES,
            "{} bytes kept, over the {MAX_STDERR_BYTES} cap",
            text.len()
        );
        assert!(
            text.contains("line 9999"),
            "the last thing a dying server says is the useful part"
        );
        assert!(
            !text.contains("line 0\n"),
            "the front should have been dropped"
        );
    }

    #[test]
    fn the_restart_budget_runs_out_and_then_refills() {
        let mut budget = RestartBudget::default();
        for expected in 1..=MAX_RESTARTS_PER_WINDOW {
            assert_eq!(budget.take().expect("within budget"), expected);
        }
        assert!(budget.take().is_err(), "the budget must run out");

        // Pretend the window elapsed.
        budget.window_started = Instant::now() - RESTART_WINDOW;
        assert_eq!(budget.take().expect("a new window"), 1);
    }
}
