//! Noticing `SIGTERM` and draining rather than dying mid-turn
//! (M8 / `GW-001`, deliverable 5).
//!
//! # What was wrong
//!
//! Nothing installed a handler, so the default disposition applied: the
//! process ended between two instructions. A turn in flight was simply gone —
//! its transcript half-written, its ingress event accepted and never settled,
//! its user waiting. `stop` then escalated to `SIGKILL` after a fixed wait,
//! with nothing in between for the gateway to *do*.
//!
//! # What this is
//!
//! A flag, and nothing else. The handler stores `true` into an
//! [`AtomicBool`](std::sync::atomic::AtomicBool) and returns — the only work
//! that is safe inside a signal handler, where allocating, locking, or logging
//! can deadlock against whatever the interrupted thread was holding.
//! Everything real happens on the ordinary control flow that polls it: the
//! router stops accepting, the shards drain what they have, and the ledger is
//! asked what never finished.
//!
//! The drain is *bounded*. A shard wedged on a tool that ignores its deadline
//! must not turn a `SIGTERM` into a hang, so the wait has a deadline of its
//! own, after which the gateway reports what it is abandoning and exits. That
//! report is the point: an operator who restarts after a forced exit needs to
//! know which conversations were mid-turn, and the alternative to reporting it
//! is not knowing.

use agentos_interfaces::{Channel, InboundEvent};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

/// Set by the signal handler, read by every loop that can stop.
///
/// Process-wide because signals are: there is one disposition per process, and
/// a per-gateway flag would be a lie about what `SIGTERM` interrupts.
static REQUESTED: AtomicBool = AtomicBool::new(false);

/// Whether a shutdown has been asked for.
pub fn shutdown_requested() -> bool {
    REQUESTED.load(Ordering::SeqCst)
}

/// Ask for a shutdown from ordinary code — a supervisor loop that has decided
/// to stop, or a test.
pub fn request_shutdown() {
    REQUESTED.store(true, Ordering::SeqCst);
}

/// The single cancellation root and grace deadline for a serving process.
///
/// Every participant clones this handle. The first shutdown cause fixes the
/// deadline; later channel errors and signals observe that same instant rather
/// than buying their own fresh grace period.
#[derive(Clone, Debug)]
pub struct ProcessShutdown {
    cancellation: CancellationToken,
    grace: Duration,
    deadline: Arc<Mutex<Option<Instant>>>,
}

impl ProcessShutdown {
    pub fn new(cancellation: CancellationToken, grace: Duration) -> Self {
        Self {
            cancellation,
            grace,
            deadline: Arc::new(Mutex::new(None)),
        }
    }

    /// Begin the process drain, returning the one deadline shared by every
    /// caller. Cancellation happens after publication of the deadline.
    pub fn begin(&self) -> Instant {
        let deadline = {
            let mut current = self
                .deadline
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *current.get_or_insert_with(|| Instant::now() + self.grace)
        };
        self.cancellation.cancel();
        deadline
    }

    /// Turn a process signal/control request into the shared drain boundary.
    pub fn observe_process_request(&self) -> Option<Instant> {
        shutdown_requested().then(|| self.begin())
    }

    pub fn deadline(&self) -> Option<Instant> {
        *self
            .deadline
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.deadline()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }
}

/// Why a channel receive future returned.
#[derive(Debug)]
pub enum ReceiveOutcome {
    Cancelled,
    Closed,
    Received(Box<InboundEvent>),
}

/// Await a channel without allowing a transport's indefinitely-blocked
/// receive future to hold the process past cancellation.
pub async fn receive_or_cancel<C>(channel: &mut C, shutdown: &ProcessShutdown) -> ReceiveOutcome
where
    C: Channel,
{
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => ReceiveOutcome::Cancelled,
        inbound = channel.receive() => match inbound {
            Some(inbound) => ReceiveOutcome::Received(Box::new(inbound)),
            None => ReceiveOutcome::Closed,
        },
    }
}

/// Clear the flag. For tests; a served process never un-asks.
#[cfg(test)]
pub(crate) fn reset_shutdown() {
    REQUESTED.store(false, Ordering::SeqCst);
}

#[cfg(unix)]
extern "C" fn handle(_signal: libc::c_int) {
    // The entire handler. Anything else here — a log line, a lock, an
    // allocation — can deadlock against the interrupted thread.
    REQUESTED.store(true, Ordering::SeqCst);
}

/// Install the handler for `SIGTERM` and `SIGINT`.
///
/// Idempotent, and safe to call from any thread: `sigaction` is process-wide.
/// `SIGINT` as well as `SIGTERM` because an operator who started the gateway in
/// the foreground stops it with Ctrl-C, and that turn deserves the same drain.
#[cfg(unix)]
pub fn install_shutdown_handler() -> Result<(), std::io::Error> {
    for signal in [libc::SIGTERM, libc::SIGINT] {
        // SAFETY: `sigaction` is being given a zeroed `sigaction` struct with
        // a valid extern "C" handler and no flags requiring extra fields. The
        // handler itself does one relaxed-free atomic store, which is
        // async-signal-safe.
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = handle as *const () as usize;
            libc::sigemptyset(&mut action.sa_mask);
            // No `SA_RESTART`: a blocking read interrupted by the signal
            // should return `EINTR` so the loop around it gets a chance to
            // notice the flag, rather than resuming its wait.
            action.sa_flags = 0;
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
pub fn install_shutdown_handler() -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "signal-driven shutdown is only implemented for unix",
    ))
}

/// How long the gateway waits for in-flight turns after a shutdown is asked
/// for, when nothing says otherwise.
///
/// Long enough for a turn that is merely slow — an LLM call plus a tool — and
/// short enough that an operator restarting a service does not conclude it has
/// hung. Past it the gateway reports what it abandoned and exits.
pub const DEFAULT_SHUTDOWN_GRACE_SECS: u64 = 30;

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes the process-wide flag: these tests set and clear it, and two
    /// of them at once would see each other's state.
    static FLAG: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn the_flag_starts_clear_and_latches() {
        let _guard = FLAG.lock().unwrap_or_else(|err| err.into_inner());
        reset_shutdown();
        assert!(!shutdown_requested());
        request_shutdown();
        assert!(shutdown_requested());
        // Latching, not edge-triggered: a loop that polls once per second must
        // not be able to miss it.
        assert!(shutdown_requested());
        reset_shutdown();
    }

    /// A real signal, delivered to this process, reaches the flag. Nothing
    /// else in this module is worth much if this does not hold.
    #[cfg(unix)]
    #[test]
    fn a_sigterm_sets_the_flag() {
        let _guard = FLAG.lock().unwrap_or_else(|err| err.into_inner());
        install_shutdown_handler().expect("the handler installs");
        reset_shutdown();
        // SAFETY: `kill` on this process with `SIGTERM`, which now has a
        // handler installed rather than the default terminate disposition.
        unsafe { libc::kill(std::process::id() as libc::pid_t, libc::SIGTERM) };
        for _ in 0..100 {
            if shutdown_requested() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(
            shutdown_requested(),
            "SIGTERM did not reach the shutdown flag"
        );
        reset_shutdown();
        // Leave the default disposition in place for the rest of the binary.
        // SAFETY: restoring `SIG_DFL` for one signal.
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = libc::SIG_DFL;
            libc::sigemptyset(&mut action.sa_mask);
            libc::sigaction(libc::SIGTERM, &action, std::ptr::null_mut());
        }
    }
}
