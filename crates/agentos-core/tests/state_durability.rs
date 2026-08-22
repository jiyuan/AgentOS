//! M8 / `GW-001`, deliverable 4: state the runtime owns is replaced atomically
//! and is private under a permissive process umask.
//!
//! Its own test binary on purpose. `umask(2)` is process-global, so a test
//! that sets it cannot share a process with tests that create files and expect
//! the ambient one. Everything here needs the permissive umask — that is the
//! deployment the acceptance criterion names — so they share a process with
//! each other and nothing else, serialized through one lock.

mod support;

use agentos_core::crons::{CronSchedule, CronStore, CronTask};
use agentos_core::paths::write_private_atomic;
use agentos_proto::{ChannelId, ConversationId};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Mutex;

/// Serializes the umask change. Two tests setting it at once would each
/// restore the other's value.
static UMASK: Mutex<()> = Mutex::new(());

/// Run `body` with the most permissive umask there is, then restore.
fn under_permissive_umask<T>(body: impl FnOnce() -> T) -> T {
    let _guard = UMASK.lock().unwrap_or_else(|err| err.into_inner());
    // SAFETY: `umask` is a process-global setting with no unsafe precondition
    // beyond that; the lock makes the change and its restore one critical
    // section, and this binary contains only tests that want it.
    let previous = unsafe { libc::umask(0) };
    let out = body();
    unsafe { libc::umask(previous) };
    out
}

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path)
        .unwrap_or_else(|err| panic!("{} should exist: {err}", path.display()))
        .permissions()
        .mode()
        & 0o777
}

#[test]
fn a_cron_task_is_written_privately_under_a_permissive_umask() {
    let tree = support::temp_tree("cron-modes");
    let root = tree.path().join("crons");
    let store = CronStore::new(root.clone());
    let task = CronTask::new(
        "nightly",
        ChannelId::new("telegram"),
        ConversationId::new("ops"),
        "summarise yesterday",
        CronSchedule::new("0 0 3 * * *").expect("the expression parses"),
    );

    under_permissive_umask(|| store.save_task(&task).expect("the task saves"));

    let path = root.join("nightly.toml");
    assert_eq!(
        mode_of(&path),
        0o600,
        "a cron task carries the prompt it fires and the conversation it fires into"
    );
    assert_eq!(
        mode_of(&root),
        0o700,
        "a listable cron directory leaks every task id"
    );
}

/// The general mechanism, at the umask that makes the difference visible.
/// `paths::durable`'s own unit test asserts the same thing against the ambient
/// umask; this is the one the acceptance criterion asks for.
#[test]
fn a_replaced_state_file_stays_private_under_a_permissive_umask() {
    let tree = support::temp_tree("state-modes");
    let path = tree.path().join("run/state.json");

    let control = under_permissive_umask(|| {
        write_private_atomic(&path, b"first").expect("the first write succeeds");
        write_private_atomic(&path, b"second").expect("the replacement succeeds");

        // What the same bytes through `fs::write` would have been, at this
        // umask, in the same directory.
        let control = tree.path().join("run/control.json");
        std::fs::write(&control, b"second").expect("the control write succeeds");
        control
    });

    assert_eq!(std::fs::read(&path).expect("readable"), b"second");
    assert_eq!(mode_of(&path), 0o600);
    assert_eq!(
        mode_of(&control),
        0o666,
        "at umask 0 a plain write is world-writable, which is the whole point"
    );
}

/// Atomicity, observed rather than argued: a reader racing a replacement sees
/// one whole version or the other, never a truncation and never an absence.
#[test]
fn a_concurrent_reader_never_sees_a_partial_replacement() {
    let tree = support::temp_tree("state-atomic");
    let path = tree.path().join("state.json");
    let small = vec![b'a'; 4 * 1024];
    let large = vec![b'b'; 512 * 1024];
    write_private_atomic(&path, &small).expect("the seed write succeeds");

    let writer_path = path.clone();
    let writer = std::thread::spawn(move || {
        for round in 0..40 {
            let bytes = if round % 2 == 0 { &large } else { &small };
            write_private_atomic(&writer_path, bytes).expect("the write succeeds");
        }
    });

    let mut reads = 0;
    while !writer.is_finished() {
        let observed = std::fs::read(&path).expect("the state file is never absent");
        assert!(
            observed.iter().all(|byte| *byte == observed[0]),
            "a torn write: the file mixed two versions"
        );
        assert!(
            observed.len() == 4 * 1024 || observed.len() == 512 * 1024,
            "a truncated write: {} bytes",
            observed.len()
        );
        reads += 1;
    }
    writer.join().expect("the writer finishes");
    assert!(reads > 0, "the reader never got a look in");
}
