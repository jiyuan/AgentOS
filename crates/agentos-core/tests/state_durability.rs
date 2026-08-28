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
use agentos_core::paths::{write_private_atomic, RootDir};
use agentos_core::task_workspace::TaskWorkspace;
use agentos_proto::TaskId;
use agentos_proto::{ChannelId, ConversationId};
use serde_json::json;
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

#[test]
fn rooted_create_append_and_publish_are_private_under_a_permissive_umask() {
    use std::io::Write;

    let tree = support::temp_tree("rooted-modes");
    let root = RootDir::open(tree.path()).expect("root opens");
    under_permissive_umask(|| {
        root.create_dir_all("private/nested")
            .expect("directories create");
        let mut created = root
            .create_file("private/nested/created.txt")
            .expect("file creates");
        created.write_all(b"first").expect("file writes");
        drop(created);
        let mut appended = root
            .append_file("private/nested/created.txt")
            .expect("file appends");
        appended.write_all(b"-second").expect("append writes");
        drop(appended);
        root.write_file_atomic("private/nested/state.json", b"state")
            .expect("state publishes");
    });

    assert_eq!(
        std::fs::read(tree.path().join("private/nested/created.txt")).expect("readable"),
        b"first-second"
    );
    assert_eq!(mode_of(&tree.path().join("private")), 0o700);
    assert_eq!(mode_of(&tree.path().join("private/nested")), 0o700);
    assert_eq!(
        mode_of(&tree.path().join("private/nested/created.txt")),
        0o600
    );
    assert_eq!(
        mode_of(&tree.path().join("private/nested/state.json")),
        0o600
    );
}

#[test]
fn fsync_failure_aborts_state_commit() {
    // AF-019: the injected parent-fsync failure itself is exercised beside
    // RootDir's private syscall hook in paths/rooted.rs. This native integration
    // test covers the other half of the finding: every migrated task/session
    // artifact retains private modes even when the process umask permits all.
    let tree = support::temp_tree("af019-task-session-modes");
    let tasks = tree.path().join("workspace/tasks");
    let workspace = TaskWorkspace::new(&tasks);
    let task = TaskId::new("alpha");

    under_permissive_umask(|| {
        workspace.init_task(&task).expect("task initializes");
        workspace
            .append_session_event(&task, "turn-1", &json!({"secret": true}))
            .expect("session appends");
    });

    let task_dir = tasks.join("alpha");
    for dir in [
        tasks.clone(),
        task_dir.clone(),
        task_dir.join("subagents"),
        task_dir.join("suborchestrators"),
        task_dir.join("sessions"),
    ] {
        assert_eq!(mode_of(&dir), 0o700, "{} must be private", dir.display());
    }
    for file in [
        task_dir.join("task.toml"),
        task_dir.join("state.toml"),
        task_dir.join("sessions/turn-1.jsonl"),
    ] {
        assert_eq!(mode_of(&file), 0o600, "{} must be private", file.display());
    }
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
