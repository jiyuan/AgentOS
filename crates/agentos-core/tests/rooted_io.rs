//! Native regressions for model-selected filesystem consumers.

mod support;

use agentos_core::paths::{ContainmentError, RootDir};
use agentos_core::skills::validate_skill;
use agentos_core::task_workspace::TaskWorkspace;
use agentos_proto::TaskId;

#[cfg(unix)]
#[test]
fn all_model_selected_paths_refuse_symlink_escape_and_swap() {
    // AF-018: task, skill, and generic rooted access all refuse planted links
    // before an outside object is opened. Attachment publication uses this
    // same RootDir::write_file_atomic path and has a focused channel unit test.
    let tree = support::temp_tree("af018-rooted-consumers");
    let common = tree.path().join("workspace");
    let tasks = common.join("tasks");
    let outside = tree.path().join("outside");
    std::fs::create_dir_all(&tasks).expect("task root");
    std::fs::create_dir_all(&outside).expect("outside root");
    let canary = outside.join("canary");
    std::fs::write(&canary, b"untouched").expect("canary");

    let workspace = TaskWorkspace::new(&tasks);
    std::os::unix::fs::symlink(&outside, tasks.join("alpha")).expect("task link");
    assert!(workspace.init_task(&TaskId::new("alpha")).is_err());
    assert!(!outside.join("task.toml").exists());

    std::os::unix::fs::symlink(&outside, common.join("main")).expect("main link");
    assert!(workspace.init_task(&TaskId::new("main")).is_err());
    assert!(!outside.join("state.toml").exists());

    let skills = common.join("skills");
    std::fs::create_dir_all(&skills).expect("skills root");
    std::os::unix::fs::symlink(&outside, skills.join("audit-skill")).expect("skill link");
    assert!(validate_skill(&skills, "audit-skill").is_err());

    let rooted = RootDir::open(&common).expect("common root opens");
    for selected in ["../outside/pwned", "/tmp/agentos-af018-pwned"] {
        assert!(matches!(
            rooted.write_file_atomic(selected, b"pwned"),
            Err(ContainmentError::Traversal { .. } | ContainmentError::Absolute { .. })
        ));
    }

    // Exercise the rename race directly. A successful operation may land in
    // the original directory descriptor after it is renamed, but never in the
    // symlink target that temporarily occupies its old name.
    let live = common.join("live");
    std::fs::create_dir_all(&live).expect("live dir");
    let common_for_swap = common.clone();
    let outside_for_swap = outside.clone();
    let swapper = std::thread::spawn(move || {
        for _ in 0..250 {
            if std::fs::rename(common_for_swap.join("live"), common_for_swap.join("parked")).is_ok()
            {
                let _ = std::os::unix::fs::symlink(&outside_for_swap, common_for_swap.join("live"));
                let _ = std::fs::remove_file(common_for_swap.join("live"));
                let _ =
                    std::fs::rename(common_for_swap.join("parked"), common_for_swap.join("live"));
            }
        }
    });
    for _ in 0..250 {
        let _ = rooted.write_file_atomic("live/state", b"inside");
    }
    swapper.join().expect("swapper finishes");

    assert_eq!(std::fs::read(&canary).expect("canary reads"), b"untouched");
    assert!(!outside.join("state").exists());
    assert!(!outside.join("pwned").exists());
}
