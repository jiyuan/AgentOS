//! A5 invariant: the import-boundary checker must actually reject violations.
//!
//! `scripts/check-import-boundaries.sh` passing on a clean tree proves
//! nothing by itself — a broken pattern would pass vacuously. The script's
//! `--self-test` mode feeds it violating and clean fixture manifests and
//! asserts each is classified correctly, using the same pattern the real
//! check runs.

use std::path::Path;
use std::process::Command;

#[test]
fn import_boundary_checker_rejects_violating_manifests() {
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/check-import-boundaries.sh");
    let output = Command::new("bash")
        .arg(&script)
        .arg("--self-test")
        .output()
        .expect("boundary checker script is runnable");
    assert!(
        output.status.success(),
        "import-boundary self-test failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
