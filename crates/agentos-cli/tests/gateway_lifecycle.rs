//! M8 / `GW-001`, deliverable 5: who is serving is established by a held lock,
//! and `SIGTERM` is a bounded drain rather than a process ending between two
//! instructions.
//!
//! These spawn the real `agentos-gateway` binary. Nothing smaller would do:
//! the lock only means anything across processes, and the whole claim being
//! tested is that the *kernel* releases it when the holder ends.
//!
//! The gateway is started with no channels enabled, so it reaches its idle
//! loop without needing a provider, a token, or a network. That is the
//! narrowest configuration in which the lifecycle is real.

#![cfg(unix)]

use agentos_core::gateway;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const GATEWAY: &str = env!("CARGO_BIN_EXE_agentos-gateway");

struct Deployment {
    root: PathBuf,
}

impl Deployment {
    fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "agentos-lifecycle-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("workspace")).expect("the tree is creatable");
        // No channels: the gateway holds its control file and idles, which is
        // the whole lifecycle without a provider or a network in it.
        std::fs::write(
            root.join("workspace/agent.toml"),
            "[agent]\nid = \"lifecycle\"\n\n[gateway]\nshutdown_grace_secs = 5\n",
        )
        .expect("the config is writable");
        Self { root }
    }

    fn pid_path(&self) -> PathBuf {
        self.root.join("workspace/run/agentos-gateway.pid")
    }

    fn log_path(&self) -> PathBuf {
        self.root.join("logs/agentos-gateway.log")
    }

    fn serve(&self) -> Child {
        Command::new(GATEWAY)
            .arg("serve")
            .arg("--pid-path")
            .arg(self.pid_path())
            .arg("--log-path")
            .arg(self.log_path())
            .arg("--config")
            .arg(self.root.join("workspace/agent.toml"))
            .arg("--session-db-path")
            .arg(self.root.join("workspace/agentos.sqlite"))
            .env("AGENTOS_HOME", &self.root)
            .env("AGENTOS_ENABLED_CHANNELS", "")
            .env("AGENTOS_GATEWAY_OWNER_TOKEN", "lifecycle-token")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("the gateway binary runs")
    }

    fn log(&self) -> String {
        std::fs::read_to_string(self.log_path()).unwrap_or_default()
    }
}

impl Drop for Deployment {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn wait_until(deadline: Duration, mut ready: impl FnMut() -> bool) -> bool {
    let until = Instant::now() + deadline;
    while Instant::now() < until {
        if ready() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    ready()
}

fn wait_for_holder(path: &Path) -> gateway::ControlRecord {
    assert!(
        wait_until(Duration::from_secs(20), || gateway::holder(path)
            .expect("the query runs")
            .is_some()),
        "the gateway never took its control file"
    );
    gateway::holder(path)
        .expect("the query runs")
        .expect("just observed")
}

#[test]
fn the_serving_process_holds_the_control_file_and_releases_it_on_sigterm() {
    let deployment = Deployment::new("drain");
    let mut child = deployment.serve();

    let record = wait_for_holder(&deployment.pid_path());
    assert_eq!(
        record.pid,
        child.id(),
        "the lock must be held by the process that serves, not by whoever wrote the file"
    );
    assert_eq!(record.token.as_ref(), "lifecycle-token");

    let started = Instant::now();
    gateway::terminate_holder(&deployment.pid_path())
        .expect("the signal is sent")
        .expect("the gateway holds the file");

    assert!(
        wait_until(Duration::from_secs(20), || child
            .try_wait()
            .expect("the child is inspectable")
            .is_some()),
        "the gateway did not exit on SIGTERM within twenty seconds"
    );
    let took = started.elapsed();
    let status = child.try_wait().expect("inspectable").expect("exited");
    assert!(status.success(), "the gateway exited with {status}");

    let log = deployment.log();
    assert!(
        log.contains("shutting down on signal"),
        "the gateway must say it is draining, got: {log}"
    );
    assert!(
        log.contains("AgentOS gateway service stopped"),
        "the gateway must reach its own end rather than being cut off, got: {log}"
    );

    // The lock is released by the kernel when the process ends, so nothing
    // needs to have run for this to be true.
    assert_eq!(
        gateway::holder(&deployment.pid_path()).expect("the query runs"),
        None,
        "the control file still names a holder after the process exited"
    );
    assert!(
        took < Duration::from_secs(20),
        "the drain took {took:?}, which is not bounded in any useful sense"
    );
}

/// The reason the lock exists. Two gateways on one control file is two
/// gateways on one database.
#[test]
fn a_second_gateway_refuses_to_start_over_a_held_control_file() {
    let deployment = Deployment::new("contend");
    let mut first = deployment.serve();
    wait_for_holder(&deployment.pid_path());

    let second = Command::new(GATEWAY)
        .arg("serve")
        .arg("--pid-path")
        .arg(deployment.pid_path())
        .arg("--log-path")
        .arg(deployment.log_path())
        .arg("--config")
        .arg(deployment.root.join("workspace/agent.toml"))
        .env("AGENTOS_HOME", &deployment.root)
        .env("AGENTOS_ENABLED_CHANNELS", "")
        .stdin(Stdio::null())
        .output()
        .expect("the gateway binary runs");
    assert!(
        !second.status.success(),
        "a second gateway started over a held control file"
    );
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("is held by pid"),
        "the refusal must name the holder, got: {stderr}"
    );

    let _ = gateway::terminate_holder(&deployment.pid_path());
    let _ = first.wait();
}

/// `status` and `stop` must not mistake a file left by a crashed process for a
/// running gateway — the distinction `kill -0` could not make, because pids
/// are recycled.
#[test]
fn a_crashed_gateway_leaves_a_file_that_reads_as_stopped() {
    let deployment = Deployment::new("crash");
    let mut child = deployment.serve();
    let record = wait_for_holder(&deployment.pid_path());

    // SIGKILL: no drain, no release, no chance to clean up. Exactly what the
    // stale-file case is.
    child.kill().expect("the child is killable");
    child.wait().expect("the child is reapable");

    assert_eq!(
        gateway::read_record(&deployment.pid_path())
            .expect("the record parses")
            .map(|found| found.pid),
        Some(record.pid),
        "the file should still be there, pid and all"
    );
    assert_eq!(
        gateway::holder(&deployment.pid_path()).expect("the query runs"),
        None,
        "a killed process must not read as a running gateway"
    );

    let status = Command::new(GATEWAY)
        .arg("status")
        .arg("--pid-path")
        .arg(deployment.pid_path())
        .env("AGENTOS_HOME", &deployment.root)
        .output()
        .expect("the gateway binary runs");
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("stopped") && stdout.contains("stale"),
        "status must report stopped-with-a-stale-file, got: {stdout}"
    );

    // And the next gateway starts straight into it, with no manual cleanup.
    let mut restarted = deployment.serve();
    wait_for_holder(&deployment.pid_path());
    let _ = gateway::terminate_holder(&deployment.pid_path());
    let _ = restarted.wait();
}
