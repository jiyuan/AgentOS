//! Explicit, backup-gated migration of legacy persistence identity.

use super::{agent_config_path, process_is_running, read_pid, session_path, ServiceConfig};
use agentos_core::config::WorkspaceConfig;
use agentos_core::memory::{inspect_persistence, migrate_persistence, MigrationReport};
use agentos_proto::AgentId;
use std::path::PathBuf;

pub(super) fn run(config: &ServiceConfig, args: &[String]) -> Result<(), String> {
    let dry_run = args.iter().any(|arg| arg == "--dry-run");
    let backup = backup_arg(args)?;
    let workspace = WorkspaceConfig::load(&agent_config_path(config))
        .map_err(|err| format!("failed to load workspace config: {err}"))?;
    let agent = AgentId::new(workspace.agent.id);
    let database = session_path(config);

    let report = if dry_run {
        if backup.is_some() {
            return Err("--backup cannot be combined with --dry-run".to_owned());
        }
        inspect_persistence(&database, &agent).map_err(|err| err.to_string())?
    } else {
        refuse_while_gateway_runs(config)?;
        let backup_path = backup.as_ref().ok_or_else(|| {
            "migration requires --backup PATH; inspect first with --dry-run".to_owned()
        })?;
        migrate_persistence(&database, backup_path, &agent).map_err(|err| err.to_string())?
    };
    print_report(&report, backup.as_ref());
    Ok(())
}

fn backup_arg(args: &[String]) -> Result<Option<PathBuf>, String> {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == "--backup" {
            return args
                .next()
                .map(PathBuf::from)
                .map(Some)
                .ok_or_else(|| "--backup requires a path".to_owned());
        }
        if let Some(path) = arg.strip_prefix("--backup=") {
            return Ok(Some(PathBuf::from(path)));
        }
    }
    Ok(None)
}

fn refuse_while_gateway_runs(config: &ServiceConfig) -> Result<(), String> {
    if let Some(pid) = read_pid(&config.pid_path)? {
        if process_is_running(pid) {
            return Err(format!(
                "gateway pid {pid} is running; stop it before migrating persistence"
            ));
        }
    }
    Ok(())
}

fn print_report(report: &MigrationReport, backup: Option<&PathBuf>) {
    println!(
        "{}",
        serde_json::to_string_pretty(report).expect("migration report serializes")
    );
    if let Some(backup) = backup {
        println!("Backup: {}", backup.display());
        println!(
            "Rollback: stop AgentOS, preserve the failed database for diagnosis, then replace it with this backup."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::backup_arg;
    use std::path::PathBuf;

    #[test]
    fn backup_accepts_split_and_equals_forms() {
        assert_eq!(
            backup_arg(&["--backup".to_owned(), "copy.sqlite".to_owned()]).unwrap(),
            Some(PathBuf::from("copy.sqlite"))
        );
        assert_eq!(
            backup_arg(&["--backup=other.sqlite".to_owned()]).unwrap(),
            Some(PathBuf::from("other.sqlite"))
        );
    }
}
