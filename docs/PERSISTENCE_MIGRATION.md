# Persistence migration

AgentOS persistence schema version 1 keys sessions and private memory by the
complete typed principal. Databases created before this version report
`PRAGMA user_version = 0` and must be migrated explicitly before the runtime
will open them.

## Inspect first

The dry run is read-only:

```sh
agentos-gateway migrate --dry-run
```

Use `--config PATH` and `--session-db-path PATH` when the deployment does not
use the standard `$AGENTOS_HOME/workspace/` paths. The JSON report includes row
counts, principal or namespace splits, and rows that cannot be assigned without
guessing. A legacy conversation containing several channels, senders, or agents
is reported as a collision even when its annotated turns can be split safely.

## Apply

Stop the gateway and supply a new backup path:

```sh
agentos-gateway stop
agentos-gateway migrate --backup /secure/backup/agentos-pre-v1.sqlite
```

The backup is a consistent SQLite snapshot created with `VACUUM INTO`. AgentOS
refuses to overwrite an existing path. After the backup completes, a prepared
migration marker is committed, then all data changes and `PRAGMA user_version`
advance together in one immediate transaction. A crash or disk-full error rolls
that transaction back; rerunning the same command with the same backup path
resumes from the prepared marker.

Resolvable legacy session turns are split into canonical `SessionKey` rows.
Resolvable memory namespaces are rewritten to injective version-1 namespaces.
Rows whose owner cannot be proven are moved, unchanged, to
`legacy_session_quarantine` or `legacy_memory_quarantine`; they are never merged
into a guessed principal. The dry-run report explains each quarantine reason.

Legacy paused-run JSON is upgraded in memory when loaded. The source file is not
rewritten and remains present until the resumed run succeeds.

## Roll back

Stop AgentOS, preserve the failed or migrated database for diagnosis, and copy
the migration backup back to the configured session database path. The backup
retains schema version 0 and all original rows. The older AgentOS build can then
open it; the version-1 runtime will again require migration.
