use super::common::{cron_root_for_tests, default_cron_dir, elapsed_ms, result_metadata};
use crate::crons::{CronSchedule, CronStore, CronTask};
use agentos_interfaces::tool::{
    SandboxMode, Tool, ToolError, ToolPersistenceScope, ToolSafety, ToolSideEffect, ToolSpec,
};
use agentos_proto::{ChannelId, ConversationId, ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Tool wrapping `crate::crons::CronStore::save_task` so sub-agents can
/// register a recurring task end-to-end (TOML file written under
/// `workspace/crons/<id>.toml`) inside the normal run-loop approval and
/// guardrail flow.
///
/// The gateway's scheduler picks new files up from disk on its next polling
/// cycle, so once this tool returns success the task is live without any
/// daemon restart.
#[derive(Default)]
pub struct CronCreatorTool;

/// Deserialised tool input.
///
/// Note: `root` (and the test-only override) is intentionally *not* exposed
/// on the LLM-visible schema. The model picking its own cron directory is a
/// foot-gun — it'll happily write to `workspace/` and then claim success.
/// The runtime resolves the directory itself via `$AGENTOS_HOME/workspace/crons`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CronCreateArgs {
    /// Human-readable identifier, alphanumeric / `-` / `_` only. Used as the
    /// on-disk filename and to dedupe scheduler entries.
    id: String,
    /// Channel that should receive the recurring envelope (e.g. "telegram",
    /// "feishu"). Must match the registered `Channel::id()`.
    channel_id: String,
    /// Conversation id to deliver to (the user chat for Telegram, `oc_...`
    /// for Feishu, etc).
    conversation_id: String,
    /// User-side prompt the scheduler will replay each tick.
    prompt: String,
    /// Standard 5-field cron expression (`min hour day-of-month month
    /// day-of-week`), evaluated in UTC. The expression is the absolute source
    /// of truth — the scheduler fires whenever wall-clock time crosses a
    /// matching instant, with no stored "next due" cursor.
    expression: String,
}

#[async_trait]
impl Tool for CronCreatorTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("cron_create"),
            description: Arc::from(
                "Schedule a recurring AgentOS task. Persists a TOML file under \
                 workspace/crons/<id>.toml; the gateway scheduler picks it up \
                 on its next cycle and replays the supplied prompt whenever \
                 wall-clock time matches the cron expression. Use this \
                 whenever a user asks to schedule, automate, or repeat a chat \
                 instruction.",
            ),
            input_schema: json!({
                "type": "object",
                "required": ["id", "channel_id", "conversation_id", "prompt", "expression"],
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Alphanumeric / -/_ identifier. Used as the filename and dedupe key."
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel to deliver to: telegram | feishu | tui."
                    },
                    "conversation_id": {
                        "type": "string",
                        "description": "Conversation id to deliver to (Telegram chat id, Feishu oc_..., etc)."
                    },
                    "prompt": {
                        "type": "string",
                        "description": "The user-side message the scheduler replays each tick."
                    },
                    "expression": {
                        "type": "string",
                        "description": "5-field cron expression in UTC: 'min hour day-of-month month day-of-week'. Example: '17 2 * * *' for 02:17 daily."
                    }
                }
            }),
            safety: ToolSafety::new(
                ToolSideEffect::PersistentMutation,
                ToolPersistenceScope::CrossConversation,
            ),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let parsed: CronCreateArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let start = Instant::now();

        let schedule = CronSchedule::new(parsed.expression.as_str())
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;

        let mut task = CronTask::new(
            parsed.id.as_str(),
            ChannelId::new(parsed.channel_id.as_str()),
            ConversationId::new(parsed.conversation_id.as_str()),
            parsed.prompt.as_str(),
            schedule,
        );

        // Anchor the task to creation time. Without this, `last_fired_unix`
        // stays `None` and `is_due` compares against `0`, so every past
        // occurrence reads as pending: a task created at 14:00 with a
        // `17 2 * * *` schedule would fire on the scheduler's very next tick
        // instead of at 02:17 the following day.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
        task.last_fired_unix = task
            .schedule
            .previous_fire_unix(now)
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;

        let store = CronStore::new(cron_root_for_tests().unwrap_or_else(default_cron_dir));
        store
            .save_task(&task)
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;

        let path = store
            .task_path(&task.id)
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
        let message = format!(
            "created cron '{}' (schedule '{}') at {}",
            task.id,
            task.schedule.expression,
            path.display()
        );
        let bytes_out = message.len() as u64;
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(message),
            metadata: result_metadata(elapsed_ms(start), bytes_out),
        })
    }
}

/// Tool: enumerate every persisted cron task. Reads `workspace/crons/*.toml`
/// via `CronStore::load_scheduler` and returns a compact JSON-encoded summary
/// the model can reason about ("delete the broken one", "tell me what runs
/// every day at 9am", etc).
#[derive(Default)]
pub struct CronListTool;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CronListArgs {}

#[async_trait]
impl Tool for CronListTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("cron_list"),
            description: Arc::from(
                "Enumerate every persisted cron task (id, channel, conversation, \
                 cron expression, next-fire, enabled). Use this when the user \
                 asks which crons exist or wants to confirm a previous schedule.",
            ),
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
            safety: ToolSafety::new(
                ToolSideEffect::ReadOnly,
                ToolPersistenceScope::CrossConversation,
            ),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let _parsed: CronListArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let start = Instant::now();
        let store = CronStore::new(cron_root_for_tests().unwrap_or_else(default_cron_dir));
        let scheduler = store
            .load_scheduler()
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
        let summaries = scheduler
            .tasks()
            .iter()
            .map(|task| {
                let next_fire_unix = task.schedule.next_fire_unix(now).ok().flatten();
                json!({
                    "id": task.id.as_ref(),
                    "channel_id": task.channel_id.as_str(),
                    "conversation_id": task.conversation_id.as_str(),
                    "prompt": task.prompt.as_ref(),
                    "expression": task.schedule.expression.as_ref(),
                    "last_fired_unix": task.last_fired_unix,
                    "next_fire_unix": next_fire_unix,
                    "enabled": task.enabled,
                })
            })
            .collect::<Vec<_>>();
        let body = serde_json::to_string(&summaries)
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let bytes_out = body.len() as u64;
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(body),
            metadata: result_metadata(elapsed_ms(start), bytes_out),
        })
    }
}

/// Tool: delete a persisted cron task by id. Just removes the TOML file —
/// the scheduler will stop replaying it on its next cycle. Idempotent: a
/// missing file is treated as a no-op so retries are safe.
#[derive(Default)]
pub struct CronRemoveTool;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CronRemoveArgs {
    id: String,
}

#[async_trait]
impl Tool for CronRemoveTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("cron_remove"),
            description: Arc::from(
                "Delete a persisted cron task by id. Use this when the user asks \
                 to cancel, remove, or stop a scheduled task. Idempotent.",
            ),
            input_schema: json!({
                "type": "object",
                "required": ["id"],
                "properties": {
                    "id": {
                        "type": "string",
                        "description": "Cron id as previously returned by cron_create or cron_list."
                    }
                }
            }),
            safety: ToolSafety::new(
                ToolSideEffect::PersistentMutation,
                ToolPersistenceScope::CrossConversation,
            ),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let parsed: CronRemoveArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let start = Instant::now();
        let store = CronStore::new(cron_root_for_tests().unwrap_or_else(default_cron_dir));
        let path = store
            .task_path(&parsed.id)
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
        let message = match std::fs::remove_file(&path) {
            Ok(()) => format!("removed cron '{}' ({})", parsed.id, path.display()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                format!("cron '{}' was not present (no-op)", parsed.id)
            }
            Err(err) => return Err(ToolError::Failed(err.to_string().into())),
        };
        let bytes_out = message.len() as u64;
        Ok(ToolResult {
            call_id: call.id.clone(),
            status: ToolStatus::Succeeded,
            content: Arc::from(message),
            metadata: result_metadata(elapsed_ms(start), bytes_out),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::common::test_support::{tool_call, CronDirGuard};
    use super::*;
    use serde_json::Value;

    #[tokio::test]
    async fn cron_creator_tool_persists_task_file() {
        let guard = CronDirGuard::new("cron-creator-tool");
        let args = json!({
            "id": "daily-digest",
            "channel_id": "telegram",
            "conversation_id": "5480467472",
            "prompt": "Summarize the day's notes.",
            "expression": "17 2 * * *",
        });
        let raw = RawValue::from_string(args.to_string()).unwrap();
        let result = CronCreatorTool
            .call(&tool_call("cron_create", "call_1"), &raw)
            .await
            .unwrap();
        assert_eq!(result.status, ToolStatus::Succeeded);
        assert!(result.content.contains("daily-digest"));

        let task_path = guard.dir.join("daily-digest.toml");
        assert!(task_path.is_file());
        let body = std::fs::read_to_string(&task_path).unwrap();
        let task: CronTask = toml::from_str(&body).unwrap();
        assert_eq!(task.id.as_ref(), "daily-digest");
        assert_eq!(task.channel_id.as_str(), "telegram");
        assert_eq!(task.conversation_id.as_str(), "5480467472");
        assert_eq!(task.prompt.as_ref(), "Summarize the day's notes.");
        assert_eq!(task.schedule.expression.as_ref(), "17 2 * * *");
        // Anchored at creation so the task does not fire before its schedule.
        assert!(task.last_fired_unix.is_some());
    }

    #[tokio::test]
    async fn cron_creator_tool_anchors_task_so_it_does_not_fire_immediately() {
        use crate::crons::CronScheduler;
        let guard = CronDirGuard::new("cron-creator-anchored");
        let args = json!({
            "id": "anchored",
            "channel_id": "telegram",
            "conversation_id": "1",
            "prompt": "audit",
            "expression": "17 2 * * *",
        });
        let raw = RawValue::from_string(args.to_string()).unwrap();
        CronCreatorTool
            .call(&tool_call("cron_create", "create"), &raw)
            .await
            .unwrap();

        let body = std::fs::read_to_string(guard.dir.join("anchored.toml")).unwrap();
        let task: CronTask = toml::from_str(&body).unwrap();
        assert!(
            task.last_fired_unix.is_some(),
            "created task must be anchored"
        );

        // A task created "now" must not be due "now" — it waits for the next
        // matching instant rather than back-firing for today's earlier tick.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let scheduler = CronScheduler::new([task]);
        assert!(
            scheduler.due_invocations(now).unwrap().is_empty(),
            "freshly created cron fired immediately instead of waiting for its schedule",
        );
    }

    #[tokio::test]
    async fn cron_creator_tool_rejects_root_override_from_caller() {
        let _guard = CronDirGuard::new("cron-creator-rooted");
        let args = json!({
            "id": "rooted",
            "channel_id": "telegram",
            "conversation_id": "1",
            "prompt": "hi",
            "expression": "17 2 * * *",
            "root": "workspace",
        });
        let raw = RawValue::from_string(args.to_string()).unwrap();
        let err = CronCreatorTool
            .call(&tool_call("cron_create", "call_root"), &raw)
            .await
            .unwrap_err();
        let ToolError::Failed(msg) = err;
        assert!(msg.contains("unknown field") && msg.contains("root"));
    }

    #[tokio::test]
    async fn cron_creator_tool_requires_an_expression() {
        let _guard = CronDirGuard::new("cron-creator-no-expression");
        let args = json!({
            "id": "x",
            "channel_id": "telegram",
            "conversation_id": "1",
            "prompt": "hi",
        });
        let raw = RawValue::from_string(args.to_string()).unwrap();
        let err = CronCreatorTool
            .call(&tool_call("cron_create", "call_2"), &raw)
            .await
            .unwrap_err();
        let ToolError::Failed(msg) = err;
        assert!(msg.contains("expression"));
    }

    #[tokio::test]
    async fn cron_creator_tool_rejects_invalid_expression() {
        let _guard = CronDirGuard::new("cron-creator-bad-expression");
        let args = json!({
            "id": "x",
            "channel_id": "telegram",
            "conversation_id": "1",
            "prompt": "hi",
            "expression": "not a cron",
        });
        let raw = RawValue::from_string(args.to_string()).unwrap();
        let err = CronCreatorTool
            .call(&tool_call("cron_create", "call_3"), &raw)
            .await
            .unwrap_err();
        let ToolError::Failed(msg) = err;
        assert!(msg.contains("invalid cron expression"));
    }

    #[tokio::test]
    async fn cron_creator_tool_rejects_invalid_id() {
        let _guard = CronDirGuard::new("cron-creator-bad-id");
        let args = json!({
            "id": "has spaces!",
            "channel_id": "telegram",
            "conversation_id": "1",
            "prompt": "hi",
            "expression": "17 2 * * *",
        });
        let raw = RawValue::from_string(args.to_string()).unwrap();
        let err = CronCreatorTool
            .call(&tool_call("cron_create", "call_4"), &raw)
            .await
            .unwrap_err();
        let ToolError::Failed(msg) = err;
        assert!(msg.contains("invalid cron id"));
    }

    #[tokio::test]
    async fn cron_list_tool_returns_persisted_tasks() {
        let guard = CronDirGuard::new("cron-list-tool");
        for id in ["one", "two"] {
            let args = json!({
                "id": id,
                "channel_id": "telegram",
                "conversation_id": "1",
                "prompt": format!("ping-{id}"),
                "expression": "0 * * * *",
            });
            let raw = RawValue::from_string(args.to_string()).unwrap();
            CronCreatorTool
                .call(&tool_call("cron_create", "create"), &raw)
                .await
                .unwrap();
        }
        let raw = RawValue::from_string("{}".to_owned()).unwrap();
        let result = CronListTool
            .call(&tool_call("cron_list", "list"), &raw)
            .await
            .unwrap();
        let body: Vec<Value> = serde_json::from_str(&result.content).unwrap();
        assert_eq!(body.len(), 2);
        let ids: Vec<&str> = body.iter().map(|t| t["id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"one"));
        assert!(ids.contains(&"two"));
        drop(guard);
    }

    #[tokio::test]
    async fn cron_remove_tool_deletes_file_and_is_idempotent() {
        let guard = CronDirGuard::new("cron-remove-tool");
        let create_args = json!({
            "id": "doomed",
            "channel_id": "telegram",
            "conversation_id": "1",
            "prompt": "x",
            "expression": "0 * * * *",
        });
        CronCreatorTool
            .call(
                &tool_call("cron_create", "create"),
                &RawValue::from_string(create_args.to_string()).unwrap(),
            )
            .await
            .unwrap();
        assert!(guard.dir.join("doomed.toml").is_file());

        let remove_args = RawValue::from_string(r#"{"id":"doomed"}"#.to_owned()).unwrap();
        let result = CronRemoveTool
            .call(&tool_call("cron_remove", "remove-1"), &remove_args)
            .await
            .unwrap();
        assert_eq!(result.status, ToolStatus::Succeeded);
        assert!(result.content.contains("removed"));
        assert!(!guard.dir.join("doomed.toml").exists());

        let result = CronRemoveTool
            .call(&tool_call("cron_remove", "remove-2"), &remove_args)
            .await
            .unwrap();
        assert_eq!(result.status, ToolStatus::Succeeded);
        assert!(result.content.contains("no-op"));
    }
}
