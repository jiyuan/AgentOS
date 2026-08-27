//! Channel-aware maintenance driven by a bounded process timer.
//!
//! One loop runs beside each enabled transport, never on a conversation
//! shard. Crons remain channel-bound; reflection and retention contend through
//! their durable leases. Since every loop holds the same `AgentRuntime`, the
//! retention winner covers the process-wide job registry and the shared
//! ingress table regardless of which channel acquired leadership.

use super::{
    build_reflection_cron, fire_due_crons, log_line, run_memory_reflection, run_retention_sweep,
    unix_seconds, ServiceConfig, CRON_SCAN_INTERVAL_SECS, RETENTION_SWEEP_INTERVAL_SECS,
};
use agentos_cli::slash::SessionUsage;
use agentos_core::crons::CronStore;
use agentos_core::gateway::{GatewayService, IngressLedger, MaintenanceCadence};
use agentos_core::runtime::AgentRuntime;
use agentos_interfaces::Egress;
use std::sync::Arc;
use std::time::Duration;

pub(super) async fn run_channel_maintenance(
    config: &ServiceConfig,
    channel_name: &'static str,
    runtime: Arc<AgentRuntime>,
    egress: Arc<dyn Egress>,
    ledger: Arc<IngressLedger>,
    session_usage: Arc<SessionUsage>,
) -> Result<(), String> {
    let cron_store = CronStore::new(config.home.join("workspace/crons"));
    let mut reflection_cron = match build_reflection_cron(&runtime, config) {
        Ok(schedule) => schedule,
        Err(err) => {
            let _ = log_line(
                config,
                &format!("{channel_name} reflection schedule failed: {err}"),
            );
            None
        }
    };
    let mut ticker = MaintenanceCadence::new(
        Duration::from_secs(CRON_SCAN_INTERVAL_SECS),
        Duration::from_secs(RETENTION_SWEEP_INTERVAL_SECS),
    )
    .map_err(|err| format!("invalid gateway maintenance cadence: {err}"))?
    .start();

    loop {
        let tick = tokio::select! {
            _ = runtime.cancellation().cancelled() => return Ok(()),
            tick = ticker.tick() => tick,
        };
        let started_unix = unix_seconds();

        let deps_scope = runtime.deps_scope();
        let input_guardrails = deps_scope.input_guardrails();
        let output_guardrails = deps_scope.output_guardrails();
        let tool_guardrails = deps_scope.tool_guardrails();
        let deps = deps_scope.deps_with_guardrails(
            &input_guardrails,
            &output_guardrails,
            &tool_guardrails,
        );
        let service = GatewayService::new(&deps, Arc::from(runtime.active_agent.as_str()))
            .with_approval_expiry(runtime.workspace_config.approval.expiry());

        let cron_status = match fire_due_crons(
            config,
            egress.as_ref(),
            channel_name,
            &cron_store,
            &service,
            &session_usage,
            started_unix,
        )
        .await
        {
            Ok(0) => "checked",
            Ok(_) => "ran",
            Err(err) => {
                let _ = log_line(
                    config,
                    &format!("{channel_name} cron maintenance failed: {err}"),
                );
                "failed"
            }
        };

        let reflection_status = match run_memory_reflection(
            config,
            channel_name,
            reflection_cron.as_mut(),
            &runtime.memory_manager,
            &runtime.session,
            started_unix,
        )
        .await
        {
            Ok(status) => status.as_str(),
            Err(err) => {
                let _ = log_line(
                    config,
                    &format!("{channel_name} reflection maintenance failed: {err}"),
                );
                "failed"
            }
        };

        let retention_status = if tick.retention_due {
            match run_retention_sweep(config, channel_name, &runtime, &ledger).await {
                Ok(status) => status.as_str(),
                Err(err) => {
                    let _ = log_line(
                        config,
                        &format!("{channel_name} retention maintenance failed: {err}"),
                    );
                    "failed"
                }
            }
        } else {
            "not_due"
        };

        // One bounded line per scan is the operational last-run/lag surface.
        // `lag_ms` is scheduler lag before work began; `duration_ms` is work
        // time. Keeping them separate tells overload from a slow cron.
        let _ = log_line(
            config,
            &format!(
                "{channel_name} maintenance sequence={} started_at={} completed_at={} \
                 lag_ms={} duration_ms={} cron={} reflection={} retention={} ingress={} jobs={}",
                tick.sequence,
                started_unix,
                unix_seconds(),
                tick.lag.as_millis(),
                tick.started_at.elapsed().as_millis(),
                cron_status,
                reflection_status,
                retention_status,
                retention_status,
                retention_status,
            ),
        );
    }
}
