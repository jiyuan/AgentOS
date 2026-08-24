//! Pause + resume benchmark for the `ask_user` approval path.
//!
//! Two measurements:
//!
//! - `ask_user_pause_resume`: a full interruption cycle. The run pauses at
//!   `Approve` (`PolicyDecision::AskUser`), the paused `RunState` is
//!   serialized to JSON and deserialized back (simulating persistence across
//!   a process boundary), the interruption is approved, and the run resumes
//!   through `resume_approved` to `Finish`.
//! - `paused_state_json_round_trip`: just the `RunState` serialize +
//!   deserialize cost for a paused run, isolating the persistence overhead
//!   from the loop transitions.
//!
//! Run with `cargo bench -p agentos-core --bench pause_resume`.

mod support;

use agentos_core::approve::Policy;
use agentos_core::r#loop::{
    resume_approved, route, ApprovalBinding, ApprovalTicket, ResumeWitness, Routed, RunLoopState,
};
use agentos_interfaces::RunState;
use agentos_proto::{ChannelId, ConversationId, Envelope, Message, MessageRole};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::sync::Arc;
use support::{
    current_thread_runtime, drive, echo_tool_registry, fresh_state, make_deps,
    ScriptedToolOrchestrator, BENCH_TOOL_NAME,
};

fn pause_run(
    runtime: &tokio::runtime::Runtime,
    deps: &agentos_core::r#loop::LoopDeps<'_>,
) -> RunState {
    match runtime.block_on(drive(deps, fresh_state())) {
        RunLoopState::Paused(state) => state,
        other => panic!("expected paused run, got {other:?}"),
    }
}

fn approval_witness(state: &RunState) -> ResumeWitness {
    let approval = state.pending_approval().expect("pending approval");
    let ticket = ApprovalTicket::parse(&approval.approval_ticket).expect("ticket parses");
    let binding = ApprovalBinding::new(
        approval.approval_instance_id.clone(),
        ticket.clone(),
        approval.id.clone(),
        approval.prompting_principal.clone(),
        None,
    )
    .expect("instance matches ticket");
    let answer = Envelope {
        channel_id: ChannelId::new("bench"),
        conversation_id: ConversationId::new("bench"),
        sender: Arc::from("bench-user"),
        message: Message::text(MessageRole::User, format!("/approve {ticket}")),
        metadata: Default::default(),
    };
    let Routed::Decides { witness } = route(Some(&binding), &answer) else {
        panic!("answer creates witness");
    };
    witness
}

fn bench_ask_user_pause_resume(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let orchestrator = ScriptedToolOrchestrator;
    let ask_policy = Policy::ask_user_tools([BENCH_TOOL_NAME]);
    let tools = echo_tool_registry();

    c.bench_function("loop_overhead/ask_user_pause_resume", |b| {
        b.iter(|| {
            let deps = make_deps(&orchestrator, &ask_policy, Some(&tools));
            let paused = pause_run(&runtime, &deps);

            let json = serde_json::to_string(&paused).expect("paused RunState serializes");
            let restored: RunState =
                serde_json::from_str(&json).expect("paused RunState deserializes");
            let witness = approval_witness(&restored);

            let mut current = resume_approved(restored, witness).expect("approved state resumes");
            let finished = runtime.block_on(async {
                loop {
                    current = current.step(&deps).await.expect("resume step succeeds");
                    if matches!(current, RunLoopState::Finish(_) | RunLoopState::Paused(_)) {
                        return current;
                    }
                }
            });
            assert!(
                matches!(finished, RunLoopState::Finish(_)),
                "resumed run finishes"
            );
            black_box(finished);
        });
    });
}

fn bench_paused_state_round_trip(c: &mut Criterion) {
    let runtime = current_thread_runtime();
    let orchestrator = ScriptedToolOrchestrator;
    let ask_policy = Policy::ask_user_tools([BENCH_TOOL_NAME]);
    let tools = echo_tool_registry();
    let deps = make_deps(&orchestrator, &ask_policy, Some(&tools));
    let paused = pause_run(&runtime, &deps);

    c.bench_function("loop_overhead/paused_state_json_round_trip", |b| {
        b.iter(|| {
            let json = serde_json::to_string(&paused).expect("paused RunState serializes");
            let restored: RunState =
                serde_json::from_str(&json).expect("paused RunState deserializes");
            black_box(restored);
        });
    });
}

criterion_group!(
    benches,
    bench_ask_user_pause_resume,
    bench_paused_state_round_trip
);
criterion_main!(benches);
