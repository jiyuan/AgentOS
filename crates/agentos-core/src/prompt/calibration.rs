//! Measuring the token estimator against a provider's own count.
//!
//! Roadmap item C1 left one thing undone: `prompt::tokens` is a heuristic, and
//! nothing had ever compared it to a real `Usage.input_tokens`. Until that
//! comparison exists, `[compaction].pressure_percent` is a number somebody
//! reasoned their way to rather than measured — and C3's own notes say it must
//! not be tuned before this has been run.
//!
//! # What is measured
//!
//! [`corpus`] is a fixed set of requests, each one a traffic class this
//! deployment actually sees. Every case is sent to the live provider through
//! the ordinary `Llm::complete_messages` path, so the number that comes back
//! includes the provider's chat template, its per-message framing, and its tool
//! schema serialization — everything the estimator is trying to predict and
//! none of which is visible from inside this crate. The estimate is computed
//! exactly as `assemble` computes it: the sum over messages plus the tool
//! schemas.
//!
//! # Why the corpus is fixed rather than sampled from traffic
//!
//! Two reasons. Real traffic cannot be replayed without the conversations it
//! came from, so a measurement taken from it is not reproducible by the next
//! person. And a corpus that names its classes tells you *where* the estimator
//! is wrong, which is the part that matters: a single blended error figure
//! averaged over English and Chinese hides that they are wrong in opposite
//! directions.
//!
//! The `minimal` case exists to measure the provider's fixed per-request
//! overhead. Every other case's error should be read with that figure in mind —
//! a 40% error on a 12-token request is the template, not the estimator.
//!
//! # Running it
//!
//! ```text
//! agentos-gateway calibrate            # live call, writes the record
//! agentos-gateway calibrate --check    # re-check the estimator offline
//! ```
//!
//! The live run writes `docs/TOKEN_CALIBRATION.md` and the recorded samples in
//! `crates/agentos-core/tests/golden/token_calibration.json`. From then on
//! `tests/token_calibration.rs` replays those samples offline, so a change to
//! the estimator's constants is measured against a real provider's numbers
//! without anyone spending another call.

use super::tokens;
use agentos_interfaces::tool::{SandboxMode, ToolSpec};
use agentos_proto::{Message, MessageRole, ToolCall, ToolCallId};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue};
use std::sync::Arc;

/// Bounds of the generated block in `docs/TOKEN_CALIBRATION.md`. The prose
/// around them is written by hand and kept; only what sits between them is
/// replaced. Spliced with [`crate::config::catalog::splice`], which the
/// generated catalogs already use for the same job.
pub const BEGIN_MARKER: &str = "<!-- BEGIN GENERATED CALIBRATION -->";
pub const END_MARKER: &str = "<!-- END GENERATED CALIBRATION -->";

/// One request whose estimate is compared against the provider's count.
pub struct Case {
    /// Stable identifier. Names the traffic class, and keys the record.
    pub id: &'static str,
    /// What this class is, and why it is in the corpus.
    pub note: &'static str,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
}

impl Case {
    /// What `assemble` would record as this request's `total_tokens`.
    pub fn estimated_tokens(&self) -> usize {
        self.messages
            .iter()
            .map(tokens::estimate_message)
            .sum::<usize>()
            + tokens::estimate_tool_specs(&self.tools)
    }

    /// Characters across every message body. Carried into the record as a
    /// fingerprint: a corpus edited after a recording no longer describes the
    /// numbers stored beside it, and the offline check says so rather than
    /// comparing an estimate against an actual from different text.
    pub fn chars(&self) -> usize {
        self.messages
            .iter()
            .map(|message| message.content.chars().count())
            .sum()
    }
}

/// A tool schema close in size to the real built-ins, without depending on
/// them. The registry's specs change as tools gain arguments, which would
/// silently invalidate every recorded sample; the corpus has to be stable
/// across the releases whose estimator it is checking.
fn shell_spec() -> ToolSpec {
    ToolSpec {
        name: Arc::from("shell"),
        description: Arc::from(
            "Run a shell command in the workspace and return its combined output. \
             Output beyond the configured limit is truncated in the middle.",
        ),
        input_schema: json!({
            "type": "object",
            "required": ["command"],
            "properties": {
                "command": { "type": "string", "description": "The command to run." },
                "args": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Arguments passed to the command."
                },
                "cwd": { "type": "string", "description": "Working directory." },
                "timeout_ms": { "type": "integer", "description": "Deadline in milliseconds." }
            }
        }),
        sandbox: SandboxMode::WorkspaceWrite,
        timeout_ms: Some(30_000),
    }
}

fn file_spec() -> ToolSpec {
    ToolSpec {
        name: Arc::from("file"),
        description: Arc::from("Read, write, append to, or list files under the workspace root."),
        input_schema: json!({
            "type": "object",
            "required": ["operation", "path"],
            "properties": {
                "operation": {
                    "type": "string",
                    "enum": ["read", "write", "append", "list"],
                    "description": "What to do."
                },
                "path": { "type": "string", "description": "Workspace-relative path." },
                "content": { "type": "string", "description": "Body, for write and append." }
            }
        }),
        sandbox: SandboxMode::WorkspaceWrite,
        timeout_ms: Some(10_000),
    }
}

/// English prose, in the register a user actually writes in.
const ENGLISH_PROSE: &str = "\
The gateway has been restarting every few minutes since yesterday afternoon and I cannot \
tell from the log whether it is the Feishu long connection dropping or the process itself \
dying. The last few lines before each restart are always the websocket read returning an \
error, but there is no panic and no exit code recorded anywhere I can find. I would like to \
know whether the reconnect path is being taken at all, and if it is, why it does not seem to \
recover the way the Telegram poller does. If it turns out to be the proxy interfering, say \
so plainly rather than adding another retry layer on top of one that is already not working.";

/// Chinese prose of comparable substance. The class a 4:1 divisor gets wrong by
/// roughly four times, and the traffic this deployment actually carries.
const CHINESE_PROSE: &str = "\
网关从昨天下午开始每隔几分钟就重启一次，我从日志里看不出到底是飞书的长连接断了，还是进程\
自己挂了。每次重启前的最后几行都是 websocket 读取返回错误，但既没有 panic，也没有在任何\
地方记录退出码。我想知道重连的分支到底有没有走到，如果走到了，为什么它没有像 Telegram 的\
轮询那样恢复过来。如果最后查出来是代理在中间捣乱，就直接说清楚，不要在一个本来就没起作用\
的重试上面再加一层重试。";

/// The normal case here: Chinese with English identifiers embedded.
const MIXED_PROSE: &str = "\
请帮我看一下 crates/agentos-core/src/prompt/tokens.rs 里的 estimate_text，\
它对 ASCII 用 4:1，对非 ASCII 按每字符一个 token 计算。我想确认这个偏高的估计在 \
DeepSeek 上到底偏了多少，如果误差超过 15%，就要重新校准 pressure_percent 这个阈值。";

/// Symbol-dense ASCII. Code tokenizes far worse than 4:1, so this is where the
/// estimator is most likely to be low — the dangerous direction.
const CODE_BLOCK: &str = "\
```rust
pub fn estimate_text(text: &str) -> usize {
    let mut ascii = 0usize;
    let mut wide = 0usize;
    for character in text.chars() {
        if character.is_ascii() { ascii += 1; } else { wide += 1; }
    }
    ascii.div_ceil(ASCII_CHARS_PER_TOKEN) + wide
}
```
Why does `div_ceil` matter here? Because `4 / 4 == 1` but `5 / 4 == 1` too, and \
rounding down is an under-estimate — the one direction this must never take.";

/// The traffic classes the estimator is measured against.
pub fn corpus() -> Vec<Case> {
    vec![
        Case {
            id: "minimal",
            note: "One short user turn. Measures the provider's fixed per-request overhead, \
                   which every other case's error should be read against.",
            messages: vec![Message::text(MessageRole::User, "ping")],
            tools: Vec::new(),
        },
        Case {
            id: "english_prose",
            note: "English, the register the 4:1 rule was derived from.",
            messages: vec![Message::text(MessageRole::User, ENGLISH_PROSE)],
            tools: Vec::new(),
        },
        Case {
            id: "chinese_prose",
            note: "Chinese. A 4:1 divisor under-counts this by about four times, which is \
                   why the estimator counts wide characters one for one.",
            messages: vec![Message::text(MessageRole::User, CHINESE_PROSE)],
            tools: Vec::new(),
        },
        Case {
            id: "mixed_scripts",
            note: "Chinese with English identifiers embedded. This deployment's normal case.",
            messages: vec![Message::text(MessageRole::User, MIXED_PROSE)],
            tools: Vec::new(),
        },
        Case {
            id: "code_block",
            note: "Symbol-dense ASCII. Code tokenizes worse than 4:1, so this is where the \
                   estimate is most likely to fall below the truth.",
            messages: vec![Message::text(MessageRole::User, CODE_BLOCK)],
            tools: Vec::new(),
        },
        Case {
            id: "multi_turn",
            note: "A system turn plus four conversational turns. Isolates the per-message \
                   overhead constant, which a single-message case cannot separate from the \
                   provider's fixed cost.",
            messages: vec![
                Message::text(
                    MessageRole::System,
                    "You are a careful engineering assistant. Answer briefly.",
                ),
                Message::text(MessageRole::User, "Is the gateway running?"),
                Message::text(
                    MessageRole::Assistant,
                    "Yes — pid 4812, up for six hours, two channels attached.",
                ),
                Message::text(MessageRole::User, "网关现在有多少个会话在跑？"),
                Message::text(
                    MessageRole::Assistant,
                    "Three conversations are active across two shards.",
                ),
            ],
            tools: Vec::new(),
        },
        Case {
            id: "tool_schemas",
            note: "A short turn plus two tool schemas. Schemas carry no messages but occupy \
                   the same window, and on a small request they outweigh the conversation.",
            messages: vec![Message::text(
                MessageRole::User,
                "List the files under workspace/skills and tell me which are stale.",
            )],
            tools: vec![shell_spec(), file_spec()],
        },
        Case {
            id: "tool_round_trip",
            note: "An assistant tool-call turn and its result. The shape that grows a \
                   transcript fastest, and the one whose cost is easiest to miss because \
                   the assistant message has almost no text.",
            messages: vec![
                Message::text(
                    MessageRole::User,
                    "Show me the last twenty lines of the gateway log.",
                ),
                Message {
                    role: MessageRole::Assistant,
                    content: Arc::from(""),
                    attachments: Vec::new(),
                    tool_calls: vec![ToolCall {
                        id: ToolCallId::new("call_calibration_1"),
                        name: Arc::from("shell"),
                        args: RawValue::from_string(
                            r#"{"command":"tail","args":["-n","20","workspace/gateway.log"]}"#
                                .to_owned(),
                        )
                        .expect("static args are valid JSON"),
                    }],
                    tool_call_id: None,
                    metadata: Default::default(),
                },
                Message {
                    role: MessageRole::Tool,
                    content: Arc::from(
                        "2026-08-17T03:11:02Z INFO agentos_core::gateway shard=0 \
                         conversation=telegram:8814 turn started\n\
                         2026-08-17T03:11:04Z INFO agentos_llm::usage provider=deepseek \
                         model=deepseek-chat input=1841 output=96\n\
                         2026-08-17T03:11:04Z INFO agentos_core::loop plan_finished turn=0\n",
                    ),
                    attachments: Vec::new(),
                    tool_calls: Vec::new(),
                    tool_call_id: Some(ToolCallId::new("call_calibration_1")),
                    metadata: Default::default(),
                },
            ],
            tools: vec![shell_spec()],
        },
    ]
}

/// One case's estimate beside the provider's own count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sample {
    pub case: String,
    /// Corpus fingerprint — see [`Case::chars`].
    pub chars: usize,
    pub messages: usize,
    pub tools: usize,
    pub estimated_tokens: usize,
    /// `Usage.input_tokens` as the provider reported it.
    pub actual_tokens: u64,
}

impl Sample {
    /// Signed error as a percentage of the provider's count. Positive is an
    /// over-estimate (compacts early, safe); negative is an under-estimate
    /// (lets a request reach the provider's hard limit, which is the failure
    /// the estimator's high bias exists to avoid).
    pub fn error_percent(&self) -> f64 {
        if self.actual_tokens == 0 {
            return 0.0;
        }
        let estimated = self.estimated_tokens as f64;
        let actual = self.actual_tokens as f64;
        (estimated - actual) / actual * 100.0
    }
}

/// One live run's record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Calibration {
    pub provider: String,
    pub model: String,
    /// UTC date of the run. Providers change their tokenizers between model
    /// revisions, so a record without a date cannot be judged stale.
    pub recorded_at: String,
    pub samples: Vec<Sample>,
}

/// What a run says about the estimator as a whole.
#[derive(Clone, Debug, PartialEq)]
pub struct Summary {
    pub samples: usize,
    /// Largest over-estimate, in percent. Zero if every case was low.
    pub worst_over_percent: f64,
    /// Largest under-estimate, as a negative percent. Zero if every case was
    /// high. This is the number that decides whether the estimator is safe.
    pub worst_under_percent: f64,
    pub mean_error_percent: f64,
    pub median_abs_percent: f64,
    /// Cases whose absolute error is within C1's stated ~15% target.
    pub within_target: usize,
}

/// C1's stated accuracy target: within ~15% of the provider's count.
pub const TARGET_PERCENT: f64 = 15.0;

pub fn summarize(samples: &[Sample]) -> Summary {
    let mut errors: Vec<f64> = samples.iter().map(Sample::error_percent).collect();
    let count = errors.len();
    if count == 0 {
        return Summary {
            samples: 0,
            worst_over_percent: 0.0,
            worst_under_percent: 0.0,
            mean_error_percent: 0.0,
            median_abs_percent: 0.0,
            within_target: 0,
        };
    }
    let mean = errors.iter().sum::<f64>() / count as f64;
    let worst_over = errors.iter().cloned().fold(0.0, f64::max);
    let worst_under = errors.iter().cloned().fold(0.0, f64::min);
    let within = errors
        .iter()
        .filter(|error| error.abs() <= TARGET_PERCENT)
        .count();

    let mut absolute: Vec<f64> = errors.iter().map(|error| error.abs()).collect();
    absolute.sort_by(|a, b| a.partial_cmp(b).expect("error percentages are finite"));
    let median = if count.is_multiple_of(2) {
        (absolute[count / 2 - 1] + absolute[count / 2]) / 2.0
    } else {
        absolute[count / 2]
    };
    errors.clear();

    Summary {
        samples: count,
        worst_over_percent: worst_over,
        worst_under_percent: worst_under,
        mean_error_percent: mean,
        median_abs_percent: median,
        within_target: within,
    }
}

/// The human-readable record, written to `docs/TOKEN_CALIBRATION.md`.
pub fn report_markdown(calibration: &Calibration) -> String {
    let notes: Vec<(&str, &str)> = corpus()
        .iter()
        .map(|case| (case.id, case.note))
        .collect::<Vec<_>>();
    let summary = summarize(&calibration.samples);

    let mut out = String::new();
    out.push_str(&format!(
        "Recorded {} against `{}` on `{}`.\n\n",
        calibration.recorded_at, calibration.model, calibration.provider
    ));
    out.push_str(&format!(
        "**{} of {} cases within ±{:.0}%.** Median absolute error {:.1}%, mean signed error \
         {:+.1}%. Worst over-estimate {:+.1}%, worst under-estimate {:+.1}%.\n\n",
        summary.within_target,
        summary.samples,
        TARGET_PERCENT,
        summary.median_abs_percent,
        summary.mean_error_percent,
        summary.worst_over_percent,
        summary.worst_under_percent,
    ));
    out.push_str("| Case | Chars | Estimated | Actual | Error |\n");
    out.push_str("|---|---:|---:|---:|---:|\n");
    for sample in &calibration.samples {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {:+.1}% |\n",
            sample.case,
            sample.chars,
            sample.estimated_tokens,
            sample.actual_tokens,
            sample.error_percent(),
        ));
    }
    out.push_str("\n## What each case is\n\n");
    for (id, note) in notes {
        out.push_str(&format!("- **`{id}`** — {note}\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(case: &str, estimated: usize, actual: u64) -> Sample {
        Sample {
            case: case.to_owned(),
            chars: 0,
            messages: 1,
            tools: 0,
            estimated_tokens: estimated,
            actual_tokens: actual,
        }
    }

    #[test]
    fn every_case_has_a_distinct_id_and_some_content() {
        let corpus = corpus();
        let mut ids: Vec<&str> = corpus.iter().map(|case| case.id).collect();
        ids.sort_unstable();
        let distinct = ids.len();
        ids.dedup();
        assert_eq!(
            distinct,
            ids.len(),
            "case ids must be unique: they key the record"
        );
        for case in &corpus {
            assert!(!case.messages.is_empty(), "{} has no messages", case.id);
            assert!(case.estimated_tokens() > 0, "{} estimates zero", case.id);
        }
    }

    #[test]
    fn the_corpus_covers_both_character_classes() {
        // A corpus of only English would measure the half of the estimator
        // that was never in doubt.
        let corpus = corpus();
        let wide: usize = corpus
            .iter()
            .flat_map(|case| case.messages.iter())
            .flat_map(|message| message.content.chars())
            .filter(|character| !character.is_ascii())
            .count();
        assert!(wide > 200, "only {wide} wide characters in the corpus");
    }

    #[test]
    fn the_estimate_matches_what_assemble_would_record() {
        // `assemble` sums `estimate_message` over the messages and adds the
        // tool schemas. A case that computed it any other way would be
        // measuring something the runtime never emits.
        let case = corpus()
            .into_iter()
            .find(|case| case.id == "tool_schemas")
            .expect("the corpus has a tool case");
        let expected = case
            .messages
            .iter()
            .map(tokens::estimate_message)
            .sum::<usize>()
            + tokens::estimate_tool_specs(&case.tools);
        assert_eq!(case.estimated_tokens(), expected);
        assert!(
            tokens::estimate_tool_specs(&case.tools) > 100,
            "the tool schemas should dominate this case"
        );
    }

    #[test]
    fn error_is_signed_against_the_provider_count() {
        assert_eq!(sample("high", 115, 100).error_percent(), 15.0);
        assert_eq!(sample("low", 85, 100).error_percent(), -15.0);
        assert_eq!(sample("exact", 100, 100).error_percent(), 0.0);
        // A provider that reported nothing cannot be compared against.
        assert_eq!(sample("missing", 100, 0).error_percent(), 0.0);
    }

    #[test]
    fn the_summary_separates_the_two_directions() {
        let summary = summarize(&[
            sample("a", 120, 100),
            sample("b", 90, 100),
            sample("c", 104, 100),
        ]);
        assert_eq!(summary.samples, 3);
        assert_eq!(summary.worst_over_percent, 20.0);
        assert_eq!(summary.worst_under_percent, -10.0);
        // Two of three within ±15%: the 20% over-estimate is out.
        assert_eq!(summary.within_target, 2);
        assert_eq!(summary.median_abs_percent, 10.0);
        assert!((summary.mean_error_percent - 4.666_666).abs() < 0.001);
    }

    #[test]
    fn an_empty_run_summarizes_to_zero_rather_than_dividing_by_it() {
        assert_eq!(summarize(&[]).samples, 0);
        assert_eq!(summarize(&[]).median_abs_percent, 0.0);
    }

    #[test]
    fn the_report_names_every_case_it_recorded() {
        let calibration = Calibration {
            provider: "deepseek".to_owned(),
            model: "deepseek-chat".to_owned(),
            recorded_at: "2026-08-17".to_owned(),
            samples: vec![sample("minimal", 5, 6)],
        };
        let report = report_markdown(&calibration);
        assert!(report.contains("| `minimal` |"));
        assert!(report.contains("deepseek-chat"));
        // The notes explain what a class is, so a reader of the table knows
        // what "mixed_scripts" was without opening the source.
        assert!(report.contains("**`mixed_scripts`**"));
    }
}
