//! `agentos-gateway calibrate` — measure the token estimator against a live
//! provider, or re-check it offline against what a past run recorded.
//!
//! Roadmap item C1's outstanding accuracy check. The corpus and the arithmetic
//! live in `agentos_core::prompt::calibration`, next to the estimator they
//! measure; this is the part that spends real money and decides where the
//! record lands.
//!
//! Without `--check` it sends every case in the corpus to the configured
//! provider and writes what came back. That is a handful of small requests —
//! cheap, but not free, and it reaches the network, so it is never run by
//! `cargo test` or by CI. `--check` replays the recorded numbers against the
//! current estimator and touches nothing outside the process.

use agentos_core::config::catalog;
use agentos_core::prompt::calibration::{self, Calibration, Sample, TARGET_PERCENT};
use agentos_llm::{EnvLlm, Llm, LlmModelController, LlmModelTier};
use agentos_proto::{Usage, TOKEN_USAGE_METADATA_KEY};
use std::path::{Path, PathBuf};

/// The human-readable record. Checked in, so the measured numbers are readable
/// without re-running anything.
const REPORT: &str = "docs/token-calibration.md";
/// The machine-readable record `tests/token_calibration.rs` replays.
const RECORD: &str = "crates/agentos-core/tests/golden/token_calibration.json";

pub(super) fn run(root: &Path, check: bool) -> Result<(), String> {
    if check {
        return recheck(root);
    }
    measure(root)
}

/// Replay the recorded provider counts against today's estimator.
///
/// Offline. This is what tells you whether a change to the estimator's
/// constants moved it toward or away from the numbers a provider actually
/// reported, without spending another call to find out.
fn recheck(root: &Path) -> Result<(), String> {
    let path = root.join(RECORD);
    let recorded: Calibration =
        serde_json::from_str(&std::fs::read_to_string(&path).map_err(|err| {
            format!("{}: {err}; run `agentos-gateway calibrate`", path.display())
        })?)
        .map_err(|err| format!("{}: {err}", path.display()))?;

    let mut samples = Vec::with_capacity(recorded.samples.len());
    for case in calibration::corpus() {
        let Some(previous) = recorded
            .samples
            .iter()
            .find(|sample| sample.case == case.id)
        else {
            return Err(format!(
                "'{}' has no recorded provider count; re-record with `agentos-gateway calibrate`",
                case.id
            ));
        };
        if previous.chars != case.chars() {
            return Err(format!(
                "'{}' has changed since it was recorded ({} chars now, {} then); the recorded \
                 count describes text that no longer exists — re-record with \
                 `agentos-gateway calibrate`",
                case.id,
                case.chars(),
                previous.chars
            ));
        }
        samples.push(Sample {
            case: case.id.to_owned(),
            chars: case.chars(),
            messages: case.messages.len(),
            tools: case.tools.len(),
            estimated_tokens: case.estimated_tokens(),
            actual_tokens: previous.actual_tokens,
        });
    }

    let current = Calibration {
        samples,
        ..recorded
    };
    print!("{}", calibration::report_markdown(&current));
    Ok(())
}

/// Send every case to the configured provider and record what it counted.
fn measure(root: &Path) -> Result<(), String> {
    let controller = LlmModelController::new();
    let llm = EnvLlm::new(LlmModelTier::Medium, controller)
        .map_err(|err| format!("cannot build the configured provider: {err}"))?;
    let selection = llm.current_selection().ok_or_else(|| {
        "no provider configured; set AGENTOS_LLM_PROVIDER and the matching API key in .env"
            .to_owned()
    })?;
    if selection.provider.as_ref() == "builtin.echo" {
        return Err(
            "the echo provider reports no token usage; point AGENTOS_LLM_PROVIDER at a real \
             provider to calibrate against it"
                .to_owned(),
        );
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("cannot start a tokio runtime: {err}"))?;

    let mut samples = Vec::new();
    for case in calibration::corpus() {
        eprintln!("calibrating {} ...", case.id);
        let response = runtime
            .block_on(llm.complete_messages(&case.messages, &case.tools))
            .map_err(|err| format!("{}: the provider call failed: {err}", case.id))?;
        let usage: Usage = response
            .metadata
            .get(TOKEN_USAGE_METADATA_KEY)
            .ok_or_else(|| {
                format!(
                    "{}: the reply carried no token usage; this provider cannot be calibrated \
                     against",
                    case.id
                )
            })
            .and_then(|raw| {
                serde_json::from_value(raw.clone())
                    .map_err(|err| format!("{}: malformed token usage: {err}", case.id))
            })?;
        if usage.input_tokens == 0 {
            return Err(format!(
                "{}: the provider reported zero input tokens, which cannot be compared against",
                case.id
            ));
        }
        samples.push(Sample {
            case: case.id.to_owned(),
            chars: case.chars(),
            messages: case.messages.len(),
            tools: case.tools.len(),
            estimated_tokens: case.estimated_tokens(),
            actual_tokens: usage.input_tokens,
        });
    }

    let calibration = Calibration {
        provider: selection.provider.to_string(),
        model: selection.model.to_string(),
        recorded_at: today(),
        samples,
    };

    let record = root.join(RECORD);
    write_file(
        &record,
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&calibration)
                .map_err(|err| format!("cannot serialize the record: {err}"))?
        ),
    )?;
    println!("wrote {RECORD}");

    let report = root.join(REPORT);
    let existing = std::fs::read_to_string(&report).unwrap_or_else(|_| scaffold());
    let body = calibration::report_markdown(&calibration);
    let updated = catalog::splice(
        &existing,
        calibration::BEGIN_MARKER,
        calibration::END_MARKER,
        &body,
    )
    .map_err(|err| format!("{}: {err}", report.display()))?;
    write_file(&report, &updated)?;
    println!("wrote {REPORT}");

    let summary = calibration::summarize(&calibration.samples);
    println!(
        "{} of {} cases within ±{:.0}%; worst under-estimate {:+.1}%",
        summary.within_target, summary.samples, TARGET_PERCENT, summary.worst_under_percent
    );
    Ok(())
}

fn scaffold() -> String {
    format!(
        "# Token calibration\n\n\
         What `agentos_core::prompt::tokens` estimated, beside what the provider actually \
         counted. Regenerate with `agentos-gateway calibrate` (live call) or re-check the \
         estimator against these same numbers offline with `agentos-gateway calibrate --check`.\n\n\
         {}\n{}\n",
        calibration::BEGIN_MARKER,
        calibration::END_MARKER
    )
}

fn write_file(path: &Path, body: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|err| format!("cannot create {}: {err}", parent.display()))?;
    }
    std::fs::write(path, body).map_err(|err| format!("cannot write {}: {err}", path.display()))
}

/// Today, as `YYYY-MM-DD` UTC. Days since the epoch through the civil-date
/// algorithm — a date stamp is not worth a chrono dependency.
fn today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if month <= 2 { year + 1 } else { year };
    format!("{year:04}-{month:02}-{day:02}")
}

/// The repository root, for a command run from a checkout. Mirrors
/// `catalog::default_root` — both write checked-in documentation.
pub(super) fn default_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scaffold a first run writes has to be spliceable, or the very next
    /// run cannot update the file it just created.
    #[test]
    fn the_scaffold_carries_the_markers_the_splice_needs() {
        let spliced = catalog::splice(
            &scaffold(),
            calibration::BEGIN_MARKER,
            calibration::END_MARKER,
            "body\n",
        )
        .expect("the scaffold carries both markers");
        assert!(spliced.contains("body\n"));
        assert!(spliced.contains("agentos-gateway calibrate"));
    }

    #[test]
    fn the_date_stamp_is_a_calendar_date() {
        let stamp = today();
        let parts: Vec<&str> = stamp.split('-').collect();
        assert_eq!(parts.len(), 3, "{stamp} is not YYYY-MM-DD");
        let year: i64 = parts[0].parse().expect("a numeric year");
        let month: u32 = parts[1].parse().expect("a numeric month");
        let day: u32 = parts[2].parse().expect("a numeric day");
        assert!(year >= 2025, "{stamp} predates this code");
        assert!((1..=12).contains(&month), "{stamp} has no such month");
        assert!((1..=31).contains(&day), "{stamp} has no such day");
    }
}
