//! Roadmap C1's accuracy check, replayed offline.
//!
//! `docs/TOKEN_CALIBRATION.md` and `tests/golden/token_calibration.json` record
//! what a real provider counted for each case in
//! `prompt::calibration::corpus()`. This test re-scores today's estimator
//! against those recorded counts, so the measurement keeps its value long after
//! the call that produced it — a change to the estimator's constants is checked
//! against a provider's real numbers without anyone spending another request.
//!
//! Nothing here reaches the network. Re-recording is deliberate and manual:
//! `agentos-gateway calibrate`.

use agentos_core::prompt::calibration::{self, Calibration, Sample, TARGET_PERCENT};

const RECORD: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/token_calibration.json"
);

const RERECORD: &str = "re-record with `cargo run -p agentos-cli --bin agentos-gateway -- \
                        calibrate` (this spends real provider calls) and commit \
                        docs/TOKEN_CALIBRATION.md alongside it";

fn recorded() -> Calibration {
    let body = std::fs::read_to_string(RECORD)
        .unwrap_or_else(|err| panic!("{RECORD} is missing: {err}; {RERECORD}"));
    serde_json::from_str(&body).unwrap_or_else(|err| panic!("{RECORD} is malformed: {err}"))
}

/// Today's estimate for every case, beside the count the provider reported.
fn rescored() -> Vec<Sample> {
    let recorded = recorded();
    // A case dropped from the corpus would otherwise leave a stale row in the
    // record and the report, describing a class nobody measures any more.
    assert_eq!(
        recorded.samples.len(),
        calibration::corpus().len(),
        "the record holds {} cases and the corpus has {}; {RERECORD}",
        recorded.samples.len(),
        calibration::corpus().len()
    );
    calibration::corpus()
        .into_iter()
        .map(|case| {
            let previous = recorded
                .samples
                .iter()
                .find(|sample| sample.case == case.id)
                .unwrap_or_else(|| {
                    panic!("'{}' has no recorded provider count; {RERECORD}", case.id)
                });
            assert_eq!(
                case.chars(),
                previous.chars,
                "corpus case '{}' has been edited since it was recorded, so the recorded count \
                 describes text that no longer exists; {RERECORD}",
                case.id
            );
            Sample {
                case: case.id.to_owned(),
                chars: case.chars(),
                messages: case.messages.len(),
                tools: case.tools.len(),
                estimated_tokens: case.estimated_tokens(),
                actual_tokens: previous.actual_tokens,
            }
        })
        .collect()
}

/// The prose report says what the record says. `docs/TOKEN_CALIBRATION.md` is
/// what a reader actually opens; a hand-edited number in it would be a measured
/// figure that was never measured.
#[test]
fn the_checked_in_report_matches_the_record() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/TOKEN_CALIBRATION.md"
    );
    let existing = std::fs::read_to_string(path)
        .unwrap_or_else(|err| panic!("{path} is missing: {err}; {RERECORD}"));
    let expected = agentos_core::config::catalog::splice(
        &existing,
        calibration::BEGIN_MARKER,
        calibration::END_MARKER,
        &calibration::report_markdown(&recorded()),
    )
    .expect("the report keeps its markers");
    assert_eq!(
        existing, expected,
        "docs/TOKEN_CALIBRATION.md no longer matches the recorded samples; regenerate it with \
         `cargo run -p agentos-cli --bin agentos-gateway -- calibrate --check > /dev/null` or \
         {RERECORD}"
    );
}

/// The record describes the estimator that is in the tree. If it does not, the
/// checked-in report is describing an estimator nobody is running — the same
/// class of drift the generated catalogs (X4) exist to prevent.
#[test]
fn the_recorded_report_describes_todays_estimator() {
    let recorded = recorded();
    for sample in rescored() {
        let previous = recorded
            .samples
            .iter()
            .find(|entry| entry.case == sample.case)
            .expect("rescoring already proved every case is recorded");
        assert_eq!(
            sample.estimated_tokens, previous.estimated_tokens,
            "the estimator now says {} tokens for '{}' where the calibration recorded {}; the \
             checked-in error figures no longer describe it, so {RERECORD}",
            sample.estimated_tokens, sample.case, previous.estimated_tokens
        );
    }
}

/// The measurement's headline: on requests shaped like real traffic, the
/// estimate is close enough to threshold against.
///
/// The composites are the cases that matter for pressure. A request large
/// enough to approach a context window is a transcript — prose, tool output,
/// schemas, and code together — never one character class on its own. The
/// single-class cases below bound how wrong each ingredient can be; these bound
/// the mixtures they appear in.
#[test]
fn realistic_mixed_requests_are_within_the_stated_target() {
    for sample in rescored() {
        if !matches!(
            sample.case.as_str(),
            "multi_turn" | "tool_round_trip" | "mixed_scripts"
        ) {
            continue;
        }
        let error = sample.error_percent();
        assert!(
            error.abs() <= TARGET_PERCENT + 1.0,
            "'{}' is {error:+.1}% off the provider's count, outside C1's ±{TARGET_PERCENT:.0}% \
             target",
            sample.case
        );
    }
}

/// The single-class extremes, pinned at the values the provider actually
/// produced. This is the test that would have caught the claim `prompt::tokens`
/// used to make — that the estimator is never dangerously low — which the code
/// case disproves.
#[test]
fn the_measured_extremes_stay_where_the_calibration_found_them() {
    let by_case = |id: &str| {
        rescored()
            .into_iter()
            .find(|sample| sample.case == id)
            .unwrap_or_else(|| panic!("'{id}' is in the corpus"))
            .error_percent()
    };

    // Chinese counted one token per character. Safe, and by a wide margin —
    // this is the single largest reason a pressure reading runs high.
    let chinese = by_case("chinese_prose");
    assert!(
        (25.0..40.0).contains(&chinese),
        "chinese_prose moved to {chinese:+.1}%; the calibration found +32%"
    );

    // Tool schemas: JSON packs better under a byte-pair encoder than 4:1, so
    // counting their text at the prose rate over-estimates them substantially.
    let schemas = by_case("tool_schemas");
    assert!(
        (30.0..45.0).contains(&schemas),
        "tool_schemas moved to {schemas:+.1}%; the calibration found +38%"
    );

    // The dangerous direction, and the reason the module no longer claims to
    // be safe by construction: symbol-dense ASCII tokenizes worse than 4:1, so
    // a request made mostly of code reads low. Bounded here so a change that
    // makes it worse has to say so.
    let code = by_case("code_block");
    assert!(
        (-22.0..0.0).contains(&code),
        "code_block moved to {code:+.1}%; the calibration found -19%, and a larger \
         under-estimate widens the gap C4 has to recover from"
    );
}

/// The threshold `[compaction].pressure_percent` defaults to has to survive the
/// worst under-estimate a realistic request can carry, or compaction fires
/// after the provider has already rejected the request.
#[test]
fn the_default_pressure_threshold_leaves_room_for_the_measured_error() {
    // `minimal` is off by 28% and by two tokens: the provider's fixed
    // per-request framing, which no threshold is ever read against. Pressure is
    // only meaningful on requests large enough for that constant to vanish.
    let measured: Vec<f64> = rescored()
        .into_iter()
        .filter(|sample| sample.actual_tokens >= 50)
        .map(|sample| sample.error_percent())
        .collect();
    assert!(
        measured.len() >= 5,
        "only {} cases are large enough to threshold against",
        measured.len()
    );
    let worst_meaningful = measured.into_iter().fold(0.0_f64, f64::min).abs();

    let pressure = agentos_core::config::CompactionConfig::default().pressure_percent as f64;
    // What the request is really at when the estimate reads `pressure`. This is
    // the arithmetic the 84 default comes from; it must still hold, or the
    // trigger fires after the provider has already rejected the request and C4
    // is left to recover from something that was predictable.
    let true_percent = pressure * (1.0 + worst_meaningful / 100.0);
    assert!(
        true_percent <= 100.0,
        "an estimate reading {pressure}% of the window is {true_percent:.1}% of it in truth at \
         the measured {worst_meaningful:.1}% under-estimate; compaction.pressure_percent must \
         be at most {:.0}",
        100.0 / (1.0 + worst_meaningful / 100.0)
    );
}
