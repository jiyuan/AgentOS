//! What this deployment's config actually resolved to (M7 / `CFG-001`,
//! deliverable 7).
//!
//! `agentos-gateway config` used to be fifty-five hand-written `println!`s
//! naming a subset of the keys. Two things were wrong with that. It went stale
//! silently — a key added to a struct never appeared, and a key removed left a
//! line printing a field that no longer existed until someone noticed — and it
//! answered only "what is the value", never "did I set that or is it the
//! default", "is this key even still effective", or "is anything I wrote being
//! ignored". Those are the questions an operator opens the command to answer.
//!
//! So this is derived from the same walk that generates
//! `docs/CONFIG_CATALOG.md`, over the *loaded* config rather than the default
//! one. A key cannot be missing from it, because the generator finds keys by
//! walking the structs.
//!
//! # What `source` can and cannot tell you
//!
//! `Source::File` means the effective value differs from the default. A
//! config that explicitly writes the default value is indistinguishable from
//! one that omits the key, and this says `Default` for both. Distinguishing
//! them would mean parsing the TOML alongside the typed config and tracking
//! presence per key — worth doing if the ambiguity ever bites, and dishonest
//! to paper over in the meantime.

use super::catalog::rows_against;
use super::WorkspaceConfig;
use std::fmt::Write as _;

/// Where a key's effective value came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Source {
    /// The built-in default. Also what an `agent.toml` that writes the default
    /// value explicitly reports — see the module docs.
    Default,
    /// This deployment's `agent.toml` changed it.
    File,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::File => "file",
        }
    }
}

/// How much of a key's promise the runtime keeps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Maturity {
    /// Decides something, with no caveat. The overwhelming majority.
    Effective,
    /// Still parses so an older `agent.toml` keeps loading, warns at load, and
    /// decides nothing.
    Deprecated,
    /// Decides something, and carries an exposure or a missing contract. Off
    /// by default.
    Preview,
}

impl Maturity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Effective => "effective",
            Self::Deprecated => "deprecated",
            Self::Preview => "preview",
        }
    }
}

/// One key, as this deployment resolved it.
#[derive(Clone, Debug)]
pub struct Entry {
    pub key: String,
    pub value: String,
    pub default: String,
    pub source: Source,
    pub maturity: Maturity,
}

impl Entry {
    /// Whether this entry is something the operator should look at: a
    /// deprecated key they set, or a preview capability they turned on.
    ///
    /// Not an error — both are legal, and a deployment may want the preview.
    /// It is the list worth reading before a release upgrade.
    pub fn is_noteworthy(&self) -> bool {
        self.source == Source::File
            && matches!(self.maturity, Maturity::Deprecated | Maturity::Preview)
    }
}

/// Renderings the catalog walk uses for a row that has no value of its own: a
/// table header, or a field of a record inside an array.
///
/// Kept out of the diagnostic. The catalog is a schema and needs those rows;
/// this answers "what is this deployment set to", and a line reading
/// `guardrails.shell_profiles.program=—` answers nothing.
const STRUCTURAL: [&str; 2] = ["(table)", "—"];

/// Every key `agent.toml` accepts, with what `config` resolved it to.
pub fn entries(config: &WorkspaceConfig) -> Vec<Entry> {
    let maturity = super::catalog::maturity_table();
    rows_against(config)
        .into_iter()
        .filter(|(_, value, _)| !STRUCTURAL.contains(&value.as_str()))
        .map(|(key, value, default)| {
            let source = if value == default {
                Source::Default
            } else {
                Source::File
            };
            let maturity = maturity
                .get(key.as_str())
                .copied()
                .unwrap_or(Maturity::Effective);
            Entry {
                key,
                value,
                default,
                source,
                maturity,
            }
        })
        .collect()
}

/// The `agentos-gateway config` body: one `key=value` line per key, annotated
/// where the annotation is worth the width.
///
/// Deliberately still one key per line rather than a table. It is grepped, and
/// it is diffed between two machines that are supposed to be configured the
/// same way — both of which a table would make worse.
pub fn report(config: &WorkspaceConfig) -> String {
    let entries = entries(config);
    let mut out = String::new();
    for entry in &entries {
        let _ = write!(out, "{}={}", entry.key, entry.value);
        // An effective key at its default needs no annotation; that is the
        // overwhelming majority of lines and the reason the file stays
        // readable.
        if entry.source == Source::File {
            let _ = write!(out, "  [set; default {}]", entry.default);
        }
        if entry.maturity != Maturity::Effective {
            let _ = write!(out, "  [{}]", entry.maturity.as_str());
        }
        out.push('\n');
    }

    let noteworthy: Vec<&Entry> = entries
        .iter()
        .filter(|entry| entry.is_noteworthy())
        .collect();
    if !noteworthy.is_empty() {
        out.push('\n');
        for entry in noteworthy {
            let _ = writeln!(
                out,
                "note: {} is set and is {}",
                entry.key,
                entry.maturity.as_str()
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_config_reports_every_key_as_default() {
        let entries = entries(&WorkspaceConfig::default());
        assert!(entries.len() > 80, "got {} keys", entries.len());
        assert!(
            !entries.iter().any(|entry| entry.value == "(table)"),
            "a table header is not a value an operator can be set to"
        );
        assert!(
            entries.iter().all(|entry| entry.source == Source::Default),
            "the default config cannot have set anything"
        );
        assert!(
            !entries.iter().any(Entry::is_noteworthy),
            "a default config sets nothing, so nothing is worth calling out"
        );
    }

    #[test]
    fn a_changed_key_is_reported_as_set_with_its_default_beside_it() {
        let mut config = WorkspaceConfig::default();
        config.agent.max_turns = 99;
        let entries = entries(&config);
        let entry = entries
            .iter()
            .find(|entry| entry.key == "agent.max_turns")
            .expect("the key is enumerated");
        assert_eq!(entry.source, Source::File);
        assert_eq!(entry.value, "`99`");
        assert_eq!(entry.default, "`16`");

        let report = report(&config);
        assert!(
            report.contains("agent.max_turns=`99`  [set; default `16`]"),
            "got: {report}"
        );
    }

    #[test]
    fn setting_a_deprecated_key_is_called_out() {
        let mut config = WorkspaceConfig::default();
        config.agent.memory = Some(std::sync::Arc::from("memory.sqlite"));
        let report = report(&config);
        assert!(report.contains("agent.memory="), "got: {report}");
        assert!(report.contains("[deprecated]"), "got: {report}");
        assert!(
            report.contains("note: agent.memory is set and is deprecated"),
            "got: {report}"
        );
    }

    #[test]
    fn turning_on_a_preview_capability_is_called_out() {
        let mut config = WorkspaceConfig::default();
        config.channels.provisional_streaming = true;
        let report = report(&config);
        assert!(
            report.contains("note: channels.provisional_streaming is set and is preview"),
            "got: {report}"
        );
    }

    /// The whole point of deriving this from the catalog walk: a key that
    /// exists in the structs cannot be missing from the diagnostic.
    #[test]
    fn every_catalogued_key_appears() {
        let keys: std::collections::BTreeSet<String> = entries(&WorkspaceConfig::default())
            .into_iter()
            .map(|entry| entry.key)
            .collect();
        for expected in [
            "agent.id",
            "memory.policy.writes",
            "channels.provisional_streaming",
            "spill.retention_days",
            "limits.tool_result_inline_bytes",
        ] {
            assert!(keys.contains(expected), "{expected} is missing");
        }
    }
}
