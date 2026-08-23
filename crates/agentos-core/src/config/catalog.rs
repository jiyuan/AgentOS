//! The config surface, derived from the config structs (roadmap item X4).
//!
//! This roadmap's own opening was the argument: two documented baselines the
//! tree contradicted. A catalog maintained by hand is a catalog that drifts,
//! and the drift is invisible — nothing fails when a new key goes undocumented
//! or a removed one keeps its entry.
//!
//! So the surface is *derived*, from two sources that cannot disagree with the
//! code because they are the code:
//!
//! - **Keys, types and nesting** come from the config structs' own source,
//!   `include_str!`-ed at compile time. Not read from disk at runtime: the
//!   catalog a binary produces has to describe the binary, not whatever tree it
//!   happens to be run from.
//! - **Defaults** come from serializing [`WorkspaceConfig::default`], so a
//!   default that changes changes the catalog.
//! - **Prose** comes from each field's own `///` doc comment, which is where it
//!   already lives. A field without one is a [`Problem`], not a blank cell —
//!   adding a config key and not saying what it does fails the check.
//!
//! # What it cannot see
//!
//! The parser reads Rust source with string matching rather than a syntax tree.
//! That is a deliberate trade for one dependency-free file: it handles the
//! shape every config struct in this crate is written in — `pub name: Type,`
//! preceded by `///` lines and optional `#[serde(...)]` attributes — and would
//! silently miss anything exotic. The guard is that missing docs are reported,
//! so the failure mode is a loud gap rather than a quiet one; and
//! `every_section_is_reachable` pins that every top-level key of the serialized
//! default is one the walk actually reached.

use super::WorkspaceConfig;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;

/// Config source, compiled in so the catalog describes this binary rather than
/// whatever checkout it is run from.
const SOURCES: &[&str] = &[
    include_str!("mod.rs"),
    include_str!("approval.rs"),
    include_str!("compaction.rs"),
    include_str!("gateway.rs"),
    include_str!("jobs.rs"),
    include_str!("limits.rs"),
    include_str!("mcp.rs"),
    include_str!("memory.rs"),
    include_str!("orchestrator.rs"),
    include_str!("policy.rs"),
    include_str!("retention.rs"),
    include_str!("spill.rs"),
    include_str!("subagents.rs"),
];

/// Keys whose fields have no doc comment yet, and which the check therefore
/// tolerates. A ratchet in both directions — see the file's own header.
const UNDOCUMENTED: &str = include_str!("undocumented.txt");

/// Keys that are deprecated or preview rather than plainly effective. See the
/// file's own header for why the list exists and what is not on it.
const MATURITY: &str = include_str!("maturity.txt");

/// The maturity of every key that is not plainly effective.
pub(super) fn maturity_table() -> BTreeMap<&'static str, super::effective::Maturity> {
    use super::effective::Maturity;
    MATURITY
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (kind, key) = line.split_once(char::is_whitespace)?;
            let maturity = match kind {
                "deprecated" => Maturity::Deprecated,
                "preview" => Maturity::Preview,
                // Unreadable lines are dropped rather than guessed at; the
                // test below fails on anything this cannot parse.
                _ => return None,
            };
            Some((key.trim(), maturity))
        })
        .collect()
}

/// Every key paired with `(value, default)` for this config.
///
/// The same walk `config_markdown` uses, run twice — once against the loaded
/// config and once against the default — so the effective-config diagnostic
/// cannot enumerate a different set of keys from the catalog.
pub(super) fn rows_against(config: &WorkspaceConfig) -> Vec<(String, String, String)> {
    let parsed = structs();
    let allowed = undocumented();
    let render = |value: &WorkspaceConfig| {
        let serialized = serde_json::to_value(value)
            .expect("the workspace config serializes; every field derives Serialize");
        let mut rows = Vec::new();
        let mut problems = Vec::new();
        walk(
            "WorkspaceConfig",
            "",
            Some(&serialized),
            &parsed,
            &allowed,
            &mut rows,
            &mut problems,
            0,
        );
        rows
    };
    let effective = render(config);
    let defaults: BTreeMap<String, String> = render(&WorkspaceConfig::default())
        .into_iter()
        .map(|row| (row.key, row.default))
        .collect();
    effective
        .into_iter()
        .map(|row| {
            let default = defaults.get(&row.key).cloned().unwrap_or_default();
            (row.key, row.default, default)
        })
        .collect()
}

/// Markers the generated block sits between, so a file can carry prose the
/// generator does not own.
pub const BEGIN_MARKER: &str = "<!-- BEGIN GENERATED: config -->";
pub const END_MARKER: &str = "<!-- END GENERATED: config -->";

/// Something the catalog could not derive. Reported rather than rendered as a
/// blank, because a blank reads as "this key does nothing".
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Problem {
    pub key: String,
    pub detail: &'static str,
}

/// The acknowledged documentation debt, as a set.
fn undocumented() -> std::collections::BTreeSet<&'static str> {
    UNDOCUMENTED
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.key, self.detail)
    }
}

/// One field of one config struct, as the source declares it.
#[derive(Clone, Debug)]
struct Field {
    name: String,
    type_name: String,
    doc: String,
}

/// Every config struct in this crate, by name.
fn structs() -> BTreeMap<String, Vec<Field>> {
    let mut parsed = BTreeMap::new();
    for source in SOURCES {
        parse_structs(source, &mut parsed);
    }
    parsed
}

/// Pull `pub struct Name { ... }` blocks and their `pub` fields out of one
/// source file.
fn parse_structs(source: &str, into: &mut BTreeMap<String, Vec<Field>>) {
    let mut lines = source.lines().peekable();
    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("pub struct ") else {
            continue;
        };
        // `pub struct Name {` — tuple and unit structs carry no keys.
        let Some(name) = rest.strip_suffix(" {") else {
            continue;
        };
        let mut fields = Vec::new();
        let mut doc = String::new();
        for body in lines.by_ref() {
            let body = body.trim();
            if body == "}" {
                break;
            }
            if let Some(text) = body.strip_prefix("///") {
                if !doc.is_empty() {
                    doc.push(' ');
                }
                doc.push_str(text.trim());
                continue;
            }
            if body.starts_with("#[") || body.is_empty() || body.starts_with("//") {
                // Attributes and blank lines sit between a doc and its field
                // without ending either.
                continue;
            }
            if let Some(field) = body.strip_prefix("pub ") {
                if let Some((field_name, type_name)) = field.split_once(": ") {
                    fields.push(Field {
                        name: field_name.trim().to_owned(),
                        type_name: type_name.trim_end_matches(',').trim().to_owned(),
                        doc: std::mem::take(&mut doc),
                    });
                    continue;
                }
            }
            // Anything else (a private field, a nested item) ends the run of
            // doc lines so it cannot be attached to the wrong field.
            doc.clear();
        }
        into.insert(name.to_owned(), fields);
    }
}

/// A rendered row: the dotted key, its type, its default, and what it does.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Row {
    key: String,
    type_name: String,
    default: String,
    doc: String,
}

/// Walk the config structs from `WorkspaceConfig`, pairing each key with the
/// default the serialized default config carries at that path.
fn rows() -> (Vec<Row>, Vec<Problem>) {
    let parsed = structs();
    let defaults = serde_json::to_value(WorkspaceConfig::default())
        .expect("the workspace config serializes; every field derives Serialize");
    let mut rows = Vec::new();
    let mut problems = Vec::new();
    walk(
        "WorkspaceConfig",
        "",
        Some(&defaults),
        &parsed,
        &undocumented(),
        &mut rows,
        &mut problems,
        0,
    );
    (rows, problems)
}

/// Depth bound. The config tree is three levels at most; anything deeper is a
/// cycle, which would otherwise recurse forever.
const MAX_DEPTH: usize = 6;

#[allow(clippy::too_many_arguments)]
fn walk(
    struct_name: &str,
    prefix: &str,
    defaults: Option<&Value>,
    parsed: &BTreeMap<String, Vec<Field>>,
    allowed: &std::collections::BTreeSet<&'static str>,
    rows: &mut Vec<Row>,
    problems: &mut Vec<Problem>,
    depth: usize,
) {
    if depth >= MAX_DEPTH {
        return;
    }
    let Some(fields) = parsed.get(struct_name) else {
        return;
    };
    for field in fields {
        let key = if prefix.is_empty() {
            field.name.clone()
        } else {
            format!("{prefix}.{}", field.name)
        };
        let value = defaults.and_then(|value| value.get(&field.name));
        let nested = nested_struct(&field.type_name);

        match (field.doc.trim().is_empty(), allowed.contains(key.as_str())) {
            // A new key with no doc: the catalog refuses to invent one.
            (true, false) => problems.push(Problem {
                key: key.clone(),
                detail: "has no doc comment; add one, or the catalog cannot describe it",
            }),
            // A listed key that has since been documented: the list has to
            // shrink, or it stops meaning anything.
            (false, true) => problems.push(Problem {
                key: key.clone(),
                detail: "now has a doc comment; remove it from config/undocumented.txt",
            }),
            _ => {}
        }

        match nested.filter(|name| parsed.contains_key(*name)) {
            // A nested table: describe it, then descend. An array of records
            // has no default to show — the default is "none of them".
            Some(inner) => {
                rows.push(Row {
                    key: key.clone(),
                    type_name: field.type_name.clone(),
                    default: if field.type_name.starts_with("Vec<") {
                        "(none)".to_owned()
                    } else {
                        "(table)".to_owned()
                    },
                    doc: field.doc.clone(),
                });
                let inner_defaults = if field.type_name.starts_with("Vec<") {
                    None
                } else {
                    value
                };
                walk(
                    inner,
                    &key,
                    inner_defaults,
                    parsed,
                    allowed,
                    rows,
                    problems,
                    depth + 1,
                );
            }
            None => rows.push(Row {
                key,
                type_name: field.type_name.clone(),
                default: value.map_or_else(|| "—".to_owned(), render_default),
                doc: field.doc.clone(),
            }),
        }
    }
}

/// The config struct a field's type refers to, if any: `LimitsConfig`,
/// `Vec<SubAgentConfig>`, `Option<MemoryConfig>`.
fn nested_struct(type_name: &str) -> Option<&str> {
    let inner = type_name
        .strip_prefix("Vec<")
        .or_else(|| type_name.strip_prefix("Option<"))
        .map(|rest| rest.trim_end_matches('>'))
        .unwrap_or(type_name);
    inner.ends_with("Config").then_some(inner)
}

fn render_default(value: &Value) -> String {
    match value {
        Value::String(text) if text.is_empty() => "\"\"".to_owned(),
        Value::String(text) => format!("`{text}`"),
        Value::Null => "(unset)".to_owned(),
        Value::Array(items) if items.is_empty() => "(empty)".to_owned(),
        Value::Object(fields) if fields.is_empty() => "(empty)".to_owned(),
        other => format!("`{other}`"),
    }
}

/// Render the config catalog, or the reasons it could not be rendered.
///
/// Problems are returned rather than rendered so a caller can fail the build:
/// a catalog with a blank description is worse than no catalog, because it
/// looks complete.
pub fn config_markdown() -> Result<String, Vec<Problem>> {
    let (rows, problems) = rows();
    if !problems.is_empty() {
        return Err(problems);
    }
    let mut out = String::new();
    out.push_str(
        "<!-- Generated by `agentos-gateway catalog`. Do not edit by hand: run that \
         instead. -->\n\n| Key | Type | Default | Description |\n|---|---|---|---|\n",
    );
    let mut undescribed = 0;
    for row in &rows {
        if row.doc.trim().is_empty() {
            undescribed += 1;
        }
        let _ = writeln!(
            out,
            "| `{}` | `{}` | {} | {} |",
            row.key,
            row.type_name,
            row.default,
            if row.doc.trim().is_empty() {
                "*(undocumented — see `config/undocumented.txt`)*".to_owned()
            } else {
                escape_cell(&row.doc)
            }
        );
    }
    if undescribed > 0 {
        // Counted in the file itself: a debt nobody can see is a debt nobody
        // pays.
        let _ = write!(
            out,
            "\n{undescribed} of {} keys have no description yet. They are listed in \
             `crates/agentos-core/src/config/undocumented.txt`; writing the doc comment on \
             the field is what removes a line from it.\n",
            rows.len()
        );
    }
    Ok(out)
}

/// Render the tool catalog from the specs a registry actually holds.
///
/// From `ToolSpec`s rather than a written list, for the same reason as the
/// config side: a tool whose deadline or sandbox mode changed would otherwise
/// keep its old row, and the row is what an operator reads before deciding
/// whether to allowlist it.
pub fn tool_markdown(specs: &[agentos_interfaces::tool::ToolSpec]) -> String {
    let mut sorted: Vec<&agentos_interfaces::tool::ToolSpec> = specs.iter().collect();
    sorted.sort_by(|left, right| left.name.cmp(&right.name));
    let mut out = String::new();
    out.push_str(
        "<!-- Generated by `agentos-gateway catalog`. Do not edit by hand: run that \
        instead. -->\n\n| Tool | Side effect | Persistence scope | Sandbox | Deadline | Description |\n|---|---|---|---|---|---|\n",
    );
    for spec in sorted {
        let _ = writeln!(
            out,
            "| `{}` | `{}` | `{}` | `{}` | {} | {} |",
            spec.name,
            spec.safety.side_effect.as_str(),
            spec.safety.persistence_scope.as_str(),
            spec.sandbox.as_str(),
            spec.timeout_ms.map_or_else(
                || "*(deployment default)*".to_owned(),
                |ms| format!("`{ms} ms`")
            ),
            escape_cell(spec.description.as_ref())
        );
    }
    out
}

/// Markers for the tool block, so both catalogs can share one file if a
/// deployment wants them together.
pub const TOOL_BEGIN_MARKER: &str = "<!-- BEGIN GENERATED: tools -->";
pub const TOOL_END_MARKER: &str = "<!-- END GENERATED: tools -->";

/// Replace the text between `begin` and `end` in `document`, leaving
/// everything outside the markers alone.
///
/// Markers rather than whole-file generation so a catalog can carry prose the
/// generator does not own — an intro, a note about a deprecated key — without
/// that prose being wiped on the next run.
pub fn splice(document: &str, begin: &str, end: &str, body: &str) -> Result<String, String> {
    let Some(start) = document.find(begin) else {
        return Err(format!("missing marker: {begin}"));
    };
    let Some(finish) = document.find(end) else {
        return Err(format!("missing marker: {end}"));
    };
    if finish < start {
        return Err(format!("markers out of order: {end} precedes {begin}"));
    }
    Ok(format!(
        "{}{begin}\n{body}{}",
        &document[..start],
        &document[finish..]
    ))
}

/// A pipe in a doc comment would end the table cell early and shift every
/// column after it.
fn escape_cell(text: &str) -> String {
    text.replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole point: a key that exists in the struct appears in the
    /// catalog. If this can be made to fail by adding a field, the catalog is
    /// derived.
    #[test]
    fn every_bounded_section_reaches_the_catalog() {
        let markdown = config_markdown().expect("every config key is documented");
        for key in [
            "limits.tool_result_inline_bytes",
            "limits.directory_list_entries",
            "limits.tool_output_bytes",
            "compaction.pressure_percent",
            "compaction.model",
            "spill.root",
            "spill.retention_days",
            "gateway.shards",
            "approval.expiry_seconds",
            "jobs.max_concurrent",
            "memory.hydrate_max_fragments",
        ] {
            assert!(markdown.contains(&format!("`{key}`")), "missing {key}");
        }
    }

    /// Defaults are read from the serialized default config, not written out,
    /// so changing one changes the catalog.
    #[test]
    fn defaults_come_from_the_default_config() {
        let markdown = config_markdown().expect("every config key is documented");
        assert!(
            markdown.contains(&format!(
                "`{}`",
                WorkspaceConfig::default().limits.tool_timeout_ms
            )),
            "the tool timeout default should appear as written in the struct"
        );
    }

    /// A section the walk never reached would be a silently missing chunk of
    /// the surface — exactly the drift this item exists to stop.
    #[test]
    fn every_section_is_reachable() {
        let markdown = config_markdown().expect("every config key is documented");
        let defaults = serde_json::to_value(WorkspaceConfig::default()).expect("serializes");
        let sections = defaults.as_object().expect("the config is a table");
        for section in sections.keys() {
            assert!(
                markdown.contains(&format!("| `{section}`")),
                "section '{section}' never reached the catalog"
            );
        }
    }

    /// A field with no doc comment is reported, not rendered blank.
    #[test]
    fn an_undocumented_field_is_a_problem_not_a_blank_cell() {
        let mut parsed = BTreeMap::new();
        parse_structs(
            "pub struct SampleConfig {\n    /// Documented.\n    pub kept: usize,\n    pub bare: usize,\n}\n",
            &mut parsed,
        );
        let fields = parsed.get("SampleConfig").expect("the struct parses");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].doc, "Documented.");
        assert!(fields[1].doc.is_empty());
    }

    /// Attributes and blank lines sit between a doc comment and its field
    /// without breaking the pairing — which is how every bounded section in
    /// this crate is written.
    #[test]
    fn attributes_do_not_detach_a_doc_from_its_field() {
        let mut parsed = BTreeMap::new();
        parse_structs(
            "pub struct SampleConfig {\n    /// First line.\n    /// Second line.\n    #[serde(default)]\n    pub value: usize,\n}\n",
            &mut parsed,
        );
        let fields = parsed.get("SampleConfig").expect("the struct parses");
        assert_eq!(fields[0].doc, "First line. Second line.");
        assert_eq!(fields[0].type_name, "usize");
    }

    /// A pipe in a doc comment would otherwise end its table cell early and
    /// shift every column after it.
    #[test]
    fn a_pipe_in_a_doc_comment_cannot_break_the_table() {
        assert_eq!(escape_cell("a | b"), "a \\| b");
    }

    /// The tool table is built from specs, so a changed deadline or sandbox
    /// mode changes the row rather than leaving a stale one.
    #[test]
    fn the_tool_catalog_reports_what_the_spec_says() {
        let spec = agentos_interfaces::tool::ToolSpec {
            name: std::sync::Arc::from("shell"),
            description: std::sync::Arc::from("Runs a | command"),
            input_schema: serde_json::json!({}),
            safety: Default::default(),
            sandbox: agentos_interfaces::tool::SandboxMode::WorkspaceWrite,
            timeout_ms: Some(300_000),
        };
        let markdown = tool_markdown(&[spec]);
        assert!(markdown.contains(
            "| `shell` | `unspecified` | `unspecified` | `workspace_write` | `300000 ms` |"
        ));
        // And a pipe in a description cannot shift the columns after it.
        assert!(markdown.contains("Runs a \\| command"));
    }

    /// A tool that declares no deadline says so, rather than showing a number
    /// it does not have.
    #[test]
    fn a_tool_without_its_own_deadline_says_which_one_applies() {
        let spec = agentos_interfaces::tool::ToolSpec {
            name: std::sync::Arc::from("file"),
            description: std::sync::Arc::from("Reads."),
            input_schema: serde_json::json!({}),
            safety: Default::default(),
            sandbox: agentos_interfaces::tool::SandboxMode::FullAccess,
            timeout_ms: None,
        };
        assert!(tool_markdown(&[spec]).contains("*(deployment default)*"));
    }

    /// Splicing leaves prose outside the markers alone: a catalog that wiped
    /// its own introduction on every run would not be kept.
    #[test]
    fn splicing_replaces_only_what_is_between_the_markers() {
        let document = format!("intro\n{BEGIN_MARKER}\nstale\n{END_MARKER}\noutro\n");
        let spliced = splice(&document, BEGIN_MARKER, END_MARKER, "fresh\n").expect("splices");
        assert!(spliced.starts_with("intro\n"));
        assert!(spliced.ends_with("outro\n"));
        assert!(spliced.contains("fresh"));
        assert!(!spliced.contains("stale"));
    }

    /// A missing marker is an error, not a silent no-op that would leave the
    /// catalog stale while the command reported success.
    #[test]
    fn a_missing_marker_is_an_error() {
        assert!(splice("no markers here", BEGIN_MARKER, END_MARKER, "body").is_err());
        assert!(splice(
            &format!("{END_MARKER}\n{BEGIN_MARKER}"),
            BEGIN_MARKER,
            END_MARKER,
            "body"
        )
        .is_err());
    }

    #[test]
    fn nested_config_types_are_recognised_through_vec_and_option() {
        assert_eq!(nested_struct("LimitsConfig"), Some("LimitsConfig"));
        assert_eq!(nested_struct("Vec<SubAgentConfig>"), Some("SubAgentConfig"));
        assert_eq!(nested_struct("Option<MemoryConfig>"), Some("MemoryConfig"));
        assert_eq!(nested_struct("usize"), None);
        assert_eq!(nested_struct("BTreeMap<Arc<str>, u64>"), None);
    }
}

#[cfg(test)]
mod maturity_tests {
    use super::*;

    /// Every line parses, so a typo is not silently dropped into "effective".
    #[test]
    fn the_maturity_list_is_wholly_readable() {
        let declared = MATURITY
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .count();
        assert_eq!(
            declared,
            maturity_table().len(),
            "a line in maturity.txt did not parse as `<deprecated|preview> <key>`"
        );
    }

    /// A key that no longer exists cannot stay listed, and every key that is
    /// listed has to be one the config actually accepts.
    #[test]
    fn every_listed_key_still_exists() {
        let keys: std::collections::BTreeSet<String> = rows_against(&WorkspaceConfig::default())
            .into_iter()
            .map(|(key, _, _)| key)
            .collect();
        for key in maturity_table().keys() {
            assert!(
                keys.contains(*key),
                "maturity.txt lists '{key}', which the config no longer has"
            );
        }
    }

    /// A deprecation announced in a doc comment has to be in the list too, so
    /// the diagnostic and the catalog cannot disagree about which keys are on
    /// their way out.
    #[test]
    fn a_doc_comment_cannot_deprecate_a_key_on_its_own() {
        let table = maturity_table();
        let (rows, problems) = rows();
        assert!(problems.is_empty(), "{problems:?}");
        for row in rows {
            if !row.doc.contains("**Deprecated.**") {
                continue;
            }
            assert_eq!(
                table.get(row.key.as_str()).copied(),
                Some(super::super::effective::Maturity::Deprecated),
                "'{}' says it is deprecated but maturity.txt does not list it",
                row.key
            );
        }
    }
}
