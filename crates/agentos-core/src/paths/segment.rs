//! One name, and nothing that could be a path.
//!
//! M4 / `FS-001`. Task ids, sub-agent names, orchestrator template names and
//! session ids all end up as a single component of a path on disk, and all of
//! them are chosen by something other than the operator — a model's plan, an
//! envelope from a channel, a config file. Before this they were joined
//! straight onto a root:
//!
//! ```text
//! self.task_dir(task_id).join("subagents").join(name)
//! ```
//!
//! `Path::join` treats an absolute argument as a *replacement* for the whole
//! left-hand side, so a `name` of `/etc/cron.d` does not land beneath the task
//! directory at all; a `name` of `../../..` walks out of it. Neither is exotic
//! input for a value a language model produced.
//!
//! [`path_segment`] refuses anything that is not exactly one ordinary name.
//! It is deliberately a *rejection* rather than an encoding: these are
//! identifiers something else chose and will use again to find what it wrote,
//! so quietly rewriting one would break the lookup rather than the attack.

use std::fmt;
use thiserror::Error;

/// Longest single filename most filesystems accept (`NAME_MAX`). A longer one
/// fails at the syscall anyway; refusing it here makes the message say which
/// identifier was at fault.
const MAX_SEGMENT_BYTES: usize = 255;

/// Why a value cannot be used as one component of a path.
///
/// Carries the `kind` — "task id", "sub-agent name" — because by the time this
/// surfaces in a log the reader has no other way to tell which of the four
/// identifiers in the call was the bad one.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PathSegmentError {
    #[error("{kind} is empty")]
    Empty { kind: &'static str },
    #[error("{kind} {value:?} is a relative-path marker, not a name")]
    RelativeMarker { kind: &'static str, value: String },
    #[error("{kind} {value:?} contains a path separator")]
    Separator { kind: &'static str, value: String },
    #[error("{kind} {value:?} contains a control character")]
    Control { kind: &'static str, value: String },
    #[error("{kind} is {len} bytes; at most {MAX_SEGMENT_BYTES} are usable as a filename")]
    TooLong { kind: &'static str, len: usize },
}

/// Accept `value` as exactly one ordinary path component, or say why not.
///
/// Rejects, in order: the empty string; `.` and `..`; anything containing `/`,
/// `\`, or `:`; anything containing a control character including NUL; and
/// anything over [`MAX_SEGMENT_BYTES`].
///
/// `\` and `:` are refused even though this runtime targets Unix, where
/// neither separates anything. A name that is harmless here and a traversal on
/// a Windows host is still a traversal — these identifiers travel, into
/// archives, into sync targets, into another machine's copy of the workspace —
/// and no caller has ever needed a backslash in a task id.
pub fn path_segment<'a>(kind: &'static str, value: &'a str) -> Result<&'a str, PathSegmentError> {
    if value.is_empty() {
        return Err(PathSegmentError::Empty { kind });
    }
    if value == "." || value == ".." {
        return Err(PathSegmentError::RelativeMarker {
            kind,
            value: value.to_owned(),
        });
    }
    if value.len() > MAX_SEGMENT_BYTES {
        return Err(PathSegmentError::TooLong {
            kind,
            len: value.len(),
        });
    }
    if value.contains(['/', '\\', ':']) {
        return Err(PathSegmentError::Separator {
            kind,
            value: value.to_owned(),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(PathSegmentError::Control {
            kind,
            value: value.to_owned(),
        });
    }
    Ok(value)
}

/// [`path_segment`] for a value that is rendered rather than borrowed — a
/// `TaskId`, a `SessionId`, anything whose `Display` is what reaches the path.
pub fn displayed_segment(
    kind: &'static str,
    value: &impl fmt::Display,
) -> Result<String, PathSegmentError> {
    let rendered = value.to_string();
    path_segment(kind, &rendered)?;
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_ordinary_name_is_accepted() {
        assert_eq!(path_segment("task id", "task-42"), Ok("task-42"));
        assert_eq!(path_segment("task id", "main"), Ok("main"));
        // Unicode is a name, not an attack; only separators and control
        // characters are.
        assert_eq!(path_segment("sub-agent name", "研究员"), Ok("研究员"));
    }

    /// The `join` semantics this exists for: an absolute argument replaces the
    /// root entirely, so `root.join("/etc/cron.d")` *is* `/etc/cron.d`.
    #[test]
    fn an_absolute_path_is_not_a_name() {
        assert!(matches!(
            path_segment("sub-agent name", "/etc/cron.d"),
            Err(PathSegmentError::Separator { .. })
        ));
    }

    #[test]
    fn traversal_in_any_shape_is_refused() {
        for value in ["..", "../..", "a/../b", "./x", "sessions/../../etc"] {
            assert!(
                path_segment("task id", value).is_err(),
                "{value:?} must not be usable as a single component"
            );
        }
    }

    /// A name that is one component here and two on a Windows host.
    #[test]
    fn a_backslash_is_refused_even_though_unix_allows_it() {
        assert!(matches!(
            path_segment("template name", r"..\..\windows"),
            Err(PathSegmentError::Separator { .. })
        ));
    }

    #[test]
    fn a_nul_or_newline_cannot_be_smuggled_into_a_filename() {
        assert!(matches!(
            path_segment("session id", "a\0b"),
            Err(PathSegmentError::Control { .. })
        ));
        assert!(matches!(
            path_segment("session id", "a\nb"),
            Err(PathSegmentError::Control { .. })
        ));
    }

    #[test]
    fn the_empty_string_names_nothing() {
        assert_eq!(
            path_segment("task id", ""),
            Err(PathSegmentError::Empty { kind: "task id" })
        );
    }

    #[test]
    fn a_name_longer_than_the_filesystem_allows_is_refused_by_name() {
        let long = "x".repeat(MAX_SEGMENT_BYTES + 1);
        let error = path_segment("task id", &long).expect_err("too long");
        // The point of catching it here rather than at the syscall: the
        // message says which identifier was wrong.
        assert!(error.to_string().contains("task id"), "{error}");
    }

    /// The error names the identifier, so a log line is actionable without
    /// reading the call site.
    #[test]
    fn the_error_says_which_identifier_was_at_fault() {
        let error = path_segment("sub-agent name", "../escape").expect_err("traversal");
        assert!(error.to_string().contains("sub-agent name"), "{error}");
    }
}
