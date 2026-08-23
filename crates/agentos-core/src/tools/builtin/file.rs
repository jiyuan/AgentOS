use super::common::{elapsed_ms, result_metadata, workspace_root};
use crate::paths::{DirEntry, RootDir};
use agentos_interfaces::tool::{
    SandboxMode, Tool, ToolError, ToolPersistenceScope, ToolSafety, ToolSideEffect, ToolSpec,
};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Entries one `list` call shows before truncating.
///
/// A cap on the turn, not on the directory: a listing of ten thousand names is
/// tokens spent on nothing a model can act on.
pub const DEFAULT_DIRECTORY_LIST_ENTRIES: usize = 200;
/// Bytes one `read` call returns when it does not ask for a size.
pub const DEFAULT_FILE_READ_BYTES: usize = 64 * 1024;
/// Bytes one `read` call returns however much it asks for.
pub const MAX_FILE_READ_BYTES: usize = 256 * 1024;

/// Read and listing bounds this tool works to (roadmap item X3).
///
/// Carried on the tool rather than read from a global, so a sub-agent's
/// registry and the parent's can differ and neither has to consult anything at
/// call time.
pub struct FileTool {
    directory_list_entries: usize,
    read_bytes: usize,
    read_max_bytes: usize,
}

impl Default for FileTool {
    fn default() -> Self {
        Self {
            directory_list_entries: DEFAULT_DIRECTORY_LIST_ENTRIES,
            read_bytes: DEFAULT_FILE_READ_BYTES,
            read_max_bytes: MAX_FILE_READ_BYTES,
        }
    }
}

impl FileTool {
    /// The tool bounded by a deployment's `[limits]`.
    pub fn with_limits(
        directory_list_entries: usize,
        read_bytes: usize,
        read_max_bytes: usize,
    ) -> Self {
        Self {
            directory_list_entries,
            read_bytes,
            read_max_bytes,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileArgs {
    operation: String,
    path: PathBuf,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    max_bytes: Option<usize>,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    tail: Option<bool>,
    #[serde(default)]
    include_metadata: Option<bool>,
    #[serde(default)]
    modified_within_hours: Option<u64>,
}

#[async_trait]
impl Tool for FileTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: Arc::from("file"),
            description: Arc::from("Read or write a UTF-8 file."),
            input_schema: json!({
                "type": "object",
                "required": ["operation", "path"],
                "properties": {
                    "operation": { "type": "string", "enum": ["read", "write"] },
                    "path": { "type": "string" },
                    "content": { "type": "string" },
                    // Built from the configured bounds, not written out: a
                    // schema that advertised a ceiling the tool no longer
                    // enforces would have the model asking for reads it cannot
                    // get, or not asking for ones it could.
                    "max_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": self.read_max_bytes,
                        "description": format!(
                            "Maximum bytes to return for read operations. Defaults to {} and is capped at {}.",
                            self.read_bytes, self.read_max_bytes
                        )
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Byte offset for read operations. Ignored when tail is true."
                    },
                    "tail": {
                        "type": "boolean",
                        "description": "When true, return the final max_bytes bytes of the file."
                    },
                    "include_metadata": {
                        "type": "boolean",
                        "description": "When reading a directory, include entry size and last modification time."
                    },
                    "modified_within_hours": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "When reading a directory, include only files modified within this many hours. Directories are still listed."
                    }
                }
            }),
            safety: ToolSafety::new(
                ToolSideEffect::PersistentMutation,
                ToolPersistenceScope::Workspace,
            ),
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let parsed: FileArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let start = Instant::now();
        // M4 / `FS-001`: containment is the kernel's, not a string check's.
        // Every open below happens against a descriptor for a directory this
        // root walked to with `O_NOFOLLOW`, so no symlink — planted before the
        // call or swapped in during it — can move where the operation lands.
        let root = RootDir::open(workspace_root())
            .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
        match parsed.operation.as_str() {
            "read" => {
                if root.is_dir(&parsed.path) {
                    let listing = read_directory_listing(
                        &root,
                        &parsed.path,
                        parsed.include_metadata.unwrap_or(false),
                        parsed.modified_within_hours,
                        self.directory_list_entries,
                    )?;
                    let bytes_out = listing.len() as u64;
                    return Ok(ToolResult {
                        call_id: call.id.clone(),
                        status: ToolStatus::Succeeded,
                        content: Arc::from(listing),
                        metadata: result_metadata(elapsed_ms(start), bytes_out),
                    });
                }
                let max_bytes = parsed
                    .max_bytes
                    .unwrap_or(self.read_bytes)
                    .min(self.read_max_bytes);
                let file = root
                    .open_file(&parsed.path)
                    .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
                let (content, original_bytes, truncated) = read_file_slice(
                    file,
                    max_bytes,
                    parsed.offset.unwrap_or(0),
                    parsed.tail.unwrap_or(false),
                )?;
                let bytes_out = content.len() as u64;
                let mut metadata = result_metadata(elapsed_ms(start), bytes_out);
                metadata.insert(
                    Arc::from("file_bytes"),
                    serde_json::Value::from(original_bytes),
                );
                metadata.insert(Arc::from("truncated"), serde_json::Value::Bool(truncated));
                Ok(ToolResult {
                    call_id: call.id.clone(),
                    status: ToolStatus::Succeeded,
                    content: Arc::from(content),
                    metadata,
                })
            }
            "write" => {
                let content = parsed.content.unwrap_or_default();
                // Models routinely request writes into directories that
                // don't exist yet (e.g. `workspace/skills/rss-digest/SKILL.md`).
                // `create_file` creates the parent chain so they don't get
                // ENOENT on first touch — through the same no-follow walk, so
                // a link planted in the middle of a not-yet-existing path
                // fails rather than deciding where the new file lands.
                let mut file = root
                    .create_file(&parsed.path)
                    .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
                file.write_all(content.as_bytes())
                    .map_err(|err| ToolError::Failed(err.to_string().into()))?;
                let message = format!("wrote {} bytes", content.len());
                Ok(ToolResult {
                    call_id: call.id.clone(),
                    status: ToolStatus::Succeeded,
                    content: Arc::from(message),
                    metadata: result_metadata(elapsed_ms(start), content.len() as u64),
                })
            }
            operation => Err(ToolError::Failed(
                format!("unsupported file operation: {operation}").into(),
            )),
        }
    }
}

fn read_file_slice(
    mut file: File,
    max_bytes: usize,
    offset: u64,
    tail: bool,
) -> Result<(String, u64, bool), ToolError> {
    let file_len = file
        .metadata()
        .map_err(|err| ToolError::Failed(err.to_string().into()))?
        .len();
    let start = if tail {
        file_len.saturating_sub(max_bytes as u64)
    } else {
        offset.min(file_len)
    };
    file.seek(SeekFrom::Start(start))
        .map_err(|err| ToolError::Failed(err.to_string().into()))?;

    let remaining = file_len.saturating_sub(start) as usize;
    let read_len = remaining.min(max_bytes);
    let mut buf = vec![0; read_len];
    file.read_exact(&mut buf)
        .map_err(|err| ToolError::Failed(err.to_string().into()))?;

    let prefix_truncated = start > 0;
    let suffix_truncated = start + (read_len as u64) < file_len;
    let mut content = String::from_utf8_lossy(&buf).into_owned();
    if prefix_truncated {
        content.insert_str(
            0,
            &format!("[file read truncated: omitted first {start} of {file_len} bytes]\n"),
        );
    }
    if suffix_truncated {
        if !content.ends_with('\n') {
            content.push('\n');
        }
        content.push_str(&format!(
            "[file read truncated: returned bytes {}..{} of {}; use offset, tail, or a smaller file to continue]",
            start,
            start + read_len as u64,
            file_len
        ));
    }

    Ok((content, file_len, prefix_truncated || suffix_truncated))
}

fn read_directory_listing(
    root: &RootDir,
    path: &PathBuf,
    include_metadata: bool,
    modified_within_hours: Option<u64>,
    limit: usize,
) -> Result<String, ToolError> {
    let modified_cutoff = modified_within_hours
        .map(|hours| SystemTime::now() - Duration::from_secs(hours.saturating_mul(60 * 60)));
    let listed = root
        .read_dir(path)
        .map_err(|err| ToolError::Failed(Arc::from(err.to_string())))?;
    let mut entries = listed
        .into_iter()
        .filter(|entry| within_cutoff(entry, modified_cutoff))
        .map(|entry| render_entry(&entry, include_metadata))
        .collect::<Vec<_>>();
    entries.sort();
    let truncated = entries.len() > limit;
    entries.truncate(limit);
    let mut listing = entries.join("\n");
    if truncated {
        if !listing.is_empty() {
            listing.push('\n');
        }
        listing.push_str("...");
    }
    Ok(listing)
}

/// Whether an entry survives a `modified_within_hours` filter.
///
/// Only files are filtered: a directory is a place to look next, and hiding
/// one because it has not been touched recently would hide everything under
/// it too. An entry whose modification time could not be read is kept —
/// listing it without a timestamp says more than silently dropping it.
fn within_cutoff(entry: &DirEntry, cutoff: Option<SystemTime>) -> bool {
    let Some(cutoff) = cutoff else {
        return true;
    };
    if entry.is_dir {
        return true;
    }
    entry.modified.is_none_or(|modified| modified >= cutoff)
}

/// One listing line.
///
/// `/` marks a directory and `@` a symlink, as `ls -F` does. The `@` is not
/// decoration: a link is listed because it is there, and refused if read —
/// showing it without saying what it is would make the refusal look arbitrary.
fn render_entry(entry: &DirEntry, include_metadata: bool) -> String {
    let suffix = match (entry.is_dir, entry.is_symlink) {
        (_, true) => "@",
        (true, _) => "/",
        _ => "",
    };
    let name = format!("{}{suffix}", entry.name);
    if !include_metadata {
        return name;
    }
    let modified_secs = entry
        .modified
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    format!("{name}\tmodified_unix={modified_secs}\tbytes={}", entry.len)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch tree plus a root opened on it, which is how every listing and
    /// read below reaches the filesystem now.
    fn fixture(label: &str) -> (PathBuf, RootDir) {
        let dir = std::env::temp_dir().join(format!(
            "agentos-file-tool-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("fixture dir creates");
        let root = RootDir::open(&dir).expect("root opens");
        (dir, root)
    }

    #[test]
    fn read_directory_listing_returns_sorted_entries_and_marks_dirs() {
        let (dir, root) = fixture("listing");
        std::fs::create_dir_all(dir.join("here/nested")).unwrap();
        std::fs::write(dir.join("here/b.txt"), b"b").unwrap();
        std::fs::write(dir.join("here/a.txt"), b"a").unwrap();

        let listing = read_directory_listing(
            &root,
            &PathBuf::from("here"),
            false,
            None,
            DEFAULT_DIRECTORY_LIST_ENTRIES,
        )
        .expect("directory listing should succeed");

        assert_eq!(listing, "a.txt\nb.txt\nnested/");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A symlink is listed, marked, and — as the containment tests show —
    /// refused if read. Hiding it would make the refusal look arbitrary.
    #[test]
    fn a_symlink_is_listed_with_a_marker_rather_than_as_its_target() {
        let (dir, root) = fixture("listing-link");
        std::fs::create_dir_all(dir.join("here")).unwrap();
        std::fs::write(dir.join("here/real.txt"), b"real").unwrap();
        std::os::unix::fs::symlink("/etc", dir.join("here/elsewhere")).unwrap();

        let listing = read_directory_listing(
            &root,
            &PathBuf::from("here"),
            false,
            None,
            DEFAULT_DIRECTORY_LIST_ENTRIES,
        )
        .expect("listing succeeds");

        assert_eq!(listing, "elsewhere@\nreal.txt");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_directory_listing_can_filter_recent_files_and_include_metadata() {
        let (dir, root) = fixture("metadata-listing");
        std::fs::create_dir_all(dir.join("here")).unwrap();
        std::fs::write(dir.join("here/recent.log"), b"recent").unwrap();

        let listing = read_directory_listing(
            &root,
            &PathBuf::from("here"),
            true,
            Some(24),
            DEFAULT_DIRECTORY_LIST_ENTRIES,
        )
        .expect("metadata listing should succeed");

        assert!(listing.contains("recent.log"));
        assert!(listing.contains("modified_unix="));
        assert!(listing.contains("bytes=6"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_file_slice_defaults_to_bounded_tail_or_range() {
        let (dir, root) = fixture("slice");
        std::fs::write(dir.join("data.bin"), b"0123456789abcdef").unwrap();

        let (head, file_bytes, truncated) =
            read_file_slice(root.open_file("data.bin").unwrap(), 4, 0, false)
                .expect("head read should succeed");
        let (tail, _, tail_truncated) =
            read_file_slice(root.open_file("data.bin").unwrap(), 4, 0, true)
                .expect("tail read should succeed");

        assert_eq!(file_bytes, 16);
        assert!(truncated);
        assert!(head.starts_with("0123"));
        assert!(head.contains("returned bytes 0..4 of 16"));
        assert!(tail_truncated);
        assert!(tail.contains("cdef"));
        assert!(tail.starts_with("[file read truncated: omitted first 12 of 16 bytes]"));

        let _ = std::fs::remove_dir_all(dir);
    }

    /// The configured cap is what truncates, not the compiled-in default —
    /// otherwise `[limits].directory_list_entries` would be a key that reads
    /// back correctly and changes nothing.
    #[test]
    fn a_configured_listing_cap_is_what_truncates() {
        let (dir, root) = fixture("listing-cap");
        std::fs::create_dir_all(dir.join("here")).unwrap();
        for index in 0..5 {
            std::fs::write(dir.join(format!("here/entry-{index}")), b"x").expect("entry writes");
        }

        let listing = read_directory_listing(&root, &PathBuf::from("here"), false, None, 2)
            .expect("listing succeeds");
        let lines: Vec<&str> = listing.lines().collect();
        // Two entries plus the truncation marker.
        assert_eq!(lines.len(), 3, "got: {listing}");
        assert_eq!(lines[2], "...");

        let full = read_directory_listing(&root, &PathBuf::from("here"), false, None, 10)
            .expect("listing succeeds");
        assert_eq!(full.lines().count(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
