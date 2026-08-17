use super::common::{elapsed_ms, result_metadata, safe_workspace_path, workspace_root};
use agentos_interfaces::tool::{SandboxMode, Tool, ToolError, ToolSpec};
use agentos_proto::{ToolCall, ToolResult, ToolStatus};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, value::RawValue};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DIRECTORY_LIST_LIMIT: usize = 200;
const DEFAULT_READ_MAX_BYTES: usize = 64 * 1024;
const MAX_READ_MAX_BYTES: usize = 256 * 1024;

#[derive(Default)]
pub struct FileTool;

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
                    "max_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 262144,
                        "description": "Maximum bytes to return for read operations. Defaults to 65536 and is capped at 262144."
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
            sandbox: SandboxMode::FullAccess,
            timeout_ms: None,
        }
    }

    async fn call(&self, call: &ToolCall, args: &RawValue) -> Result<ToolResult, ToolError> {
        let parsed: FileArgs = serde_json::from_str(args.get())
            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
        let start = Instant::now();
        let safe_path = safe_workspace_path(&workspace_root(), &parsed.path)
            .map_err(|err| ToolError::Failed(Arc::from(err)))?;
        match parsed.operation.as_str() {
            "read" => {
                if safe_path.is_dir() {
                    let listing = read_directory_listing(
                        &safe_path,
                        parsed.include_metadata.unwrap_or(false),
                        parsed.modified_within_hours,
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
                    .unwrap_or(DEFAULT_READ_MAX_BYTES)
                    .min(MAX_READ_MAX_BYTES);
                let (content, original_bytes, truncated) = read_file_slice(
                    &safe_path,
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
                // Create the parent chain so they don't get ENOENT on first
                // touch — they can always rm afterwards if it wasn't desired.
                if let Some(parent) = safe_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)
                            .map_err(|err| ToolError::Failed(err.to_string().into()))?;
                    }
                }
                std::fs::write(&safe_path, content.as_bytes())
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
    path: &Path,
    max_bytes: usize,
    offset: u64,
    tail: bool,
) -> Result<(String, u64, bool), ToolError> {
    let mut file =
        std::fs::File::open(path).map_err(|err| ToolError::Failed(err.to_string().into()))?;
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
    path: &PathBuf,
    include_metadata: bool,
    modified_within_hours: Option<u64>,
) -> Result<String, ToolError> {
    let modified_cutoff = modified_within_hours
        .map(|hours| SystemTime::now() - Duration::from_secs(hours.saturating_mul(60 * 60)));
    let mut entries = std::fs::read_dir(path)
        .map_err(|err| ToolError::Failed(err.to_string().into()))?
        .map(|entry| {
            let entry = entry.map_err(|err| ToolError::Failed(err.to_string().into()))?;
            let file_type = entry
                .file_type()
                .map_err(|err| ToolError::Failed(err.to_string().into()))?;
            let metadata = entry
                .metadata()
                .map_err(|err| ToolError::Failed(err.to_string().into()))?;
            if let Some(cutoff) = modified_cutoff {
                if file_type.is_file() {
                    let modified = metadata
                        .modified()
                        .map_err(|err| ToolError::Failed(err.to_string().into()))?;
                    if modified < cutoff {
                        return Ok(None);
                    }
                }
            }
            let suffix = if file_type.is_dir() { "/" } else { "" };
            let name = format!("{}{}", entry.file_name().to_string_lossy(), suffix);
            if !include_metadata {
                return Ok(Some(name));
            }
            let modified_secs = metadata
                .modified()
                .ok()
                .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs())
                .unwrap_or_default();
            Ok(Some(format!(
                "{name}\tmodified_unix={modified_secs}\tbytes={}",
                metadata.len()
            )))
        })
        .collect::<Result<Vec<_>, ToolError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    entries.sort();
    let truncated = entries.len() > DIRECTORY_LIST_LIMIT;
    entries.truncate(DIRECTORY_LIST_LIMIT);
    let mut listing = entries.join("\n");
    if truncated {
        if !listing.is_empty() {
            listing.push('\n');
        }
        listing.push_str("...");
    }
    Ok(listing)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_directory_listing_returns_sorted_entries_and_marks_dirs() {
        let dir = std::env::temp_dir().join(format!(
            "agentos-file-tool-listing-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(dir.join("b.txt"), b"b").unwrap();
        std::fs::write(dir.join("a.txt"), b"a").unwrap();

        let listing =
            read_directory_listing(&dir, false, None).expect("directory listing should succeed");

        assert_eq!(listing, "a.txt\nb.txt\nnested/");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_directory_listing_can_filter_recent_files_and_include_metadata() {
        let dir = std::env::temp_dir().join(format!(
            "agentos-file-tool-metadata-listing-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("recent.log"), b"recent").unwrap();

        let listing =
            read_directory_listing(&dir, true, Some(24)).expect("metadata listing should succeed");

        assert!(listing.contains("recent.log"));
        assert!(listing.contains("modified_unix="));
        assert!(listing.contains("bytes=6"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_file_slice_defaults_to_bounded_tail_or_range() {
        let path = std::env::temp_dir().join(format!(
            "agentos-file-tool-slice-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, b"0123456789abcdef").unwrap();

        let (head, file_bytes, truncated) =
            read_file_slice(&path, 4, 0, false).expect("head read should succeed");
        let (tail, _, tail_truncated) =
            read_file_slice(&path, 4, 0, true).expect("tail read should succeed");

        assert_eq!(file_bytes, 16);
        assert!(truncated);
        assert!(head.starts_with("0123"));
        assert!(head.contains("returned bytes 0..4 of 16"));
        assert!(tail_truncated);
        assert!(tail.contains("cdef"));
        assert!(tail.starts_with("[file read truncated: omitted first 12 of 16 bytes]"));

        let _ = std::fs::remove_file(path);
    }
}
