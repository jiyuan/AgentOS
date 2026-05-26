//! Filesystem layout for inbound channel attachments.
//!
//! Channels download user-supplied files into an injected attachments root using
//! `<channel>/<conversation>/<message_id>/<name>` beneath that root, so
//! downstream orchestrators and tools can read them without needing
//! channel-specific auth.
//!
//! Production callers should pass the root chosen by their runtime path layer.
//! `from_env()` derives the root from `AGENTOS_HOME` joined with
//! `workspace/attachments`, matching the layout the runtime sets up.

use agentos_interfaces::ChannelError;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct AttachmentStore {
    root: PathBuf,
    channel: String,
}

impl AttachmentStore {
    pub(crate) fn from_env(channel: &str) -> Self {
        let root = agentos_interfaces::agentos_home(None)
            .join("workspace")
            .join("attachments");
        Self::new(root, channel)
    }

    pub(crate) fn new(root: impl Into<PathBuf>, channel: &str) -> Self {
        Self {
            root: root.into(),
            channel: channel.to_owned(),
        }
    }

    /// Build the on-disk path an inbound attachment should be written to.
    /// Creates the parent directory.
    pub(crate) fn target_path(
        &self,
        conversation: &str,
        message_id: &str,
        name: &str,
    ) -> Result<PathBuf, ChannelError> {
        let safe_conv = sanitize_segment(conversation);
        let safe_msg = sanitize_segment(message_id);
        let safe_name = sanitize_filename(name);
        let mut path = self.root.clone();
        path.push(&self.channel);
        path.push(safe_conv.as_ref());
        path.push(safe_msg.as_ref());
        fs::create_dir_all(&path).map_err(|err| {
            ChannelError::Backend(Arc::from(format!(
                "create attachment dir {} failed: {err}",
                path.display()
            )))
        })?;
        path.push(safe_name.as_ref());
        Ok(path)
    }
}

/// Stat a file written by curl and return its size in bytes.
pub(crate) fn file_size(path: &Path) -> Option<u64> {
    fs::metadata(path).ok().map(|meta| meta.len())
}

/// Strip path separators, NULs, and parent-traversal sequences from a path
/// segment so user-controlled values can't escape the attachments root.
fn sanitize_segment(input: &str) -> Arc<str> {
    let cleaned: String = input
        .chars()
        .map(|c| match c {
            '/' | '\\' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let trimmed = cleaned.trim_matches(|c: char| c == '.' || c.is_whitespace());
    if trimmed.is_empty() {
        Arc::from("_")
    } else {
        Arc::from(trimmed)
    }
}

fn sanitize_filename(input: &str) -> Arc<str> {
    let cleaned: String = input
        .chars()
        .map(|c| match c {
            '/' | '\\' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    let trimmed = cleaned.trim_matches(|c: char| c == '.' || c.is_whitespace());
    if trimmed.is_empty() {
        Arc::from("attachment.bin")
    } else {
        Arc::from(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn sanitize_strips_separators_and_traversal() {
        // Both segment and filename keep the result free of path separators,
        // even if the surrounding underscores look ugly.
        let cleaned = sanitize_segment("../../etc/passwd");
        assert!(!cleaned.contains('/') && !cleaned.contains('\\'));
        assert_eq!(sanitize_segment("oc_abc/def").as_ref(), "oc_abc_def");
        let evil = sanitize_filename("..\\evil.exe");
        assert!(!evil.contains('/') && !evil.contains('\\'));
        assert!(evil.contains("evil.exe"));
        assert_eq!(sanitize_filename("").as_ref(), "attachment.bin");
        assert_eq!(sanitize_segment("...").as_ref(), "_");
    }

    #[test]
    fn target_path_creates_parent_and_lays_out_segments() {
        let _guard = ENV_LOCK.lock().unwrap();
        // from_env() resolves to $AGENTOS_HOME/workspace/attachments, so point
        // AGENTOS_HOME at the temp dir and expect that layout under it.
        let tmp = env::temp_dir().join(format!("agentos-attachments-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp);
        let prev_home = env::var_os("AGENTOS_HOME");
        env::set_var("AGENTOS_HOME", &tmp);
        let store = AttachmentStore::from_env("telegram");
        match prev_home {
            Some(v) => env::set_var("AGENTOS_HOME", v),
            None => env::remove_var("AGENTOS_HOME"),
        }

        let path = store.target_path("12345", "67", "photo.jpg").unwrap();
        assert!(path.ends_with("telegram/12345/67/photo.jpg"));
        assert!(path.parent().unwrap().is_dir());
        let _ = fs::remove_dir_all(&tmp);
    }
}
