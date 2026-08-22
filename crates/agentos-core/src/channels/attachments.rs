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

use crate::paths::path_segment;
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

/// Encode a path segment so user-controlled values can neither escape the
/// attachments root nor collide with one another.
///
/// This replaced characters — `/`, `\`, NUL, controls — with `_`, which kept
/// the segment inside the root but was not injective: `oc_abc/def` and
/// `oc_abc_def` landed in the same directory, and a test asserted that
/// equality rather than reporting it. Two conversations could therefore write
/// each other's attachments (`ID-001`).
///
/// `encode_component` is the same encoding principals use, so a safe segment
/// stays readable and anything else becomes `~<base64url>` — which contains no
/// separator, no NUL, no control character, and no `..`.
fn sanitize_segment(input: &str) -> Arc<str> {
    Arc::from(agentos_proto::encode_component(input))
}

/// Bytes of a sender-supplied filename that survive into the stored name.
///
/// Generous for a real filename, short enough to leave room beneath a
/// `NAME_MAX` of 255 for anything a caller appends.
const MAX_FILENAME_BYTES: usize = 200;

/// Make a sender-supplied filename usable as the last component of a path.
///
/// Unlike [`sanitize_segment`] this does not have to be injective — two
/// attachments with the same name in the same message are the sender's
/// problem, and they already land in a directory keyed by conversation and
/// message. It only has to be *one name*, so the result is checked with
/// [`path_segment`] rather than assumed (M4 / `FS-001`); anything that still
/// is not a name becomes `attachment.bin`.
fn sanitize_filename(input: &str) -> Arc<str> {
    let cleaned: String = input
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '\0' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .take_while_within(MAX_FILENAME_BYTES)
        .collect();
    let trimmed = cleaned.trim_matches(|c: char| c == '.' || c.is_whitespace());
    match path_segment("attachment filename", trimmed) {
        Ok(name) => Arc::from(name),
        Err(_) => Arc::from("attachment.bin"),
    }
}

/// `take` counts characters; a filename budget is in bytes, and a multi-byte
/// character must not be split across the boundary.
trait TakeWithinBytes: Iterator<Item = char> + Sized {
    fn take_while_within(self, max_bytes: usize) -> impl Iterator<Item = char> {
        let mut used = 0;
        self.take_while(move |character| {
            used += character.len_utf8();
            used <= max_bytes
        })
    }
}

impl<I: Iterator<Item = char>> TakeWithinBytes for I {}

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
        assert!(!cleaned.contains(".."));
        // Inverted. This asserted `"oc_abc_def"` for both — the collision, not
        // a property. Distinct inputs must land in distinct directories.
        assert_ne!(
            sanitize_segment("oc_abc/def"),
            sanitize_segment("oc_abc_def")
        );
        assert_eq!(sanitize_segment("oc_abc_def").as_ref(), "oc_abc_def");
        let evil = sanitize_filename("..\\evil.exe");
        assert!(!evil.contains('/') && !evil.contains('\\'));
        assert!(evil.contains("evil.exe"));
        assert_eq!(sanitize_filename("").as_ref(), "attachment.bin");
        // M4 / `FS-001`: the result is checked as a single component rather
        // than assumed to be one, so anything that survives the mapping and
        // still is not a name falls back rather than being written.
        assert_eq!(sanitize_filename("..").as_ref(), "attachment.bin");
        assert_eq!(sanitize_filename("   ").as_ref(), "attachment.bin");
        assert_eq!(
            sanitize_filename("report:2026.pdf").as_ref(),
            "report_2026.pdf"
        );
        let overlong = sanitize_filename(&"x".repeat(1_000));
        assert!(
            overlong.len() <= MAX_FILENAME_BYTES,
            "a name the filesystem would refuse is bounded here instead: {} bytes",
            overlong.len()
        );
        // Truncation must not split a character in half.
        let multibyte = sanitize_filename(&"é".repeat(1_000));
        assert!(multibyte.len() <= MAX_FILENAME_BYTES);
        assert!(multibyte.chars().all(|character| character == 'é'));
        // Every traversal-ish segment still encodes to something inert, and
        // to something *different* from every other such segment.
        for segment in ["...", "..", ".", "", "/", "\\"] {
            let encoded = sanitize_segment(segment);
            assert!(!encoded.contains('/') && !encoded.contains('\\') && !encoded.contains(".."));
        }
        let distinct: std::collections::BTreeSet<Arc<str>> = ["...", "..", ".", "", "/", "\\"]
            .iter()
            .map(|segment| sanitize_segment(segment))
            .collect();
        assert_eq!(distinct.len(), 6, "traversal segments must stay distinct");
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
