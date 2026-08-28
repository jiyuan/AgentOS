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

use crate::config::{DEFAULT_ATTACHMENTS_PER_MESSAGE, DEFAULT_ATTACHMENT_BYTES};
use crate::paths::{path_segment, RootDir};
use agentos_interfaces::ChannelError;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

pub(crate) struct AttachmentStore {
    root: PathBuf,
    rooted: Arc<OnceLock<Arc<RootDir>>>,
    channel: String,
    /// `[limits].attachment_bytes` — what one file may cost this machine
    /// (M4 / `ING-001`).
    max_bytes: u64,
    /// `[limits].attachments_per_message` — how many downloads one inbound
    /// message may cause.
    max_per_message: usize,
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
            rooted: Arc::new(OnceLock::new()),
            channel: channel.to_owned(),
            max_bytes: DEFAULT_ATTACHMENT_BYTES,
            max_per_message: DEFAULT_ATTACHMENTS_PER_MESSAGE,
        }
    }

    /// Point this store at a different root, keeping its limits.
    ///
    /// Preserving the limits is what makes the channel builders
    /// order-independent: `with_attachments_root().with_attachment_limits()`
    /// and the reverse produce the same store, which is the sort of thing that
    /// is otherwise discovered by a bound quietly not applying.
    pub(crate) fn with_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.root = root.into();
        self.rooted = Arc::new(OnceLock::new());
        self
    }

    /// Apply a deployment's `[limits]` to what this store will accept.
    pub(crate) fn with_limits(mut self, max_bytes: u64, max_per_message: usize) -> Self {
        self.max_bytes = max_bytes;
        self.max_per_message = max_per_message;
        self
    }

    /// Bytes of one attachment that may be written before the download is
    /// abandoned.
    ///
    /// Enforced on the bytes as they arrive rather than on the size a channel
    /// reports: that number is the sender's claim, and nothing holds the
    /// sender to it.
    pub(crate) fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// How many attachments of one message are downloaded. The rest are
    /// reported to the model as skipped rather than silently dropped.
    pub(crate) fn max_per_message(&self) -> usize {
        self.max_per_message
    }

    fn root_dir(&self) -> Result<Arc<RootDir>, ChannelError> {
        if let Some(root) = self.rooted.get() {
            return Ok(root.clone());
        }
        crate::paths::create_private_dir(&self.root).map_err(|err| {
            ChannelError::Backend(Arc::from(format!(
                "create attachment root {} failed: {err}",
                self.root.display()
            )))
        })?;
        let root = Arc::new(RootDir::open(&self.root).map_err(|err| {
            ChannelError::Backend(Arc::from(format!(
                "open attachment root {} failed: {err}",
                self.root.display()
            )))
        })?);
        let _ = self.rooted.set(root.clone());
        Ok(self.rooted.get().cloned().unwrap_or(root))
    }

    fn relative_path(&self, conversation: &str, message_id: &str, name: &str) -> PathBuf {
        Path::new(&self.channel)
            .join(sanitize_segment(conversation).as_ref())
            .join(sanitize_segment(message_id).as_ref())
            .join(sanitize_filename(name).as_ref())
    }

    /// Publish a completely downloaded attachment through a descriptor-rooted
    /// temporary file. The final name is never visible partially written.
    pub(crate) fn publish(
        &self,
        conversation: &str,
        message_id: &str,
        name: &str,
        bytes: &[u8],
    ) -> Result<PathBuf, ChannelError> {
        let relative = self.relative_path(conversation, message_id, name);
        self.root_dir()?
            .write_file_atomic(&relative, bytes)
            .map_err(|err| {
                ChannelError::Backend(Arc::from(format!(
                    "publish attachment {} failed: {err}",
                    self.root.join(&relative).display()
                )))
            })?;
        Ok(self.root.join(relative))
    }
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
    use std::fs;
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

        let path = store.publish("12345", "67", "photo.jpg", b"photo").unwrap();
        assert!(path.ends_with("telegram/12345/67/photo.jpg"));
        assert!(path.parent().unwrap().is_dir());
        assert_eq!(fs::read(path).unwrap(), b"photo");
        let _ = fs::remove_dir_all(&tmp);
    }

    /// A store that has been given limits keeps them across a change of root,
    /// so `with_attachments_root().with_attachment_limits()` and the reverse
    /// agree. A bound that silently stops applying because two builder calls
    /// were written in the other order is worse than no bound.
    #[test]
    fn limits_survive_a_change_of_root_in_either_order() {
        let rooted_then_limited = AttachmentStore::new("/tmp/a", "telegram")
            .with_root("/tmp/b")
            .with_limits(4_096, 2);
        let limited_then_rooted = AttachmentStore::new("/tmp/a", "telegram")
            .with_limits(4_096, 2)
            .with_root("/tmp/b");

        for store in [&rooted_then_limited, &limited_then_rooted] {
            assert_eq!(store.max_bytes(), 4_096);
            assert_eq!(store.max_per_message(), 2);
            assert_eq!(store.root, PathBuf::from("/tmp/b"));
        }
    }

    /// The bound a channel reads when a deployment has said nothing. M4 /
    /// `ING-001`: unbounded was the previous answer.
    #[test]
    fn an_unconfigured_store_is_still_bounded() {
        let store = AttachmentStore::new("/tmp/a", "telegram");
        assert_eq!(store.max_bytes(), DEFAULT_ATTACHMENT_BYTES);
        assert_eq!(store.max_per_message(), DEFAULT_ATTACHMENTS_PER_MESSAGE);
    }

    #[cfg(unix)]
    #[test]
    fn publication_refuses_a_planted_directory_symlink() {
        let root = env::temp_dir().join(format!(
            "agentos-attachment-link-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        let outside = root.with_extension("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let canary = outside.join("canary");
        fs::write(&canary, b"untouched").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("telegram")).unwrap();

        let store = AttachmentStore::new(&root, "telegram");
        assert!(store
            .publish("conversation", "message", "x.txt", b"pwned")
            .is_err());
        assert_eq!(fs::read(&canary).unwrap(), b"untouched");
        assert!(!outside.join("conversation/message/x.txt").exists());

        fs::remove_dir_all(&root).ok();
        fs::remove_dir_all(&outside).ok();
    }
}
