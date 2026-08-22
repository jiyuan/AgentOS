//! Turning names and paths that something else chose into filesystem access
//! that stays where it was pointed.
//!
//! M4 / `FS-001`, extended by M8 / `GW-001`. Three separate problems, because
//! they have three separate fixes:
//!
//! - [`segment`] is about *identifiers* — a task id, a sub-agent name, a
//!   session id — which become one component of a path and must therefore be
//!   one component of a path. Rejected if not, never rewritten: whoever chose
//!   the name will use it again to find what was written under it.
//! - [`durable`] is about *replacement* — a state file the runtime owns and
//!   rewrites, which must never be observed half-written and must never be
//!   readable by anyone but this user.
//! - [`rooted`] is about *paths* — where a model-supplied `path` argument
//!   actually lands. Lexical checks answer for the string; [`rooted::RootDir`]
//!   answers for the filesystem, one `openat(O_NOFOLLOW)` at a time.

pub mod durable;
mod nofollow;
pub mod rooted;
pub mod segment;

pub use durable::{create_private_dir, write_private_atomic, DurableWriteError};
pub use rooted::{ContainmentError, DirEntry, RootDir};
pub use segment::{displayed_segment, path_segment, PathSegmentError};
