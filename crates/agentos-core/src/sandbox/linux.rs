//! Landlock: the Linux backend for [`super::Sandbox`].
//!
//! Landlock lets an *unprivileged* process give up access it currently has.
//! That is the whole reason it fits here — no root, no helper daemon, no
//! container runtime. Three syscalls: build a ruleset saying which rights are
//! governed, add rules granting some of them back beneath specific paths, then
//! restrict the calling thread.
//!
//! # Why the restriction happens in `pre_exec`
//!
//! `landlock_restrict_self` restricts the calling thread irreversibly, and
//! every child inherits it. Applying it anywhere in the agent would sandbox the
//! agent — for the rest of the process's life. So it is applied in the forked
//! child, after `fork` and before `exec`, where it can only affect the tool.
//!
//! The closure `pre_exec` runs is subject to the same rules as any code between
//! fork and exec in a threaded program: no allocation, no locks, nothing that
//! is not async-signal-safe. Everything expensive therefore happens *before*
//! the fork — the directory descriptors are opened and the rule structs are
//! built in the parent, and the closure does nothing but issue syscalls over
//! data it already owns.
//!
//! # Rights are masked to the running kernel's ABI
//!
//! `landlock_create_ruleset` rejects a ruleset naming a right the kernel does
//! not know (`REFER` arrived in ABI 2, `TRUNCATE` in ABI 3). Asking for
//! everything and letting the kernel sort it out therefore fails closed on an
//! older kernel, so the handled set is masked by the ABI the kernel reports.

use super::{Availability, Sandbox, SandboxError};
use std::os::fd::{AsRawFd, OwnedFd};
#[allow(unused_imports)]
use std::os::unix::process::CommandExt;

const SYS_LANDLOCK_CREATE_RULESET: libc::c_long = 444;
const SYS_LANDLOCK_ADD_RULE: libc::c_long = 445;
const SYS_LANDLOCK_RESTRICT_SELF: libc::c_long = 446;

/// `landlock_create_ruleset(NULL, 0, LANDLOCK_CREATE_RULESET_VERSION)` returns
/// the ABI the kernel supports instead of creating anything.
const LANDLOCK_CREATE_RULESET_VERSION: u32 = 1;
const LANDLOCK_RULE_PATH_BENEATH: libc::c_int = 1;

// Filesystem access rights, by the ABI that introduced them.
const WRITE_FILE: u64 = 1 << 1;
const REMOVE_DIR: u64 = 1 << 4;
const REMOVE_FILE: u64 = 1 << 5;
const MAKE_CHAR: u64 = 1 << 6;
const MAKE_DIR: u64 = 1 << 7;
const MAKE_REG: u64 = 1 << 8;
const MAKE_SOCK: u64 = 1 << 9;
const MAKE_FIFO: u64 = 1 << 10;
const MAKE_BLOCK: u64 = 1 << 11;
const MAKE_SYM: u64 = 1 << 12;
/// ABI 2: linking or renaming across directories.
const REFER: u64 = 1 << 13;
/// ABI 3: truncating a file you may not otherwise write.
const TRUNCATE: u64 = 1 << 14;

/// Every right that means "changes the filesystem", at ABI 1.
const WRITE_RIGHTS_V1: u64 = WRITE_FILE
    | REMOVE_DIR
    | REMOVE_FILE
    | MAKE_CHAR
    | MAKE_DIR
    | MAKE_REG
    | MAKE_SOCK
    | MAKE_FIFO
    | MAKE_BLOCK
    | MAKE_SYM;

/// Writable in every sandboxed mode, including `read_only`.
///
/// `command > /dev/null` and `2>/dev/null` are ordinary shell, and a
/// `read_only` sandbox that refuses them reads as the tool being broken rather
/// than as the sandbox working. Writing to the null device changes nothing, so
/// there is nothing to protect.
const ALWAYS_WRITABLE: &[&str] = &["/dev/null"];

/// `struct landlock_ruleset_attr`. Only the first field is sent: the kernel
/// zero-extends a shorter struct, so this is the one shape that works on every
/// ABI from 1 upwards.
#[repr(C)]
struct RulesetAttr {
    handled_access_fs: u64,
}

/// `struct landlock_path_beneath_attr`, which the kernel declares packed.
#[repr(C, packed)]
struct PathBeneathAttr {
    allowed_access: u64,
    parent_fd: i32,
}

/// The ABI the running kernel supports, or `None` when Landlock is absent —
/// not compiled in, or disabled in the LSM list.
fn abi() -> Option<u32> {
    // SAFETY: the version query takes a null attr and a zero size by contract.
    let version = unsafe {
        libc::syscall(
            SYS_LANDLOCK_CREATE_RULESET,
            std::ptr::null::<RulesetAttr>(),
            0usize,
            LANDLOCK_CREATE_RULESET_VERSION,
        )
    };
    (version > 0).then_some(version as u32)
}

/// The write rights this kernel understands.
fn handled_rights(abi: u32) -> u64 {
    let mut rights = WRITE_RIGHTS_V1;
    if abi >= 2 {
        rights |= REFER;
    }
    if abi >= 3 {
        rights |= TRUNCATE;
    }
    rights
}

pub(super) fn availability() -> Availability {
    match abi() {
        Some(_) => Availability::Enforced("landlock"),
        None => Availability::Unavailable(
            "this kernel has no Landlock support (needs Linux 5.13+ with the LSM enabled)",
        ),
    }
}

/// Build the ruleset in the parent and install it in the child.
pub(super) fn harden(
    sandbox: &Sandbox,
    command: &mut tokio::process::Command,
) -> Result<(), SandboxError> {
    let Some(abi) = abi() else {
        return Err(SandboxError::Unavailable {
            mode: sandbox.mode().as_str(),
            reason: "this kernel has no Landlock support",
        });
    };
    let handled = handled_rights(abi);

    // Opened here, before the fork: opening a path is not async-signal-safe,
    // and a path that does not exist should fail the call rather than the
    // child.
    let mut granted: Vec<(OwnedFd, u64)> = Vec::new();
    for path in ALWAYS_WRITABLE {
        if let Some(fd) = open_path(std::path::Path::new(path)) {
            granted.push((fd, WRITE_FILE & handled));
        }
    }
    for path in sandbox.writable() {
        let fd = open_path(path).ok_or_else(|| {
            SandboxError::Setup(format!(
                "cannot grant writes beneath '{}': it does not exist or cannot be opened",
                path.display()
            ))
        })?;
        granted.push((fd, handled));
    }

    // SAFETY: the closure runs in the forked child before `exec`. It allocates
    // nothing, takes no locks, and calls only `prctl` and the Landlock
    // syscalls — all async-signal-safe. Everything it reads was built above,
    // in the parent, and moved in.
    unsafe {
        command.pre_exec(move || {
            restrict(handled, &granted)?;
            Ok(())
        });
    }
    Ok(())
}

/// Apply the ruleset to the calling thread. Runs in the forked child.
fn restrict(handled: u64, granted: &[(OwnedFd, u64)]) -> std::io::Result<()> {
    let attr = RulesetAttr {
        handled_access_fs: handled,
    };
    // SAFETY: `attr` outlives the call and its size is passed explicitly.
    let ruleset = unsafe {
        libc::syscall(
            SYS_LANDLOCK_CREATE_RULESET,
            &attr as *const RulesetAttr,
            std::mem::size_of::<RulesetAttr>(),
            0u32,
        )
    };
    if ruleset < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let ruleset = ruleset as libc::c_int;

    for (fd, allowed) in granted {
        let rule = PathBeneathAttr {
            allowed_access: *allowed,
            parent_fd: fd.as_raw_fd(),
        };
        // SAFETY: `rule` outlives the call; `fd` is open and owned by the
        // caller for the duration.
        let added = unsafe {
            libc::syscall(
                SYS_LANDLOCK_ADD_RULE,
                ruleset,
                LANDLOCK_RULE_PATH_BENEATH,
                &rule as *const PathBeneathAttr,
                0u32,
            )
        };
        if added != 0 {
            let err = std::io::Error::last_os_error();
            close(ruleset);
            return Err(err);
        }
    }

    // Landlock refuses to restrict a thread that could still gain privileges
    // through a setuid exec, which is the point: without this, the tool could
    // escape by running something.
    // SAFETY: `prctl` with these arguments has no memory effects.
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        let err = std::io::Error::last_os_error();
        close(ruleset);
        return Err(err);
    }
    // SAFETY: `ruleset` is a live fd from `create_ruleset` above.
    let restricted = unsafe { libc::syscall(SYS_LANDLOCK_RESTRICT_SELF, ruleset, 0u32) };
    let outcome = if restricted == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    };
    close(ruleset);
    outcome
}

/// `close`, ignoring the result: this runs between fork and exec, where there
/// is nowhere to report to and the fd dies with the `exec` regardless.
fn close(fd: libc::c_int) {
    // SAFETY: `fd` is owned by this function's caller chain and closed once.
    unsafe { libc::close(fd) };
}

/// Open a path for use as a Landlock rule target. `O_PATH` because the rule
/// needs to name the file, not read it — which also means a directory the
/// child may not read can still be granted writes.
fn open_path(path: &std::path::Path) -> Option<OwnedFd> {
    use std::os::fd::FromRawFd;
    let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).ok()?;
    // SAFETY: `c_path` is a valid NUL-terminated string that outlives the call.
    let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
    if fd < 0 {
        return None;
    }
    // SAFETY: `fd` is a fresh descriptor this function exclusively owns.
    Some(unsafe { OwnedFd::from_raw_fd(fd) })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Whatever this kernel reports, the answer is a fact about the machine
    /// rather than about the build.
    #[test]
    fn availability_reflects_the_running_kernel() {
        match (abi(), availability()) {
            (Some(_), Availability::Enforced(name)) => assert_eq!(name, "landlock"),
            (None, Availability::Unavailable(reason)) => assert!(reason.contains("Landlock")),
            (abi, availability) => panic!("mismatched {abi:?} and {availability:?}"),
        }
    }

    /// Rights the running kernel does not know would make `create_ruleset`
    /// fail, so the handled set only ever grows with the ABI.
    #[test]
    fn handled_rights_grow_with_the_abi() {
        assert_eq!(handled_rights(1) & (REFER | TRUNCATE), 0);
        assert_eq!(handled_rights(2) & REFER, REFER);
        assert_eq!(handled_rights(2) & TRUNCATE, 0);
        assert_eq!(handled_rights(3) & TRUNCATE, TRUNCATE);
        assert!(handled_rights(1) & WRITE_FILE != 0);
        // Reads are never handled: this sandbox bounds writes.
        assert_eq!(handled_rights(6) & (1 << 2), 0);
    }

    /// The struct the kernel is handed has to be the shape the kernel expects,
    /// and `path_beneath` is declared packed — 12 bytes, not 16.
    #[test]
    fn the_rule_struct_is_packed() {
        assert_eq!(std::mem::size_of::<PathBeneathAttr>(), 12);
        assert_eq!(std::mem::size_of::<RulesetAttr>(), 8);
    }
}
