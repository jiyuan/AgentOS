//! The `openat` walk that makes [`RootDir`] a boundary rather than a
//! convention.
//!
//! Split out of `rooted.rs` because it is the only part that is platform
//! code: everything there is about *what* containment means, everything here
//! is about which syscalls provide it. See the module docs on
//! [`super::rooted`] for why no-follow resolution is the requirement.
//!
//! [`RootDir`]: super::rooted::RootDir

#[cfg(unix)]
mod imp {
    use crate::paths::rooted::{
        ContainmentError, Descend, DirEntry, LeafFailure, LeafMode, RootDir,
    };
    use std::ffi::CString;
    use std::fs::File;
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::path::Path;

    /// Every `openat` here carries `O_NOFOLLOW` and `O_CLOEXEC`: no component
    /// may be a symlink, and no descriptor leaks into a tool subprocess.
    const BASE_FLAGS: libc::c_int = libc::O_NOFOLLOW | libc::O_CLOEXEC;

    /// Whether the component `name` under `dir` is a symbolic link, given the
    /// error opening it produced.
    ///
    /// `O_NOFOLLOW` is documented to report `ELOOP`, and does — except in
    /// combination with `O_DIRECTORY`, where Linux reports `ENOTDIR` instead,
    /// which is also what a plain file reports. The two are only
    /// distinguishable by looking, so on `ENOTDIR` this asks. One extra
    /// syscall, on the error path, to keep "there is a symlink in your path"
    /// from being reported as "not a directory".
    fn is_symlink_refusal(dir: libc::c_int, name: &str, error: &io::Error) -> bool {
        match error.raw_os_error() {
            Some(libc::ELOOP) | Some(libc::EMLINK) => return true,
            Some(libc::ENOTDIR) => {}
            _ => return false,
        }
        let Ok(name) = c_name(name) else {
            return false;
        };
        // SAFETY: `dir` is a live directory descriptor, `name` is a
        // NUL-terminated C string, and `stat` is a valid writable destination
        // for the duration of the call.
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        let probed =
            unsafe { libc::fstatat(dir, name.as_ptr(), &mut stat, libc::AT_SYMLINK_NOFOLLOW) };
        probed == 0 && (stat.st_mode & libc::S_IFMT) == libc::S_IFLNK
    }

    fn c_name(name: &str) -> io::Result<CString> {
        CString::new(name).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
    }

    fn openat(
        dir: libc::c_int,
        name: &str,
        flags: libc::c_int,
        mode: libc::mode_t,
    ) -> io::Result<OwnedFd> {
        let name = c_name(name)?;
        // SAFETY: `dir` is a live directory descriptor owned by the caller for
        // the duration of the call, and `name` is a NUL-terminated C string
        // that outlives it.
        let fd = unsafe { libc::openat(dir, name.as_ptr(), flags, libc::c_uint::from(mode)) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `openat` returned a fresh descriptor that nothing else owns.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn mkdirat(dir: libc::c_int, name: &str) -> io::Result<()> {
        let name = c_name(name)?;
        // SAFETY: as `openat` above.
        let created = unsafe { libc::mkdirat(dir, name.as_ptr(), 0o700) };
        if created < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }
        Ok(())
    }

    impl RootDir {
        /// Walk `names` from the root, one `openat` per component.
        ///
        /// This is the containment. Each step opens a *directory* with
        /// `O_NOFOLLOW`, so a component that is a symlink — planted before the
        /// walk or swapped in during it — fails here instead of redirecting
        /// the rest of the path.
        pub(crate) fn descend(
            &self,
            path: &Path,
            names: &[&str],
            mode: Descend,
        ) -> Result<OwnedFd, ContainmentError> {
            let mut current = self
                .fd
                .try_clone()
                .map_err(|source| ContainmentError::Root {
                    root: self.root.clone(),
                    source,
                })?;
            for name in names {
                let parent = current.as_raw_fd();
                if mode == Descend::Create {
                    mkdirat(parent, name)
                        .map_err(|source| self.step_error(path, parent, name, source))?;
                }
                current = openat(
                    parent,
                    name,
                    BASE_FLAGS | libc::O_RDONLY | libc::O_DIRECTORY,
                    0,
                )
                .map_err(|source| self.step_error(path, parent, name, source))?;
            }
            Ok(current)
        }

        fn step_error(
            &self,
            path: &Path,
            parent: libc::c_int,
            name: &str,
            source: io::Error,
        ) -> ContainmentError {
            if is_symlink_refusal(parent, name, &source) {
                return ContainmentError::Symlink {
                    root: self.root.clone(),
                    path: path.to_owned(),
                    component: (*name).to_owned(),
                };
            }
            ContainmentError::Io {
                root: self.root.clone(),
                path: path.to_owned(),
                source,
            }
        }
    }

    pub(crate) fn open_leaf_at(
        parent: &OwnedFd,
        leaf: &str,
        mode: LeafMode,
    ) -> Result<File, LeafFailure> {
        let flags = match mode {
            LeafMode::Read => BASE_FLAGS | libc::O_RDONLY,
            LeafMode::Write => BASE_FLAGS | libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
        };
        openat(parent.as_raw_fd(), leaf, flags, 0o600)
            .map(File::from)
            .map_err(|source| {
                if is_symlink_refusal(parent.as_raw_fd(), leaf, &source) {
                    LeafFailure::Symlink
                } else {
                    LeafFailure::Io(source)
                }
            })
    }

    /// Set `errno` to zero.
    ///
    /// The one operation in this module that has no portable spelling: the
    /// symbol holding thread-local `errno` differs per libc, and `std` exposes
    /// a reader but no writer. Where neither symbol is known, the function
    /// does nothing and the `readdir` loop below reads a null return as
    /// end-of-directory, which is what it would have concluded anyway.
    fn clear_errno() {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        // SAFETY: `__errno_location` returns a valid pointer to this thread's
        // `errno`, which is ours to write.
        unsafe {
            *libc::__errno_location() = 0
        };
        #[cfg(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "freebsd",
            target_os = "dragonfly"
        ))]
        // SAFETY: as above; `__error` is the same accessor under a different
        // name.
        unsafe {
            *libc::__error() = 0
        };
    }

    /// Read a directory through its descriptor rather than its path.
    ///
    /// `fdopendir` takes ownership of the descriptor and `closedir` releases
    /// it, which is why `dir` is consumed and then handed to the `DIR*`. Each
    /// entry is then stat'd against `dirfd(stream)` — the same directory,
    /// still by descriptor — so a listing cannot be redirected between naming
    /// an entry and describing it.
    pub(crate) fn list_dir(dir: OwnedFd) -> io::Result<Vec<DirEntry>> {
        use std::os::fd::IntoRawFd;

        let raw = dir.into_raw_fd();
        // SAFETY: `raw` is a live directory descriptor this function now owns.
        let stream = unsafe { libc::fdopendir(raw) };
        if stream.is_null() {
            let error = io::Error::last_os_error();
            // SAFETY: `fdopendir` failed, so it did not take the descriptor.
            unsafe { libc::close(raw) };
            return Err(error);
        }
        let mut entries = Vec::new();
        loop {
            // `readdir` reports end-of-directory and error the same way — a
            // null return — so `errno` is cleared first to tell them apart.
            clear_errno();
            // SAFETY: `stream` is a live `DIR*` owned by this function.
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                let error = io::Error::last_os_error();
                // SAFETY: `stream` is live and is not used again.
                unsafe { libc::closedir(stream) };
                if error.raw_os_error().unwrap_or(0) != 0 {
                    return Err(error);
                }
                return Ok(entries);
            }
            // SAFETY: `readdir` returned a non-null entry valid until the next
            // call on this stream, and `d_name` is NUL-terminated.
            let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) };
            let name = name.to_string_lossy().into_owned();
            if name == "." || name == ".." {
                continue;
            }
            // SAFETY: `stream` is a live `DIR*`.
            let dirfd = unsafe { libc::dirfd(stream) };
            entries.push(describe(dirfd, name));
        }
    }

    /// Fill in an entry's facts, or leave them blank.
    ///
    /// A `stat` that fails is not an error for the listing: the entry was
    /// there a moment ago and is gone or unreadable now, which is ordinary in
    /// a directory something else is writing. Reporting the name with nothing
    /// attached is more useful than failing the whole call.
    fn describe(dirfd: libc::c_int, name: String) -> DirEntry {
        let blank = DirEntry {
            name: name.clone(),
            is_dir: false,
            is_symlink: false,
            len: 0,
            modified: None,
        };
        let Ok(c_name) = c_name(&name) else {
            return blank;
        };
        // SAFETY: `dirfd` is live for the duration of the call, `c_name` is
        // NUL-terminated, and `stat` is a valid writable destination.
        let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
        let probed =
            unsafe { libc::fstatat(dirfd, c_name.as_ptr(), &mut stat, libc::AT_SYMLINK_NOFOLLOW) };
        if probed != 0 {
            return blank;
        }
        let kind = stat.st_mode & libc::S_IFMT;
        DirEntry {
            name,
            is_dir: kind == libc::S_IFDIR,
            is_symlink: kind == libc::S_IFLNK,
            len: stat.st_size.max(0) as u64,
            modified: u64::try_from(stat.st_mtime)
                .ok()
                .map(|secs| std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs)),
        }
    }
}

#[cfg(not(unix))]
mod imp {
    use crate::paths::rooted::{
        ContainmentError, Descend, DirEntry, LeafFailure, LeafMode, RootDir,
    };
    use std::fs::File;
    use std::io;
    use std::path::Path;

    /// A placeholder descriptor type for platforms with no `openat`.
    pub(crate) struct NoFd;

    impl RootDir {
        pub(crate) fn descend(
            &self,
            _path: &Path,
            _names: &[&str],
            _mode: Descend,
        ) -> Result<NoFd, ContainmentError> {
            Err(ContainmentError::Unsupported)
        }
    }

    pub(crate) fn open_leaf_at(
        _parent: &NoFd,
        _leaf: &str,
        _mode: LeafMode,
    ) -> Result<File, LeafFailure> {
        Err(LeafFailure::Io(io::Error::from(io::ErrorKind::Unsupported)))
    }

    pub(crate) fn list_dir(_dir: NoFd) -> io::Result<Vec<DirEntry>> {
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}

#[cfg(unix)]
pub(super) use imp::{list_dir, open_leaf_at};
#[cfg(not(unix))]
pub(super) use imp::{list_dir, open_leaf_at, NoFd};
