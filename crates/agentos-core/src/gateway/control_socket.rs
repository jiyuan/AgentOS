//! Authenticated, holder-directed process control.
//!
//! A control-file record is useful only while its lock is held. Reading the
//! holder and then calling `kill(pid)` creates a gap in which that holder can
//! exit and the kernel can recycle its numeric pid. The socket protocol closes
//! that gap without trying to make pid lookup atomic: a request captures the
//! holder's private token, and whichever process owns the socket when the
//! request arrives accepts it only if that exact token is still current.

use super::control::{holder, ControlError, ControlRecord};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// The socket path paired with a control file.
pub fn control_socket_path(control_path: &Path) -> PathBuf {
    #[cfg(unix)]
    {
        use sha2::{Digest, Sha256};
        use std::os::unix::ffi::OsStrExt;

        let digest = Sha256::digest(control_path.as_os_str().as_bytes());
        let mut name = String::from("agentos-control-");
        for byte in &digest[..16] {
            use std::fmt::Write as _;
            write!(&mut name, "{byte:02x}").expect("writing to a String cannot fail");
        }
        name.push_str(".sock");
        Path::new("/tmp").join(name)
    }
    #[cfg(not(unix))]
    {
        let mut socket = control_path.as_os_str().to_os_string();
        socket.push(".sock");
        PathBuf::from(socket)
    }
}

/// A shutdown request bound to the exact holder observed at capture time.
#[derive(Clone, Debug)]
pub struct ShutdownRequest {
    control_path: PathBuf,
    socket_path: PathBuf,
    record: ControlRecord,
}

impl ShutdownRequest {
    /// Capture the currently locked holder. `None` means nobody is serving.
    pub fn capture(control_path: &Path) -> Result<Option<Self>, ControlError> {
        let Some(record) = holder(control_path)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            control_path: control_path.to_path_buf(),
            socket_path: control_socket_path(control_path),
            record,
        }))
    }

    pub fn record(&self) -> &ControlRecord {
        &self.record
    }

    /// Send the authenticated drain request.
    #[cfg(unix)]
    pub fn send(self) -> Result<ControlRecord, ControlError> {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixStream;

        let io = |source| ControlError::Io {
            path: self.socket_path.clone(),
            source,
        };
        let mut stream = UnixStream::connect(&self.socket_path).map_err(io)?;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .map_err(io)?;
        stream
            .set_write_timeout(Some(std::time::Duration::from_secs(2)))
            .map_err(io)?;
        stream
            .write_all(format!("shutdown {}\n", self.record.control_token).as_bytes())
            .map_err(io)?;
        let mut response = [0_u8; 16];
        let read = stream.read(&mut response).map_err(io)?;
        if &response[..read] != b"ok\n" {
            return Err(ControlError::TargetChanged {
                path: self.control_path,
            });
        }
        Ok(self.record)
    }

    #[cfg(not(unix))]
    pub fn send(self) -> Result<ControlRecord, ControlError> {
        Err(ControlError::Io {
            path: self.socket_path,
            source: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "authenticated gateway control is only implemented for unix",
            ),
        })
    }
}

/// A bound endpoint. Binding and starting its accept loop precede publication
/// of the serving lock, so a visible holder is always able to receive a stop.
#[derive(Debug)]
pub struct ControlEndpoint {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    #[cfg(unix)]
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ControlEndpoint {
    #[cfg(unix)]
    pub fn bind(control_path: &Path, token: impl Into<Arc<str>>) -> Result<Self, ControlError> {
        use std::io::{Read, Write};
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::{UnixListener, UnixStream};

        let path = control_socket_path(control_path);
        let io = |source| ControlError::Io {
            path: path.clone(),
            source,
        };
        if let Some(parent) = path.parent() {
            crate::paths::create_private_dir(parent).map_err(|err| ControlError::Io {
                path: err.path().to_path_buf(),
                source: err.into_io(),
            })?;
        }

        match UnixStream::connect(&path) {
            Ok(_) => {
                return Err(io(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    "gateway control endpoint is already accepting connections",
                )))
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                ) =>
            {
                if err.kind() == std::io::ErrorKind::ConnectionRefused {
                    std::fs::remove_file(&path).map_err(io)?;
                }
            }
            Err(err) => return Err(io(err)),
        }

        let listener = UnixListener::bind(&path).map_err(io)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).map_err(io)?;
        listener.set_nonblocking(true).map_err(io)?;
        let token = token.into();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker = std::thread::Builder::new()
            .name("agentos-control".to_owned())
            .spawn(move || {
                while !worker_stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let _ = stream
                                .set_read_timeout(Some(std::time::Duration::from_millis(250)));
                            let mut request = [0_u8; 512];
                            let read = stream.read(&mut request).unwrap_or(0);
                            let expected = format!("shutdown {token}\n");
                            if request[..read] == *expected.as_bytes() {
                                super::shutdown::request_shutdown();
                                let _ = stream.write_all(b"ok\n");
                            } else {
                                let _ = stream.write_all(b"stale\n");
                            }
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(io)?;
        Ok(Self {
            path,
            stop,
            worker: Some(worker),
        })
    }

    #[cfg(not(unix))]
    pub fn bind(control_path: &Path, _token: impl Into<Arc<str>>) -> Result<Self, ControlError> {
        Err(ControlError::Io {
            path: control_socket_path(control_path),
            source: std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "authenticated gateway control is only implemented for unix",
            ),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ControlEndpoint {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        #[cfg(unix)]
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}
