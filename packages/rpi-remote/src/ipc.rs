//! Local-IPC transport: a Unix-domain socket on POSIX, a named pipe on Windows.
//!
//! The mesh only needs a stream of JSONL frames, so the transport is abstracted
//! down to "connect to an address" / "listen on an address" plus the address
//! string itself. On Windows a Unix-domain socket path has no portable
//! `std` API, so the address is translated to a per-user named pipe
//! (`\\.\pipe\rpi-remote-<hash>`) and opened with `std::fs::File`, which Win32
//! supports for pipes.

#[cfg(test)]
use std::io::BufReader;
use std::io::{self, BufRead, Write};

/// A bidirectional JSONL stream to a broker or client.
pub type Stream = StreamImpl;

#[cfg(unix)]
pub type StreamImpl = std::os::unix::net::UnixStream;

#[cfg(windows)]
pub type StreamImpl = std::fs::File;

/// Connects to the broker endpoint at `path`.
pub fn connect(path: &str) -> io::Result<Stream> {
    #[cfg(unix)]
    {
        std::os::unix::net::UnixStream::connect(path)
    }
    #[cfg(windows)]
    {
        use std::fs::OpenOptions;
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(pipe_name(path))
    }
}

/// Whether `path` denotes a Windows named pipe rather than a filesystem path.
pub const fn uses_named_pipe() -> bool {
    cfg!(windows)
}

/// Translates a mesh socket path into the platform address used by the OS.
///
/// On POSIX this is the path unchanged. On Windows the path is hashed into a
/// stable per-user pipe name, because a pipe name is global to the machine and
/// the raw path may exceed the pipe-name limits.
pub fn ipc_address(path: &str) -> String {
    #[cfg(unix)]
    {
        path.to_owned()
    }
    #[cfg(windows)]
    {
        pipe_name(path)
    }
}

#[cfg(windows)]
fn pipe_name(path: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    path.hash(&mut hasher);
    format!(r"\\.\pipe\rpi-remote-{:016x}", hasher.finish())
}

/// A broker listener.
///
/// A newtype rather than a bare `UnixListener` so the non-Unix build can keep
/// the "unsupported" error in one place; Windows has no `std` Unix-domain
/// socket type at all.
#[cfg(unix)]
pub struct Listener(std::os::unix::net::UnixListener);

/// Placeholder for platforms without a Unix-domain socket.
#[cfg(not(unix))]
pub struct Listener;

impl Listener {
    /// Binds `path`. `path` must already be an IPC address (see
    /// [`ipc_address`]).
    ///
    /// Never unlinks an existing endpoint: removing one that a live holder
    /// owns would *steal* the lock/session rather than report contention.
    /// Clearing a genuinely stale endpoint is the caller's job, because only
    /// the caller knows how to probe liveness first (see
    /// `election::remove_stale`).
    pub fn bind(path: &str) -> io::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Listener(std::os::unix::net::UnixListener::bind(path)?))
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the rpi-remote local mesh requires a Unix-domain socket; \
                 Windows builds use `rpi --server` as the broker transport",
            ))
        }
    }

    /// Sets whether the listener accepts in non-blocking mode.
    ///
    /// Needed by the broker's accept loop, which must be able to poll its stop
    /// signal: a blocking `accept()` never returns merely because the listener's
    /// owners were dropped.
    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        #[cfg(unix)]
        {
            self.0.set_nonblocking(nonblocking)
        }
        #[cfg(not(unix))]
        {
            let _ = (nonblocking, self);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "non-blocking accept is not supported on this platform",
            ))
        }
    }

    /// Blocks for the next client connection.
    pub fn accept(&self) -> io::Result<Stream> {
        #[cfg(unix)]
        {
            let (stream, _) = self.0.accept()?;
            Ok(stream)
        }
        #[cfg(not(unix))]
        {
            Err(io::Error::new(io::ErrorKind::Unsupported, "unsupported"))
        }
    }
}

/// Reads the next newline-terminated line from `reader`, reusing `buffer`.
///
/// Returns `Ok(None)` at EOF. Lines longer than `MAX_LINE_BYTES` are rejected
/// rather than buffered without bound.
pub fn read_line<R: BufRead>(reader: &mut R, buffer: &mut String) -> io::Result<Option<String>> {
    const MAX_LINE_BYTES: usize = 4 * 1024 * 1024;
    buffer.clear();
    loop {
        let mut chunk = String::new();
        let count = reader.read_line(&mut chunk)?;
        if count == 0 {
            return if buffer.is_empty() {
                Ok(None)
            } else {
                Ok(Some(std::mem::take(buffer)))
            };
        }
        buffer.push_str(&chunk);
        if buffer.ends_with('\n') {
            let line = std::mem::take(buffer);
            return Ok(Some(line.trim_end_matches(['\n', '\r']).to_owned()));
        }
        if buffer.len() > MAX_LINE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "mesh line exceeds the maximum length",
            ));
        }
    }
}

/// Wraps a stream in a buffered reader for line-delimited reads.
///
/// Test-only: production readers own their `BufReader` directly so the read
/// half and the write half can be split without an extra allocation.
#[cfg(test)]
pub fn buffered(stream: Stream) -> BufReader<Stream> {
    BufReader::new(stream)
}

/// Writes one already-serialized JSONL frame and flushes it.
pub fn write_line<W: Write>(writer: &mut W, line: &str) -> io::Result<()> {
    writer.write_all(line.as_bytes())?;
    if !line.ends_with('\n') {
        writer.write_all(b"\n")?;
    }
    writer.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_newline_delimited_lines() {
        let input = b"one\ntwo\r\nthree";
        let mut reader = std::io::BufReader::new(&input[..]);
        let mut buffer = String::new();
        assert_eq!(
            read_line(&mut reader, &mut buffer).unwrap().as_deref(),
            Some("one")
        );
        assert_eq!(
            read_line(&mut reader, &mut buffer).unwrap().as_deref(),
            Some("two")
        );
        assert_eq!(
            read_line(&mut reader, &mut buffer).unwrap().as_deref(),
            Some("three")
        );
        assert_eq!(read_line(&mut reader, &mut buffer).unwrap(), None);
    }

    #[cfg(unix)]
    #[test]
    fn listener_round_trips_a_line() {
        let dir = std::env::temp_dir().join(format!("rpi-remote-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("broker.sock");
        let path_str = path.to_string_lossy().to_string();
        let listener = Listener::bind(&path_str).unwrap();
        let client = std::thread::spawn({
            let path_str = path_str.clone();
            move || {
                let mut stream = connect(&path_str).unwrap();
                write_line(&mut stream, "hello\n").unwrap();
            }
        });
        let stream = listener.accept().unwrap();
        let mut reader = buffered(stream.try_clone().unwrap());
        let mut buffer = String::new();
        assert_eq!(
            read_line(&mut reader, &mut buffer).unwrap().as_deref(),
            Some("hello")
        );
        client.join().unwrap();
        drop(stream);
        std::fs::remove_dir_all(&dir).ok();
    }
}
