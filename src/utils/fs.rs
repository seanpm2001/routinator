//! File-system related utilities.

use std::io;
use std::fs::{File, OpenOptions};
use std::path::Path;


//------------ ExclusiveFile -------------------------------------------------

/// A file with exclusive access.
///
/// A file opened through this type cannot be opened again by some other
/// process. Whether it can be opened again by the same process depends on
/// the underlying system so don’t try that.
///
/// A file can be opened via [`open`][Self::open]. This will create the file
/// if it doesn’t exist and open it for reading and writing. You can either
/// drop the value or, preferable, call [`close`][Self::close] explicitely.
/// This will make sure that you get to see any potential error when closing
/// and, more importantly, unlocking. If closing fails, the file may stay
/// locked until you exit the process, so you might want to exit if this
/// happens.
///
/// The type provides access to the file via the `std::io::Read`, `Write`,
/// and `Seek` traits. It also provides a limited subset of the methods
/// offered by `std::io::File` (these are added on a need-to-have basis).
#[derive(Debug)]
pub struct ExclusiveFile {
    /// The underlying file.
    file: File,
}

impl ExclusiveFile {
    /// Opens an exclusive file.
    ///
    /// Opens the file specified by `path` for reading and writing and blocks
    /// access to it by other processes and possibly re-opening it by the same
    /// process again, too. If the file doesn’t exist, it will be created.
    ///
    /// Returns an error if opening fails. Because there is no portable way
    /// to indicate opening failing because of an already locked file, this
    /// will always just be a generic IO error.
    pub fn open(path: &Path) -> Result<Self, io::Error> {
        Self::_open(path)
    }

    #[cfg(windows)]
    fn _open(path: &Path) -> Result<Self, io::Error> {
        use std::os::windows::fs::OpenOptionsExt;

        Ok(Self {
            file: OpenOptions::new()
                .read(true).write(true).create(true)
                .share_mode(0) // 0 means: don’t share with other processes.
                .open(path)?
        })
    }

    #[cfg(not(windows))]
    fn _open(path: &Path) -> Result<Self, io::Error> {
        use std::os::fd::AsRawFd;
        use nix::libc::c_short;

        let file = OpenOptions::new().read(true).write(true).create(true)
            .open(path)?;

        let _ = nix::fcntl::fcntl(
            file.as_raw_fd(),
            nix::fcntl::FcntlArg::F_SETLK(&nix::libc::flock {
                l_type: (nix::libc::F_RDLCK | nix::libc::F_WRLCK) as c_short,
                l_whence: nix::libc::SEEK_SET as c_short,
                l_start: 0,
                l_len: 0,
                l_pid: 0,
            })
        )?;

        Ok(Self { file })
    }

    /// Closes the file.
    ///
    /// This is in principle the same as dropping the value, but this method
    /// allows you to check the return value. If closing fails, the file may
    /// still be locked until your process exits which may mean you cannot
    /// re-open it later.
    pub fn close(mut self) -> Result<(), io::Error> {
        self._unlock()
    }

    #[cfg(windows)]
    pub fn _unlock(&mut self) -> Result<(), io::Error> {
        Ok(())
    }

    #[cfg(not(windows))]
    pub fn _unlock(&mut self) -> Result<(), io::Error> {
        use std::os::fd::AsRawFd;
        use nix::libc::c_short;

        nix::fcntl::fcntl(
            self.file.as_raw_fd(),
            nix::fcntl::FcntlArg::F_SETLK(&nix::libc::flock {
                l_type: nix::libc::F_UNLCK as c_short,
                l_whence: nix::libc::SEEK_SET as c_short,
                l_start: 0,
                l_len: 0,
                l_pid: 0,
            })
        )?;
        Ok(())
    }

    /// Set the length of the file.
    ///
    /// This simply delegates to the underlying `std::io::File`, so all the
    /// caveats for `File::set_len` apply.
    pub fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        self.file.set_len(size)
    }
}

#[cfg(not(windows))]
impl Drop for ExclusiveFile {
    fn drop(&mut self) {
        let _ = self._unlock();
    }
}

impl io::Read for ExclusiveFile {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.file.read(buf)
    }
}

impl io::Write for ExclusiveFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.file.flush()
    }
}

impl io::Seek for ExclusiveFile {
    fn seek(&mut self, pos: io::SeekFrom) -> Result<u64, io::Error> {
        self.file.seek(pos)
    }
}


//============ Tests =========================================================

#[cfg(test)]
mod test {
    use super::*;

    /// Test concurrent access to `ExclusiveFile` on Unix systems.
    ///
    /// This test must be run with `--test-threads 1` because it forks and
    /// does things in the child which aren’t allowed in a multi-threaded
    /// process. It therefore is disabled by default and needs to be
    /// activated with either `--ignored` or `--include-ignored`.
    ///
    /// Also, because I’m not sure if `std::thread::sleep` makes a process
    /// multi-threaded, we currently have to wait for multiples of full
    /// seconds so the test takes forever.
    ///
    /// We currently don’t have an equivalent test for Windows since it
    /// doesn’t have a fork equivalent and I can’t think of a simple way to
    /// run the competing process.
    #[test]
    #[ignore]
    #[cfg(not(windows))]
    fn unix_concurrent_access() {
        use std::io::Write;
        use nix::unistd::ForkResult;
        use nix::unistd::{fork, sleep};
        use nix::sys::wait::WaitStatus;
        use nix::sys::wait::waitpid;

        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("test.file");

        // Test 1: Create the file in a child, try to open the file here and
        //         see if that fails. Try to open the file here again after
        //         the child has exited and see if that succeeds.

        let pid = match unsafe { fork() }.unwrap() {
            ForkResult::Parent { child, .. } => child,
            ForkResult::Child => {
                let mut file = ExclusiveFile::open(&path).unwrap();
                assert_eq!(file.write(b"foo").unwrap(), 3);
                sleep(2);
                std::process::exit(0);
            }
        };

        sleep(1);
        assert!(ExclusiveFile::open(&path).is_err());
        let status = waitpid(pid, None).unwrap();
        assert!(matches!(status, WaitStatus::Exited(_, 0)));

        let _ = ExclusiveFile::open(&path).unwrap();

        // Test 2: Create the file in a child and then close it.Try to open
        //         the file here again and see if that succeeds.

        let pid = match unsafe { fork() }.unwrap() {
            ForkResult::Parent { child, .. } => child,
            ForkResult::Child => {
                let mut file = ExclusiveFile::open(&path).unwrap();
                assert_eq!(file.write(b"foo").unwrap(), 3);
                file.close().unwrap();
                sleep(2);
                std::process::exit(0);
            }
        };

        let _ = ExclusiveFile::open(&path).unwrap();

        let status = waitpid(pid, None).unwrap();
        assert!(matches!(status, WaitStatus::Exited(_, 0)));

        // Test 3: Same as test 2 but we drop the file in the child rather
        //         then closing it.
        let pid = match unsafe { fork() }.unwrap() {
            ForkResult::Parent { child, .. } => child,
            ForkResult::Child => {
                let mut file = ExclusiveFile::open(&path).unwrap();
                assert_eq!(file.write(b"foo").unwrap(), 3);
                drop(file);
                sleep(2);
                std::process::exit(0);
            }
        };

        let _ = ExclusiveFile::open(&path).unwrap();

        let status = waitpid(pid, None).unwrap();
        assert!(matches!(status, WaitStatus::Exited(_, 0)));
    }
}

