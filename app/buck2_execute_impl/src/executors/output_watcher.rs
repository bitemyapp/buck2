/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Linux-only speculative output hints, never an assertion of output finality.
//!
//! Limits are per observer: 4,096 declared roots, 4,096 kernel watches, 4,096
//! pending directory/root visits, 4,096 bytes per absolute path, a 64 KiB event
//! buffer, and 256 candidates per batch. Exhaustion disables the optimization
//! with an error rather than claiming complete coverage. Besides the project
//! root and inotify descriptors, only the current directory iterator retains a
//! descriptor; watched directories retain paths, not open files. Initial walks
//! register directories without reporting old files. No filesystem polling is
//! used, and missing/coalesced events are left to ordinary final processing.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::ffi::CString;
use std::ffi::OsStr;
use std::ffi::c_char;
use std::ffi::c_int;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::ReadDir;
use std::io;
use std::io::Read;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

use tokio::io::unix::AsyncFd;

const MAX_ROOTS: usize = 4096;
const MAX_WATCHES: usize = 4096;
const MAX_PENDING: usize = 4096;
const MAX_PATH_BYTES: usize = 4096;
const EVENT_BUFFER_BYTES: usize = 64 * 1024;
const MAX_BATCH: usize = 256;
const WORK_BEFORE_YIELD: usize = 1024;
const EVENT_HEADER_BYTES: usize = 16;
const MAX_NAME_BYTES: usize = 255;

// Linux UAPI (the native executor is built for Linux x86-64/aarch64).
const O_NONBLOCK: c_int = 0x800;
const O_DIRECTORY: c_int = 0x10000;
const O_NOFOLLOW: c_int = 0x20000;
const O_CLOEXEC: c_int = 0x80000;
const IN_CLOSE_WRITE: u32 = 0x0000_0008;
const IN_MOVED_TO: u32 = 0x0000_0080;
const IN_CREATE: u32 = 0x0000_0100;
const IN_DELETE_SELF: u32 = 0x0000_0400;
const IN_MOVE_SELF: u32 = 0x0000_0800;
const IN_Q_OVERFLOW: u32 = 0x0000_4000;
const IN_IGNORED: u32 = 0x0000_8000;
const IN_ONLYDIR: u32 = 0x0100_0000;
const IN_ISDIR: u32 = 0x4000_0000;
const WATCH_MASK: u32 =
    IN_CLOSE_WRITE | IN_MOVED_TO | IN_CREATE | IN_DELETE_SELF | IN_MOVE_SELF | IN_ONLYDIR;

unsafe extern "C" {
    fn inotify_init1(flags: c_int) -> c_int;
    fn inotify_add_watch(fd: c_int, pathname: *const c_char, mask: u32) -> c_int;
    fn openat(dirfd: c_int, pathname: *const c_char, flags: c_int, ...) -> c_int;
}

enum Visit {
    Root(PathBuf),
    Directory { path: PathBuf, discover_files: bool },
}

struct DirectoryCursor {
    path: PathBuf,
    entries: ReadDir,
    discover_files: bool,
}

pub(crate) struct OutputWatcher {
    project_root: PathBuf,
    root: File,
    outputs: Vec<PathBuf>,
    events: AsyncFd<File>,
    watches: HashMap<c_int, PathBuf>,
    pending: VecDeque<Visit>,
    directory: Option<DirectoryCursor>,
    event_buffer: Box<[u8]>,
    event_start: usize,
    event_end: usize,
    failed: bool,
}

impl OutputWatcher {
    /// Must be called inside a Tokio runtime with its I/O driver enabled.
    /// `project_root` is a trusted canonical directory; outputs are absolute
    /// declared paths strictly below it. Missing outputs may become files or
    /// directories. A root is only traversed when it actually opens as a
    /// directory, without following symlinks.
    pub(crate) fn new(project_root: &Path, outputs: Vec<PathBuf>) -> io::Result<Self> {
        validate_absolute_path(project_root)?;
        if outputs.len() > MAX_ROOTS {
            return Err(limit_error("declared output roots"));
        }
        let mut relative_outputs = Vec::with_capacity(outputs.len());
        for output in outputs {
            validate_absolute_path(&output)?;
            let relative = output.strip_prefix(project_root).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "output is outside project root",
                )
            })?;
            if relative.as_os_str().is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "output must be strictly below project root",
                ));
            }
            relative_outputs.push(relative.to_path_buf());
        }
        relative_outputs.sort_unstable();
        relative_outputs.dedup();
        // A directory root already owns any nested declared roots. A regular
        // file cannot have descendants, so this also preserves file roots.
        let mut roots: Vec<PathBuf> = Vec::with_capacity(relative_outputs.len());
        for output in relative_outputs {
            if !roots.last().is_some_and(|root| output.starts_with(root)) {
                roots.push(output);
            }
        }

        // Even the trusted root is opened component by component. No later
        // watch registration or candidate read resolves an absolute live path.
        let slash = OpenOptions::new()
            .read(true)
            .custom_flags(O_DIRECTORY | O_NOFOLLOW)
            .open("/")?;
        let root = open_directory_at(&slash, project_root.strip_prefix("/").unwrap())?;
        drop(slash);
        tokio::runtime::Handle::try_current()
            .map_err(|error| io::Error::other(error.to_string()))?;
        let descriptor = unsafe { inotify_init1(O_NONBLOCK | O_CLOEXEC) };
        if descriptor < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: a successful inotify_init1 returns a new owned descriptor.
        let events = AsyncFd::new(unsafe { File::from_raw_fd(descriptor) })?;
        let mut watcher = Self {
            project_root: project_root.to_path_buf(),
            root,
            outputs: roots,
            events,
            watches: HashMap::new(),
            pending: VecDeque::new(),
            directory: None,
            event_buffer: vec![0; EVENT_BUFFER_BYTES].into_boxed_slice(),
            event_start: 0,
            event_end: 0,
            failed: false,
        };
        for index in 0..watcher.outputs.len() {
            let output = watcher.outputs[index].clone();
            drop(watcher.visit_root(output, false)?);
        }
        // Initial contents are not candidates, including those in nested
        // directories. Real close/move events during registration stay queued.
        while watcher.directory.is_some() || !watcher.pending.is_empty() {
            drop(watcher.scan_one()?);
        }
        Ok(watcher)
    }

    /// Returns speculative absolute paths, at most 256 at a time. A successful
    /// return does not mean the writer has exited, or even stopped writing.
    /// Cancellation while waiting consumes no candidates. After any error the
    /// observer remains failed; the caller must use ordinary final processing.
    pub(crate) async fn next_batch(&mut self) -> io::Result<Vec<PathBuf>> {
        if self.failed {
            return Err(io::Error::other("output candidate observer is disabled"));
        }
        let result = self.next_batch_inner().await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    async fn next_batch_inner(&mut self) -> io::Result<Vec<PathBuf>> {
        let mut batch = Vec::with_capacity(MAX_BATCH);
        let mut work = 0;
        loop {
            let candidate = if self.event_start < self.event_end {
                self.process_event()?
            } else if self.directory.is_some() || !self.pending.is_empty() {
                self.scan_one()?
            } else {
                if !batch.is_empty() {
                    return Ok(batch);
                }
                let mut readiness = self.events.readable().await?;
                match readiness.try_io(|events| {
                    let mut file = events.get_ref();
                    file.read(&mut self.event_buffer[..])
                }) {
                    Ok(Ok(0)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "inotify descriptor closed",
                        ));
                    }
                    Ok(Ok(length)) => {
                        self.event_start = 0;
                        self.event_end = length;
                    }
                    Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {}
                }
                continue;
            };
            if let Some(path) = candidate {
                batch.push(self.project_root.join(path));
                if batch.len() == MAX_BATCH {
                    return Ok(batch);
                }
            }
            work += 1;
            if work == WORK_BEFORE_YIELD {
                // Never suspend with candidates only in this future's local
                // state: cancellation would otherwise silently discard them.
                if !batch.is_empty() {
                    return Ok(batch);
                }
                tokio::task::yield_now().await;
                work = 0;
            }
        }
    }

    /// Opens a candidate through the retained project directory descriptor.
    /// The caller must still create an independent immutable snapshot and
    /// validate against the authoritative final output; this is a live file.
    pub(crate) fn open_candidate(&self, path: &Path) -> io::Result<File> {
        validate_absolute_path(path)?;
        let relative = path.strip_prefix(&self.project_root).map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "candidate escapes project root",
            )
        })?;
        if !self.owns(relative) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "candidate is outside declared outputs",
            ));
        }
        let name = relative.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "candidate has no filename")
        })?;
        let directory = open_directory_at(&self.root, relative.parent().unwrap())?;
        let file = open_component(directory.as_raw_fd(), name, O_NOFOLLOW | O_NONBLOCK)?;
        if !file.metadata()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "candidate is not a regular file",
            ));
        }
        Ok(file)
    }

    fn owns(&self, path: &Path) -> bool {
        self.outputs.iter().any(|output| path.starts_with(output))
    }

    fn check_path_length(&self, path: &Path) -> io::Result<()> {
        // Include the separator even for `/`; this is a conservative bound.
        if self.project_root.as_os_str().len() + 1 + path.as_os_str().len() > MAX_PATH_BYTES {
            return Err(limit_error("path bytes"));
        }
        Ok(())
    }

    fn enqueue(&mut self, visit: Visit) -> io::Result<()> {
        if self.pending.len() == MAX_PENDING {
            return Err(limit_error("pending directory visits"));
        }
        self.pending.push_back(visit);
        Ok(())
    }

    fn watch_directory(&mut self, path: &Path, directory: &File) -> io::Result<bool> {
        self.check_path_length(path)?;
        // Check before the syscall: even a failing observer must never exceed
        // its kernel-watch cap. At capacity, conservatively stop rather than
        // attempt a registration that might be a new inode.
        if self.watches.len() == MAX_WATCHES {
            return Err(limit_error("directory watches"));
        }
        // The proc symlink intentionally names our already-open directory,
        // rather than resolving the compiler's mutable path a second time.
        let proc_path = CString::new(format!("/proc/self/fd/{}", directory.as_raw_fd()))
            .expect("descriptor path contains no NUL");
        let descriptor = unsafe {
            inotify_add_watch(
                self.events.get_ref().as_raw_fd(),
                proc_path.as_ptr(),
                WATCH_MASK,
            )
        };
        if descriptor < 0 {
            let error = io::Error::last_os_error();
            if is_disappeared(&error) {
                return Ok(false);
            }
            return Err(error);
        }
        if let Some(previous) = self.watches.get_mut(&descriptor) {
            if previous.as_path() == path {
                return Ok(false);
            }
            // A directory moved within the output namespace. Queued old
            // events may now be attributed to its new path; both are hints.
            *previous = path.to_path_buf();
        } else {
            self.watches.insert(descriptor, path.to_path_buf());
        }
        Ok(true)
    }

    fn visit_root(&mut self, path: PathBuf, discover_files: bool) -> io::Result<Option<PathBuf>> {
        // Only the nearest existing parent is watched, never recursively
        // scanned. Its creation events let us advance down missing ancestors.
        let mut parent = path.parent().unwrap();
        loop {
            match open_directory_at(&self.root, parent) {
                Ok(directory) => {
                    self.watch_directory(parent, &directory)?;
                    break;
                }
                Err(error) if is_disappeared(&error) => {
                    let Some(ancestor) = parent.parent() else {
                        return Err(error);
                    };
                    parent = ancestor;
                }
                Err(error) => return Err(error),
            }
        }
        match open_directory_at(&self.root, &path) {
            Ok(directory) => {
                let added = self.watch_directory(&path, &directory)?;
                if added || discover_files {
                    self.enqueue(Visit::Directory {
                        path,
                        discover_files,
                    })?;
                }
                Ok(None)
            }
            Err(error) if is_disappeared(&error) => {
                if discover_files {
                    match self.open_candidate(&self.project_root.join(&path)) {
                        Ok(_) => return Ok(Some(path)),
                        Err(error)
                            if is_disappeared(&error)
                                || error.kind() == io::ErrorKind::InvalidInput => {}
                        Err(error) => return Err(error),
                    }
                }
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn scan_one(&mut self) -> io::Result<Option<PathBuf>> {
        if self.directory.is_none() {
            let Some(visit) = self.pending.pop_front() else {
                return Ok(None);
            };
            match visit {
                Visit::Root(path) => return self.visit_root(path, true),
                Visit::Directory {
                    path,
                    discover_files,
                } => {
                    let directory = match open_directory_at(&self.root, &path) {
                        Ok(directory) => directory,
                        Err(error) if is_disappeared(&error) => return Ok(None),
                        Err(error) => return Err(error),
                    };
                    // read_dir owns its own transient descriptor. Child opens
                    // are re-anchored at the project root, not this iterator,
                    // so a renamed iterator cannot open an escaped descendant.
                    let entries =
                        match std::fs::read_dir(format!("/proc/self/fd/{}", directory.as_raw_fd()))
                        {
                            Ok(entries) => entries,
                            Err(error) if is_disappeared(&error) => return Ok(None),
                            Err(error) => return Err(error),
                        };
                    self.directory = Some(DirectoryCursor {
                        path,
                        entries,
                        discover_files,
                    });
                }
            }
        }
        let cursor = self.directory.as_mut().unwrap();
        let Some(entry) = cursor.entries.next() else {
            self.directory = None;
            return Ok(None);
        };
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if is_disappeared(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let path = cursor.path.join(entry.file_name());
        let discover_files = cursor.discover_files;
        let kind = match entry.file_type() {
            Ok(kind) => kind,
            Err(error) if is_disappeared(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        self.check_path_length(&path)?;
        if kind.is_dir() {
            match open_directory_at(&self.root, &path) {
                Ok(directory) => {
                    let added = self.watch_directory(&path, &directory)?;
                    if added || discover_files {
                        self.enqueue(Visit::Directory {
                            path,
                            discover_files,
                        })?;
                    }
                }
                Err(error) if is_disappeared(&error) => {}
                Err(error) => return Err(error),
            }
        } else if discover_files && kind.is_file() {
            return Ok(Some(path));
        }
        Ok(None)
    }

    fn process_event(&mut self) -> io::Result<Option<PathBuf>> {
        let (event, length) = parse_event(&self.event_buffer[self.event_start..self.event_end])?;
        self.event_start += length;
        if event.mask & IN_Q_OVERFLOW != 0 {
            return Err(io::Error::other(
                "inotify output candidate queue overflowed",
            ));
        }
        if event.mask & IN_IGNORED != 0 {
            self.watches.remove(&event.descriptor);
            return Ok(None);
        }
        let Some(directory) = self.watches.get(&event.descriptor) else {
            return Ok(None);
        };
        let Some(name) = event.name else {
            // MOVE_SELF can leave a stale watch path, but the kernel watch
            // remains counted. All later reads resolve the declared path anew,
            // so moving a watched directory outside cannot grant access there.
            return Ok(None);
        };
        let mask = event.mask;
        let path = directory.join(name);
        if mask & IN_ISDIR != 0 {
            if mask & (IN_CREATE | IN_MOVED_TO) == 0 {
                return Ok(None);
            }
            if self.owns(&path) {
                self.check_path_length(&path)?;
                match open_directory_at(&self.root, &path) {
                    Ok(directory) => {
                        self.watch_directory(&path, &directory)?;
                        self.enqueue(Visit::Directory {
                            path,
                            discover_files: true,
                        })?;
                    }
                    Err(error) if is_disappeared(&error) => {}
                    Err(error) => return Err(error),
                }
            } else {
                // An ancestor of a missing output appeared. Visit only the
                // declared roots below it; never scan this parent or siblings.
                for index in 0..self.outputs.len() {
                    if self.outputs[index].starts_with(&path) {
                        self.enqueue(Visit::Root(self.outputs[index].clone()))?;
                    }
                }
            }
        } else if mask & (IN_CLOSE_WRITE | IN_MOVED_TO) != 0 && self.owns(&path) {
            self.check_path_length(&path)?;
            return Ok(Some(path));
        }
        Ok(None)
    }
}

fn limit_error(resource: &str) -> io::Error {
    io::Error::other(format!(
        "output candidate observer limit exceeded: {resource}"
    ))
}

fn validate_absolute_path(path: &Path) -> io::Result<()> {
    if !path.is_absolute()
        || path.as_os_str().len() > MAX_PATH_BYTES
        || path.as_os_str().as_bytes().contains(&0)
        || path
            .components()
            .any(|component| component == Component::ParentDir)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected a bounded absolute path without parent traversal or NUL",
        ));
    }
    Ok(())
}

fn is_disappeared(error: &io::Error) -> bool {
    // ENOENT, ENOTDIR, ELOOP: a directory or symlink may have been replaced
    // between notification, traversal, and open. None warrants following it.
    matches!(error.raw_os_error(), Some(2 | 20 | 40))
}

fn open_directory_at(root: &File, path: &Path) -> io::Result<File> {
    let mut directory: Option<File> = None;
    for component in path.components() {
        let Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "directory traversal requires relative normal components",
            ));
        };
        let parent = directory.as_ref().unwrap_or(root);
        directory = Some(open_component(
            parent.as_raw_fd(),
            name,
            O_DIRECTORY | O_NOFOLLOW,
        )?);
    }
    match directory {
        Some(directory) => Ok(directory),
        None => root.try_clone(),
    }
}

fn open_component(directory: RawFd, name: &OsStr, flags: c_int) -> io::Result<File> {
    let bytes = name.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_NAME_BYTES
        || bytes.contains(&0)
        || bytes.contains(&b'/')
        || bytes == b"."
        || bytes == b".."
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid path component",
        ));
    }
    let mut terminated = [0u8; MAX_NAME_BYTES + 1];
    terminated[..bytes.len()].copy_from_slice(bytes);
    // O_RDONLY is zero. O_NONBLOCK on candidate leaves prevents FIFO opens
    // from waiting before the regular-file metadata check can reject them.
    let descriptor = unsafe { openat(directory, terminated.as_ptr().cast(), flags | O_CLOEXEC) };
    if descriptor < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openat returned a new descriptor and ownership is transferred.
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

struct Event<'a> {
    descriptor: c_int,
    mask: u32,
    name: Option<&'a OsStr>,
}

fn parse_event(bytes: &[u8]) -> io::Result<(Event<'_>, usize)> {
    let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed inotify record");
    if bytes.len() < EVENT_HEADER_BYTES {
        return Err(malformed());
    }
    // Decode bytes rather than casting: read buffers need not be aligned, and
    // even malformed lengths must never lead to an out-of-bounds dereference.
    let descriptor = i32::from_ne_bytes(bytes[0..4].try_into().unwrap());
    let mask = u32::from_ne_bytes(bytes[4..8].try_into().unwrap());
    let name_length = u32::from_ne_bytes(bytes[12..16].try_into().unwrap()) as usize;
    if name_length > MAX_NAME_BYTES + 1 || name_length > bytes.len() - EVENT_HEADER_BYTES {
        return Err(malformed());
    }
    let length = EVENT_HEADER_BYTES + name_length;
    let name = if name_length == 0 {
        None
    } else {
        let padded = &bytes[EVENT_HEADER_BYTES..length];
        let end = padded
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(malformed)?;
        let name = &padded[..end];
        if name.is_empty() || name.contains(&b'/') || name == b"." || name == b".." {
            return Err(malformed());
        }
        Some(OsStr::from_bytes(name))
    };
    Ok((
        Event {
            descriptor,
            mask,
            name,
        },
        length,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::time::Duration;

    use super::*;

    async fn observe(
        watcher: &mut OutputWatcher,
        expected: &[PathBuf],
    ) -> io::Result<HashSet<PathBuf>> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut observed = HashSet::new();
            while !expected.iter().all(|path| observed.contains(path)) {
                let batch = watcher.next_batch().await?;
                assert!(!batch.is_empty());
                assert!(batch.len() <= MAX_BATCH);
                observed.extend(batch);
            }
            Ok(observed)
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "missing candidate"))?
    }

    #[tokio::test]
    async fn observes_closed_files_before_producer_exits_without_initial_stale_files()
    -> io::Result<()> {
        let project = tempfile::tempdir()?;
        let root = project.path().canonicalize()?;
        let output = root.join("output");
        std::fs::create_dir_all(output.join("nested"))?;
        let stale = output.join("nested/stale");
        std::fs::write(&stale, b"old")?;
        let mut watcher = OutputWatcher::new(&root, vec![output.clone()])?;
        let fresh = output.join("nested/fresh");
        let producer_path = fresh.clone();
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let producer = std::thread::spawn(move || {
            std::fs::write(producer_path, b"closed but producer is alive").unwrap();
            let _ = wait.recv();
        });
        let observed = observe(&mut watcher, &[fresh]).await?;
        assert!(!observed.contains(&stale));
        assert!(!producer.is_finished());
        release.send(()).unwrap();
        producer.join().unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn discovers_late_ancestors_and_moved_in_trees_without_scanning_siblings()
    -> io::Result<()> {
        let project = tempfile::tempdir()?;
        let root = project.path().canonicalize()?;
        let output = root.join("late/parent/output");
        let sibling = root.join("late/unowned/file");
        let mut watcher = OutputWatcher::new(&root, vec![output.clone()])?;
        std::fs::create_dir_all(output.join("nested"))?;
        std::fs::create_dir_all(sibling.parent().unwrap())?;
        std::fs::write(&sibling, b"not ours")?;
        let late = output.join("nested/late");
        std::fs::write(&late, b"created before child watch")?;
        let observed = observe(&mut watcher, &[late]).await?;
        assert!(!observed.contains(&sibling));

        let staging = root.join("staging");
        std::fs::create_dir_all(staging.join("deep"))?;
        std::fs::write(staging.join("deep/moved"), b"already closed")?;
        std::fs::rename(staging, output.join("moved"))?;
        observe(&mut watcher, &[output.join("moved/deep/moved")]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn discovers_file_root_created_before_its_parent_watch() -> io::Result<()> {
        let project = tempfile::tempdir()?;
        let root = project.path().canonicalize()?;
        let output = root.join("late/parent/file");
        let mut watcher = OutputWatcher::new(&root, vec![output.clone()])?;
        std::fs::create_dir_all(output.parent().unwrap())?;
        std::fs::write(&output, b"file root")?;
        observe(&mut watcher, &[output]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn rejects_symlinks_fifos_traversal_and_outside_candidates() -> io::Result<()> {
        unsafe extern "C" {
            fn mkfifo(path: *const c_char, mode: u32) -> c_int;
        }
        let project = tempfile::tempdir()?;
        let root = project.path().canonicalize()?;
        let output = root.join("output");
        std::fs::create_dir_all(output.join("parent"))?;
        let watcher = OutputWatcher::new(&root, vec![output.clone()])?;
        let outside = root.join("outside");
        std::fs::create_dir(&outside)?;
        std::fs::write(outside.join("file"), b"outside")?;
        symlink(outside.join("file"), output.join("link"))?;
        assert!(watcher.open_candidate(&output.join("link")).is_err());
        assert!(watcher.open_candidate(&outside.join("file")).is_err());
        assert!(
            watcher
                .open_candidate(&output.join("../outside/file"))
                .is_err()
        );
        assert!(watcher.open_candidate(&output).is_err());

        let fifo = output.join("fifo");
        let fifo_name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { mkfifo(fifo_name.as_ptr(), 0o600) }, 0);
        assert!(watcher.open_candidate(&fifo).is_err());

        std::fs::rename(output.join("parent"), root.join("old-parent"))?;
        symlink(&outside, output.join("parent"))?;
        assert!(watcher.open_candidate(&output.join("parent/file")).is_err());
        let good = output.join("regular");
        std::fs::write(&good, b"owned")?;
        assert_eq!(watcher.open_candidate(&good)?.metadata()?.len(), 5);
        Ok(())
    }

    #[tokio::test]
    async fn bounds_batches_and_preserves_unparsed_events() -> io::Result<()> {
        let project = tempfile::tempdir()?;
        let root = project.path().canonicalize()?;
        let output = root.join("output");
        std::fs::create_dir(&output)?;
        let mut watcher = OutputWatcher::new(&root, vec![output.clone()])?;
        let mut expected = Vec::new();
        for index in 0..MAX_BATCH + 17 {
            let path = output.join(index.to_string());
            std::fs::write(&path, b"candidate")?;
            expected.push(path);
        }
        let observed = observe(&mut watcher, &expected).await?;
        assert_eq!(observed.len(), expected.len());
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_wait_does_not_poison_observer() -> io::Result<()> {
        let project = tempfile::tempdir()?;
        let root = project.path().canonicalize()?;
        let output = root.join("file");
        let mut watcher = OutputWatcher::new(&root, vec![output.clone()])?;
        assert!(
            tokio::time::timeout(Duration::from_millis(1), watcher.next_batch())
                .await
                .is_err()
        );
        let mut producer = File::create(&output)?;
        producer.write_all(b"not closed yet")?;
        drop(producer);
        observe(&mut watcher, &[output]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn overflow_is_explicit_and_permanently_disables_observer() -> io::Result<()> {
        let project = tempfile::tempdir()?;
        let root = project.path().canonicalize()?;
        let mut watcher = OutputWatcher::new(&root, vec![root.join("file")])?;
        watcher.event_buffer[0..4].copy_from_slice(&(-1i32).to_ne_bytes());
        watcher.event_buffer[4..8].copy_from_slice(&IN_Q_OVERFLOW.to_ne_bytes());
        watcher.event_end = EVENT_HEADER_BYTES;
        assert!(
            watcher
                .next_batch()
                .await
                .unwrap_err()
                .to_string()
                .contains("overflow")
        );
        assert!(
            watcher
                .next_batch()
                .await
                .unwrap_err()
                .to_string()
                .contains("disabled")
        );
        Ok(())
    }

    #[test]
    fn rejects_excess_roots_before_registering_descriptors() -> io::Result<()> {
        let project = tempfile::tempdir()?;
        let root = project.path().canonicalize()?;
        // No Tokio runtime: validation must reject before AsyncFd creation.
        assert!(OutputWatcher::new(&root, vec![root.join("file"); MAX_ROOTS + 1]).is_err());
        assert!(OutputWatcher::new(&root, vec![root.join("../escape")]).is_err());
        Ok(())
    }

    #[test]
    fn malformed_event_records_are_rejected() {
        assert!(parse_event(&[0; EVENT_HEADER_BYTES - 1]).is_err());
        let mut record = [0u8; EVENT_HEADER_BYTES + 16];
        record[12..16].copy_from_slice(&u32::MAX.to_ne_bytes());
        assert!(parse_event(&record).is_err());
        record[12..16].copy_from_slice(&16u32.to_ne_bytes());
        record[EVENT_HEADER_BYTES..].fill(b'x');
        assert!(parse_event(&record).is_err());
        record[EVENT_HEADER_BYTES..].fill(0);
        record[EVENT_HEADER_BYTES..EVENT_HEADER_BYTES + 3].copy_from_slice(b"../");
        assert!(parse_event(&record).is_err());
        record[EVENT_HEADER_BYTES..].fill(0);
        record[EVENT_HEADER_BYTES..EVENT_HEADER_BYTES + 2].copy_from_slice(b"..");
        assert!(parse_event(&record).is_err());
        record[EVENT_HEADER_BYTES..EVENT_HEADER_BYTES + 2].copy_from_slice(b"ok");
        let (event, length) = parse_event(&record).unwrap();
        assert_eq!(event.name, Some(OsStr::new("ok")));
        assert_eq!(length, record.len());
    }
}
