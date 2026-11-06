/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Bounded ownership of private speculative output snapshots.
//!
//! A capture reserves space for two independent snapshots: a normalized
//! publication view and a stable CAS upload source. Registration trusts only the
//! caller's hash of the private snapshot, never metadata from a live compiler
//! output. The final materializer supplies its independently computed digest.
//! The registry owns weak references; action sessions own unconsumed entries,
//! while consumed publication files and uploads own their retention separately.
//! Publication views keep their final 0644/0755 mode. Immutability comes from
//! exclusive private ownership and closed writer handles, not a chmod seal.

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::Permissions;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Weak;

use buck2_common::file_ops::metadata::FileDigest;
use buck2_common::file_ops::metadata::FileMetadata;
use parking_lot::Mutex;
use tempfile::TempDir;
use tempfile::TempPath;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

const RETAINED_BYTES: u64 = 512 * 1024 * 1024;
const SNAPSHOT_BLOCK: u64 = 4096;
const RETAINED_PAIRS: usize = 4096;
const DIRECTORY_PREFIX: &str = ".buck2-output-pipeline-";
const O_NOFOLLOW: i32 = 0x20000;
const O_NONBLOCK: i32 = 0x800;

unsafe extern "C" {
    fn geteuid() -> u32;
}

/// A private capture directory whose lock survives as long as either snapshot.
/// The caller must place it outside every declared output root, under an owned
/// output parent. Field order keeps the lock held throughout directory removal.
pub(crate) struct CaptureDirectory {
    directory: TempDir,
    _lock: File,
}

impl CaptureDirectory {
    pub(crate) fn create(parent: &Path) -> io::Result<Arc<Self>> {
        clean_stale_directories(parent)?;
        let directory = tempfile::Builder::new()
            .prefix(DIRECTORY_PREFIX)
            .tempdir_in(parent)?;
        fs::set_permissions(directory.path(), Permissions::from_mode(0o700))?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(O_NOFOLLOW | O_NONBLOCK)
            .open(directory.path().join("lock"))?;
        lock.set_permissions(Permissions::from_mode(0o600))?;
        // A concurrent scavenger can win the crash-before-lock window. Never
        // wait on it or continue using a directory that it might be deleting.
        lock.try_lock().map_err(io::Error::from)?;
        let current = fs::symlink_metadata(directory.path().join("lock"))?;
        if !same_inode(&lock.metadata()?, &current) {
            return Err(io::Error::other("Capture directory lock was replaced"));
        }
        Ok(Arc::new(Self {
            directory,
            _lock: lock,
        }))
    }

    pub(crate) fn path(&self) -> &Path {
        self.directory.path()
    }
}

fn same_inode(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}

fn private_directory(metadata: &fs::Metadata, uid: u32) -> bool {
    metadata.is_dir() && metadata.uid() == uid && metadata.mode() & 0o7777 == 0o700
}

fn private_lock(metadata: &fs::Metadata, uid: u32) -> bool {
    metadata.is_file()
        && metadata.uid() == uid
        && metadata.nlink() == 1
        && metadata.mode() & 0o7777 == 0o600
}

/// Best-effort scavenging of recognized, unlocked directories only. Iteration
/// retains neither a list of candidates nor their file descriptors. Linux's
/// remove_dir_all does not follow symlinks, including links inside the tree.
fn clean_stale_directories(parent: &Path) -> io::Result<()> {
    let uid = unsafe { geteuid() };
    for entry in fs::read_dir(parent)? {
        let Ok(entry) = entry else { continue };
        if !entry
            .file_name()
            .as_encoded_bytes()
            .starts_with(DIRECTORY_PREFIX.as_bytes())
        {
            continue;
        }
        let path = entry.path();
        let Ok(directory_metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if !private_directory(&directory_metadata, uid) {
            continue;
        }
        let lock_path = path.join("lock");
        let original_lock = match fs::symlink_metadata(&lock_path) {
            Ok(metadata) if private_lock(&metadata, uid) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // remove_dir is itself the emptiness check: a concurrently
                // created lock, an unrecognized file, or a symlink prevents it.
                let _ignored = fs::remove_dir(&path);
                continue;
            }
            _ => continue,
        };
        let Ok(lock) = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(O_NOFOLLOW | O_NONBLOCK)
            .open(&lock_path)
        else {
            continue;
        };
        let Ok(lock_metadata) = lock.metadata() else {
            continue;
        };
        if !private_lock(&lock_metadata, uid)
            || !same_inode(&original_lock, &lock_metadata)
            || lock.try_lock().is_err()
        {
            continue;
        }
        let Ok(current_directory) = fs::symlink_metadata(&path) else {
            continue;
        };
        let Ok(current_lock) = fs::symlink_metadata(&lock_path) else {
            continue;
        };
        if !private_directory(&current_directory, uid)
            || !same_inode(&directory_metadata, &current_directory)
            || !private_lock(&current_lock, uid)
            || !same_inode(&lock_metadata, &current_lock)
        {
            continue;
        }
        // Keep the exclusive lock alive until removal has finished. A live
        // directory cannot be scavenged by another process opening its lock.
        let _ignored = fs::remove_dir_all(&path);
    }
    Ok(())
}

struct RetentionBudget {
    blocks: Arc<Semaphore>,
    pairs: Arc<Semaphore>,
}

impl RetentionBudget {
    fn new(blocks: usize, pairs: usize) -> Self {
        Self {
            blocks: Arc::new(Semaphore::new(blocks)),
            pairs: Arc::new(Semaphore::new(pairs)),
        }
    }

    fn reserve(
        &self,
        size: u64,
        directory: Arc<CaptureDirectory>,
    ) -> Option<Arc<CaptureRetention>> {
        let blocks = size.checked_add(SNAPSHOT_BLOCK - 1)? / SNAPSHOT_BLOCK;
        let blocks = blocks.max(1);
        let charged = u32::try_from(blocks.checked_mul(2)?).ok()?;
        let pair = self.pairs.clone().try_acquire_owned().ok()?;
        let bytes = self.blocks.clone().try_acquire_many_owned(charged).ok()?;
        Some(Arc::new(CaptureRetention {
            directory,
            snapshot_capacity: blocks * SNAPSHOT_BLOCK,
            _bytes: bytes,
            _pair: pair,
        }))
    }
}

static BUDGET: LazyLock<RetentionBudget> = LazyLock::new(|| {
    RetentionBudget::new((RETAINED_BYTES / SNAPSHOT_BLOCK) as usize, RETAINED_PAIRS)
});

/// Shared by the publication view and the independent CAS upload snapshot.
/// No source file descriptor is retained. Directory cleanup precedes permit
/// release when this is the last owner of the directory.
pub(crate) struct CaptureRetention {
    directory: Arc<CaptureDirectory>,
    snapshot_capacity: u64,
    _bytes: OwnedSemaphorePermit,
    _pair: OwnedSemaphorePermit,
}

impl CaptureRetention {
    pub(crate) fn directory(&self) -> &Path {
        self.directory.path()
    }
}

/// Nonblocking admission, charging each of two snapshots in 4 KiB blocks, with
/// one block per empty snapshot. Oversized captures are rejected, not exempted.
pub(crate) fn reserve(
    size: u64,
    directory: Arc<CaptureDirectory>,
) -> Option<Arc<CaptureRetention>> {
    BUDGET.reserve(size, directory)
}

pub(crate) struct PreparedFile {
    // Drop the file before its retention lease can release permits/directory.
    pub(crate) path: TempPath,
    pub(crate) metadata: FileMetadata,
    pub(crate) retention: Arc<CaptureRetention>,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct Key {
    digest: FileDigest,
    executable: bool,
    device: u64,
}

#[derive(Default)]
struct Registry {
    entries: Mutex<HashMap<Key, Weak<PreparedEntry>>>,
}

static REGISTRY: LazyLock<Arc<Registry>> = LazyLock::new(|| Arc::new(Registry::default()));

/// An action keeps this handle until materialization or cancellation. Once its
/// file is taken, this handle retains neither the directory nor any permits.
pub(crate) struct PreparedEntry {
    key: Key,
    registry: Arc<Registry>,
    file: Mutex<Option<PreparedFile>>,
}

impl PreparedEntry {
    pub(crate) fn is_available(&self) -> bool {
        self.file.lock().is_some()
    }
}

impl Registry {
    fn register(self: &Arc<Self>, file: PreparedFile) -> io::Result<Arc<PreparedEntry>> {
        // All filesystem operations, including dropping an unused duplicate,
        // happen without the registry mutex held.
        let metadata = fs::symlink_metadata(&file.path)?;
        let expected_mode = if file.metadata.is_executable {
            0o755
        } else {
            0o644
        };
        if file.path.parent() != Some(file.retention.directory())
            || !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != unsafe { geteuid() }
            || metadata.len() != file.metadata.digest.size()
            || metadata.len() > file.retention.snapshot_capacity
            || metadata.mode() & 0o7777 != expected_mode
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Prepared output must be a private normalized regular snapshot of the expected size",
            ));
        }
        let key = Key {
            digest: *file.metadata.digest.data(),
            executable: file.metadata.is_executable,
            device: metadata.dev(),
        };
        let mut entries = self.entries.lock();
        let existing = entries.get(&key).and_then(Weak::upgrade);
        let available = existing
            .as_ref()
            .is_some_and(|entry| entry.file.lock().is_some());
        if available {
            drop(entries);
            // The incoming file and its lease drop outside the registry lock.
            return Ok(existing.unwrap());
        }
        let entry = Arc::new(PreparedEntry {
            key,
            registry: self.clone(),
            file: Mutex::new(Some(file)),
        });
        entries.insert(key, Arc::downgrade(&entry));
        drop(entries);
        // This can be the final strong reference, whose destructor locks the
        // registry. Never drop it while holding either mutex.
        drop(existing);
        Ok(entry)
    }

    fn take(
        &self,
        destination: &Path,
        digest: &FileDigest,
        executable: bool,
    ) -> io::Result<Option<PreparedFile>> {
        if self.entries.lock().is_empty() {
            return Ok(None);
        }
        let metadata = match fs::symlink_metadata(destination) {
            Ok(metadata) => metadata,
            // Cache restoration can publish a view before its destination
            // exists. That path uses ordinary staging, not a producer snapshot.
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        if !metadata.is_file() {
            return Ok(None);
        }
        // Only the filesystem device is read from the destination. Its size,
        // mtime and contents cannot substitute for the caller's final digest.
        Ok(self.take_key(Key {
            digest: *digest,
            executable,
            device: metadata.dev(),
        }))
    }

    fn take_key(&self, key: Key) -> Option<PreparedFile> {
        let entry = {
            let mut entries = self.entries.lock();
            entries.remove(&key).and_then(|entry| entry.upgrade())
        };
        // Removing the weak row first bounds consumed/dead registry metadata
        // and lets a subsequent registration safely install a replacement.
        entry.as_ref().and_then(|entry| entry.file.lock().take())
    }
}

impl Drop for PreparedEntry {
    fn drop(&mut self) {
        let identity = self as *const Self;
        let mut entries = self.registry.entries.lock();
        if entries
            .get(&self.key)
            .is_some_and(|entry| std::ptr::eq(entry.as_ptr(), identity))
        {
            entries.remove(&self.key);
        }
        // The guard drops before the file field: no filesystem work or permit
        // release under this lock, and an older entry cannot erase its successor.
    }
}

/// The caller has already hashed, normalized and synchronized this private
/// publication file. Registration validates its shape, not a live output path.
pub(crate) fn register(file: PreparedFile) -> io::Result<Arc<PreparedEntry>> {
    REGISTRY.register(file)
}

/// Consume at most one matching prepared publication view on the destination
/// filesystem. The caller must provide the authoritative final output digest.
pub(crate) fn take(
    destination: &Path,
    digest: &FileDigest,
    executable: bool,
) -> io::Result<Option<PreparedFile>> {
    REGISTRY.take(destination, digest, executable)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;
    use std::sync::Barrier;

    use buck2_common::file_ops::metadata::TrackedFileDigest;
    use buck2_execute::digest_config::DigestConfig;

    use super::*;

    fn budget() -> RetentionBudget {
        RetentionBudget::new((RETAINED_BYTES / SNAPSHOT_BLOCK) as usize, RETAINED_PAIRS)
    }

    fn prepared(
        budget: &RetentionBudget,
        directory: Arc<CaptureDirectory>,
        bytes: &[u8],
        executable: bool,
    ) -> io::Result<PreparedFile> {
        let retention = budget.reserve(bytes.len() as u64, directory).unwrap();
        let mut file = tempfile::NamedTempFile::new_in(retention.directory())?;
        file.write_all(bytes)?;
        file.as_file()
            .set_permissions(Permissions::from_mode(if executable {
                0o755
            } else {
                0o644
            }))?;
        file.as_file().sync_all()?;
        Ok(PreparedFile {
            path: file.into_temp_path(),
            metadata: FileMetadata {
                digest: TrackedFileDigest::from_content(
                    bytes,
                    DigestConfig::testing_default().cas_digest_config(),
                ),
                is_executable: executable,
            },
            retention,
        })
    }

    fn stale_directory(parent: &Path, name: &str, with_lock: bool) -> io::Result<PathBuf> {
        let path = parent.join(format!("{DIRECTORY_PREFIX}{name}"));
        fs::create_dir(&path)?;
        fs::set_permissions(&path, Permissions::from_mode(0o700))?;
        if with_lock {
            let lock = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(path.join("lock"))?;
            lock.set_permissions(Permissions::from_mode(0o600))?;
        }
        Ok(path)
    }

    #[test]
    fn byte_budget_saturates_and_releases_without_singleton_exception() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let budget = budget();
        assert!(
            budget
                .reserve(RETAINED_BYTES / 2 + 1, directory.clone())
                .is_none()
        );
        assert!(budget.reserve(u64::MAX, directory.clone()).is_none());
        let full = budget
            .reserve(RETAINED_BYTES / 2, directory.clone())
            .unwrap();
        assert_eq!(budget.blocks.available_permits(), 0);
        assert!(budget.reserve(0, directory.clone()).is_none());
        let upload = full.clone();
        drop(full);
        assert!(budget.reserve(1, directory.clone()).is_none());
        drop(upload);
        assert_eq!(budget.pairs.available_permits(), RETAINED_PAIRS);
        assert!(budget.reserve(RETAINED_BYTES / 2, directory).is_some());
        Ok(())
    }

    #[test]
    fn rounds_each_snapshot_and_limits_retained_pairs() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let small = RetentionBudget::new(6, 4);
        let empty = small.reserve(0, directory.clone()).unwrap();
        assert_eq!(small.blocks.available_permits(), 4);
        let rounded = small.reserve(4097, directory.clone()).unwrap();
        assert_eq!(small.blocks.available_permits(), 0);
        assert!(small.reserve(1, directory.clone()).is_none());
        drop(rounded);
        let single = small.reserve(4096, directory.clone()).unwrap();
        assert_eq!(small.blocks.available_permits(), 2);
        drop((empty, single));
        assert_eq!(small.blocks.available_permits(), 6);
        assert_eq!(small.pairs.available_permits(), 4);

        let budget = budget();
        let retained: Vec<_> = (0..RETAINED_PAIRS)
            .map(|_| budget.reserve(0, directory.clone()).unwrap())
            .collect();
        assert!(budget.blocks.available_permits() > 0);
        assert!(budget.reserve(0, directory.clone()).is_none());
        drop(retained);
        assert_eq!(budget.pairs.available_permits(), RETAINED_PAIRS);
        assert!(budget.reserve(0, directory).is_some());
        Ok(())
    }

    #[test]
    fn deduplicates_until_consumed_and_does_not_retain_consumed_budget() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let budget = budget();
        let registry = Arc::new(Registry::default());
        let file = prepared(&budget, directory.clone(), b"snapshot", false)?;
        let path = file.path.to_path_buf();
        let digest = *file.metadata.digest.data();
        let upload = file.retention.clone();
        let retention = Arc::downgrade(&file.retention);
        let entry = registry.register(file)?;
        let duplicate = prepared(&budget, directory.clone(), b"snapshot", false)?;
        let duplicate_path = duplicate.path.to_path_buf();
        let duplicate_retention = Arc::downgrade(&duplicate.retention);
        let second_owner = registry.register(duplicate)?;
        assert!(Arc::ptr_eq(&entry, &second_owner));
        assert!(!duplicate_path.exists());
        assert!(duplicate_retention.upgrade().is_none());
        assert_eq!(registry.entries.lock().len(), 1);

        let destination = parent.path().join("output");
        fs::write(&destination, b"snapshot")?;
        let taken = registry.take(&destination, &digest, false)?.unwrap();
        assert_eq!(&*taken.path, path.as_path());
        assert!(registry.take(&destination, &digest, false)?.is_none());
        assert!(registry.entries.lock().is_empty());
        drop(taken);
        assert!(!path.exists());
        assert!(retention.upgrade().is_some());
        drop(upload);
        assert!(retention.upgrade().is_none());
        assert_eq!(budget.pairs.available_permits(), RETAINED_PAIRS);
        // Both action handles remain alive after all captured data was released.
        drop((entry, second_owner));
        Ok(())
    }

    #[test]
    fn action_cancellation_cleans_unused_file_directory_and_weak_row() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let directory_path = directory.path().to_path_buf();
        let directory_weak = Arc::downgrade(&directory);
        let budget = budget();
        let registry = Arc::new(Registry::default());
        let file = prepared(&budget, directory, b"cancelled", true)?;
        let path = file.path.to_path_buf();
        let retention = Arc::downgrade(&file.retention);
        let entry = registry.register(file)?;
        let other_owner = entry.clone();
        drop(entry);
        assert!(path.exists());
        drop(other_owner);
        assert!(!path.exists());
        assert!(!directory_path.exists());
        assert!(directory_weak.upgrade().is_none());
        assert!(retention.upgrade().is_none());
        assert!(registry.entries.lock().is_empty());
        assert_eq!(budget.pairs.available_permits(), RETAINED_PAIRS);
        Ok(())
    }

    #[test]
    fn dropping_consumed_entry_does_not_remove_same_key_replacement() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let budget = RetentionBudget::new(2, 1);
        let registry = Arc::new(Registry::default());
        let old = registry.register(prepared(&budget, directory.clone(), b"same", false)?)?;
        drop(registry.take_key(old.key).unwrap());
        // One pair is enough even though the consumed action entry still lives.
        let replacement = registry.register(prepared(&budget, directory, b"same", false)?)?;
        drop(old);
        assert_eq!(registry.entries.lock().len(), 1);
        assert!(registry.take_key(replacement.key).is_some());
        assert!(registry.entries.lock().is_empty());
        Ok(())
    }

    #[test]
    fn requires_digest_mode_and_device_not_destination_size_or_mtime() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let budget = budget();
        let registry = Arc::new(Registry::default());
        let entry = registry.register(prepared(&budget, directory, b"expected", false)?)?;
        let destination = parent.path().join("output");
        assert!(
            registry
                .take(&destination, &entry.key.digest, false)?
                .is_none()
        );
        fs::write(&destination, b"different bytes of a different size")?;
        let wrong_digest = FileDigest::from_content(
            b"unexpected",
            DigestConfig::testing_default().cas_digest_config(),
        );
        assert!(registry.take(&destination, &wrong_digest, false)?.is_none());
        assert!(
            registry
                .take(&destination, &entry.key.digest, true)?
                .is_none()
        );
        assert!(
            registry
                .take_key(Key {
                    device: entry.key.device.wrapping_add(1),
                    ..entry.key
                })
                .is_none()
        );
        assert_eq!(registry.entries.lock().len(), 1);
        // The lookup's authority is the supplied digest. Live-path metadata
        // must not be used to accept/reject a digest supplied by the final hasher.
        assert!(
            registry
                .take(&destination, &entry.key.digest, false)?
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn never_follows_destination_symlinks_and_discards_dead_weak_rows() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let budget = budget();
        let registry = Arc::new(Registry::default());
        let file = prepared(&budget, directory, b"regular", false)?;
        let destination = parent.path().join("link");
        symlink(&file.path, &destination)?;
        let entry = registry.register(file)?;
        assert!(
            registry
                .take(&destination, &entry.key.digest, false)?
                .is_none()
        );
        assert!(
            registry
                .take(parent.path(), &entry.key.digest, false)?
                .is_none()
        );
        let key = entry.key;
        drop(entry);
        registry.entries.lock().insert(key, Weak::new());
        assert!(registry.take_key(key).is_none());
        assert!(registry.entries.lock().is_empty());
        Ok(())
    }

    #[test]
    fn rejects_wrong_size_aliases_symlinks_and_nonprivate_views() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let budget = budget();
        let registry = Arc::new(Registry::default());
        let mut wrong_size = prepared(&budget, directory.clone(), b"short", false)?;
        wrong_size.metadata.digest = TrackedFileDigest::from_content(
            b"a longer digest",
            DigestConfig::testing_default().cas_digest_config(),
        );
        assert!(registry.register(wrong_size).is_err());

        let mut undercharged = prepared(&budget, directory.clone(), &vec![0; 4097], false)?;
        undercharged.retention = budget.reserve(1, directory.clone()).unwrap();
        assert!(registry.register(undercharged).is_err());

        let file = prepared(&budget, directory.clone(), b"target", false)?;
        let target = parent.path().join("target");
        fs::rename(&file.path, &target)?;
        symlink(&target, &file.path)?;
        assert!(registry.register(file).is_err());
        assert_eq!(fs::read(&target)?, b"target");

        let file = prepared(&budget, directory.clone(), b"aliased", false)?;
        let alias = parent.path().join("alias");
        fs::hard_link(&file.path, &alias)?;
        assert!(registry.register(file).is_err());
        assert_eq!(fs::read(alias)?, b"aliased");

        let file = prepared(&budget, directory.clone(), b"not-a-directory", false)?;
        let path = file.path.to_path_buf();
        fs::remove_file(&path)?;
        fs::create_dir(&path)?;
        assert!(registry.register(file).is_err());
        fs::remove_dir(path)?;

        let mut file = prepared(&budget, directory, b"outside", false)?;
        let outside = tempfile::NamedTempFile::new_in(parent.path())?.into_temp_path();
        fs::rename(&file.path, &outside)?;
        file.path = outside;
        assert!(registry.register(file).is_err());
        assert!(registry.entries.lock().is_empty());
        assert_eq!(budget.pairs.available_permits(), RETAINED_PAIRS);
        Ok(())
    }

    #[test]
    fn concurrent_takers_consume_exactly_once() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let directory = CaptureDirectory::create(parent.path())?;
        let budget = budget();
        let registry = Arc::new(Registry::default());
        let entry = registry.register(prepared(&budget, directory, b"one-shot", false)?)?;
        let barrier = Barrier::new(8);
        let count = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        barrier.wait();
                        usize::from(registry.take_key(entry.key).is_some())
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .sum::<usize>()
        });
        assert_eq!(count, 1);
        assert!(registry.entries.lock().is_empty());
        assert_eq!(budget.pairs.available_permits(), RETAINED_PAIRS);
        Ok(())
    }

    #[test]
    fn scavenges_crashed_directories_without_following_symlinks() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let outside = tempfile::tempdir()?;
        fs::write(outside.path().join("keep"), b"outside")?;
        let stale = stale_directory(parent.path(), "stale", true)?;
        fs::write(stale.join("view-dead"), b"abandoned")?;
        symlink(outside.path(), stale.join("upload-link"))?;
        let empty = stale_directory(parent.path(), "before-lock", false)?;
        let active = CaptureDirectory::create(parent.path())?;
        assert!(!stale.exists());
        assert!(!empty.exists());
        assert_eq!(fs::read(outside.path().join("keep"))?, b"outside");
        fs::write(active.path().join("view-live"), b"live")?;
        let next = CaptureDirectory::create(parent.path())?;
        assert_eq!(fs::read(active.path().join("view-live"))?, b"live");
        assert!(next.path().exists());
        Ok(())
    }

    #[test]
    fn scavenger_preserves_unrecognized_directory_and_lock_shapes() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let missing_lock = stale_directory(parent.path(), "unrecognized", false)?;
        fs::write(missing_lock.join("keep"), b"unknown")?;
        let wrong_directory_mode = stale_directory(parent.path(), "public", true)?;
        fs::set_permissions(&wrong_directory_mode, Permissions::from_mode(0o755))?;
        let wrong_lock_mode = stale_directory(parent.path(), "bad-lock", true)?;
        fs::set_permissions(wrong_lock_mode.join("lock"), Permissions::from_mode(0o644))?;
        let linked_lock = stale_directory(parent.path(), "linked-lock", true)?;
        fs::hard_link(linked_lock.join("lock"), parent.path().join("lock-alias"))?;
        let symlinked_lock = stale_directory(parent.path(), "symlinked-lock", false)?;
        symlink(
            parent.path().join("lock-alias"),
            symlinked_lock.join("lock"),
        )?;
        let directory_link = parent
            .path()
            .join(format!("{DIRECTORY_PREFIX}directory-link"));
        symlink(&missing_lock, &directory_link)?;
        let ordinary = parent.path().join("unrelated");
        fs::create_dir(&ordinary)?;
        let _active = CaptureDirectory::create(parent.path())?;
        for path in [
            missing_lock,
            wrong_directory_mode,
            wrong_lock_mode,
            linked_lock,
            symlinked_lock,
            directory_link,
            ordinary,
        ] {
            assert!(fs::symlink_metadata(path).is_ok());
        }
        Ok(())
    }
}
