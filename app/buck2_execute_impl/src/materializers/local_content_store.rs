/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Optional, machine-local content store. Objects are never hardlinked into a
//! worktree: every publication is a verified COW snapshot and atomic rename.
//! A process-shared lock protects the SQLite LRU and object namespace. The
//! database is an eviction index, not an authority for content correctness.

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::fs::Permissions;
    use std::fs::{self};
    use std::io::ErrorKind;
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::DirBuilderExt;
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::LazyLock;
    use std::sync::Mutex;

    use buck2_common::file_ops::metadata::FileDigest;
    use buck2_error::BuckErrorContext;
    use buck2_execute::digest_config::DigestConfig;
    use rusqlite::Connection;
    use rusqlite::OptionalExtension;
    use rusqlite::params;

    // Linux x86-64 UAPI.
    const FICLONE: std::ffi::c_ulong = 0x40049409;
    const O_NOFOLLOW: i32 = 0x20000;
    const O_NONBLOCK: i32 = 0x800;
    unsafe extern "C" {
        fn ioctl(fd: i32, request: std::ffi::c_ulong, ...) -> i32;
        fn geteuid() -> u32;
    }

    fn error(message: impl std::fmt::Display) -> buck2_error::Error {
        buck2_error::buck2_error!(buck2_error::ErrorTag::MaterializationError, "{}", message)
    }

    fn open_file(path: &Path) -> buck2_error::Result<File> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(O_NOFOLLOW | O_NONBLOCK)
            .open(path)?;
        if !file.metadata()?.is_file() {
            return Err(error(format!(
                "Content store requires a regular file: {}",
                path.display()
            )));
        }
        Ok(file)
    }

    fn clone_file(source: &File, destination: &File) -> buck2_error::Result<()> {
        // A copy fallback would violate the configured storage contract. A
        // cross-device or non-reflink filesystem is therefore an explicit error.
        if unsafe { ioctl(destination.as_raw_fd(), FICLONE, source.as_raw_fd()) } != 0 {
            return Err(error(format!(
                "Buck2 local content store requires same-filesystem reflinks: {}",
                std::io::Error::last_os_error()
            )));
        }
        Ok(())
    }

    fn verify(
        file: &mut File,
        expected: &FileDigest,
        config: DigestConfig,
    ) -> buck2_error::Result<()> {
        file.seek(SeekFrom::Start(0))?;
        let size = file.metadata()?.len();
        if size != expected.size() {
            return Err(error(format!(
                "Local content store size mismatch: expected {}, got {size}",
                expected.size()
            )));
        }
        let algorithm = config
            .cas_digest_config()
            .algorithm_for_family(expected.raw_digest().algorithm())
            .ok_or_else(|| error("Content digest algorithm is not enabled"))?;
        // A mutable hint can grow after metadata was checked. Never scan beyond
        // the expected artifact plus the byte needed to reject an oversized file.
        let actual = FileDigest::from_reader_for_algorithm(
            file.take(expected.size().saturating_add(1)),
            algorithm,
        )?;
        if actual != *expected {
            return Err(error(format!(
                "Local content store digest mismatch: expected {expected}, got {actual}"
            )));
        }
        Ok(())
    }

    fn remove(path: &Path) -> buck2_error::Result<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub(super) struct Store {
        root: PathBuf,
        lock: File,
        db: Connection,
        max_bytes: u64,
        max_entries: u64,
    }

    impl Store {
        fn open(root: PathBuf, max_bytes: u64, max_entries: u64) -> buck2_error::Result<Self> {
            if !root.is_absolute()
                || max_bytes == 0
                || max_entries == 0
                || max_bytes > i64::MAX as u64
                || max_entries > i64::MAX as u64
            {
                return Err(error(
                    "BUCK2_LOCAL_CONTENT_STORE needs an absolute path and positive i64 byte/entry limits",
                ));
            }
            // Only the final directory is created: its parent must be selected
            // by the operator, not silently constructed through symlinks.
            match fs::DirBuilder::new().mode(0o700).create(&root) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::AlreadyExists => {}
                Err(e) => return Err(e.into()),
            }
            let metadata = fs::symlink_metadata(&root)?;
            if !metadata.is_dir()
                || metadata.uid() != unsafe { geteuid() }
                || metadata.mode() & 0o077 != 0
                || fs::canonicalize(&root)? != root
            {
                return Err(error(
                    "BUCK2_LOCAL_CONTENT_STORE must be a canonical, owner-only directory owned by the current UID",
                ));
            }
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .mode(0o600)
                .custom_flags(O_NOFOLLOW)
                .open(root.join("lock"))?;
            let metadata = lock.metadata()?;
            if !metadata.is_file()
                || metadata.uid() != unsafe { geteuid() }
                || metadata.nlink() != 1
            {
                return Err(error("Unsafe local content store lock"));
            }
            lock.lock()?;
            // Refuse symlinked metadata, including SQLite's transient journal.
            for name in ["index.sqlite", "index.sqlite-journal"] {
                if let Ok(m) = fs::symlink_metadata(root.join(name)) {
                    if !m.is_file() || m.uid() != unsafe { geteuid() } || m.nlink() != 1 {
                        return Err(error("Unsafe local content store metadata file"));
                    }
                }
            }
            let db = Connection::open(root.join("index.sqlite"))?;
            db.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA auto_vacuum=FULL;
                CREATE TABLE IF NOT EXISTS objects (key TEXT PRIMARY KEY, size INTEGER NOT NULL, used INTEGER NOT NULL);
                CREATE INDEX IF NOT EXISTS lru ON objects(used);
                CREATE TABLE IF NOT EXISTS clock (id INTEGER PRIMARY KEY CHECK(id=1), tick INTEGER NOT NULL);
                INSERT OR IGNORE INTO clock VALUES(1,0);
                CREATE TABLE IF NOT EXISTS totals (id INTEGER PRIMARY KEY CHECK(id=1), bytes INTEGER NOT NULL, entries INTEGER NOT NULL);
                INSERT OR IGNORE INTO totals SELECT 1,COALESCE(SUM(size),0),COUNT(*) FROM objects;
                CREATE TRIGGER IF NOT EXISTS object_added AFTER INSERT ON objects BEGIN UPDATE totals SET bytes=bytes+NEW.size,entries=entries+1 WHERE id=1; END;
                CREATE TRIGGER IF NOT EXISTS object_removed AFTER DELETE ON objects BEGIN UPDATE totals SET bytes=bytes-OLD.size,entries=entries-1 WHERE id=1; END;")?;
            // Weak references do not retain file data. Keep them after object
            // eviction so a live worktree can preserve the old extent generation.
            db.execute_batch("CREATE TABLE IF NOT EXISTS views (path TEXT PRIMARY KEY, key TEXT NOT NULL, used INTEGER NOT NULL);
                CREATE INDEX IF NOT EXISTS views_by_key ON views(key,used DESC,path);
                CREATE INDEX IF NOT EXISTS views_lru ON views(used,path);
                CREATE TABLE IF NOT EXISTS view_totals (id INTEGER PRIMARY KEY CHECK(id=1), entries INTEGER NOT NULL);
                INSERT OR IGNORE INTO view_totals SELECT 1,COUNT(*) FROM views;
                CREATE TRIGGER IF NOT EXISTS view_added AFTER INSERT ON views BEGIN UPDATE view_totals SET entries=entries+1 WHERE id=1; END;
                CREATE TRIGGER IF NOT EXISTS view_removed AFTER DELETE ON views BEGIN UPDATE view_totals SET entries=entries-1 WHERE id=1; END;")?;
            let store = Self {
                root,
                lock,
                db,
                max_bytes,
                max_entries,
            };
            store.reconcile()?;
            store.evict(0, 0)?;
            store.prune_views()?;
            store.lock.unlock()?;
            Ok(store)
        }

        fn key(digest: &FileDigest) -> String {
            format!("{}-{}", digest.raw_digest().algorithm(), digest).replace(':', "-")
        }

        fn object_path(&self, key: &str) -> buck2_error::Result<PathBuf> {
            let mut parts = key.rsplitn(3, '-');
            let size = parts.next().and_then(|s| s.parse::<u64>().ok());
            let digest = parts.next().unwrap_or("");
            let algorithm = parts.next().unwrap_or("");
            let length = match algorithm {
                "SHA1" => 40,
                "SHA256" | "BLAKE3" | "BLAKE3-KEYED" => 64,
                _ => 0,
            };
            if size.is_none()
                || length == 0
                || digest.len() != length
                || !digest.bytes().all(|c| c.is_ascii_hexdigit())
            {
                return Err(error("Invalid object key in local content store index"));
            }
            Ok(self.root.join(key))
        }

        fn reconcile(&self) -> buck2_error::Result<()> {
            remove(&self.root.join("pending"))?;
            // Interrupted inserts are accounted in the index before publication;
            // an interrupted unlink may leave an overcount, never an undercount.
            let mut statement = self.db.prepare("SELECT key FROM objects")?;
            let keys = statement.query_map([], |r| r.get::<_, String>(0))?;
            for key in keys {
                let key = key?;
                if !self.object_path(&key)?.try_exists()? {
                    self.db
                        .execute("DELETE FROM objects WHERE key=?1", [&key])?;
                }
            }
            // No externally managed objects are accepted in this private root.
            for entry in fs::read_dir(&self.root)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name
                    .to_str()
                    .ok_or_else(|| error("Invalid content store filename"))?;
                if matches!(name, "lock" | "index.sqlite" | "index.sqlite-journal") {
                    continue;
                }
                let tracked: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM objects WHERE key=?1)",
                    [name],
                    |r| r.get(0),
                )?;
                if !tracked {
                    remove(&entry.path())?;
                }
            }
            Ok(())
        }

        fn forget(&self, key: &str) -> buck2_error::Result<()> {
            remove(&self.object_path(key)?)?;
            self.db.execute("DELETE FROM objects WHERE key=?1", [key])?;
            Ok(())
        }

        fn evict(&self, extra_bytes: u64, extra_entries: u64) -> buck2_error::Result<()> {
            let (mut bytes, mut entries): (u64, u64) =
                self.db
                    .query_row("SELECT bytes,entries FROM totals WHERE id=1", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
            while bytes.saturating_add(extra_bytes) > self.max_bytes
                || entries.saturating_add(extra_entries) > self.max_entries
            {
                let (key, size): (String, u64) = self.db.query_row(
                    "SELECT key,size FROM objects ORDER BY used,key LIMIT 1",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                self.forget(&key)?;
                bytes -= size;
                entries -= 1;
            }
            Ok(())
        }

        fn publish(
            &self,
            source: &File,
            destination: &Path,
            digest: &FileDigest,
            executable: bool,
            config: DigestConfig,
        ) -> buck2_error::Result<()> {
            let parent = destination
                .parent()
                .ok_or_else(|| error("Missing output parent"))?;
            let mut temp = tempfile::Builder::new()
                .prefix(".buck2-cow-")
                .tempfile_in(parent)?;
            clone_file(source, temp.as_file())?;
            // Verify the snapshot, not the source inode, so neither a mutable
            // worktree nor chmod on a cache object is trusted as verification.
            verify(temp.as_file_mut(), digest, config)?;
            temp.as_file()
                .set_permissions(Permissions::from_mode(if executable {
                    0o755
                } else {
                    0o644
                }))?;
            temp.as_file().sync_all()?;
            temp.persist(destination).map_err(|e| e.error)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        }

        fn prune_views(&self) -> buck2_error::Result<()> {
            let excess: u64 = self.db.query_row(
                "SELECT MAX(entries-?1,0) FROM view_totals WHERE id=1",
                [self.max_entries],
                |row| row.get(0),
            )?;
            if excess != 0 {
                self.db.execute(
                    "DELETE FROM views WHERE path IN (SELECT path FROM views ORDER BY used,path LIMIT ?1)",
                    [excess],
                )?;
            }
            Ok(())
        }

        fn remember_view(&self, destination: &Path, key: &str) -> buck2_error::Result<()> {
            let path = destination
                .to_str()
                .ok_or_else(|| error("Non-UTF-8 artifact view path"))?;
            let transaction = self.db.unchecked_transaction()?;
            transaction.execute(
                "INSERT INTO views VALUES(?1,?2,(SELECT tick FROM clock WHERE id=1))
                 ON CONFLICT(path) DO UPDATE SET key=excluded.key,used=excluded.used",
                params![path, key],
            )?;
            self.prune_views()?;
            transaction.commit()?;
            Ok(())
        }

        fn restore_view(
            &self,
            destination: &Path,
            key: &str,
            digest: &FileDigest,
            executable: bool,
            config: DigestConfig,
        ) -> buck2_error::Result<bool> {
            loop {
                let candidate: Option<String> = self.db.query_row(
                    "SELECT path FROM views WHERE key=?1 ORDER BY used DESC,path LIMIT 1",
                    [key],
                    |row| row.get(0),
                ).optional()?;
                let Some(candidate) = candidate else {
                    return Ok(false);
                };
                let path = Path::new(&candidate);
                // These are weak, untrusted hints: a tree may have disappeared,
                // changed its output or replaced it with a symlink.
                if path.is_absolute() {
                    if let Ok(mut source) = open_file(path) {
                        match self.publish(&source, destination, digest, executable, config) {
                            Ok(()) => return Ok(true),
                            Err(error) => {
                                // A valid candidate must not hide destination I/O
                                // or reflink errors as an ordinary cache miss.
                                if verify(&mut source, digest, config).is_ok() {
                                    return Err(error);
                                }
                            }
                        }
                    }
                }
                self.db.execute("DELETE FROM views WHERE path=?1", [&candidate])?;
            }
        }

        fn access(
            &self,
            destination: &Path,
            digest: &FileDigest,
            executable: bool,
            config: DigestConfig,
            ingest: bool,
        ) -> buck2_error::Result<bool> {
            if digest.size() > self.max_bytes {
                return Err(error(format!(
                    "Artifact ({}) exceeds BUCK2_LOCAL_CONTENT_STORE_MAX_BYTES ({})",
                    digest.size(),
                    self.max_bytes
                )));
            }
            let key = Self::key(digest);
            let object = self.root.join(&key);
            let recorded: bool = self.db.query_row(
                "SELECT EXISTS(SELECT 1 FROM objects WHERE key=?1)",
                [&key],
                |r| r.get(0),
            )?;
            if recorded {
                match open_file(&object) {
                    Ok(source) => {
                        if let Err(e) =
                            self.publish(&source, destination, digest, executable, config)
                        {
                            // A reflink or output I/O failure is not corruption;
                            // only discard an object that itself fails validation.
                            let mut source = source;
                            if verify(&mut source, digest, config).is_err() {
                                self.forget(&key)?;
                            }
                            return Err(e);
                        }
                        self.touch(&key)?;
                        self.remember_view(destination, &key)?;
                        return Ok(true);
                    }
                    Err(e) => {
                        if object.try_exists()? {
                            return Err(e);
                        }
                        self.forget(&key)?;
                    }
                }
            }
            let restored = self.restore_view(destination, &key, digest, executable, config)?;
            if !ingest && !restored {
                return Ok(false);
            }
            self.evict(digest.size(), 1)?;
            let pending = self.root.join("pending");
            remove(&pending)?;
            let source = open_file(destination)?;
            let mut staged = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&pending)?;
            clone_file(&source, &staged)?;
            verify(&mut staged, digest, config)?;
            staged.set_permissions(Permissions::from_mode(0o444))?;
            staged.sync_all()?;
            // Account before rename. A crash can leave a missing object, which
            // is a cache miss, but cannot introduce unaccounted live objects.
            self.db.execute(
                "INSERT INTO objects VALUES(?1,?2,0)",
                params![key, digest.size()],
            )?;
            fs::rename(&pending, &object)?;
            File::open(&self.root)?.sync_all()?;
            self.touch(&key)?;
            self.remember_view(destination, &key)?;
            // The original output already shares this object's extents. It is
            // not hardlinked and must retain its existing executable mode.
            Ok(true)
        }

        fn touch(&self, key: &str) -> buck2_error::Result<()> {
            self.db
                .execute_batch("UPDATE clock SET tick=tick+1 WHERE id=1;")?;
            self.db.execute(
                "UPDATE objects SET used=(SELECT tick FROM clock WHERE id=1) WHERE key=?1",
                [key],
            )?;
            Ok(())
        }
    }

    static STORE: LazyLock<Result<Option<Mutex<Store>>, String>> = LazyLock::new(|| {
        let Some(root) = std::env::var_os("BUCK2_LOCAL_CONTENT_STORE") else {
            return Ok(None);
        };
        let limit = |name: &str, default: u64| -> Result<u64, String> {
            match std::env::var(name) {
                Ok(s) => s.parse().map_err(|e| format!("{name}: {e}")),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(e) => Err(e.to_string()),
            }
        };
        let bytes = limit(
            "BUCK2_LOCAL_CONTENT_STORE_MAX_BYTES",
            10 * 1024 * 1024 * 1024,
        )?;
        let entries = limit("BUCK2_LOCAL_CONTENT_STORE_MAX_ENTRIES", 100_000)?;
        Store::open(PathBuf::from(root), bytes, entries)
            .map(|s| Some(Mutex::new(s)))
            .map_err(|e| format!("{e:#}"))
    });

    fn with_store<T>(
        absent: T,
        operation: impl FnOnce(&Store) -> buck2_error::Result<T>,
    ) -> buck2_error::Result<T> {
        let store = STORE.as_ref().map_err(error)?;
        let Some(store) = store else {
            return Ok(absent);
        };
        let store = store
            .lock()
            .map_err(|_| error("Local content store mutex poisoned"))?;
        store.lock.lock()?;
        let result = operation(&store);
        let unlock = store.lock.unlock();
        result
            .buck_error_context("Buck2 verified local content store")
            .and_then(|v| {
                unlock?;
                Ok(v)
            })
    }

    pub(super) fn access(
        destination: &Path,
        digest: &FileDigest,
        executable: bool,
        config: DigestConfig,
        ingest: bool,
    ) -> buck2_error::Result<bool> {
        with_store(false, |store| {
            remove(&store.root.join("pending"))?;
            store.evict(0, 0)?;
            store.access(destination, digest, executable, config, ingest)
        })
    }

    pub(super) fn published(
        temporary: &Path,
        destination: &Path,
        digest: &FileDigest,
    ) -> buck2_error::Result<()> {
        with_store((), |store| {
            store.db.execute(
                "DELETE FROM views WHERE path=?1",
                [temporary.to_str().ok_or_else(|| error("Non-UTF-8 temporary artifact path"))?],
            )?;
            store.remember_view(destination, &Store::key(digest))
        })
    }

    #[cfg(test)]
    mod tests {
        use buck2_common::cas_digest::DigestAlgorithm;

        use super::*;

        #[test]
        fn hint_open_rejects_fifo_without_waiting() -> buck2_error::Result<()> {
            unsafe extern "C" {
                fn mkfifo(path: *const std::ffi::c_char, mode: u32) -> i32;
            }
            let temp = tempfile::tempdir()?;
            let path = temp.path().join("stale-view");
            let c_path = std::ffi::CString::new(path.as_os_str().as_encoded_bytes())
                .expect("temporary path contains no NUL");
            assert_eq!(unsafe { mkfifo(c_path.as_ptr(), 0o600) }, 0);
            let (send, receive) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = send.send(open_file(&path).is_err());
            });
            assert!(
                receive
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("opening a stale FIFO must not wait for a writer")
            );
            Ok(())
        }

        #[test]
        fn hint_verification_bounds_reads_by_expected_size() -> buck2_error::Result<()> {
            let mut file = tempfile::tempfile()?;
            file.set_len(16 * 1024 * 1024)?;
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let digest = FileDigest::from_content(b"expected", config.cas_digest_config());
            assert!(verify(&mut file, &digest, config).is_err());
            assert!(file.stream_position()? <= digest.size() + 1);
            Ok(())
        }

        #[test]
        fn reflink_downloads_respect_descriptor_limit() -> buck2_error::Result<()> {
            super::super::download_tests::check_descriptor_limit(
                true,
                "materializers::local_content_store::linux::tests::reflink_downloads_respect_descriptor_limit",
            )
        }

        #[test]
        fn reflink_store_recovers_evicted_objects_from_live_views() -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let root = temp.path().join("store");
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let bytes = vec![41u8; 128 * 1024];
            let other = vec![42u8; 128 * 1024];
            let digest = FileDigest::from_content(&bytes, config.cas_digest_config());
            let other_digest = FileDigest::from_content(&other, config.cas_digest_config());
            let a = temp.path().join("a");
            let b = temp.path().join("b");
            let c = temp.path().join("c");
            fs::write(&a, &bytes)?;
            fs::write(&b, &other)?;
            let store = Store::open(root.clone(), 128 * 1024, 8)?;
            store.access(&a, &digest, false, config, true)?;
            store.access(&b, &other_digest, false, config, true)?;
            assert!(!root.join(Store::key(&digest)).exists());
            drop(store);

            let store = Store::open(root.clone(), 128 * 1024, 8)?;
            // No ingest/download: the still-live original view supplies the
            // verified COW snapshot even after canonical eviction and restart.
            assert!(store.access(&c, &digest, true, config, false)?);
            assert_eq!(fs::read(&c)?, bytes);
            assert_ne!(fs::metadata(&a)?.ino(), fs::metadata(&c)?.ino());
            assert_eq!(fs::metadata(&c)?.mode() & 0o777, 0o755);
            assert!(root.join(Store::key(&digest)).exists());
            assert!(!root.join(Store::key(&other_digest)).exists());
            fs::write(&c, b"changed new view")?;
            assert_eq!(fs::read(&a)?, bytes);
            Ok(())
        }

        #[test]
        fn reflink_store_discards_stale_and_symlinked_view_hints() -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let root = temp.path().join("store");
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let bytes = vec![51u8; 128 * 1024];
            let other = vec![52u8; 128 * 1024];
            let digest = FileDigest::from_content(&bytes, config.cas_digest_config());
            let other_digest = FileDigest::from_content(&other, config.cas_digest_config());
            let a = temp.path().join("a");
            let b = temp.path().join("b");
            let c = temp.path().join("c");
            let d = temp.path().join("d");
            let victim = temp.path().join("victim");
            let output = temp.path().join("output");
            fs::write(&a, &bytes)?;
            fs::write(&d, &other)?;
            fs::write(&victim, &bytes)?;
            let store = Store::open(root.clone(), 128 * 1024, 8)?;
            store.access(&a, &digest, false, config, true)?;
            assert!(store.access(&b, &digest, false, config, false)?);
            assert!(store.access(&c, &digest, false, config, false)?);
            store.access(&d, &other_digest, false, config, true)?;
            assert!(!root.join(Store::key(&digest)).exists());
            fs::write(&a, b"edited original")?;
            fs::remove_file(&b)?;
            fs::remove_file(&c)?;
            std::os::unix::fs::symlink(&victim, &c)?;
            assert!(!store.access(&output, &digest, false, config, false)?);
            assert!(!output.exists());
            assert_eq!(fs::read(&victim)?, bytes);
            assert_eq!(fs::read(&a)?, b"edited original");
            Ok(())
        }

        #[test]
        fn reflink_store_restart_eviction_corruption_and_modes() -> buck2_error::Result<()> {
            // This test requires the same Linux reflink filesystem as production;
            // it deliberately does not turn an unsupported filesystem into a pass.
            let temp = tempfile::tempdir()?;
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let bytes = vec![37u8; 128 * 1024];
            let digest = FileDigest::from_content(&bytes, config.cas_digest_config());
            let a = temp.path().join("a");
            let b = temp.path().join("b");
            fs::write(&a, &bytes)?;
            let root = temp.path().join("store");
            let store = Store::open(root.clone(), 256 * 1024, 2)?;
            store.access(&a, &digest, false, config, true)?;
            assert!(store.access(&b, &digest, true, config, false)?);
            assert_eq!(fs::metadata(&b)?.mode() & 0o777, 0o755);
            assert_ne!(fs::metadata(&a)?.ino(), fs::metadata(&b)?.ino());
            fs::write(&b, b"mutated output")?;
            assert_eq!(fs::read(&a)?, bytes);
            drop(store);
            let store = Store::open(root.clone(), 256 * 1024, 2)?;
            assert!(store.access(&b, &digest, false, config, false)?);
            assert_eq!(fs::read(&b)?, bytes);
            assert_eq!(fs::metadata(&b)?.mode() & 0o777, 0o644);
            let object = root.join(Store::key(&digest));
            fs::set_permissions(&object, Permissions::from_mode(0o644))?;
            fs::write(&object, b"corrupt")?;
            assert!(store.access(&b, &digest, false, config, false).is_err());
            assert_eq!(fs::read(&b)?, bytes);
            assert!(!object.exists());
            store.access(&a, &digest, false, config, true)?;
            drop(store);
            let store = Store::open(root, 64 * 1024, 1)?;
            assert!(!object.exists());
            assert_eq!(fs::read(&b)?, bytes);
            assert!(store.access(&a, &digest, false, config, true).is_err());
            Ok(())
        }

        #[test]
        fn reflink_store_concurrent_identical_and_divergent_bounded() -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let root = temp.path().join("store");
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            std::thread::scope(|scope| -> buck2_error::Result<()> {
                let mut threads = Vec::new();
                for worker in 0..4 {
                    let root = &root;
                    let parent = temp.path();
                    threads.push(scope.spawn(move || -> buck2_error::Result<()> {
                        let store = Store::open(root.to_owned(), 512 * 1024, 4)?;
                        for iteration in 0..12 {
                            let value = if iteration < 6 {
                                iteration
                            } else {
                                iteration + worker * 12
                            };
                            let bytes = vec![value as u8; 128 * 1024];
                            let digest =
                                FileDigest::from_content(&bytes, config.cas_digest_config());
                            let path = parent.join(format!("{worker}-{iteration}"));
                            fs::write(&path, &bytes)?;
                            store.lock.lock()?;
                            let result =
                                store.access(&path, &digest, worker % 2 == 0, config, true);
                            store.lock.unlock()?;
                            result?;
                            assert_eq!(fs::read(&path)?, bytes);
                        }
                        Ok(())
                    }));
                }
                for thread in threads {
                    thread.join().expect("store worker panicked")?;
                }
                Ok(())
            })?;
            let store = Store::open(root, 512 * 1024, 4)?;
            let (bytes, entries): (u64, u64) =
                store
                    .db
                    .query_row("SELECT bytes,entries FROM totals", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
            assert!(bytes <= 512 * 1024 && entries <= 4);
            let actual: (u64, u64) = store.db.query_row(
                "SELECT COALESCE(SUM(size),0),COUNT(*) FROM objects",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            assert_eq!((bytes, entries), actual);
            let (views, actual_views): (u64, u64) = store.db.query_row(
                "SELECT (SELECT entries FROM view_totals WHERE id=1),COUNT(*) FROM views",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert!(views <= 4);
            assert_eq!(views, actual_views);
            Ok(())
        }

        #[test]
        fn reflink_store_recovers_interrupted_publication_and_rejects_symlinks()
        -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let root = temp.path().join("store");
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let data = vec![11; 8192];
            let digest = FileDigest::from_content(&data, config.cas_digest_config());
            let key = Store::key(&digest);
            let store = Store::open(root.clone(), 65536, 8)?;
            fs::write(root.join("pending"), b"interrupted snapshot")?;
            fs::write(root.join("orphan"), b"untracked")?;
            store.db.execute(
                "INSERT INTO objects VALUES(?1,?2,0)",
                params![key, digest.size()],
            )?;
            drop(store);
            let store = Store::open(root.clone(), 65536, 8)?;
            assert!(!root.join("pending").exists());
            assert!(!root.join("orphan").exists());
            let output = temp.path().join("output");
            assert!(!store.access(&output, &digest, false, config, false)?);
            let source = temp.path().join("source");
            fs::write(&source, &data)?;
            std::os::unix::fs::symlink(&source, &output)?;
            assert!(store.access(&output, &digest, false, config, true).is_err());
            assert_eq!(fs::read(&source)?, data);
            drop(store);
            let alias = temp.path().join("alias");
            std::os::unix::fs::symlink(&root, &alias)?;
            assert!(Store::open(alias, 65536, 8).is_err());
            fs::set_permissions(&root, Permissions::from_mode(0o755))?;
            assert!(Store::open(root, 65536, 8).is_err());
            Ok(())
        }

        #[test]
        fn reflink_store_rejects_corrupt_index_paths() -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let victim = temp.path().join("victim");
            fs::write(&victim, b"outside store")?;
            let root = temp.path().join("store");
            let store = Store::open(root.clone(), 65536, 8)?;
            store
                .db
                .execute("INSERT INTO objects VALUES('../victim',1,0)", [])?;
            drop(store);
            assert!(Store::open(root, 65536, 8).is_err());
            assert_eq!(fs::read(victim)?, b"outside store");
            Ok(())
        }
    }
}

use buck2_common::file_ops::metadata::FileDigest;
use buck2_directory::directory::directory::Directory;
use buck2_directory::directory::directory_iterator::DirectoryIterator;
use buck2_directory::directory::directory_iterator::DirectoryIteratorPathStack;
use buck2_directory::directory::entry::DirectoryEntry;
use buck2_directory::directory::walk::unordered_entry_walk;
use buck2_execute::digest_config::DigestConfig;
use buck2_execute::directory::ActionDirectory;
use buck2_execute::directory::ActionDirectoryEntry;
use buck2_execute::directory::ActionDirectoryMember;
use buck2_fs::paths::abs_norm_path::AbsNormPath;

pub(crate) fn file(
    path: &AbsNormPath,
    digest: &FileDigest,
    executable: bool,
    config: DigestConfig,
    ingest: bool,
) -> buck2_error::Result<bool> {
    #[cfg(target_os = "linux")]
    {
        linux::access(path.as_path(), digest, executable, config, ingest)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (path, digest, executable, config, ingest);
        if std::env::var_os("BUCK2_LOCAL_CONTENT_STORE").is_some() {
            return Err(buck2_error::buck2_error!(
                buck2_error::ErrorTag::MaterializationError,
                "BUCK2_LOCAL_CONTENT_STORE requires Linux reflinks"
            ));
        }
        Ok(false)
    }
}

pub(crate) fn enabled() -> bool {
    static ENABLED: std::sync::LazyLock<bool> =
        std::sync::LazyLock::new(|| std::env::var_os("BUCK2_LOCAL_CONTENT_STORE").is_some());
    *ENABLED
}

pub(crate) fn artifact<D: ActionDirectory>(
    path: &AbsNormPath,
    entry: &ActionDirectoryEntry<D>,
    config: DigestConfig,
) -> buck2_error::Result<()> {
    if !enabled() {
        return Ok(());
    }
    let mut walk = unordered_entry_walk(entry.as_ref().map_dir(Directory::as_ref));
    while let Some((relative, entry)) = walk.next() {
        if let DirectoryEntry::Leaf(ActionDirectoryMember::File(metadata)) = entry {
            file(
                &path.join(relative.get()),
                metadata.digest.data(),
                metadata.is_executable,
                config,
                true,
            )?;
        }
    }
    Ok(())
}

/// A remote download is staged beside its destination. Failed downloads and
/// digest mismatches never become visible as successfully materialized files.
pub(crate) struct Download {
    destination: buck2_fs::paths::abs_norm_path::AbsNormPathBuf,
    temporary: tempfile::TempPath,
    metadata: buck2_common::file_ops::metadata::FileMetadata,
}

impl Download {
    pub(crate) fn prepare(
        destination: buck2_fs::paths::abs_norm_path::AbsNormPathBuf,
        metadata: buck2_common::file_ops::metadata::FileMetadata,
        config: DigestConfig,
    ) -> buck2_error::Result<Option<Self>> {
        if file(
            &destination,
            metadata.digest.data(),
            metadata.is_executable,
            config,
            false,
        )? {
            return Ok(None);
        }
        let parent = destination.parent().ok_or_else(|| {
            buck2_error::buck2_error!(
                buck2_error::ErrorTag::MaterializationError,
                "Missing download output parent"
            )
        })?;
        let temporary = tempfile::Builder::new()
            .prefix(".buck2-download-")
            .tempfile_in(parent.as_path())?
            .into_temp_path();
        Ok(Some(Self {
            destination,
            temporary,
            metadata,
        }))
    }

    pub(crate) fn name(&self) -> buck2_error::Result<String> {
        Ok(
            buck2_fs::paths::abs_norm_path::AbsNormPathBuf::new(self.temporary.to_path_buf())?
                .as_maybe_relativized_str()?
                .to_owned(),
        )
    }

    pub(crate) fn finish(self, config: DigestConfig) -> buck2_error::Result<()> {
        let path =
            buck2_fs::paths::abs_norm_path::AbsNormPathBuf::new(self.temporary.to_path_buf())?;
        if !file(
            &path,
            self.metadata.digest.data(),
            self.metadata.is_executable,
            config,
            true,
        )? {
            // Remote transport is not an integrity authority, even with the
            // optional shared store disabled.
            let expected = self.metadata.digest.data();
            let algorithm = config
                .cas_digest_config()
                .algorithm_for_family(expected.raw_digest().algorithm())
                .ok_or_else(|| {
                    buck2_error::buck2_error!(
                        buck2_error::ErrorTag::MaterializationError,
                        "Disabled download digest algorithm"
                    )
                })?;
            let actual = FileDigest::from_reader_for_algorithm(
                std::fs::File::open(path.as_path())?,
                algorithm,
            )?;
            if actual != *expected {
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::MaterializationError,
                    "Downloaded artifact digest mismatch: expected {}, got {}",
                    expected,
                    actual
                ));
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // The transport opens our pre-existing temporary file; its create
            // mode does not change that inode's initial 0600 permissions.
            std::fs::set_permissions(
                path.as_path(),
                std::fs::Permissions::from_mode(if self.metadata.is_executable {
                    0o755
                } else {
                    0o644
                }),
            )?;
        }
        std::fs::File::open(path.as_path())?.sync_all()?;
        self.temporary
            .persist(self.destination.as_path())
            .map_err(|e| e.error)?;
        #[cfg(target_os = "linux")]
        linux::published(
            path.as_path(),
            self.destination.as_path(),
            self.metadata.digest.data(),
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod download_tests {
    use buck2_common::file_ops::metadata::FileMetadata;
    use buck2_common::file_ops::metadata::TrackedFileDigest;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;

    use super::*;

    #[test]
    fn downloaded_content_is_verified_before_atomic_publication() -> buck2_error::Result<()> {
        let temp = tempfile::tempdir()?;
        let output = AbsNormPathBuf::new(temp.path().join("output"))?;
        let config = DigestConfig::testing_default();
        let metadata = || FileMetadata {
            digest: TrackedFileDigest::from_content(b"correct", config.cas_digest_config()),
            is_executable: true,
        };
        std::fs::write(output.as_path(), b"previous")?;
        let download = Download::prepare(output.clone(), metadata(), config)?.unwrap();
        std::fs::write(download.name()?, b"corrupt")?;
        assert!(download.finish(config).is_err());
        assert_eq!(std::fs::read(output.as_path())?, b"previous");
        let download = Download::prepare(output.clone(), metadata(), config)?.unwrap();
        std::fs::write(download.name()?, b"correct")?;
        download.finish(config)?;
        assert_eq!(std::fs::read(output.as_path())?, b"correct");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(output.as_path())?.permissions().mode() & 0o777,
                0o755
            );
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn many_pending_downloads_respect_descriptor_limit() -> buck2_error::Result<()> {
        check_descriptor_limit(
            false,
            "materializers::local_content_store::download_tests::many_pending_downloads_respect_descriptor_limit",
        )
    }

    #[cfg(target_os = "linux")]
    pub(super) fn check_descriptor_limit(
        shared_store: bool,
        test_name: &str,
    ) -> buck2_error::Result<()> {
        const CHILD: &str = "BUCK2_MATERIALIZER_FD_REGRESSION_CHILD";
        if std::env::var_os(CHILD).is_none() {
            use std::os::unix::process::CommandExt;

            #[repr(C)]
            struct Limit {
                current: u64,
                maximum: u64,
            }
            unsafe extern "C" {
                fn setrlimit(resource: i32, limit: *const Limit) -> i32;
            }

            let temp = tempfile::tempdir()?;
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command.args(["--exact", test_name, "--nocapture"]);
            command.env(CHILD, "1");
            command.env_remove("BUCK2_LOCAL_CONTENT_STORE");
            if shared_store {
                command.env("BUCK2_LOCAL_CONTENT_STORE", temp.path().join("store"));
            }
            // Apply the limit only in the child, never in the parallel
            // parent test harness. setrlimit is async-signal-safe.
            unsafe {
                command.pre_exec(|| {
                    if setrlimit(
                        7,
                        &Limit {
                            current: 64,
                            maximum: 64,
                        },
                    ) != 0
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
            let result = command.output()?;
            assert!(
                result.status.success(),
                "shared_store={shared_store}: {}",
                String::from_utf8_lossy(&result.stderr)
            );
            return Ok(());
        }

        let temp = tempfile::tempdir()?;
        let config = DigestConfig::testing_default();
        let mut pending = Vec::new();
        for index in 0..512 {
            let output = AbsNormPathBuf::new(temp.path().join(format!("output-{index}")))?;
            let metadata = FileMetadata {
                digest: TrackedFileDigest::from_content(b"downloaded", config.cas_digest_config()),
                is_executable: false,
            };
            pending.push(Download::prepare(output, metadata, config)?.unwrap());
        }
        for download in &pending {
            std::fs::write(download.name()?, b"downloaded")?;
        }
        for download in pending {
            download.finish(config)?;
        }
        for index in 0..512 {
            assert_eq!(
                std::fs::read(temp.path().join(format!("output-{index}")))?,
                b"downloaded"
            );
        }
        Ok(())
    }
}
