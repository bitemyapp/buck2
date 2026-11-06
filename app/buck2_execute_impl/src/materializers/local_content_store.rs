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
//!
//! Artifact ingestion holds both locks for one bounded group at a time. Groups
//! contain at most 16 destinations and target 64 MiB of staged logical bytes;
//! one larger file is admitted alone, provided it fits the configured cache.
//! At most two retained file descriptors per destination are needed. All COW
//! snapshots are verified before the separate per-file sync pass. FULL SQLite
//! accounting commits precede canonical renames, and namespace removals are
//! directory-synced before deletion accounting commits. Publication directory
//! syncs and the access/weak-view transaction are shared within each group.
//! Before admission, each group clears a fixed staging namespace under both
//! locks, including leftovers from a dead peer whose survivors remain open.
//! Staging stays directly beside canonical objects, not across a subvolume.

const INGEST_BATCH_FILES: usize = 16;

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::HashMap;
    use std::collections::HashSet;
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
    use buck2_common::file_ops::metadata::FileMetadata;
    use buck2_error::BuckErrorContext;
    use buck2_execute::digest_config::DigestConfig;
    use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
    use rayon::iter::IntoParallelRefIterator;
    use rayon::iter::ParallelIterator;
    use rusqlite::Connection;
    use rusqlite::OptionalExtension;
    use rusqlite::params;
    use tempfile::NamedTempFile;

    const INGEST_BATCH_BYTES: u64 = 64 * 1024 * 1024;

    // Materialization already runs on Tokio's blocking pool while holding store
    // locks. Never submit nested blocking work there: a saturated pool could
    // deadlock. These dedicated workers only synchronize borrowed file handles.
    static FILE_SYNCS: LazyLock<Result<rayon::ThreadPool, String>> = LazyLock::new(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(super::INGEST_BATCH_FILES)
            .spawn_handler(|thread| {
                buck2_util::threads::thread_spawn("buck2-cow-io", move || thread.run())
                    .map(|_| ())
            })
            .build()
            .map_err(|error| error.to_string())
    });

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

    struct IngestRequest<'a> {
        destination: &'a Path,
        executable: bool,
        object: usize,
    }

    enum IngestSource {
        Canonical(File),
        Produced(File),
        Recovered(NamedTempFile),
    }

    struct IngestObject<'a> {
        digest: &'a FileDigest,
        key: String,
        path: PathBuf,
        first_request: usize,
        new: bool,
        source: Option<IngestSource>,
        staged: Option<NamedTempFile>,
    }

    struct Publication<'a> {
        file: NamedTempFile,
        destination: &'a Path,
    }

    struct GroupResult {
        consumed: usize,
        found: bool,
    }

    pub(super) struct Store {
        root: PathBuf,
        staging: [PathBuf; super::INGEST_BATCH_FILES],
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
            let staging = std::array::from_fn(|slot| root.join(format!(".buck2-pending-{slot}")));
            let store = Self {
                root,
                staging,
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

        // Caller holds the shared lock. Fixed slots bound cleanup independently
        // of canonical object count and survive another process dying mid-group.
        fn clear_staging(&self) -> buck2_error::Result<()> {
            for path in &self.staging {
                remove(path)?;
            }
            // Also cover a previous peer dying after unlink but before syncing,
            // even when all slots are now observed absent.
            File::open(&self.root)?.sync_all()?;
            Ok(())
        }

        fn stage_canonical(&self, slot: usize) -> buck2_error::Result<NamedTempFile> {
            Ok(tempfile::Builder::new()
                .prefix(self.staging[slot].file_name().expect("canonical staging filename"))
                .rand_bytes(0)
                .tempfile_in(&self.root)?)
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
            // Untracked entries below include unique staging files from failed
            // or interrupted groups, as well as the former single-file pending.
            // Interrupted inserts are accounted before publication; missing
            // canonical objects are cache misses, not unaccounted live objects.
            // A failed group can roll back accounting after unlinking objects.
            // Make those observed absences durable before deleting index rows.
            File::open(&self.root)?.sync_all()?;
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
            File::open(&self.root)?.sync_all()?;
            Ok(())
        }

        fn evict(&self, extra_bytes: u64, extra_entries: u64) -> buck2_error::Result<()> {
            let transaction = self.db.unchecked_transaction()?;
            if self.evict_in_transaction(extra_bytes, extra_entries, std::iter::empty())? {
                File::open(&self.root)?.sync_all()?;
            }
            transaction.commit()?;
            Ok(())
        }

        // The caller must sync removed names before committing this transaction.
        // Protect this group's sources: their bytes already count toward the
        // cap, and a new group never needs more distinct objects than can fit.
        fn evict_in_transaction<'a>(
            &self,
            extra_bytes: u64,
            extra_entries: u64,
            protected: impl IntoIterator<Item = &'a str>,
        ) -> buck2_error::Result<bool> {
            let (mut bytes, mut entries): (u64, u64) =
                self.db
                    .query_row("SELECT bytes,entries FROM totals WHERE id=1", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
            if bytes.saturating_add(extra_bytes) <= self.max_bytes
                && entries.saturating_add(extra_entries) <= self.max_entries
            {
                return Ok(false);
            }
            // Most groups need no eviction: avoid allocating a protection set
            // or preparing the victim query on that hot path.
            let protected: HashSet<&str> = protected.into_iter().collect();
            let mut removed = false;
            let mut statement = self
                .db
                .prepare("SELECT key,size FROM objects ORDER BY used,key")?;
            while bytes.saturating_add(extra_bytes) > self.max_bytes
                || entries.saturating_add(extra_entries) > self.max_entries
            {
                let (key, size) = {
                    let mut rows = statement.query([])?;
                    let mut candidate = None;
                    while let Some(row) = rows.next()? {
                        let key = row
                            .get_ref(0)?
                            .as_str()
                            .map_err(error)
                            .buck_error_context("Invalid local content store object key")?;
                        if !protected.contains(key) {
                            candidate = Some((key.to_owned(), row.get::<_, u64>(1)?));
                            break;
                        }
                    }
                    candidate.ok_or_else(|| error("Content store group exceeds cache capacity"))?
                };
                remove(&self.object_path(&key)?)?;
                self.db.execute("DELETE FROM objects WHERE key=?1", [&key])?;
                removed = true;
                bytes -= size;
                entries -= 1;
            }
            Ok(removed)
        }

        fn stage_publication(
            &self,
            source: &File,
            destination: &Path,
            digest: &FileDigest,
            executable: bool,
            config: DigestConfig,
        ) -> buck2_error::Result<NamedTempFile> {
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
            Ok(temp)
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

        // Call inside the access/publication transaction so LRU and bounded
        // weak-reference changes require one durable metadata commit.
        fn remember_view(&self, destination: &Path, key: &str) -> buck2_error::Result<()> {
            let path = destination
                .to_str()
                .ok_or_else(|| error("Non-UTF-8 artifact view path"))?;
            self.db.execute(
                "INSERT INTO views VALUES(?1,?2,(SELECT tick FROM clock WHERE id=1))
                 ON CONFLICT(path) DO UPDATE SET key=excluded.key,used=excluded.used",
                params![path, key],
            )?;
            self.prune_views()?;
            Ok(())
        }

        fn stage_recovery(
            &self,
            destination: &Path,
            key: &str,
            digest: &FileDigest,
            executable: bool,
            config: DigestConfig,
        ) -> buck2_error::Result<Option<NamedTempFile>> {
            loop {
                let candidate: Option<String> = self.db.query_row(
                    "SELECT path FROM views WHERE key=?1 ORDER BY used DESC,path LIMIT 1",
                    [key],
                    |row| row.get(0),
                ).optional()?;
                let Some(candidate) = candidate else {
                    return Ok(None);
                };
                let path = Path::new(&candidate);
                // These are weak, untrusted hints: a tree may have disappeared,
                // changed its output or replaced it with a symlink.
                if path.is_absolute() {
                    if let Ok(mut source) = open_file(path) {
                        match self.stage_publication(&source, destination, digest, executable, config) {
                            Ok(staged) => return Ok(Some(staged)),
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

        fn access_group<'a>(
            &self,
            files: impl Iterator<Item = (&'a Path, &'a FileDigest, bool)>,
            config: DigestConfig,
            ingest: bool,
        ) -> buck2_error::Result<GroupResult> {
            let capacity = files.size_hint().1
                .unwrap_or(super::INGEST_BATCH_FILES)
                .min(super::INGEST_BATCH_FILES);
            let mut requests = Vec::with_capacity(capacity);
            let mut objects: Vec<IngestObject<'a>> = Vec::with_capacity(capacity);
            let mut by_digest = HashMap::with_capacity(capacity);
            let mut staged_bytes = 0u64;
            // Recovery stages both a canonical object and its first view.
            // Reserve that worst case rather than exceeding the staging budget.
            let byte_limit = self.max_bytes.saturating_mul(2).min(INGEST_BATCH_BYTES);
            for (destination, digest, executable) in files.take(super::INGEST_BATCH_FILES) {
                if digest.size() > self.max_bytes {
                    return Err(error(format!(
                        "Artifact ({}) exceeds BUCK2_LOCAL_CONTENT_STORE_MAX_BYTES ({})",
                        digest.size(),
                        self.max_bytes
                    )));
                }
                let next_bytes = staged_bytes.saturating_add(digest.size().saturating_mul(2));
                let known = by_digest.get(digest).copied();
                if !requests.is_empty()
                    && (next_bytes > byte_limit
                        || (known.is_none() && objects.len() as u64 >= self.max_entries))
                {
                    break;
                }
                let object = match known {
                    Some(index) => index,
                    None => {
                        let index = objects.len();
                        let key = Self::key(digest);
                        objects.push(IngestObject {
                            digest,
                            path: self.root.join(&key),
                            key,
                            first_request: requests.len(),
                            new: true,
                            source: None,
                            staged: None,
                        });
                        by_digest.insert(digest, index);
                        index
                    }
                };
                requests.push(IngestRequest {
                    destination,
                    executable,
                    object,
                });
                staged_bytes = next_bytes;
                // A file larger than the target staging budget is a group of
                // one, not a new size restriction on otherwise valid artifacts.
                if staged_bytes >= byte_limit {
                    break;
                }
            }
            if requests.is_empty() {
                return Ok(GroupResult { consumed: 0, found: true });
            }
            self.clear_staging()?;

            let transaction = self.db.unchecked_transaction()?;
            let mut removed_names = false;
            for object in &mut objects {
                let recorded: bool = self.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM objects WHERE key=?1)",
                    [&object.key],
                    |row| row.get(0),
                )?;
                if recorded {
                    match open_file(&object.path) {
                        Ok(source) => {
                            object.new = false;
                            object.source = Some(IngestSource::Canonical(source));
                            continue;
                        }
                        Err(error) => {
                            if object.path.try_exists()? {
                                return Err(error);
                            }
                            // Also remove a dangling symlink, without following it.
                            remove(&object.path)?;
                            removed_names = true;
                            self.db.execute("DELETE FROM objects WHERE key=?1", [&object.key])?;
                        }
                    }
                }
                let first = &requests[object.first_request];
                object.source = Some(match self.stage_recovery(
                    first.destination,
                    &object.key,
                    object.digest,
                    first.executable,
                    config,
                )? {
                    Some(staged) => IngestSource::Recovered(staged),
                    None if ingest => IngestSource::Produced(open_file(first.destination)?),
                    None => {
                        // Lookup-only calls contain one file. Retain invalid-hint
                        // cleanup, but never evict unrelated objects for a miss.
                        if removed_names {
                            File::open(&self.root)?.sync_all()?;
                        }
                        transaction.commit()?;
                        return Ok(GroupResult { consumed: requests.len(), found: false });
                    }
                });
            }

            let new_bytes = objects.iter().filter(|object| object.new)
                .map(|object| object.digest.size()).sum();
            let new_entries = objects.iter().filter(|object| object.new).count() as u64;
            removed_names |= self.evict_in_transaction(
                new_bytes,
                new_entries,
                objects.iter().map(|object| object.key.as_str()),
            )?;

            let mut publications = Vec::with_capacity(requests.len());
            // Reserve room before creating canonical staging files, so even
            // those temporary CAS bytes/entries fit alongside remaining objects.
            // A recovered view is beside its destination, not inside the CAS.
            for (slot, object) in objects.iter_mut().filter(|object| object.new).enumerate() {
                let source = object.source.take().expect("group source prepared");
                let mut staged = self.stage_canonical(slot)?;
                match &source {
                    IngestSource::Produced(file) => clone_file(file, staged.as_file())?,
                    IngestSource::Recovered(file) => clone_file(file.as_file(), staged.as_file())?,
                    IngestSource::Canonical(_) => unreachable!("new canonical object"),
                }
                verify(staged.as_file_mut(), object.digest, config)?;
                staged.as_file().set_permissions(Permissions::from_mode(0o444))?;
                object.staged = Some(staged);
                if let IngestSource::Recovered(file) = source {
                    publications.push(Publication {
                        file,
                        destination: requests[object.first_request].destination,
                    });
                }
            }
            for (index, request) in requests.iter().enumerate() {
                let object = &mut objects[request.object];
                if object.new && index == object.first_request {
                    // The original produced file already shares the new COW
                    // object's extents and retains its existing executable mode.
                    continue;
                }
                let publication = {
                    let source = if let Some(staged) = &object.staged {
                        staged.as_file()
                    } else {
                        match object.source.as_ref().expect("canonical source prepared") {
                            IngestSource::Canonical(file) => file,
                            _ => unreachable!("existing canonical object"),
                        }
                    };
                    self.stage_publication(
                        source, request.destination, object.digest, request.executable, config,
                    )
                };
                match publication {
                    Ok(file) => publications.push(Publication {
                        file,
                        destination: request.destination,
                    }),
                    Err(error) => {
                        // Destination I/O/reflink errors are not corruption.
                        if let Some(IngestSource::Canonical(source)) = object.source.as_mut() {
                            if verify(source, object.digest, config).is_err() {
                                remove(&object.path)?;
                                self.db.execute("DELETE FROM objects WHERE key=?1", [&object.key])?;
                                // No new accounting or canonical renames have
                                // occurred. Commit only removals/hint cleanup.
                                File::open(&self.root)?.sync_all()?;
                                transaction.commit()?;
                            }
                        }
                        return Err(error);
                    }
                }
            }

            // Every verified snapshot must be durable before accounting or
            // publication. The dedicated pool joins all work, including on an
            // error, before any stage can be removed or renamed.
            {
                let mut files = objects
                    .iter()
                    .filter_map(|object| object.staged.as_ref())
                    .chain(publications.iter().map(|publication| &publication.file));
                if let Some(first) = files.next() {
                    if files.next().is_none() {
                        // Singleton downloads and files do not start a pool.
                        first.as_file().sync_all()?;
                    } else {
                        FILE_SYNCS.as_ref().map_err(error)?.install(|| {
                            objects
                                .par_iter()
                                .filter_map(|object| object.staged.as_ref())
                                .chain(publications.par_iter().map(|publication| &publication.file))
                                .try_for_each(|file| file.as_file().sync_all())
                        })?;
                    }
                }
            }
            if removed_names {
                File::open(&self.root)?.sync_all()?;
            }
            for object in objects.iter().filter(|object| object.new) {
                self.db.execute(
                    "INSERT INTO objects VALUES(?1,?2,0)",
                    params![object.key, object.digest.size()],
                )?;
            }
            // FULL accounting is durable BEFORE any canonical rename. Failure
            // can leave accounted misses; it cannot publish unaccounted objects.
            transaction.commit()?;
            for object in &mut objects {
                if let Some(staged) = object.staged.take() {
                    staged.persist(&object.path).map_err(|error| error.error)?;
                }
            }
            if new_entries != 0 {
                File::open(&self.root)?.sync_all()?;
            }
            let mut parents = HashSet::with_capacity(publications.len());
            for publication in publications {
                let parent = publication.destination.parent()
                    .ok_or_else(|| error("Missing output parent"))?;
                publication.file.persist(publication.destination).map_err(|error| error.error)?;
                parents.insert(parent);
            }
            for parent in parents {
                File::open(parent)?.sync_all()?;
            }
            let transaction = self.db.unchecked_transaction()?;
            for request in &requests {
                self.record_access(request.destination, &objects[request.object].key)?;
            }
            transaction.commit()?;
            Ok(GroupResult { consumed: requests.len(), found: true })
        }

        fn access(
            &self,
            destination: &Path,
            digest: &FileDigest,
            executable: bool,
            config: DigestConfig,
            ingest: bool,
        ) -> buck2_error::Result<bool> {
            Ok(self.access_group(
                std::iter::once((destination, digest, executable)),
                config,
                ingest,
            )?.found)
        }

        // The entire group's accesses share the caller's FULL transaction.
        fn record_access(&self, destination: &Path, key: &str) -> buck2_error::Result<()> {
            self.db
                .execute_batch("UPDATE clock SET tick=tick+1 WHERE id=1;")?;
            self.db.execute(
                "UPDATE objects SET used=(SELECT tick FROM clock WHERE id=1) WHERE key=?1",
                [key],
            )?;
            self.remember_view(destination, key)?;
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
            store.access(destination, digest, executable, config, ingest)
        })
    }

    pub(super) fn ingest_files(
        files: &[(AbsNormPathBuf, FileMetadata)],
        config: DigestConfig,
    ) -> buck2_error::Result<()> {
        let mut offset = 0;
        while offset < files.len() {
            // Byte/entry limits may split the outer descriptor-bounded batch.
            // Release BOTH locks between those groups, not just between artifacts.
            offset += with_store(files.len() - offset, |store| {
                let group = store.access_group(
                    files[offset..].iter().map(|(path, metadata)| {
                        (path.as_path(), metadata.digest.data(), metadata.is_executable)
                    }),
                    config,
                    true,
                )?;
                Ok(group.consumed)
            })?;
        }
        Ok(())
    }

    pub(super) fn published(
        temporary: &Path,
        destination: &Path,
        digest: &FileDigest,
    ) -> buck2_error::Result<()> {
        with_store((), |store| {
            let transaction = store.db.unchecked_transaction()?;
            store.db.execute(
                "DELETE FROM views WHERE path=?1",
                [temporary.to_str().ok_or_else(|| error("Non-UTF-8 temporary artifact path"))?],
            )?;
            store.remember_view(destination, &Store::key(digest))?;
            transaction.commit()?;
            Ok(())
        })
    }

    #[cfg(test)]
    mod tests {
        use buck2_common::cas_digest::DigestAlgorithm;
        use dupe::Dupe;

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
                super::super::download_tests::exercise_pending_downloads,
            )
        }

        fn assert_bounded_and_accounted(store: &Store) -> buck2_error::Result<()> {
            let totals: (u64, u64) = store.db.query_row(
                "SELECT bytes,entries FROM totals WHERE id=1", [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let actual: (u64, u64) = store.db.query_row(
                "SELECT COALESCE(SUM(size),0),COUNT(*) FROM objects", [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert_eq!(totals, actual);
            assert!(totals.0 <= store.max_bytes && totals.1 <= store.max_entries);
            let views: (u64, u64) = store.db.query_row(
                "SELECT (SELECT entries FROM view_totals WHERE id=1),COUNT(*) FROM views", [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            assert_eq!(views.0, views.1);
            assert!(views.0 <= store.max_entries);
            let mut physical = (0u64, 0u64);
            for entry in fs::read_dir(&store.root)? {
                let entry = entry?;
                let name = entry.file_name().into_string().expect("ASCII cache filename");
                if matches!(name.as_str(), "lock" | "index.sqlite" | "index.sqlite-journal") {
                    continue;
                }
                let recorded: bool = store.db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM objects WHERE key=?1)", [&name],
                    |row| row.get(0),
                )?;
                assert!(recorded, "unaccounted canonical or leaked staging file: {name}");
                physical.0 += entry.metadata()?.len();
                physical.1 += 1;
            }
            assert_eq!(physical, totals);
            Ok(())
        }

        fn ingest_groups(
            store: &Store,
            files: &[(PathBuf, FileDigest, bool)],
            config: DigestConfig,
        ) -> buck2_error::Result<()> {
            let mut offset = 0;
            while offset < files.len() {
                store.lock.lock()?;
                let result = (|| {
                    let group = store.access_group(
                        files[offset..].iter().map(|(path, digest, executable)| {
                            (path.as_path(), digest, *executable)
                        }), config, true,
                    )?;
                    assert!(group.found);
                    assert!(group.consumed > 0 && group.consumed <= super::super::INGEST_BATCH_FILES);
                    assert_bounded_and_accounted(store)?;
                    Ok::<_, buck2_error::Error>(group)
                })();
                let unlock = store.lock.unlock();
                let group = result?;
                unlock?;
                offset += group.consumed;
            }
            Ok(())
        }

        #[test]
        fn reflink_store_batches_duplicate_digests_and_modes() -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let store = Store::open(temp.path().join("store"), 3 * 4096, 8)?;
            fs::create_dir(temp.path().join("left"))?;
            fs::create_dir(temp.path().join("right"))?;
            let mut files = Vec::new();
            for index in 0..(super::super::INGEST_BATCH_FILES * 2 + 3) {
                let bytes = vec![(index / 2 % 3) as u8; 4096];
                let executable = index % 2 != 0;
                let parent = if executable { "left" } else { "right" };
                let path = temp.path().join(parent).join(index.to_string());
                fs::write(&path, &bytes)?;
                fs::set_permissions(&path, Permissions::from_mode(if executable { 0o755 } else { 0o644 }))?;
                files.push((path, FileDigest::from_content(&bytes, config.cas_digest_config()), executable));
            }
            ingest_groups(&store, &files, config)?;
            let entries: u64 = store.db.query_row("SELECT entries FROM totals", [], |row| row.get(0))?;
            assert_eq!(entries, 3);
            for (path, digest, executable) in &files {
                assert_eq!(FileDigest::from_content(&fs::read(path)?, config.cas_digest_config()), *digest);
                assert_eq!(fs::metadata(path)?.mode() & 0o777, if *executable { 0o755 } else { 0o644 });
                assert_ne!(fs::metadata(path)?.ino(), fs::metadata(store.root.join(Store::key(digest)))?.ino());
            }
            assert_ne!(fs::metadata(&files[0].0)?.ino(), fs::metadata(&files[1].0)?.ino());
            fs::write(&files[0].0, b"independent mutation")?;
            assert_eq!(fs::read(&files[1].0)?, vec![0; 4096]);
            assert_eq!(fs::read(store.root.join(Store::key(&files[1].1)))?, vec![0; 4096]);
            Ok(())
        }

        #[test]
        fn reflink_store_batches_stream_capacity_and_clean_failed_stages() -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let root = temp.path().join("store");
            let store = Store::open(root.clone(), 8192, 2)?;
            let mut files = Vec::new();
            for index in 0..7 {
                let bytes = vec![index; 4096];
                let path = temp.path().join(format!("source-{index}"));
                fs::write(&path, &bytes)?;
                files.push((path, FileDigest::from_content(&bytes, config.cas_digest_config()), false));
            }
            // The artifact is larger than both cache limits; each group fits.
            ingest_groups(&store, &files, config)?;
            for (path, digest, _) in &files {
                assert_eq!(FileDigest::from_content(&fs::read(path)?, config.cas_digest_config()), *digest);
            }
            // Exercise the entry limit independently of the byte limit.
            let entry_limited = Store::open(temp.path().join("entry-limited"), 7 * 4096, 1)?;
            let first = entry_limited.access_group(
                files.iter().map(|(path, digest, executable)| (path.as_path(), digest, *executable)),
                config, true,
            )?;
            assert_eq!(first.consumed, 1);
            ingest_groups(&entry_limited, &files[1..], config)?;
            let good = temp.path().join("good");
            let bad = temp.path().join("bad");
            fs::write(&good, vec![90; 4096])?;
            fs::write(&bad, vec![92; 4096])?;
            let good_digest = FileDigest::from_content(&vec![90; 4096], config.cas_digest_config());
            let bad_digest = FileDigest::from_content(&vec![91; 4096], config.cas_digest_config());
            assert!(store.access_group(
                [(good.as_path(), &good_digest, false), (bad.as_path(), &bad_digest, false)].into_iter(),
                config, true,
            ).is_err());
            assert!(!root.join(Store::key(&good_digest)).exists());
            assert!(!root.join(Store::key(&bad_digest)).exists());
            for entry in fs::read_dir(&root)? {
                assert!(!entry?.file_name().as_encoded_bytes().starts_with(b".buck2-pending-"));
            }
            assert_eq!(fs::read(&good)?, vec![90; 4096]);
            assert_eq!(fs::read(&bad)?, vec![92; 4096]);
            // Rollback can leave accounted misses, never unaccounted objects.
            drop(store);
            let store = Store::open(root, 8192, 2)?;
            assert_bounded_and_accounted(&store)?;
            // A valid weak hint plus a bad destination is an error, not a miss.
            let missing_parent = temp.path().join("missing-parent").join("output");
            assert!(store.access(&missing_parent, &files[6].1, false, config, false).is_err());
            let recovered = temp.path().join("recovered");
            assert!(store.access(&recovered, &files[6].1, false, config, false)?);
            assert_eq!(fs::read(recovered)?, vec![6; 4096]);
            assert_bounded_and_accounted(&store)?;
            Ok(())
        }

        #[test]
        fn reflink_store_batch_publication_error_cleans_staged_views() -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let store = Store::open(temp.path().join("store"), 4096, 4)?;
            let source = temp.path().join("source");
            fs::write(&source, b"correct")?;
            let digest = FileDigest::from_content(b"correct", config.cas_digest_config());
            store.access(&source, &digest, false, config, true)?;
            let good = temp.path().join("good");
            fs::write(&good, b"previous")?;
            let bad = temp.path().join("directory");
            fs::create_dir(&bad)?;
            fs::write(bad.join("keep"), b"untouched")?;
            assert!(store.access_group(
                [(good.as_path(), &digest, false), (bad.as_path(), &digest, true)].into_iter(),
                config, true,
            ).is_err());
            // The first rename may already have happened. It contains verified
            // bytes, but the failing group must never be declared successful.
            assert_eq!(fs::read(&good)?, b"correct");
            assert_eq!(fs::read(bad.join("keep"))?, b"untouched");
            assert_eq!(fs::read(store.root.join(Store::key(&digest)))?, b"correct");
            for entry in fs::read_dir(temp.path())? {
                assert!(!entry?.file_name().as_encoded_bytes().starts_with(b".buck2-cow-"));
            }
            assert_bounded_and_accounted(&store)?;
            Ok(())
        }

        #[test]
        fn reflink_store_batch_staging_budget_and_sparse_large_file() -> buck2_error::Result<()> {
            let temp = tempfile::tempdir()?;
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            let large = temp.path().join("large");
            let size = INGEST_BATCH_BYTES / 2 + 4096;
            File::create(&large)?.set_len(size)?;
            let mut source = File::open(&large)?;
            let digest =
                FileDigest::from_reader_for_algorithm(&mut source, DigestAlgorithm::Sha256)?;
            drop(source);
            let small = temp.path().join("small");
            fs::write(&small, b"small")?;
            let small_digest = FileDigest::from_content(b"small", config.cas_digest_config());
            let store = Store::open(temp.path().join("store"), size + 4096, 4)?;
            let group = store.access_group(
                [(large.as_path(), &digest, false), (small.as_path(), &small_digest, false)].into_iter(),
                config, true,
            )?;
            assert_eq!(group.consumed, 1);
            assert!(group.found);
            let canonical = store.root.join(Store::key(&digest));
            assert!(fs::metadata(&canonical)?.blocks() * 512 < size);
            assert_ne!(fs::metadata(&canonical)?.ino(), fs::metadata(&large)?.ino());
            assert!(store.access(&small, &small_digest, false, config, true)?);
            assert_bounded_and_accounted(&store)?;
            let too_small = Store::open(temp.path().join("small-store"), size - 1, 4)?;
            assert!(too_small.access(&large, &digest, false, config, true).is_err());
            assert_bounded_and_accounted(&too_small)?;
            Ok(())
        }

        #[test]
        fn reflink_store_reconciles_interrupted_batch_phases() -> buck2_error::Result<()> {
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            // Reopen actual namespace/accounting states, not a simulated claim
            // of power-loss durability: zero, one, or two canonical renames.
            for phase in 0..4 {
                let temp = tempfile::tempdir()?;
                let root = temp.path().join("store");
                let store = Store::open(root.clone(), 16384, 2)?;
                let mut objects = Vec::new();
                for index in 0..2 {
                    let bytes = vec![index; 8192];
                    let source_path = temp.path().join(format!("source-{index}"));
                    fs::write(&source_path, &bytes)?;
                    let digest = FileDigest::from_content(&bytes, config.cas_digest_config());
                    let pending = store.staging[index as usize].clone();
                    let mut staged = OpenOptions::new().read(true).write(true).create_new(true)
                        .mode(0o600).open(&pending)?;
                    clone_file(&open_file(&source_path)?, &staged)?;
                    verify(&mut staged, &digest, config)?;
                    staged.set_permissions(Permissions::from_mode(0o444))?;
                    staged.sync_all()?;
                    objects.push((pending, digest));
                }
                if phase != 0 {
                    let transaction = store.db.unchecked_transaction()?;
                    for (_, digest) in &objects {
                        store.db.execute("INSERT INTO objects VALUES(?1,?2,0)", params![Store::key(digest), digest.size()])?;
                    }
                    transaction.commit()?;
                }
                let published = if phase > 1 { phase - 1 } else { 0 };
                for (pending, digest) in objects.iter().take(published) {
                    fs::rename(pending, root.join(Store::key(digest)))?;
                }
                drop(store);
                let store = Store::open(root.clone(), 16384, 2)?;
                assert_bounded_and_accounted(&store)?;
                let entries: usize = store.db.query_row("SELECT entries FROM totals", [], |row| row.get(0))?;
                assert_eq!(entries, published);
                for (index, (_, digest)) in objects.iter().enumerate() {
                    let output = temp.path().join(format!("output-{index}"));
                    assert_eq!(store.access(&output, digest, false, config, false)?, index < published);
                    if index < published {
                        assert_eq!(fs::read(&output)?, vec![index as u8; 8192]);
                    }
                }
            }
            Ok(())
        }

        #[test]
        fn reflink_store_open_survivor_cleans_dead_peer_staging() -> buck2_error::Result<()> {
            const CHILD_ROOT: &str = "BUCK2_MATERIALIZER_STAGING_CRASH_ROOT";
            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
            if let Some(root) = std::env::var_os(CHILD_ROOT) {
                let store = Store::open(PathBuf::from(root), 8192, 2)?;
                store.lock.lock()?;
                // Exit without dropping either the SQLite transaction or the
                // NamedTempFiles, leaving live stages and an uncommitted journal.
                let _transaction = store.db.unchecked_transaction()?;
                store.evict_in_transaction(8192, 2, std::iter::empty())?;
                // The surviving parent's fixture owns and removes these sources.
                let temp = tempfile::tempdir_in(store.root.parent().expect("cache parent"))?;
                let mut stages = Vec::new();
                for slot in 0..2 {
                    let bytes = vec![slot as u8; 4096];
                    let source = temp.path().join(slot.to_string());
                    fs::write(&source, &bytes)?;
                    let digest = FileDigest::from_content(&bytes, config.cas_digest_config());
                    let mut staged = store.stage_canonical(slot)?;
                    clone_file(&open_file(&source)?, staged.as_file())?;
                    verify(staged.as_file_mut(), &digest, config)?;
                    staged.as_file().set_permissions(Permissions::from_mode(0o444))?;
                    staged.as_file().sync_all()?;
                    stages.push(staged);
                }
                File::open(&store.root)?.sync_all()?;
                std::process::exit(0);
            }

            let temp = tempfile::tempdir()?;
            let root = temp.path().join("store");
            // This handle stays open throughout both peer deaths and recoveries.
            let store = Store::open(root.clone(), 8192, 2)?;
            let initial = temp.path().join("initial");
            fs::write(&initial, vec![80; 8192])?;
            let digest = FileDigest::from_content(&vec![80; 8192], config.cas_digest_config());
            store.access(&initial, &digest, false, config, true)?;
            for round in 0..2 {
                let child = std::process::Command::new(std::env::current_exe()?)
                    .args([
                        "--exact",
                        "materializers::local_content_store::linux::tests::reflink_store_open_survivor_cleans_dead_peer_staging",
                        "--nocapture",
                    ])
                    .env(CHILD_ROOT, &root)
                    .output()?;
                assert!(child.status.success(), "{}", String::from_utf8_lossy(&child.stderr));
                assert!(store.staging[0].is_file());
                assert!(store.staging[1].is_file());
                let bytes = vec![90 + round; 8192];
                let output = temp.path().join(format!("survivor-{round}"));
                fs::write(&output, &bytes)?;
                let files = [(output, FileDigest::from_content(&bytes, config.cas_digest_config()), false)];
                ingest_groups(&store, &files, config)?;
                assert_eq!(fs::read(&files[0].0)?, bytes);
                assert!(store.staging.iter().all(|path| !path.exists()));
                assert_bounded_and_accounted(&store)?;
            }
            Ok(())
        }

        #[test]
        fn reflink_store_batches_respect_descriptor_limit() -> buck2_error::Result<()> {
            super::super::download_tests::check_descriptor_limit(
                true,
                "materializers::local_content_store::linux::tests::reflink_store_batches_respect_descriptor_limit",
                || {
                    let temp = tempfile::tempdir()?;
                    let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
                    let store = Store::open(temp.path().join("store"), 16 * 4096, 64)?;
                    let mut originals = Vec::new();
                    let mut evictors = Vec::new();
                    for index in 0..16 {
                        for (files, value, prefix) in [
                            (&mut originals, index, "original"),
                            (&mut evictors, index + 32, "evictor"),
                        ] {
                            let bytes = vec![value; 4096];
                            let path = temp.path().join(format!("{prefix}-{index}"));
                            fs::write(&path, &bytes)?;
                            files.push((path, FileDigest::from_content(&bytes, config.cas_digest_config()), false));
                        }
                    }
                    ingest_groups(&store, &originals, config)?;
                    ingest_groups(&store, &evictors, config)?;
                    let mut files = Vec::new();
                    for index in 0..512 {
                        // First group retains two staging descriptors per live
                        // view recovery; later groups retain source plus snapshot.
                        files.push((
                            temp.path().join(format!("output-{index}")),
                            originals[index % 16].1.dupe(),
                            false,
                        ));
                    }
                    ingest_groups(&store, &files, config)?;
                    for (index, (path, _, _)) in files.iter().enumerate() {
                        assert_eq!(fs::read(path)?, vec![(index % 16) as u8; 4096]);
                    }
                    Ok(())
                },
            )
        }

        #[test]
        fn reflink_store_syncs_complete_with_one_blocking_worker() -> buck2_error::Result<()> {
            super::super::download_tests::check_descriptor_limit(
                true,
                "materializers::local_content_store::linux::tests::reflink_store_syncs_complete_with_one_blocking_worker",
                || {
                    // This callback runs only in the isolated, single-test child.
                    // Bound a wrong-pool deadlock without hanging the parent suite.
                    std::thread::spawn(|| {
                        std::thread::sleep(std::time::Duration::from_secs(30));
                        eprintln!("Content-store synchronization deadlocked");
                        std::process::exit(1);
                    });
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .max_blocking_threads(1)
                        .build()?;
                    runtime.block_on(async {
                        tokio::task::spawn_blocking(|| -> buck2_error::Result<()> {
                            let temp = tempfile::tempdir()?;
                            let config = DigestConfig::leak_new(vec![DigestAlgorithm::Sha256], None)?;
                            let store = Mutex::new(Store::open(temp.path().join("store"), 16384, 4)?);
                            let store = store.lock().expect("uncontended store");
                            let mut files = Vec::new();
                            for index in 0..2u8 {
                                let path = temp.path().join(format!("file-{index}"));
                                let bytes = [index + 1; 4096];
                                fs::write(&path, bytes)?;
                                files.push((
                                    path,
                                    FileDigest::from_content(&bytes, config.cas_digest_config()),
                                    false,
                                ));
                            }
                            // Exercise canonical stages, then existing-object
                            // publication snapshots, on the sole blocking worker.
                            ingest_groups(&store, &files, config)?;
                            ingest_groups(&store, &files, config)?;
                            for (index, (path, _, _)) in files.iter().enumerate() {
                                assert_eq!(fs::read(path)?, vec![index as u8 + 1; 4096]);
                            }
                            Ok(())
                        }).await.map_err(error)?
                    })
                },
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
                            let other_bytes = vec![(value + 64) as u8; 128 * 1024];
                            let other_digest = FileDigest::from_content(&other_bytes, config.cas_digest_config());
                            let other_path = parent.join(format!("{worker}-{iteration}-other"));
                            fs::write(&other_path, &other_bytes)?;
                            let files = [
                                (path, digest, worker % 2 == 0),
                                (other_path, other_digest, worker % 2 != 0),
                            ];
                            ingest_groups(&store, &files, config)?;
                            assert_eq!(fs::read(&files[0].0)?, bytes);
                            assert_eq!(fs::read(&files[1].0)?, other_bytes);
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
use buck2_common::file_ops::metadata::FileMetadata;
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
use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
use dupe::Dupe;

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
    let mut files = Vec::with_capacity(INGEST_BATCH_FILES);
    while let Some((relative, entry)) = walk.next() {
        if let DirectoryEntry::Leaf(ActionDirectoryMember::File(metadata)) = entry {
            files.push((path.join(relative.get()), metadata.dupe()));
            if files.len() == INGEST_BATCH_FILES {
                ingest_files(&files, config)?;
                files.clear();
            }
        }
    }
    ingest_files(&files, config)?;
    Ok(())
}

fn ingest_files(
    files: &[(AbsNormPathBuf, FileMetadata)],
    config: DigestConfig,
) -> buck2_error::Result<()> {
    #[cfg(target_os = "linux")]
    linux::ingest_files(files, config)?;
    #[cfg(not(target_os = "linux"))]
    for (path, metadata) in files {
        file(path, metadata.digest.data(), metadata.is_executable, config, true)?;
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
            exercise_pending_downloads,
        )
    }

    #[cfg(target_os = "linux")]
    pub(super) fn check_descriptor_limit(
        shared_store: bool,
        test_name: &str,
        exercise: fn() -> buck2_error::Result<()>,
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
        exercise()
    }

    #[cfg(target_os = "linux")]
    pub(super) fn exercise_pending_downloads() -> buck2_error::Result<()> {
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
