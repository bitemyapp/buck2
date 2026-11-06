/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Speculative bytes, never speculative action results. Filesystem events only
//! nominate candidates. Private snapshots are hashed and prepared independently
//! of compiler paths; ordinary final output hashing decides which views apply.
//! All work is action-owned and drained before its scratch/claim can be released.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fs::File;
use std::fs::Permissions;
use std::future::Future;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;

use buck2_common::file_ops::metadata::FileDigest;
use buck2_common::file_ops::metadata::FileMetadata;
use buck2_common::file_ops::metadata::TrackedFileDigest;
use buck2_error::BuckErrorContext;
use buck2_execute::digest::CasDigestToReExt;
use buck2_execute::digest_config::DigestConfig;
use buck2_execute::materialize::materializer::CasDownloadInfo;
use buck2_execute::re::manager::ManagedRemoteExecutionClient;
use buck2_execute::re::presence::NegativeCache;
use buck2_fs::paths::abs_norm_path::AbsNormPathBuf;
use dupe::Dupe;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use remote_execution::NamedDigest;
use tempfile::TempPath;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::sync::watch;

use super::output_watcher::OutputWatcher;
use crate::materializers::local_content_store;
use crate::materializers::prepared_outputs;
use crate::materializers::prepared_outputs::CaptureDirectory;
use crate::materializers::prepared_outputs::CaptureRetention;
use crate::materializers::prepared_outputs::PreparedEntry;
use crate::materializers::prepared_outputs::PreparedFile;

const BATCH_FILES: usize = 8;
const PENDING_FILES: usize = 256;
const ACTIVE_BATCHES: usize = 4;
const PREPARED_HANDLES: usize = 4096;
static WORKERS: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(ACTIVE_BATCHES)));

type Completed = buck2_error::Result<Vec<Arc<PreparedEntry>>>;

pub(crate) struct OutputPipeline {
    watcher: Option<OutputWatcher>,
    directory: Arc<CaptureDirectory>,
    client: Option<ManagedRemoteExecutionClient>,
    config: DigestConfig,
    pending: VecDeque<PathBuf>,
    queued: HashSet<PathBuf>,
    work: FuturesUnordered<BoxFuture<'static, Completed>>,
    prepared: HashMap<usize, Arc<PreparedEntry>>,
    uploads: watch::Sender<bool>,
}

impl OutputPipeline {
    pub(crate) fn new(
        project_root: &Path,
        outputs: Vec<PathBuf>,
        client: Option<ManagedRemoteExecutionClient>,
        config: DigestConfig,
    ) -> buck2_error::Result<Option<Self>> {
        let Some(parent) = outputs.first().and_then(|path| path.parent()) else {
            return Ok(None);
        };
        // Staging must not appear in any output tree, including during its final
        // walk. A parent of overlapping declared outputs is not a safe location.
        if outputs.iter().any(|output| parent.starts_with(output)) {
            return Ok(None);
        }
        let directory = CaptureDirectory::create(parent)?;
        let watcher = OutputWatcher::new(project_root, outputs)?;
        let (uploads, _) = watch::channel(true);
        Ok(Some(Self {
            watcher: Some(watcher),
            directory,
            client,
            config,
            pending: VecDeque::new(),
            queued: HashSet::new(),
            work: FuturesUnordered::new(),
            prepared: HashMap::new(),
            uploads,
        }))
    }

    /// Poll alongside the producer, without changing its cancellation protocol.
    /// This returns at producer completion; finish() drains remaining work outside
    /// the LocalExecute timing span, while retaining prepared publication views.
    pub(crate) async fn run<F: Future>(&mut self, producer: F) -> F::Output {
        tokio::pin!(producer);
        loop {
            tokio::select! {
                result = &mut producer => {
                    self.pending.clear();
                    self.queued.clear();
                    return result;
                }
                batch = async { self.watcher.as_mut().expect("enabled watcher").next_batch().await },
                    if self.watcher.is_some() => {
                    match batch {
                        Ok(paths) => {
                            for path in paths {
                                if self.pending.len() == PENDING_FILES {
                                    break;
                                }
                                if self.queued.insert(path.clone()) {
                                    self.pending.push_back(path);
                                }
                            }
                        }
                        Err(error) => {
                            // Lost hints cannot establish finality. Normal final
                            // hashing/store ingestion remains authoritative.
                            tracing::debug!(%error, "Stopping speculative output observation");
                            self.watcher = None;
                            self.pending.clear();
                            self.queued.clear();
                        }
                    }
                }
                permit = WORKERS.clone().acquire_owned(),
                    if !self.pending.is_empty() && self.work.len() < ACTIVE_BATCHES => {
                    self.launch(permit.expect("pipeline semaphore never closes"));
                }
                Some(result) = self.work.next(), if !self.work.is_empty() => self.collect(result),
            }
        }
    }

    fn launch(&mut self, permit: OwnedSemaphorePermit) {
        let mut sources = Vec::with_capacity(BATCH_FILES);
        let watcher = self
            .watcher
            .as_ref()
            .expect("pending candidates have a watcher");
        while sources.len() < BATCH_FILES {
            let Some(path) = self.pending.pop_front() else {
                break;
            };
            self.queued.remove(&path);
            // Resolve again beneath the declared roots. Events and previous
            // metadata never authorize following a swapped symlink or FIFO.
            if let Ok(source) = watcher.open_candidate(&path) {
                if let Ok(metadata) = source.metadata() {
                    if let Some(retention) =
                        prepared_outputs::reserve(metadata.len(), self.directory.clone())
                    {
                        sources.push((source, metadata, retention));
                    }
                }
            }
        }
        if sources.is_empty() {
            return;
        }
        let client = self.client.dupe();
        let config = self.config;
        let want_upload = client.is_some();
        let mut uploads_enabled = self.uploads.subscribe();
        self.work.push(async move {
            // Never detach blocking work on cancellation: finish() drains every
            // future, including its capture join, before releasing action state.
            let (uploads, prepared) = tokio::task::spawn_blocking(move || capture(sources, config, want_upload))
                .await
                .buck_error_context("Joining speculative output capture")??;
            if let Some(client) = client.filter(|_| *uploads_enabled.borrow()) {
                let upload = upload(&client, &uploads);
                tokio::pin!(upload);
                tokio::select! {
                    result = &mut upload => {
                        if let Err(error) = result {
                            tracing::debug!(%error, "Speculative CAS upload failed; final upload remains authoritative");
                        }
                    }
                    _ = uploads_enabled.changed() => {}
                }
            }
            // Paths and their retention permits outlive all transport retries.
            drop(uploads);
            drop(permit);
            Ok(prepared)
        }.boxed());
    }

    fn collect(&mut self, result: Completed) {
        match result {
            Ok(entries) => {
                if self.prepared.len() + entries.len() > PREPARED_HANDLES {
                    self.prepared.retain(|_, entry| entry.is_available());
                }
                for entry in entries {
                    let key = Arc::as_ptr(&entry) as usize;
                    if self.prepared.len() < PREPARED_HANDLES {
                        self.prepared.entry(key).or_insert(entry);
                    }
                }
            }
            Err(error) => {
                tracing::debug!(%error, "Speculative output preparation failed; final processing remains authoritative")
            }
        }
    }

    pub(crate) async fn finish(&mut self, successful: bool, cancelled: impl Future<Output = ()>) {
        self.watcher = None;
        self.pending.clear();
        self.queued.clear();
        let mut retain = successful;
        if !retain {
            self.uploads.send_replace(false);
        }
        tokio::pin!(cancelled);
        while !self.work.is_empty() {
            tokio::select! {
                _ = &mut cancelled, if retain => {
                    retain = false;
                    self.uploads.send_replace(false);
                }
                Some(result) = self.work.next() => self.collect(result),
            }
        }
        if !retain {
            self.prepared.clear();
        }
    }
}

struct Upload {
    path: TempPath,
    metadata: FileMetadata,
    _retention: Arc<CaptureRetention>,
}

/// The clone length is explicit: a live file growing after fstat must not bypass
/// retention accounting. Unsupported/unaligned reflinks use a bounded copy into
/// private storage, never a hardlink or a read of live bytes during CAS transfer.
fn snapshot(source: &mut File, destination: &mut File, size: u64) -> std::io::Result<()> {
    #[repr(C)]
    struct CloneRange {
        source_fd: i64,
        source_offset: u64,
        source_length: u64,
        destination_offset: u64,
    }
    unsafe extern "C" {
        fn ioctl(fd: i32, request: std::ffi::c_ulong, ...) -> i32;
    }
    if size == 0 {
        return Ok(());
    }
    let range = CloneRange {
        source_fd: i64::from(source.as_raw_fd()),
        source_offset: 0,
        source_length: size,
        destination_offset: 0,
    };
    if unsafe { ioctl(destination.as_raw_fd(), 0x4020940D, &range) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if !matches!(error.raw_os_error(), Some(18 | 22 | 25 | 95)) {
        return Err(error);
    }
    destination.set_len(0)?;
    destination.seek(SeekFrom::Start(0))?;
    source.seek(SeekFrom::Start(0))?;
    std::io::copy(&mut source.take(size), destination)?;
    Ok(())
}

fn capture(
    sources: Vec<(File, std::fs::Metadata, Arc<CaptureRetention>)>,
    config: DigestConfig,
    want_upload: bool,
) -> buck2_error::Result<(Vec<Upload>, Vec<Arc<PreparedEntry>>)> {
    let mut files = Vec::with_capacity(sources.len());
    for (mut source, metadata, retention) in sources {
        let mut staged = tempfile::Builder::new()
            .prefix("view-")
            .tempfile_in(retention.directory())?;
        snapshot(&mut source, staged.as_file_mut(), metadata.len())?;
        staged.as_file_mut().seek(SeekFrom::Start(0))?;
        let digest = FileDigest::from_reader(staged.as_file_mut(), config.cas_digest_config())?;
        let metadata = FileMetadata {
            digest: TrackedFileDigest::new(digest, config.cas_digest_config()),
            is_executable: metadata.mode() & 0o111 != 0,
        };
        files.push(PreparedFile {
            path: staged.into_temp_path(),
            metadata,
            retention,
        });
    }
    if local_content_store::enabled() {
        let paths = files
            .iter()
            .map(|file| {
                Ok((
                    AbsNormPathBuf::new(file.path.to_path_buf())?,
                    file.metadata.dupe(),
                ))
            })
            .collect::<buck2_error::Result<Vec<_>>>()?;
        // Only private paths are passed here: ingestion may replace them, but
        // cannot replace a compiler inode while an open writer still owns it.
        local_content_store::ingest_files(&paths, config)?;
    }
    let mut uploads = Vec::with_capacity(if want_upload { files.len() } else { 0 });
    let mut prepared = Vec::with_capacity(if local_content_store::enabled() {
        files.len()
    } else {
        0
    });
    for file in files {
        if local_content_store::enabled() {
            let mut source = File::open(&file.path)?;
            let mode = if file.metadata.is_executable {
                0o755
            } else {
                0o644
            };
            if source.metadata()?.mode() & 0o7777 != mode {
                // A newly admitted object retains the original 0600 private
                // file; normalize and sync that view. Reused store views have
                // already been normalized and synced before ingestion returns.
                source.set_permissions(Permissions::from_mode(mode))?;
                source.sync_all()?;
            }
            if want_upload {
                let mut staged = tempfile::Builder::new()
                    .prefix("upload-")
                    .tempfile_in(file.retention.directory())?;
                snapshot(
                    &mut source,
                    staged.as_file_mut(),
                    file.metadata.digest.data().size(),
                )?;
                // This source is already immutable and verified; the independent
                // upload snapshot survives final publication's rename of the view.
                staged
                    .as_file()
                    .set_permissions(Permissions::from_mode(0o444))?;
                uploads.push(Upload {
                    path: staged.into_temp_path(),
                    metadata: file.metadata.dupe(),
                    _retention: file.retention.clone(),
                });
            }
            prepared.push(prepared_outputs::register(file)?);
        } else {
            // Without native view publication no second snapshot is necessary.
            std::fs::set_permissions(&file.path, Permissions::from_mode(0o444))?;
            uploads.push(Upload {
                path: file.path,
                metadata: file.metadata,
                _retention: file.retention,
            });
        }
    }
    Ok((uploads, prepared))
}

async fn upload(
    client: &ManagedRemoteExecutionClient,
    files: &[Upload],
) -> buck2_error::Result<()> {
    let digests = files
        .iter()
        .map(|file| file.metadata.digest.dupe())
        .collect();
    let info = CasDownloadInfo::new_uploaded(client.use_case);
    let presence = client
        .check_presence(digests, NegativeCache::Allowed, &info)
        .await?;
    let missing = files
        .iter()
        .zip(presence)
        .filter(|(_, present)| present.is_none())
        .map(|(file, _)| {
            Ok(NamedDigest {
                name: file
                    .path
                    .to_str()
                    .ok_or_else(|| {
                        buck2_error::buck2_error!(
                            buck2_error::ErrorTag::Input,
                            "Non-UTF-8 speculative snapshot path"
                        )
                    })?
                    .to_owned(),
                digest: file.metadata.digest.to_re(),
                ..Default::default()
            })
        })
        .collect::<buck2_error::Result<Vec<_>>>()?;
    if !missing.is_empty() {
        client
            .upload_files_and_directories(missing, Vec::new(), Vec::new())
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[tokio::test]
    async fn cancellation_after_producer_exit_joins_capture_and_stops_upload_wait() {
        let root = tempfile::tempdir().unwrap();
        let mut pipeline = OutputPipeline::new(
            root.path(),
            vec![root.path().join("output")],
            None,
            DigestConfig::testing_default(),
        )
        .unwrap()
        .unwrap();
        let completed = root.path().join("capture-completed");
        let capture_output = completed.clone();
        let (release, blocked) = std::sync::mpsc::sync_channel(0);
        let capture = tokio::task::spawn_blocking(move || {
            if blocked.recv().is_ok() {
                std::fs::write(capture_output, b"capture joined").unwrap();
            }
        });
        let mut uploads = pipeline.uploads.subscribe();
        pipeline.work.push(
            async move {
                capture.await.unwrap();
                let enabled = *uploads.borrow();
                if enabled {
                    uploads.changed().await.unwrap();
                }
                Ok(Vec::new())
            }
            .boxed(),
        );
        let finish = pipeline.finish(true, futures::future::ready(()));
        tokio::pin!(finish);
        assert!(futures::poll!(&mut finish).is_pending());
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut finish)
            .await
            .expect("cancellation must interrupt uploads after producer success");
        assert_eq!(std::fs::read(completed).unwrap(), b"capture joined");
    }

    #[test]
    fn snapshot_survives_producer_rewrite_and_unlink() -> std::io::Result<()> {
        let mut producer = tempfile::NamedTempFile::new()?;
        producer.write_all(b"first immutable bytes")?;
        let mut captured = tempfile::tempfile()?;
        snapshot(producer.as_file_mut(), &mut captured, 21)?;
        producer.as_file_mut().seek(SeekFrom::Start(0))?;
        producer.write_all(b"later mutable content")?;
        drop(producer);
        captured.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        captured.read_to_end(&mut bytes)?;
        assert_eq!(bytes, b"first immutable bytes");
        Ok(())
    }

    #[test]
    fn snapshot_growth_never_exceeds_reserved_length() -> std::io::Result<()> {
        let mut producer = tempfile::tempfile()?;
        producer.write_all(&[0x5a; 8192])?;
        let mut captured = tempfile::tempfile()?;
        // The source grew after admission observed only its first block.
        snapshot(&mut producer, &mut captured, 4096)?;
        assert_eq!(captured.metadata()?.len(), 4096);
        captured.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        captured.read_to_end(&mut bytes)?;
        assert_eq!(bytes, vec![0x5a; 4096]);
        Ok(())
    }
}
