---
id: install
title: Installing Buck2
---

import { FbInternalOnly } from 'docusaurus-plugin-internaldocs-fb/internal';

<FbInternalOnly>

<h2>Internal Meta User</h2>

For Internal Meta Users, Buck2 is already configured and available for you.
Simply cloning the
[`fbsource`](https://www.internalfb.com/wiki/Repositories/fbsource/#cloning)
repository is all that's required to get started; no separate installation steps
for Buck2 are necessary.

If you have any issues, please check [here](../../users/faq/meta_installation).

</FbInternalOnly>

## Installing Buck2

The latest set of `buck2` executables can be found under the
[`latest` release page](https://github.com/facebook/buck2/releases/tag/latest).

Additionally, for each bi-monthly release there is a
[dotslash](https://dotslash-cli.com) file that is appropriate for committing to
a repository. This will automatically fetch the correct version and architecture
for each user, and ensures a consistent build environment for each commit in the
repo.

If no prebuilt binary is available for your platform — or you want to hack on
Buck2 itself — see [Building from Source](#building-from-source) below.

## Building from Source

Buck2 currently requires a nightly Rust toolchain. The simplest setup is via
[rustup](https://rustup.rs/), which provisions the right `rustc`/`cargo` for
you. Once it's installed, build and install `buck2` directly from GitHub:

```bash
rustup install nightly-2026-07-05
cargo +nightly-2026-07-05 install --git https://github.com/facebook/buck2.git buck2
```

This installs `buck2` into a suitable directory such as `$HOME/.cargo/bin`,
which you should add to your `$PATH`:

Linux / macOS

```sh
export PATH=$HOME/.cargo/bin:$PATH
```

Windows Powershell

```powershell
$Env:PATH += ";$HOME\.cargo\bin"
```

Verify the install with `buck2 --help`.

To hack on Buck2, build from a clone of the repo instead:

```sh
git clone https://github.com/facebook/buck2.git
cd buck2/
cargo install --path=app/buck2
```

### Using Nix

Most [Nix](https://nixos.org/nix) users provision tools directly with Nix
itself, rather than rustup. The Buck2 source ships a `flake.nix` that exposes a
`cargo`/`rustc` development shell:

```sh
git clone https://github.com/facebook/buck2.git
cd buck2/
nix develop . # add 'rustc' and 'cargo' to $PATH
cargo build --release --bin=buck2
```

A Nix package (e.g. `nix build .#buck2`) does not yet exist; see `buck2` in
nixpkgs for inspiration for writing one. An `.envrc` using the Nix flake is
provided for `direnv` users — `direnv allow` will give a usable development
environment.

#### Shared local artifact materialization (Linux fork)

This fork can share regular-file extents across independent worktrees with
`BUCK2_LOCAL_CONTENT_STORE=/absolute/canonical/cache/path`. Its parent must
already exist; the store itself is created with mode `0700` and must belong
to the daemon's UID. Set this environment before starting each Buck daemon,
and restart those daemons after changing it. Use separate roots for separate
trust domains. This is a private machine-local cache, not a network filesystem
or a replacement for the remote action cache.

Local action outputs, write/copy actions, HTTP outputs, and downloaded remote
outputs enter the same content-addressed store. Materializations use reflinks,
never writable hardlinks. Every reused snapshot is hashed against its declared
digest before atomic file publication. Executable bits are applied to each
materialization independently; directory and symlink metadata remain managed
by the existing materializer. Remote downloads are staged and verified before
publication even when the optional shared store is disabled.

The store and worktree outputs must be on the same reflink-capable filesystem.
Unsupported reflinks and artifacts exceeding the configured capacity fail
explicitly rather than silently allocating full copies. The defaults are
10 GiB (`BUCK2_LOCAL_CONTENT_STORE_MAX_BYTES`) and 100,000 files
(`BUCK2_LOCAL_CONTENT_STORE_MAX_ENTRIES`). A process-shared lock and SQLite LRU
enforce these limits during active builds, not only during idle periods.
Eviction unlinks cache objects without invalidating existing reflinks. Startup
recovers interrupted publications and removes orphaned staging files. A corrupt
object is removed and the current materialization fails; a subsequent attempt
can repopulate it from a producer or the remote CAS. Permissions are not used as
an integrity shortcut.

A separately entry-bounded index of weak output paths can recover shared extents
from a still-live worktree after canonical eviction. These hints retain no file
data and are verified before use; stale or unsafe paths are discarded. LRU and
weak-view updates share one durable SQLite transaction. Object accounting still
commits before canonical publication, with file and directory synchronization
preserved.

Local producers can prepare private output snapshots concurrently with execution.
Linux close-write/move events are advisory; only final output validation and
content hashes authorize reuse or successful action-cache publication. Four
process-wide batches of eight files share a 512 MiB/4,096-pair budget, charging
two 4-KiB-rounded snapshots per file with no oversized exception. Prepared
publication views remain writable and independent; CAS uploads use a separate
stable snapshot and bounded streaming. Cancellation joins capture work and stops
pending uploads. Failed actions can leave unreferenced CAS blobs, never a
successful result. Write permissions and action/executor policy still apply;
whole-action upload-size limits retain final-only remote uploads. This does not
pipeline remote-worker ingestion or defer final root publication beyond the
action's completion.

The capacity covers cache object logical bytes, plus filesystem block rounding,
bounded index metadata, and bounded publication staging. It does not cap live worktree
outputs or Buck's daemon state; use the
existing deferred-materialization and clean-stale policies for those. Separate
worktrees can still execute identical actions concurrently; this store shares
their resulting extents, not their action execution.

### `protoc` on non-Tier-1 platforms

Buck2 uses Protocol Buffers extensively, both internally and to talk to remote
systems for things like Remote Execution. Compiling the `.proto` files needs
the `protoc` compiler.

On Linux (aarch64/x86_64), Windows, and macOS the `cargo` build pulls a
prebuilt `protoc` from the
[`protoc-bin-vendored`](https://crates.io/crates/protoc-bin-vendored) crate, so
no setup is required.

On other operating systems, install `protoc` from another source (out of scope
here) and point the build at it before running `cargo build`:

- `BUCK2_BUILD_PROTOC` — path to the `protoc` binary
- `BUCK2_BUILD_PROTOC_INCLUDE` — path to the protocol buffers header directory

For example, with protobuf installed under `/opt/protobuf`:

```bash
export BUCK2_BUILD_PROTOC=/opt/protobuf/bin/protoc
export BUCK2_BUILD_PROTOC_INCLUDE=/opt/protobuf/include
```

### Building Buck2 with Buck2

See [Bootstrapping](../about/bootstrapping.md) for details. The gist:

```sh
reindeer --third-party-dir shim/third-party/rust buckify
buck2 build //:buck2
```
