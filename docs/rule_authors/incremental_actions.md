---
id: incremental_actions
title: Incremental Actions
---

It's possible to make certain Buck2 actions behave incrementally, that is, to
produce results for a current invocation based on the result from the previous
run. Incrementality could significantly improve performance of some actions such
as packaging (such as Apple App Bundles) or linking (MSVC incremental linking).

There are two essential requirements to make an action incremental:

- The result from the previous run should be accessible.
- An understanding of which parts of the result need to be updated; it should be
  easy to compare inputs from a previous run with inputs from the current run
  and detect those changed.

The only way to run user-defined commands in Buck2 is with `ctx.actions.run`.
Both of the above requirements are met via its `metadata_env_var`,
`metadata_path` and `no_outputs_cleanup` parameters.

When the `no_outputs_cleanup` flag is turned on, Buck2 won't perform any
deletion of old outputs for the action. That means the result from the previous
run will be accessible, but the user script has to detect which parts of it
should be deleted and perform a manual cleanup.

When the `metadata_env_var` and `metadata_path` parameters are present, Buck2
will create a JSON file on a disk before actually executing the command. The
file will contain a list of paths and hash digests for every command action
input. All paths in the file are relative to the Buck2 project root. Symlinks
are not included in metadata because it is possible for the user script to
resolve symlink and use a resolved path to get the destination hash digest from
action metadata if it's needed, as shown in the following JSON example:

```json
{
  "version": 1,
  "digests": [
    {
      "path": "buck-out/v2/gen/cell/configuration_hash/path/to/target/__target_name__/generated_file",
      "digest": "da39a3ee5e6b4b0d3255bfef95601890afd80709:10"
    },
    ...
  ]
}
```

A user script that is run as a part of an action execution is responsible for
parsing the JSON file.

The `version` field is bumped every time there is a non-backwards compatible
change to the format of the file. The user script should verify that the
provided data is of a supported version and should be updated accordingly when
the current version is newer than the supported one.

The path of the JSON file is provided to the user script via an environment
variable with a key equal to `metadata_env_var`. The user is able to specify the
part of the path relative to the result directory via `metadata_path`.

For example, if some rule implementation has the following code:

```python
result = ctx.actions.declare_output("result")
command = cmd_args(["my_script.py", "--output", result.as_output()])
ctx.actions.run(
    command,
    category = "my_category",
    metadata_env_var = "ACTION_METADATA",
    metadata_path = "action_metadata.json",
    no_outputs_cleanup = True,
)
```

Then `my_script.py` will be executed as:

```shell
ACTION_METADATA=project/relative/path/to/target/action_metadata.json my_script.py --output resolved/path/to/result
```

`my_script.py` is responsible for reading the `ACTION_METADATA` environment
variable and parsing a JSON file with the action metadata.

Parsed metadata provides information about inputs for the current run, but the
script needs somehow to obtain similar information about inputs from the
previous run. Such information could just be another output of the user script
(as with the previous result, it won't be deleted when
`no_outputs_cleanup = True`). The Format of such a file is an implementation
detail of the user script, but at the very least it should contain a list of
every source that was used to form the result and hash digests for such sources.

The rule implementation would look something like the following:

```python
result = ctx.actions.declare_output("result")
state = ctx.actions.declare_output("incremental_state.json")
command = cmd_args(["my_script.py", "--output", result.as_output(), "--incremental-state", state.as_output()])
ctx.actions.run(
    command,
    category = "my_category",
    metadata_env_var = "ACTION_METADATA",
    metadata_path = "action_metadata.json",
    no_outputs_cleanup = True,
)
```

The user script would then:

1. Parse `incremental_state.json` and delete it. Deletion prior to amending the
   result is important so it doesn't result in a situation where an incremental
   state file is out of sync with the result when the user script fails while
   changing the result. Such a corrupted state might lead to subsequent
   incorrect builds reported as "successful".
2. Parse action metadata file, compute what is needed to update the result, and
   amend it accordingly.
3. Calculate the new state and write it into the new `incremental_state.json`.

## Remote incremental state

Set `incremental_remote_outputs = True` together with
`no_outputs_cleanup = True` to make previous declared outputs available to a
remote action. Every output of such an action, including depfiles, must use
`has_content_based_path = False` when declared. Buck2 imports the previous
outputs when all of their materializer entries are available; a missing entry
causes a cold invocation. Deleting a physical file alone does not guarantee a
cold invocation, because the materializer can restore it.

Treat this state as an optimization, not an additional source of build
semantics. Correct results must not depend on whether previous state is
available. State can be absent, incompatible, or left by a failed invocation;
there is no last-successful-state guarantee. The command must detect unusable
state and rebuild safely, and must not expose stale ordinary outputs after
failure. Declare compiler state separately from libraries and executables, and
keep it out of downstream action inputs.

Successful incremental results are published under the source-only action key
used by fresh worktrees, even when execution used a different action digest
containing previous state. This alias publication respects action-level and
executor-level cache-upload permissions and the existing upload-size limit.
Failed actions do not publish successful source-key aliases.

Imported files may be read-only hardlinks shared with the remote input store.
Before modifying them, atomically replace them with owned files; use copy-on-write
copies where available. Do not chmod a shared inode to make it writable. On Unix,
Buck2's own post-action permission normalization also detaches shared regular
files before changing their mode, including after a command fails without
touching its outputs.

Incremental state has no separate per-action byte or entry quota. Compiler
session cleanup and the configured artifact-cache limits still apply; measure
state retention and physical storage for the workload being deployed.
