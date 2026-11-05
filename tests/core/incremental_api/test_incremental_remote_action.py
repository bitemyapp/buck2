# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# pyre-strict


from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
from tempfile import TemporaryDirectory
from typing import Any

import pytest
from buck2.tests.e2e_util.api.buck import Buck
from buck2.tests.e2e_util.api.buck_result import BuildResult
from buck2.tests.e2e_util.asserts import expect_failure
from buck2.tests.e2e_util.buck_workspace import buck_test
from buck2.tests.e2e_util.helper.utils import random_string


@buck_test()
async def test_incremental_file_materialized(buck: Buck) -> None:
    result = await buck.run("root//:plate", "-c", f"test.seed={random_string()}")
    assert result.stdout == "0"
    result = await buck.run("root//:plate", "-c", f"test.seed={random_string()}")
    assert result.stdout == "1"


@buck_test()
async def test_incremental_dir_materialized(buck: Buck) -> None:
    result = await buck.run("root//:mate", "-c", f"test.seed={random_string()}")
    assert result.stdout == "0"
    result = await buck.run("root//:mate", "-c", f"test.seed={random_string()}")
    assert result.stdout == "1"


@buck_test()
async def test_incremental_file_not_materialized(buck: Buck) -> None:
    result = await buck.run("root//:flute", "-c", f"test.seed={random_string()}")
    assert result.stdout == "0"
    result = await buck.run("root//:flute", "-c", f"test.seed={random_string()}")
    assert result.stdout == "1"


@buck_test()
async def test_incremental_dir_not_materialized(buck: Buck) -> None:
    result = await buck.run("root//:suite", "-c", f"test.seed={random_string()}")
    assert result.stdout == "0"
    result = await buck.run("root//:suite", "-c", f"test.seed={random_string()}")
    assert result.stdout == "1"


@buck_test()
async def test_remote_cache_is_used(buck: Buck) -> None:
    seed = random_string()
    result = await buck.run("root//:plate", "-c", f"test.seed={seed}")
    assert result.stdout == "0"
    result = await buck.run("root//:plate", "-c", f"test.seed={random_string()}")
    assert result.stdout == "1"

    # For the next build with already used seed we expect the action to be taken from the cache
    result = await buck.run("root//:plate", "-c", f"test.seed={seed}")
    assert result.stdout == "0"

    out = await buck.log("what-ran", "--format", "json")
    out = [line.strip() for line in out.stdout.splitlines()]
    out = [json.loads(line) for line in out if line]
    assert len(out) == 1, "out should have 1 line: `{}`".format(out)
    repro = out[0]
    assert repro["reproducer"]["executor"] == "Cache"


async def _incremental_library_trace(
    buck: Buck, rel_cwd: Path | None = None
) -> dict[str, Any]:
    events = [
        json.loads(line).get("Event", {})
        for line in (await buck.log("show", rel_cwd=rel_cwd)).stdout.splitlines()
        if line.strip()
    ]
    actions = [
        (event["span_id"], action)
        for event in events
        if (
            action := event.get("data", {})
            .get("SpanEnd", {})
            .get("data", {})
            .get("ActionExecution")
        )
    ]
    producers = [
        (span, action)
        for span, action in actions
        if action["name"]["category"] == "incremental_library"
    ]
    assert len(producers) == 1, producers
    span, action = producers[0]
    queries = [
        query["action_digest"]
        for event in events
        if event.get("parent_id") == span
        if (
            query := event.get("data", {})
            .get("SpanStart", {})
            .get("data", {})
            .get("ExecutorStage", {})
            .get("stage", {})
            .get("CacheQuery")
        )
    ]
    assert len(queries) == 1, queries
    assert len(action["commands"]) == 1, action
    command = next(
        iter(action["commands"][0]["details"]["command_kind"]["command"].values())
    )
    return {
        "query": queries[0],
        "command": command,
        "execution_kind": action["execution_kind"],
        "failed": action["failed"],
        "categories": [a["name"]["category"] for _, a in actions],
    }


def _assert_library_outputs(result: BuildResult, payload: str) -> dict[str, Any]:
    report = result.get_build_report()
    artifact = report.output_for_target("root//:incremental_library")
    expected = ("library=" + payload + "\n").encode()
    assert artifact.read_bytes() == expected
    consumer = report.output_for_target("root//:incremental_consumer")
    assert consumer.read_text() == hashlib.sha256(expected).hexdigest()
    return json.loads((artifact.parent / "state" / "state.json").read_text())


@buck_test(skip_for_os=["windows"])
@pytest.mark.parametrize("local", [False, True], ids=["remote", "local_upload"])
@pytest.mark.parametrize(
    "case",
    ["success", "action_upload_disabled", "executor_upload_disabled", "failure"],
)
async def test_incremental_source_key_in_fresh_worktree(
    buck: Buck, local: bool, case: str
) -> None:
    seed = random_string()
    args = [
        "root//:incremental_library",
        "root//:incremental_consumer",
        "-c",
        "build.execution_platforms=root//:incremental_execution",
        "-c",
        f"test.seed={seed}",
        "-c",
        f"test.local={str(local).lower()}",
        "-c",
        f"test.upload={str(case != 'action_upload_disabled').lower()}",
        "-c",
        f"test.executor_upload={str(case != 'executor_upload_disabled').lower()}",
        "--local-only" if local else "--remote-only",
    ]
    source = buck.cwd / "library_source.json"
    source.write_text(json.dumps({"generation": 1, "payload": seed}))
    cold = await buck.build(*args)
    assert _assert_library_outputs(cold, seed) == {
        "generation": 1,
        "previous_generation": None,
        "warm": False,
    }
    cold_trace = await _incremental_library_trace(buck)
    assert cold_trace["execution_kind"] == (1 if local else 2), cold_trace
    assert cold_trace["command"]["action_digest"] == cold_trace["query"]

    # Only the source generation changes. The library's actual bytes, and thus
    # its consumer's inputs, remain unchanged; advisory state is a separate output.
    source.write_text(
        json.dumps({"generation": 2, "payload": seed, "fail": case == "failure"})
    )
    if case == "failure":
        await expect_failure(
            buck.build(*args), stderr_regex="deliberate incremental failure"
        )
    else:
        warm = await buck.build(*args)
        assert _assert_library_outputs(warm, seed) == {
            "generation": 2,
            "previous_generation": 1,
            "warm": True,
        }
    warm_trace = await _incremental_library_trace(buck)
    assert warm_trace["execution_kind"] == (1 if local else 2), warm_trace
    assert warm_trace["command"]["action_digest"] != warm_trace["query"]
    assert warm_trace["failed"] == (case == "failure")
    if case == "failure":
        assert "incremental_consumer" not in warm_trace["categories"]

    # An independent checkout has neither the previous output nor a materializer
    # entry. Keep the isolation name and source paths unchanged so its initial
    # cache query is exactly the warm build's SOURCE key, not its execution key.
    with TemporaryDirectory(dir=buck.cwd.parent) as directory:
        fresh = Path(directory) / "checkout"
        shutil.copytree(
            buck.cwd,
            fresh,
            symlinks=True,
            ignore=shutil.ignore_patterns("buck-out", ".git"),
        )
        try:
            if case == "failure":
                await expect_failure(
                    buck.build(*args, rel_cwd=fresh),
                    stderr_regex="deliberate incremental failure",
                )
            else:
                rebuilt = await buck.build(*args, rel_cwd=fresh)
                state = _assert_library_outputs(rebuilt, seed)
                assert state["warm"] == (case == "success"), state
                assert state["previous_generation"] == (
                    1 if case == "success" else None
                ), state
            fresh_trace = await _incremental_library_trace(buck, fresh)
            assert fresh_trace["query"] == warm_trace["query"]
            assert fresh_trace["command"]["action_digest"] == warm_trace["query"]
            assert fresh_trace["failed"] == (case == "failure")
            if case == "success":
                assert fresh_trace["execution_kind"] == 3, fresh_trace
                assert fresh_trace["command"]["cache_hit"] is True
            else:
                assert fresh_trace["execution_kind"] == (1 if local else 2), fresh_trace
                assert not fresh_trace["command"].get("cache_hit", False), fresh_trace
            if case == "failure":
                assert "incremental_consumer" not in fresh_trace["categories"]
        finally:
            await buck.kill(rel_cwd=fresh)


@buck_test(skip_for_os=["windows"])
async def test_early_failure_does_not_chmod_shared_incremental_state(buck: Buck) -> None:
    seed = random_string()
    args = [
        "root//:incremental_library",
        "root//:incremental_consumer",
        "--local-only",
        "-c",
        "build.execution_platforms=root//:incremental_execution",
        "-c",
        "test.local=true",
        "-c",
        "test.upload=false",
        "-c",
        "test.executor_upload=false",
        "-c",
        f"test.seed={seed}",
    ]
    source = buck.cwd / "library_source.json"
    source.write_text(json.dumps({"generation": 1, "payload": seed}))
    built = await buck.build(*args)
    _assert_library_outputs(built, seed)
    artifact = built.get_build_report().output_for_target("root//:incremental_library")
    state = artifact.parent / "state" / "state.json"

    donor = buck.cwd / "readonly-state-donor"
    original_bytes = state.read_bytes()
    donor.write_bytes(original_bytes)
    donor.chmod(0o444)
    state.unlink()
    os.link(donor, state)
    before = donor.stat()
    assert state.stat().st_ino == before.st_ino
    assert before.st_nlink == 2

    source.write_text(
        json.dumps({"generation": 2, "payload": seed, "early_fail": True})
    )
    await expect_failure(buck.build(*args), stderr_regex="early incremental failure")
    trace = await _incremental_library_trace(buck)
    assert trace["failed"] is True
    assert trace["execution_kind"] == 1, trace
    assert "incremental_consumer" not in trace["categories"]

    after = donor.stat()
    assert (after.st_dev, after.st_ino) == (before.st_dev, before.st_ino)
    assert stat.S_IMODE(after.st_mode) == stat.S_IMODE(before.st_mode) == 0o444
    assert donor.read_bytes() == original_bytes
    assert after.st_nlink == 1
    owned = state.stat()
    assert (owned.st_dev, owned.st_ino) != (before.st_dev, before.st_ino)
    assert owned.st_mode & stat.S_IWUSR
    assert owned.st_nlink == 1
    assert state.read_bytes() == original_bytes
