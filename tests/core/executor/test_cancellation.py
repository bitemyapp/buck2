# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# pyre-strict


import asyncio
import json
import os
import signal
from collections.abc import Callable
from pathlib import Path

from buck2.tests.e2e_util.api.buck import Buck
from buck2.tests.e2e_util.api.buck_result import BuckException, BuckResult, ExitCodeV2
from buck2.tests.e2e_util.api.process import Process
from buck2.tests.e2e_util.buck_workspace import buck_test
from buck2.tests.e2e_util.helper.utils import read_invocation_record


async def _test_cancellation_helper(
    buck: Buck,
    tmp_path: Path,
    runner: Callable[[Buck, list[str]], Process[BuckResult, BuckException]],
) -> None:
    """
    This test starts a test that writes its PID to a file then runs for 60
    seconds. We test cancellation by sending a CTRL+C as soon as a test
    starts. We then check that the process exited, and that nothing else
    started (or if anything did, that they stopped).
    """
    pid_path = tmp_path / "pids"
    pid_path.mkdir()
    record_path = tmp_path / "record.json"
    opts = [
        "-c",
        f"test.pids={pid_path}",
        "-c",
        "test.duration=60",
        "--unstable-write-invocation-record",
        str(record_path),
    ]
    await buck.audit("providers", ":slow", *opts)
    command = runner(buck, [*opts, "--local-only"])

    command = await command.start()

    for _i in range(30):
        await asyncio.sleep(1)
        pids = os.listdir(pid_path)
        if pids:
            break
    else:
        raise Exception("Commands never started")

    command.send_signal(signal.SIGINT)
    await command.communicate()  # Wait for the command to exit

    # Give stuff time to settle, PIDS don't necessarily disappear
    # instantly. Also, verify that we are not starting more tests.
    await asyncio.sleep(5)

    # At this point, nothing should be alive.
    pids = os.listdir(pid_path)
    for pid in pids:
        try:
            os.kill(int(pid), 0)
        except OSError:
            pass
        else:
            raise Exception(f"PID existed: {pid}")

    record = read_invocation_record(record_path)
    assert record["exit_code"] == ExitCodeV2.SIGNAL_INTERRUPT.value
    assert record["exit_result_name"] == "SIGNAL_INTERRUPT"


@buck_test()
async def test_cancellation(buck: Buck, tmp_path: Path) -> None:
    await _test_cancellation_helper(
        buck, tmp_path, lambda buck, opts: buck.build(*opts, ":slow")
    )


@buck_test()
async def test_cancellation_bxl(buck: Buck, tmp_path: Path) -> None:
    await _test_cancellation_helper(
        buck, tmp_path, lambda buck, opts: buck.bxl(*opts, "//build.bxl:build")
    )


def _priority_queued(log: str, trace_id: str) -> list[str]:
    parents = {}
    actions = {}
    queued = []
    # A writer may be halfway through the final JSON line.
    for line in log[:log.rfind("\n") + 1].splitlines():
        event = json.loads(line).get("Event", {})
        if not event:
            continue
        assert event["trace_id"] == trace_id
        start = event.get("data", {}).get("SpanStart", {}).get("data", {})
        span = event.get("span_id")
        if start:
            parents[span] = event.get("parent_id")
        action = start.get("ActionExecution", {}).get("name", {})
        if action.get("category") == "priority_smoke":
            actions[span] = action["identifier"]
        local = start.get("ExecutorStage", {}).get("stage", {}).get("Local", {}).get("stage", {})
        if "Queued" not in local:
            continue
        current = span
        while current is not None and current not in actions:
            current = parents.get(current)
        if current in actions:
            queued.append(actions[current])
    return queued


@buck_test()
async def test_local_priority_passes_queued_actions(buck: Buck, tmp_path: Path) -> None:
    state = tmp_path / "priority"
    state.mkdir(mode=0o700)
    event_log = state / "events.json-lines"
    build_id = state / "build-id"
    command = await buck.build(
        ":priority", "--local-only", "--num-threads", "2",
        "-c", f"test.state={state}",
        "--event-log", str(event_log), "--write-build-id", str(build_id),
    ).start()

    async def wait_queued(expected: set[str]) -> list[str]:
        for _ in range(300):
            queued = []
            if event_log.exists() and build_id.exists():
                queued = _priority_queued(event_log.read_text(), build_id.read_text().strip())
            if expected.issubset(queued):
                return queued
            await asyncio.sleep(0.1)
        raise AssertionError(f"actions never queued: {expected}")

    try:
        # The active action holds both permits throughout both queue barriers.
        low_order = await wait_queued({"low_a", "low_b"})
        assert not (state / "active-end").exists()
        (state / "release-high").touch()
        await wait_queued({"critical"})
        assert not (state / "active-end").exists()
    finally:
        (state / "release-high").touch()
        (state / "release-active").touch()
        await command.communicate()
    order = (state / "order").read_text().splitlines()
    expected_lows = [name for name in low_order if name in {"low_a", "low_b"}]
    assert order == ["active-start", "active-end", "critical", *expected_lows]
