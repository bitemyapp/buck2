# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# pyre-strict


import sys

from buck2.tests.e2e_util.api.buck import Buck
from buck2.tests.e2e_util.asserts import expect_failure
from buck2.tests.e2e_util.buck_workspace import buck_test
from buck2.tests.e2e_util.helper.utils import json_get, random_string


async def _assert_locally_executed_upload_attempted(buck: Buck, count: int = 1) -> None:
    await _assert_upload_attempted(buck, count)


async def _assert_upload_attempted(buck: Buck, count: int) -> None:
    log = (await buck.log("show")).stdout.strip().splitlines()
    uploads = []
    excluded_uploads = []

    # CI lacks reliable write access to CAS, so count any upload that was
    # *attempted* — both genuine successes and infra-level rejections that
    # prove the action reached the cache-upload stage. PERMISSION_DENIED is
    # the missing-write-ACL case. INVALID_ARGUMENT is only tolerated when the
    # message identifies the specific "Outputs TTL -1 is too low" rejection
    # (raised when supporting CAS objects haven't been uploaded).
    for line in log:
        e = json_get(
            line,
            "Event",
            "data",
            "SpanEnd",
            "data",
            "CacheUpload",
        )
        if e is None:
            continue
        tolerated = (
            e["success"]
            or e["re_error_code"] == "PERMISSION_DENIED"
            or (
                e["re_error_code"] == "INVALID_ARGUMENT"
                and "Outputs TTL -1 is too low" in e.get("error", "")
            )
        )
        if tolerated:
            uploads.append(e)
        else:
            excluded_uploads.append(e)

    if len(uploads) == count:
        return
    else:
        print(f"Expected {count} uploads", file=sys.stderr)
        print(f"Actual uploads: {uploads}", file=sys.stderr)
        print(f"Excluded uploads: {excluded_uploads}", file=sys.stderr)
        raise AssertionError("Wrong number of uploads, see above")


@buck_test()
async def test_re_uploads(buck: Buck) -> None:
    args = ["-c", f"write.text={random_string()}"]
    await buck.build("root//:write", *args)
    await _assert_locally_executed_upload_attempted(buck, 1)


@buck_test()
async def test_re_uploads_dir(buck: Buck) -> None:
    args = ["-c", f"write.text={random_string()}"]
    await buck.build("root//:write_in_dir", *args)
    await _assert_locally_executed_upload_attempted(buck, 1)


@buck_test()
async def test_re_uploads_limit(buck: Buck) -> None:
    args = ["-c", f"write.text={random_string()}"]
    await buck.build("root//:write_xxl", *args)
    await _assert_locally_executed_upload_attempted(buck, 0)


@buck_test()
async def test_re_uploads_default(buck: Buck) -> None:
    args = ["-c", f"write.text={random_string()}"]
    await buck.build("root//:write_default", *args)
    await _assert_locally_executed_upload_attempted(buck, 0)

    args = [
        "-c",
        f"write.text={random_string()}",
        "-c",
        "buck2.default_allow_cache_upload=true",
    ]
    await buck.build("root//:write_default", *args)
    await _assert_locally_executed_upload_attempted(buck, 1)


@buck_test()
async def test_invalid_outputs_do_not_publish_cache_roots(buck: Buck) -> None:
    for target, error in [
        ("write_missing_output", "Action failed to produce outputs"),
        ("write_wrong_output_type", "Action didn't produce output of the right type"),
    ]:
        await expect_failure(
            buck.build(
                f"root//:{target}",
                "-c",
                f"write.text={random_string()}",
            ),
            stderr_regex=error,
        )
        # Unlike the positive upload helper, reject every upload attempt, including
        # infrastructure failures. Invalid outputs must never reach the uploader.
        log = (await buck.log("show")).stdout.strip().splitlines()
        uploads = [
            event
            for line in log
            if (
                event := json_get(
                    line, "Event", "data", "SpanEnd", "data", "CacheUpload"
                )
            )
            is not None
        ]
        assert not uploads, uploads
        commands = [
            command
            for line in log
            for command in (
                json_get(
                    line,
                    "Event",
                    "data",
                    "SpanEnd",
                    "data",
                    "ActionExecution",
                    "commands",
                )
                or []
            )
        ]
        assert [command["status"] for command in commands] == [{"Success": {}}]
        assert commands[0]["details"]["signed_exit_code"] == 0
