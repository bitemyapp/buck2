# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

"""Real local processes for the host-sharing admission-order regression test."""

import os
from pathlib import Path
import sys
import time


def main():
    state = Path(sys.argv[1])
    name = sys.argv[2]
    output = Path(sys.argv[3])

    def wait(marker):
        deadline = time.monotonic() + 90
        while not (state / marker).exists():
            if time.monotonic() > deadline:
                raise TimeoutError(marker)
            time.sleep(0.01)

    def record(event):
        descriptor = os.open(state / "order", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(descriptor, (event + "\n").encode())
        finally:
            os.close(descriptor)

    if name == "active":
        record("active-start")
        (state / "active-start").touch(mode=0o600)
        wait("release-active")
        record("active-end")
        (state / "active-end").touch(mode=0o600)
    elif name == "low_gate":
        wait("active-start")
    elif name == "high_gate":
        wait("release-high")
    else:
        record(name)
    output.write_text(name)


if __name__ == "__main__":
    main()
