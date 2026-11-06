# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

import hashlib
import json
from pathlib import Path
import sys


def replace_text(path: Path, text: str) -> None:
    # Imported RE inputs may be hard-linked and read-only. Replace them instead
    # of modifying their permissions or contents in place.
    temporary = path.with_name(path.name + ".new")
    temporary.write_text(text)
    temporary.replace(path)


def main() -> None:
    mode, *args = sys.argv[1:]
    if mode == "consume":
        source, output = map(Path, args)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(hashlib.sha256(source.read_bytes()).hexdigest())
        return

    source, artifact, state = map(Path, args)
    current = json.loads(source.read_text())
    if current.get("early_fail", False):
        # Leave every declared output untouched, including shared readonly state.
        print("early incremental failure", file=sys.stderr)
        sys.exit(24)
    state_file = state / "state.json"
    previous = json.loads(state_file.read_text()) if state_file.exists() else None
    observation = {
        "generation": current["generation"],
        "previous_generation": previous["generation"] if previous else None,
        "warm": previous is not None,
    }
    artifact.parent.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    replace_text(state_file, json.dumps(observation, sort_keys=True))
    if current.get("fail", False):
        replace_text(artifact, "partial failed output\n")
        print("deliberate incremental failure", file=sys.stderr)
        sys.exit(23)
    replace_text(artifact, "library=" + current["payload"] + "\n")


if __name__ == "__main__":
    main()
