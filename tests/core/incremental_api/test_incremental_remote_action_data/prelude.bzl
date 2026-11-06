# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

def _china_impl(ctx) -> list[Provider]:
    out = ctx.actions.declare_output("out", has_content_based_path = False)
    ctx.actions.run(
        cmd_args(["fbpython", ctx.attrs.increment] + (["--dir"] if ctx.attrs.dir else []) + ["--out", out.as_output()]),
        category = "increment",
        no_outputs_cleanup = True,
        incremental_remote_outputs = True,
        prefer_remote = True,
        env = {"INVALIDATE_ACTION": ctx.attrs.invalidate},
    )
    return [
        DefaultInfo(out),
        RunInfo(args = ["cat", cmd_args(out, format = "{}/MYFILE") if ctx.attrs.dir else out]),
    ]

china = rule(
    impl = _china_impl,
    attrs = {
        "dir": attrs.bool(default = False),
        "increment": attrs.source(),
        "invalidate": attrs.string(),
    },
)

def _whistle_impl(ctx) -> list[Provider]:
    intermediate = ctx.actions.declare_output("intermediate", has_content_based_path = False)
    ctx.actions.run(
        cmd_args(["fbpython", ctx.attrs.increment] + (["--dir"] if ctx.attrs.dir else []) + ["--out", intermediate.as_output()]),
        category = "increment",
        no_outputs_cleanup = True,
        incremental_remote_outputs = True,
        prefer_remote = True,
        env = {"INVALIDATE_ACTION": ctx.attrs.invalidate},
    )
    out = ctx.actions.declare_output("out", has_content_based_path = False)
    ctx.actions.run(
        cmd_args(["sh", "-c", 'cp -rf "$1" "$2"', "--", intermediate, out.as_output()]),
        category = "copy",
        prefer_remote = True,
        env = {"INVALIDATE_ACTION": ctx.attrs.invalidate},
    )
    return [
        DefaultInfo(out),
        RunInfo(args = ["cat", cmd_args(out, format = "{}/MYFILE") if ctx.attrs.dir else out]),
    ]

whistle = rule(
    impl = _whistle_impl,
    attrs = {
        "dir": attrs.bool(default = False),
        "increment": attrs.source(),
        "invalidate": attrs.string(),
    },
)

IncrementalLibraryInfo = provider(fields = ["artifact"])

def _incremental_library_impl(ctx):
    artifact = ctx.actions.declare_output("library.txt", has_content_based_path = False)
    state = ctx.actions.declare_output("state", dir = True, has_content_based_path = False)
    ctx.actions.run(
        cmd_args("fbpython", ctx.attrs.runner, "produce", ctx.attrs.source, artifact.as_output(), state.as_output()),
        category = "incremental_library",
        no_outputs_cleanup = True,
        incremental_remote_outputs = True,
        local_only = ctx.attrs.local,
        prefer_remote = not ctx.attrs.local,
        allow_cache_upload = ctx.attrs.upload,
        env = {"INVALIDATE_ACTION": ctx.attrs.seed},
    )
    return [
        DefaultInfo(default_output = artifact, other_outputs = [state]),
        IncrementalLibraryInfo(artifact = artifact),
    ]

incremental_library = rule(
    impl = _incremental_library_impl,
    attrs = {
        "local": attrs.bool(),
        "runner": attrs.source(),
        "seed": attrs.string(),
        "source": attrs.source(),
        "upload": attrs.bool(),
    },
)

def _incremental_consumer_impl(ctx):
    artifact = ctx.actions.declare_output("consumer.txt")
    ctx.actions.run(
        cmd_args("fbpython", ctx.attrs.runner, "consume", ctx.attrs.library[IncrementalLibraryInfo].artifact, artifact.as_output()),
        category = "incremental_consumer",
        allow_cache_upload = True,
        env = {"INVALIDATE_ACTION": ctx.attrs.seed},
    )
    return [DefaultInfo(default_output = artifact)]

incremental_consumer = rule(
    impl = _incremental_consumer_impl,
    attrs = {
        "library": attrs.dep(providers = [IncrementalLibraryInfo]),
        "runner": attrs.source(),
        "seed": attrs.string(),
    },
)

def _incremental_execution_impl(ctx):
    platform = ExecutionPlatformInfo(
        label = ctx.label.raw_target(),
        configuration = ConfigurationInfo(constraints = {}, values = {}),
        executor_config = CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = True,
            remote_cache_enabled = True,
            remote_execution_properties = {"platform": "linux-remote-execution"},
            remote_execution_use_case = "buck2-testing",
            allow_cache_uploads = ctx.attrs.upload,
            max_cache_upload_mebibytes = 1,
            use_limited_hybrid = False,
            allow_hybrid_fallbacks_on_failure = False,
        ),
    )
    return [DefaultInfo(), ExecutionPlatformRegistrationInfo(platforms = [platform])]

incremental_execution = rule(
    impl = _incremental_execution_impl,
    attrs = {"upload": attrs.bool()},
)
