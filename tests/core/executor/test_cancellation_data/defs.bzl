# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

def _slow_impl(ctx: AnalysisContext) -> list[Provider]:
    outs = {}
    for i in range(ctx.attrs.count):
        o = ctx.actions.declare_output("out/{}".format(i), has_content_based_path = False)
        ctx.actions.run(
            ["fbpython", ctx.attrs.src, ctx.attrs.duration, ctx.attrs.pids, o.as_output()],
            category = "test",
            identifier = str(i),
        )
        outs[str(i)] = o

    out = ctx.actions.symlinked_dir("outs", outs, has_content_based_path = False)
    return [DefaultInfo(out)]

slow_actions = rule(
    impl = _slow_impl,
    attrs = {
        "count": attrs.int(),
        "duration": attrs.string(),
        "pids": attrs.string(),
        "src": attrs.source(),
    },
)

def _priority_impl(ctx):
    outputs = {}
    for name in ["active", "low_gate", "high_gate", "low_a", "low_b", "critical"]:
        output = ctx.actions.declare_output(name)
        dependencies = []
        if name in ["low_a", "low_b", "high_gate"]:
            dependencies = [outputs["low_gate"]]
        elif name == "critical":
            dependencies = [outputs["high_gate"]]
        kwargs = {"weight_percentage": 0} if name.endswith("_gate") else {"weight": 2}
        ctx.actions.run(
            cmd_args(ctx.attrs.python, ctx.attrs.src, ctx.attrs.state, name, output.as_output(), hidden = dependencies),
            category = "priority_smoke",
            identifier = name,
            priority = 10 if name == "critical" else 0,
            local_only = True,
            low_pass_filter = False,
            **kwargs
        )
        outputs[name] = output
    return [DefaultInfo(default_outputs = outputs.values())]

priority_actions = rule(
    impl = _priority_impl,
    attrs = {
        "python": attrs.string(),
        "src": attrs.source(),
        "state": attrs.string(),
    },
)
