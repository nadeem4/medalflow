"""Compile this project and print the plan MedalFlow derived from its SQL.

    python run.py

Compiling is offline: no warehouse, no credentials, nothing installed beyond
MedalFlow itself. Executing that plan is `medalflow.api.run("*")`, which takes
the same selector and does need a reachable warehouse -- see README.md.
"""

from medalflow.api import compile

result = compile("*")
assert result.ok, "\n".join(str(error) for error in result.errors)
for stage in result.plan.stages:
    print(f"stage {stage.stage}:", ", ".join(op.object_name for op in stage.operations))
