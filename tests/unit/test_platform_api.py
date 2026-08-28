"""Regression test for the platform API signature (Phase 3, task 9).

`api.platform.execute` annotated its second parameter `ComputeEnvironment.ETL`
-- an enum *member*, a value, in annotation position. Three consequences: the
parameter was positionally required although its own docstring called it
optional, the annotation described no type, and `typing.get_type_hints()` on
the function raised. `test_connection`, 38 lines below, had it right all along.
"""

from typing import get_type_hints

from medalflow.api import platform
from medalflow.constants.compute import ComputeEnvironment


def test_execute_declares_compute_environment_as_a_type_with_a_default():
    hints = get_type_hints(platform.execute)

    assert hints["compute_environment"] is ComputeEnvironment


def test_execute_compute_environment_is_optional_as_documented():
    import inspect

    parameter = inspect.signature(platform.execute).parameters["compute_environment"]

    assert parameter.default is ComputeEnvironment.ETL
