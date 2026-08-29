"""The public API: compile, run, execute, and the selector grammar.

Everything here is re-exported from :mod:`medalflow` itself, with the single
exception of ``test_connection``.

The reasoning lives in the submodules rather than here, because each of them
is one decision: :mod:`~medalflow.api.compiler` for why compile errors are
collected instead of raised, :mod:`~medalflow.api.runner` for why a run stops
at the first failure, :mod:`~medalflow.api.selectors` for the four-form
grammar and why an unparseable selector raises.
"""

from .compiler import CompiledModel, CompileError, CompileResult, compile
from .platform import execute, test_connection
from .runner import ExecutedOperation, PlannedOperation, RunResult, run
from .selectors import Selector, SelectorError, parse_selector

__all__ = [
    "execute",
    "test_connection",
    # compile (ADR 002, Decision 8)
    "compile",
    "CompileResult",
    "CompileError",
    "CompiledModel",
    # run (ADR 002, Decision 7)
    "run",
    "RunResult",
    "ExecutedOperation",
    "PlannedOperation",
    # selectors (ADR 002, Decision 7)
    "Selector",
    "SelectorError",
    "parse_selector",
]
