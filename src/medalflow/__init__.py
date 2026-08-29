"""MedalFlow -- author a medallion warehouse as decorated Python, run it as SQL.

A model is a class with a decorator. ``silver_metadata`` and ``gold_metadata``
name the table it builds; the ``query_metadata`` methods inside it return the
SQL that builds it. Nothing executes at import time, so a project compiles
with no warehouse in reach.

Three entry points, and they are the whole surface:

``compile``
    Discover the models a selector reaches, order them into one cross-layer
    plan, and return it alongside the errors found. Errors are collected
    rather than raised, so one run reports every broken model instead of the
    first.
``run``
    ``compile``, then execute the surviving plan stage by stage. Refuses to
    start when compile reported errors, stops at the first failure, and
    reports what it therefore skipped.
``execute``
    One serialized operation, run now -- the seam for a caller that owns its
    own scheduling. ``run`` is built on it.

``compile`` shadows the builtin deliberately (ADR 002, Decision 8): it is the
word the domain already uses, and it is not a builtin a pipeline author
reaches for.

Also exported: ``SilverTransformationSequencer`` and ``GoldSequencer``, the
base classes a model subclasses; ``CTEError`` and ``ErrorCode``, which every
failure MedalFlow raises is an instance of and categorised by; and
``get_current_timestamp``. ``etl_metadata``, ``view_metadata`` and
``SilverSequencer`` are older names kept as aliases.

Bronze models are written the same way, but bronze is reached through
medalflow.medallion rather than from here.
"""

from medalflow.__version__ import __version__
from medalflow.api import (
    CompiledModel,
    CompileError,
    CompileResult,
    RunResult,
    SelectorError,
    compile,
    execute,
    run,
)
from medalflow.common.exceptions import CTEError, ErrorCode
from medalflow.medallion import (
    GoldSequencer,
    SilverTransformationSequencer,
    gold_metadata,
    query_metadata,
    silver_metadata,
)

# Utils (public API)
from medalflow.utils import get_current_timestamp

# Backward compatibility aliases
etl_metadata = silver_metadata
view_metadata = gold_metadata
SilverSequencer = SilverTransformationSequencer

__all__ = [
    "__version__",
    "SilverTransformationSequencer",
    "SilverSequencer",
    "GoldSequencer",
    "silver_metadata",
    "gold_metadata",
    "query_metadata",
    "etl_metadata",  # Backward compatibility alias
    "view_metadata",  # Backward compatibility alias
    # Exceptions (public API)
    "CTEError",
    "ErrorCode",
    # Utilities (public API)
    "get_current_timestamp",
    # compile (ADR 002, Decisions 7 and 8)
    "compile",
    "CompileResult",
    "CompileError",
    "CompiledModel",
    "SelectorError",
    # run (ADR 002, Decision 7)
    "run",
    "RunResult",
    # api
    "execute",
]
