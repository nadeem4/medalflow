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
