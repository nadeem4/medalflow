"""Import the optional cloud dependencies, or say how to install them.

Everything the `azure` extra provides -- the Azure SDKs, pyodbc, pandas and the
parquet/abfs plumbing pandas hands off to -- is reachable only through the
compute and storage seams. Core planning (medallion, DAG building, SQL
dependency analysis) never touches it, so a bare ``pip install medalflow``
deliberately leaves it out.

Modules that need one of those packages import it here, inside the function
that uses it, so the failure lands on the call that needed it and names the
install command instead of surfacing a ``ModuleNotFoundError`` from three
frames down.
"""

import importlib
from types import ModuleType

from medalflow.common.exceptions import CTEError, ErrorCode

#: The extra that installs every optional dependency.
EXTRA = "azure"


def require_module(module_name: str) -> ModuleType:
    """Import an optional module, or raise an actionable error.

    Args:
        module_name: Fully qualified module name, e.g. ``azure.identity``

    Returns:
        The imported module

    Raises:
        CTEError: If the module is not installed. The message names the extra
            that provides it.
    """
    try:
        return importlib.import_module(module_name)
    except ImportError as error:
        raise CTEError(
            f"'{module_name}' is not installed. It ships with MedalFlow's optional "
            f"cloud dependencies -- install them with: pip install 'medalflow[{EXTRA}]'",
            error_code=ErrorCode.MISSING_DEPENDENCY,
            details={"module": module_name, "extra": EXTRA},
            cause=error,
        ) from error
