"""Feature manager implementations.

This package contains all feature manager plugins that provide
cross-cutting functionality across the application.
"""

# Import managers to trigger auto-registration
# Each manager registers itself when imported. These imports exist purely for
# their side effect, so F401 ("imported but unused") does not apply.
from . import (
    cache,  # noqa: F401
    stats,  # noqa: F401
)
