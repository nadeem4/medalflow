"""Feature manager implementations.

This package contains all feature manager plugins that provide
cross-cutting functionality across the application.
"""

# Import managers to trigger auto-registration
# Each manager registers itself when imported
from . import cache
from . import client_config
from . import configuration
from . import powerbi
from . import stats