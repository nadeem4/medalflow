"""Regression tests for feature-manager auto-discovery.

`core/core/features/managers/__init__.py` imported a `configuration` module
that has never existed. Because a package body aborts at the failing import,
the two imports after it (`powerbi`, `stats`) never ran, so those managers
never registered — and `auto_discover` swallowed the ImportError as a warning
and set `_initialized = True`, so it never retried. `get_feature_manager('stats')`
then raised `ValueError: Unknown feature`, which broke every sequencer
construction (`medallion/base/sequencer.py:324`).
"""

import pytest

from core.core.features import registry as registry_module


def test_managers_package_imports_cleanly():
    import core.core.features.managers  # noqa: F401


@pytest.mark.parametrize("feature", ["cache", "client_config", "powerbi", "stats"])
def test_shipped_feature_is_registered(feature):
    """Every manager module that calls register_feature must be reachable.

    `get_manager` raises ValueError for an unregistered name, so a manager
    whose import never ran is indistinguishable from a typo. Instantiation may
    still return None when the feature is disabled — that is fine; what must
    not happen is "Unknown feature".
    """
    registry_module._global_registry.auto_discover()

    assert feature in registry_module._global_registry.get_all_features()
