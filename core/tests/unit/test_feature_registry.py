"""Regression tests for feature-manager auto-discovery.

`medalflow/core/features/managers/__init__.py` imported a `configuration` module
that has never existed. Because a package body aborts at the failing import,
the imports after it (including `stats`) never ran, so those managers
never registered — and `auto_discover` swallowed the ImportError as a warning
and set `_initialized = True`, so it never retried. `get_feature_manager('stats')`
then raised `ValueError: Unknown feature`, which broke every sequencer
construction (`medallion/base/sequencer.py:324`).
"""

import pytest
from medalflow.core.features import registry as registry_module
from medalflow.core.features.base import FeatureManager
from medalflow.core.features.registry import FeatureRegistry


def test_managers_package_imports_cleanly():
    import medalflow.core.features.managers  # noqa: F401


@pytest.mark.parametrize("feature", ["cache", "stats"])
def test_shipped_feature_is_registered(feature):
    """Every manager module that calls register_feature must be reachable.

    `get_manager` raises ValueError for an unregistered name, so a manager
    whose import never ran is indistinguishable from a typo. Instantiation may
    still return None when the feature is disabled — that is fine; what must
    not happen is "Unknown feature".
    """
    registry_module._global_registry.auto_discover()

    assert feature in registry_module._global_registry.get_all_features()


# --- construction failure must not be cached (Phase 3, task 9) --------------


class _CountingManager(FeatureManager):
    """Records construction attempts; failure and availability are class-level
    so a test can flip them between `get_manager` calls."""

    attempts = 0
    fail = False
    available = True

    def __init__(self):
        type(self).attempts += 1
        if type(self).fail:
            raise RuntimeError("Key Vault timed out")

    def get_feature_name(self) -> str:
        return "counting"

    def is_available(self) -> bool:
        return type(self).available

    def initialize(self, config=None) -> None:
        return None


@pytest.fixture
def counting_registry():
    _CountingManager.attempts = 0
    _CountingManager.fail = False
    _CountingManager.available = True

    registry = FeatureRegistry()
    registry.register("counting", _CountingManager)
    return registry


def test_transient_construction_failure_is_retried_on_the_next_call(counting_registry):
    """A Key Vault timeout or an ADLS blip during initialize() used to cache
    None, disabling the feature for the whole process with no retry path."""
    _CountingManager.fail = True

    assert counting_registry.get_manager("counting") is None

    _CountingManager.fail = False

    assert isinstance(counting_registry.get_manager("counting"), _CountingManager)
    assert _CountingManager.attempts == 2


def test_a_feature_disabled_in_settings_is_cached_as_none(counting_registry):
    """The deliberate case is a decision, not a failure: it is answered once
    and remembered."""
    _CountingManager.available = False

    assert counting_registry.get_manager("counting") is None
    assert counting_registry.get_manager("counting") is None
    assert _CountingManager.attempts == 1
