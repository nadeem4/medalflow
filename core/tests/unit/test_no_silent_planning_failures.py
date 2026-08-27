"""The planning path must not swallow failures (Phase 1 exit criterion).

Task 9 made `get_queries` and `_walk_silver_package` fail loudly, but two
layers above them still caught everything and carried on, so a broken model
silently vanished from the plan anyway:

- `create_plan_from_sequencers` caught every exception from `get_queries()`
  and `continue`d to the next sequencer.
- `discover_all_transformations` caught every exception from class extraction
  and from `_extract_metadata_from_class`, counting them as `error_count` and
  returning whatever survived.

dbt-style: an authoring mistake fails the whole compile, it does not quietly
shrink the plan.
"""

import logging

import pytest

from core.medallion.orchestration.execution_orchestrator import ExecutionPlanOrchestrator
from core.medallion.silver.metadata_discovery import SilverMetadataDiscovery


class _BrokenSequencer:
    def get_obj_name(self):
        return "BrokenModel"

    def get_queries(self):
        raise ValueError("Failed to build queries for model 'BrokenModel'")

    def _get_class_metadata(self):
        return {}


class _HealthySequencer:
    def get_obj_name(self):
        return "HealthyModel"

    def get_queries(self):
        return []

    def _get_class_metadata(self):
        return {}


@pytest.fixture
def orchestrator():
    instance = ExecutionPlanOrchestrator.__new__(ExecutionPlanOrchestrator)
    instance.settings = None
    instance.logger = logging.getLogger("test-orchestrator")
    return instance


def test_a_broken_sequencer_fails_the_whole_plan(orchestrator):
    with pytest.raises(Exception) as excinfo:
        orchestrator.create_plan_from_sequencers([_BrokenSequencer()])

    assert "BrokenModel" in str(excinfo.value)


def test_one_broken_sequencer_is_not_skipped_over_a_healthy_one(orchestrator):
    """The plan must not quietly shrink to the models that happened to work."""
    with pytest.raises(Exception) as excinfo:
        orchestrator.create_plan_from_sequencers(
            [_HealthySequencer(), _BrokenSequencer()]
        )

    assert "BrokenModel" in str(excinfo.value)


class _ExplodingClass:
    """Stands in for a model whose metadata cannot be read."""

    _silver_metadata = object()


@pytest.fixture
def discovery(monkeypatch):
    instance = SilverMetadataDiscovery.__new__(SilverMetadataDiscovery)
    instance.settings = None
    instance.silver_package = "irrelevant"
    instance.logger = logging.getLogger("test-discovery")
    instance._cache_manager = None

    class _Module:
        __name__ = "acme.silver.customers"

    monkeypatch.setattr(
        SilverMetadataDiscovery, "_walk_silver_package", lambda self: iter([_Module()])
    )
    monkeypatch.setattr(
        SilverMetadataDiscovery,
        "_extract_transformation_classes",
        lambda self, module: [_ExplodingClass],
    )
    return instance


def test_unreadable_model_metadata_fails_discovery(discovery):
    with pytest.raises(Exception) as excinfo:
        discovery.discover_all_transformations(force_refresh=True)

    assert "_ExplodingClass" in str(excinfo.value)
