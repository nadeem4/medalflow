"""Regression tests for the silver discovery -> orchestrator seam (Phase 1, task 7).

`SilverMetadataDiscovery` returns `TransformationMetadata` dataclasses, but
`create_plan_from_sequencers` calls `sequencer.get_obj_name()`,
`.get_queries()` and `._get_class_metadata()` on what it is given. The API
layer handed the metadata straight through, so the very first line of the
orchestrator loop raised AttributeError — outside its try block, so nothing
caught it. Discovery results must be instantiated via `metadata.sequencer_class`.
"""

import logging
from typing import List

import pytest

from core.constants.sql import QueryType
from core.medallion.silver.metadata_discovery import (
    SilverMetadataDiscovery,
    TransformationMetadata,
)
from core.medallion.silver.sequencer import SilverTransformationSequencer
from core.types.metadata import QueryMetadata


# --- api seam: sequencer_class must be instantiated ------------------------


class _RecordingOrchestrator:
    def __init__(self, settings):
        self.received = None

    def create_plan_for_silver_layer(self, silver_sequencers):
        self.received = silver_sequencers
        return _FakePlan()


class _FakePlan:
    def attach_context(self, ctx):
        return None


@pytest.fixture
def api_module(monkeypatch):
    from core.api import medallion as api

    created = {}

    class _Settings:
        silver_package_name = "acme.silver"

    class _Discovery:
        def __init__(self, package_name):
            created["package"] = package_name

        def get_transformations_by_models(self, models):
            return [_transformation("usp_load_customer", "customer")]

        def get_transformation_by_sp(self, sp_names):
            return [_transformation("usp_load_order", "order")]

    monkeypatch.setattr(api, "get_settings", lambda: _Settings())
    monkeypatch.setattr(api, "SilverMetadataDiscovery", _Discovery)
    monkeypatch.setattr(api, "ExecutionPlanOrchestrator", _RecordingOrchestrator)

    # SilverTransformationSequencer.__init__ resolves live settings of its own;
    # this test is about the seam instantiating sequencer_class at all, so keep
    # that construction offline (D6).
    from core.medallion.silver import sequencer as silver_sequencer

    monkeypatch.setattr(
        silver_sequencer.SilverTransformationSequencer,
        "__init__",
        lambda self, *a, **kw: None,
    )
    return api


def _transformation(sp_name, model_name):
    return TransformationMetadata(
        sp_name=sp_name,
        model_name=model_name,
        silver_metadata=None,
        sequencer_class=SilverTransformationSequencer,
    )


def test_model_plan_passes_sequencer_instances_not_metadata(api_module, monkeypatch):
    captured = {}

    def _capture(self, silver_sequencers):
        captured["value"] = silver_sequencers
        return _FakePlan()

    monkeypatch.setattr(_RecordingOrchestrator, "create_plan_for_silver_layer", _capture)

    api_module.get_silver_execution_plan_for_models(models="all")

    assert captured["value"]
    for sequencer in captured["value"]:
        assert isinstance(sequencer, SilverTransformationSequencer)


def test_sp_plan_passes_sequencer_instances_not_metadata(api_module, monkeypatch):
    captured = {}

    def _capture(self, silver_sequencers):
        captured["value"] = silver_sequencers
        return _FakePlan()

    monkeypatch.setattr(_RecordingOrchestrator, "create_plan_for_silver_layer", _capture)

    api_module.get_execution_plan_for_sps(sp_names="usp_load_order")

    assert captured["value"]
    for sequencer in captured["value"]:
        assert isinstance(sequencer, SilverTransformationSequencer)


# --- bronze entry point: settings + CSV table names ------------------------


def test_bronze_plan_passes_settings_and_csv_table_names(api_module, monkeypatch):
    captured = {}

    class _Bronze:
        def __init__(self, settings, table_names=None, **kwargs):
            captured["settings"] = settings
            captured["table_names"] = table_names

    def _capture(self, bronze_sequencer):
        return _FakePlan()

    monkeypatch.setattr(api_module, "BronzeSequencer", _Bronze)
    monkeypatch.setattr(
        _RecordingOrchestrator, "create_plan_for_bronze_layer", _capture, raising=False
    )

    api_module.get_bronze_execution_plan(["Customer", "Order"])

    assert captured["settings"] is not None
    assert captured["table_names"] == "Customer,Order"


# --- silver sequencer: import path and logger kwargs -----------------------


def test_enum_query_uses_the_core_query_builder_module(monkeypatch):
    """`from query_builder.factory import ...` — no such top-level package."""

    class _Builder:
        def fully_qualified_name(self, schema, object_name):
            return "[bronze].[Enumeration]"

    import core.query_builder.factory as factory

    monkeypatch.setattr(factory, "create_query_builder", lambda: _Builder())

    sequencer = SilverTransformationSequencer.__new__(SilverTransformationSequencer)
    sql = sequencer._generate_enum_query(
        QueryMetadata(type=QueryType.SELECT, table_name="Status", filter="StatusEnum")
    )

    assert "[bronze].[Enumeration]" in sql
    assert "WHERE Enum = 'StatusEnum'" in sql


def test_detail_table_transformation_logs_without_crashing(caplog):
    """`self.logger` is a stdlib Logger; table=/schema= kwargs raise TypeError.

    The crash is latent: `Logger.info` checks `isEnabledFor` before touching
    its kwargs, so with the default WARNING level the bad call is never
    reached. It fires the moment a deployment turns INFO logging on — so the
    test enables INFO.
    """
    caplog.set_level(logging.INFO, logger="test-silver")
    sequencer = SilverTransformationSequencer.__new__(SilverTransformationSequencer)
    sequencer.logger = logging.getLogger("test-silver")
    sequencer.transform_detail_to_silver = lambda sql: sql + " /*transformed*/"

    sql, metadata = sequencer._transform_query_result(
        "SELECT 1",
        QueryMetadata(
            type=QueryType.CREATE_TABLE, table_name="OrderDetail", schema_name="temp"
        ),
    )

    assert metadata.schema_name == "silver"
    assert metadata.table_name == "Order"
    assert sql.endswith("/*transformed*/")


# --- discovery contract ----------------------------------------------------


def test_get_transformation_by_sp_is_annotated_as_a_list():
    """It returns a list; the annotation said Optional[TransformationMetadata]."""
    from typing import get_type_hints

    hints = get_type_hints(SilverMetadataDiscovery.get_transformation_by_sp)

    assert hints["return"] == List[TransformationMetadata]


def test_warm_cache_is_gone():
    """It called get_all_models() and get_transformations_by_model(), neither of
    which has ever existed on this class."""
    assert not hasattr(SilverMetadataDiscovery, "warm_cache")
