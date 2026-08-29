"""Regression tests for the silver discovery -> orchestrator seam (Phase 1, task 7).

`SilverMetadataDiscovery` returns `TransformationMetadata` dataclasses, but
`create_plan_from_sequencers` calls `sequencer.get_obj_name()`,
`.get_queries()` and `._get_class_metadata()` on what it is given. The API
layer handed the metadata straight through, so the very first line of the
orchestrator loop raised AttributeError — outside its try block, so nothing
caught it. Discovery results must be instantiated via `metadata.sequencer_class`.
"""

import logging

import pytest
from medalflow.constants.sql import QueryType
from medalflow.medallion.silver.metadata_discovery import (
    SilverMetadataDiscovery,
    TransformationMetadata,
)
from medalflow.medallion.silver.sequencer import SilverTransformationSequencer
from medalflow.settings.main import MedalflowSettings
from medalflow.types.metadata import QueryMetadata

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
    from medalflow.api import medallion as api

    created = {}

    class _Settings:
        silver_package_name = "acme.silver"

    class _Discovery:
        def __init__(self, package_name):
            created["package"] = package_name

        def get_transformations_by_models(self, models):
            return [_transformation("usp_load_customer", "customer")]

        def get_transformations_by_names(self, names):
            return [_transformation("usp_load_order", "order")]

    monkeypatch.setattr(api, "get_settings", lambda: _Settings())
    monkeypatch.setattr(api, "SilverMetadataDiscovery", _Discovery)
    monkeypatch.setattr(api, "ExecutionPlanOrchestrator", _RecordingOrchestrator)

    # SilverTransformationSequencer.__init__ resolves live settings of its own;
    # this test is about the seam instantiating sequencer_class at all, so keep
    # that construction offline (D6).
    from medalflow.medallion.silver import sequencer as silver_sequencer

    monkeypatch.setattr(
        silver_sequencer.SilverTransformationSequencer,
        "__init__",
        lambda self, *a, **kw: None,
    )
    return api


def _transformation(name, model):
    return TransformationMetadata(
        name=name,
        model=model,
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


def _settings(**conventions):
    """Offline settings carrying only the conventions a test opts into.

    The enum table and the temp-Detail promotion are `conventions` entries as
    of Phase 3 task 8, so a sequencer built with `__new__` needs `.settings`
    before either path will run.
    """
    return MedalflowSettings(
        source_system="sap",
        ds_env="dev",
        name="fin",
        compute={"lake_database_name": "lakedb"},
        conventions=conventions,
    )


def test_enum_query_uses_the_core_query_builder_module(monkeypatch):
    """`from query_builder.factory import ...` — no such top-level package."""

    class _Builder:
        def fully_qualified_name(self, schema, object_name):
            return "[bronze].[Enumeration]"

        def quote_string(self, value):
            return "'{}'".format(value.replace("'", "''"))

    import medalflow.query_builder.factory as factory

    monkeypatch.setattr(factory, "create_query_builder", lambda: _Builder())

    sequencer = SilverTransformationSequencer.__new__(SilverTransformationSequencer)
    sequencer.settings = _settings(
        enum_table={
            "schema_name": "bronze",
            "table_name": "Enumeration",
            "name_column": "Enum",
            "value_column": "EnumValue",
            "value_id_column": "EnumValueID",
        }
    )
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

    The promotion itself is opt-in now, so the convention is configured here
    to reach the logging call at all. What it asserts is unchanged.
    """
    caplog.set_level(logging.INFO, logger="test-silver")
    sequencer = SilverTransformationSequencer.__new__(SilverTransformationSequencer)
    sequencer.settings = _settings(detail_tables={"table_suffix": "Detail"})
    sequencer.logger = logging.getLogger("test-silver")
    sequencer.transform_detail_to_silver = lambda sql: sql + " /*transformed*/"

    sql, metadata = sequencer._transform_query_result(
        "SELECT 1",
        QueryMetadata(type=QueryType.CREATE_TABLE, table_name="OrderDetail", schema_name="temp"),
    )

    assert metadata.schema_name == "silver"
    assert metadata.table_name == "Order"
    assert sql.endswith("/*transformed*/")


# --- discovery contract ----------------------------------------------------


def test_get_transformations_by_names_is_annotated_as_a_list():
    """It returns a list; the annotation said Optional[TransformationMetadata]."""
    from typing import get_type_hints

    hints = get_type_hints(SilverMetadataDiscovery.get_transformations_by_names)

    assert hints["return"] == list[TransformationMetadata]


def test_warm_cache_is_gone():
    """It called get_all_models() and get_transformations_by_model(), neither of
    which has ever existed on this class."""
    assert not hasattr(SilverMetadataDiscovery, "warm_cache")
