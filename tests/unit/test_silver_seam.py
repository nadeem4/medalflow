"""Regression tests for the discovery -> orchestrator seam (Phase 1, task 7).

Discovery returns metadata dataclasses -- `TransformationMetadata` for silver,
`BronzeModelMetadata` for bronze -- but the orchestrator calls
`sequencer.get_obj_name()`, `.get_queries()` and `._get_class_metadata()` on
what it is given. The API layer handed the metadata straight through, so the
very first line of the orchestrator loop raised AttributeError — outside its
try block, so nothing caught it. Discovery results must be instantiated via
`metadata.sequencer_class`, and (D5) handed the plan's settings.

The seam has moved. There is no per-layer entry point any more (ADR 002, D7):
`compile()` is where every layer's discovery meets its sequencers, so that is
where these tests point. The three per-layer variants collapse into one test
per layer for the same reason -- there is one path now, not four.
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
from medalflow.types.metadata import BronzeMetadata, QueryMetadata, SilverMetadata

# --- compile seam: sequencer_class must be instantiated --------------------


# What the seam handed to each sequencer, for the tests below to read back.
# Reset by the `compiler_seam` fixture on every use.
SEAM_CALLS: dict = {}


class _Settings:
    """The two things `compile()` asks settings for before it discovers."""

    def package_for_layer(self, layer):
        return f"acme.{layer}"


class _NoModels:
    """A layer this project does not use."""

    def __init__(self, package, settings=None):
        pass

    def discover_all(self, force_refresh=False):
        return []


def _transformation(name, model):
    return TransformationMetadata(
        name=name,
        model=model,
        silver_metadata=SilverMetadata(name=name, schema="silver", model=model),
        sequencer_class=SilverTransformationSequencer,
    )


@pytest.fixture
def compiler_seam(monkeypatch):
    """Compile a project whose discovery is faked, recording what it built.

    The sequencer takes its settings injected now (D5), but the base still
    resolves the global singleton to wire up its feature managers. These tests
    are about the seam instantiating `sequencer_class` at all, so keep that
    construction offline (D6) while recording what it was handed.
    """
    from medalflow.api import compiler

    calls = SEAM_CALLS
    calls.clear()

    def _record(self, settings=None, selection=None):
        calls["instance"] = self
        calls["settings"] = settings
        calls["selection"] = selection

    monkeypatch.setattr(compiler, "get_settings", lambda: _Settings())
    monkeypatch.setattr(SilverTransformationSequencer, "__init__", _record)
    monkeypatch.setattr(SilverTransformationSequencer, "get_queries", lambda self: [])

    def _install(**discoveries):
        monkeypatch.setattr(
            compiler,
            "LAYER_DISCOVERIES",
            {layer: discoveries.get(layer, _NoModels) for layer in ("bronze", "silver", "gold")},
        )

    return _install


def test_compile_passes_a_sequencer_instance_not_the_metadata(compiler_seam):
    class _Discovery(_NoModels):
        def discover_all(self, force_refresh=False):
            return [_transformation("DimCustomer", "customer")]

    compiler_seam(silver=_Discovery)

    from medalflow.api import compile

    result = compile("layer:silver")

    assert isinstance(SEAM_CALLS["instance"], SilverTransformationSequencer)
    assert result.errors == []


def test_the_seam_injects_settings_into_each_sequencer(compiler_seam):
    """D5: `sequencer_class()` took no arguments and the sequencer resolved
    global settings for itself. It is handed the plan's settings now."""

    class _Discovery(_NoModels):
        def discover_all(self, force_refresh=False):
            return [_transformation("DimCustomer", "customer")]

    compiler_seam(silver=_Discovery)

    from medalflow.api import compile

    compile("layer:silver")

    assert SEAM_CALLS["settings"] is not None


# --- bronze reaches its sequencers the same way ----------------------------


def test_compile_instantiates_the_discovered_bronze_sequencer(compiler_seam):
    """Bronze used to have no discovery at all. It reaches its sequencer
    classes through the same walk as the other two layers now, and `compile()`
    constructs them the same way."""
    captured = {}

    class _Bronze:
        def __init__(self, settings, selection=None, **kwargs):
            captured["settings"] = settings
            captured["selection"] = selection

        def get_queries(self):
            return []

    class _Model:
        name = "Customer"
        sequencer_class = _Bronze
        description = ""
        tags: list[str] = []
        bronze_metadata = BronzeMetadata(
            name="Customer", schema="bronze", source_system="sap", source_table="KNA1"
        )

    class _Discovery(_NoModels):
        def discover_all(self, force_refresh=False):
            return [_Model()]

    compiler_seam(bronze=_Discovery)

    from medalflow.api import compile

    result = compile("layer:bronze")

    assert captured["settings"] is not None
    assert [model.name for model in result.models] == ["Customer"]
    assert result.errors == []


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


def test_the_per_call_selection_helpers_are_gone():
    """`get_transformations_by_models` and `get_transformations_by_names` were
    the two deleted per-layer silver entry points' only reason to exist. The
    selector narrows now -- `compile("layer:silver")` for a whole layer,
    `compile("DimCustomer")` for one transformation -- so a second, silver-only
    selection mechanism is a second thing to document and keep true."""
    assert not hasattr(SilverMetadataDiscovery, "get_transformations_by_models")
    assert not hasattr(SilverMetadataDiscovery, "get_transformations_by_names")


def test_the_package_walk_survives_them():
    """`discover_all_transformations` is silver's own name for the walk and has
    callers of its own."""
    assert hasattr(SilverMetadataDiscovery, "discover_all_transformations")


def test_warm_cache_is_gone():
    """It called get_all_models() and get_transformations_by_model(), neither of
    which has ever existed on this class."""
    assert not hasattr(SilverMetadataDiscovery, "warm_cache")
