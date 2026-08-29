"""ADR 002 — the authoring contract, pinned.

The decorators are the only surface a consuming team writes against, so each
decision in ADR 002 that removes something from that surface gets a test here.
Without them a deleted parameter is one merge away from being reinstated by
someone who reads the old docstring and assumes it was an oversight.
"""

import medalflow
import medalflow.medallion
import pytest
from medalflow.constants.compute import EngineType
from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.bronze.decorators import bronze_metadata
from medalflow.medallion.gold.decorators import gold_metadata
from medalflow.medallion.silver.decorators import silver_metadata
from medalflow.operations.base import BaseOperation
from medalflow.types.metadata import SilverMetadata

# ---------------------------------------------------------------------------
# D2 — the Snapshot layer is cut from the public API
# ---------------------------------------------------------------------------

SNAPSHOT_SYMBOLS = ["SnapshotSequencer", "snapshot_metadata", "SnapshotMetadata"]


def test_snapshot_layer_is_not_importable():
    """The package itself is gone, not merely unexported."""
    import importlib

    for module in (
        "medalflow.medallion.snapshot",
        "medalflow.medallion.snapshot.decorators",
        "medalflow.medallion.snapshot.sequencer",
    ):
        try:
            importlib.import_module(module)
        except ImportError:
            continue
        raise AssertionError(f"{module} still imports")


def test_snapshot_symbols_are_off_the_public_surface():
    for symbol in SNAPSHOT_SYMBOLS:
        assert not hasattr(medalflow, symbol), f"medalflow.{symbol} survives"
        assert not hasattr(medalflow.medallion, symbol), f"medalflow.medallion.{symbol} survives"


def test_snapshot_metadata_type_is_gone():
    import medalflow.types

    assert not hasattr(medalflow.types, "SnapshotMetadata")
    assert "SnapshotMetadata" not in medalflow.types.__all__


def test_snapshot_partition_helpers_are_not_headline_api():
    """D2 applies to the utils too: advertised, with no caller behind them."""
    for helper in ("get_snapshot_datetime", "get_partition_path", "parse_snapshot_path"):
        assert helper not in medalflow.__all__, f"medalflow.{helper} is still advertised"


# ---------------------------------------------------------------------------
# D2 — Layer.SNAPSHOT stays: it is schema vocabulary, not the layer
# ---------------------------------------------------------------------------


def test_snapshot_stays_a_schema_name():
    from medalflow.constants.medallion import Layer

    assert Layer.SNAPSHOT.value == "snapshot"


# ---------------------------------------------------------------------------
# D2 — `description=` is uniform across the layer decorators and survives
#      onto the metadata model, where compile output can reach it
# ---------------------------------------------------------------------------


def test_every_layer_decorator_stores_its_description():
    @bronze_metadata(
        name="Customers", schema="bronze", source_system="d365", description="raw customers"
    )
    class Bronze:
        pass

    @silver_metadata(
        name="Load_Customer_Dim",
        model="sales",
        description="cleansed customers",
    )
    class Silver:
        pass

    @gold_metadata(name="Revenue", schema="gold", description="customer revenue")
    class Gold:
        pass

    assert Bronze._bronze_metadata.description == "raw customers"
    assert Silver._silver_metadata.description == "cleansed customers"
    assert Gold._gold_metadata.description == "customer revenue"


def test_description_is_optional_and_defaults_to_none():
    @gold_metadata(name="Revenue", schema="gold")
    class Gold:
        pass

    assert Gold._gold_metadata.description is None


# ---------------------------------------------------------------------------
# D3 — parameters that were accepted and discarded are gone, loudly
# ---------------------------------------------------------------------------


def test_silver_metadata_rejects_take_snapshot():
    with pytest.raises(TypeError, match="take_snapshot"):
        silver_metadata(
            name="Load_Customer_Dim",
            model="sales",
            take_snapshot=True,
        )


def test_query_metadata_rejects_query():
    with pytest.raises(TypeError, match="query"):
        query_metadata(type=QueryType.SELECT, query="SELECT 1")


def test_query_metadata_rejects_name():
    with pytest.raises(TypeError, match="name"):
        query_metadata(type=QueryType.UPDATE, name="DimCustomer")


def test_update_without_name_is_simply_accepted():
    """The docstring promised a ValueError that was never raised. Both are gone."""

    @query_metadata(type=QueryType.UPDATE, table_name="DimCustomer")
    def update_customers(self):
        return "UPDATE DimCustomer SET x = 1"

    assert update_customers._query_metadata.type == QueryType.UPDATE


# ---------------------------------------------------------------------------
# D4 — `preferred_engine` leaves the authoring surface; EngineType.SPARK stays
# ---------------------------------------------------------------------------


def test_silver_metadata_rejects_preferred_engine():
    with pytest.raises(TypeError, match="preferred_engine"):
        silver_metadata(
            name="Load_Customer_Dim",
            model="sales",
            preferred_engine="spark",
        )


def test_query_metadata_rejects_preferred_engine():
    with pytest.raises(TypeError, match="preferred_engine"):
        query_metadata(type=QueryType.SELECT, preferred_engine="spark")


def test_silver_metadata_model_has_no_engine_field():
    assert "preferred_engine" not in SilverMetadata.model_fields


def test_engine_hint_survives_as_internal_plumbing():
    """D4 keeps the operation-level hint; only the authoring knob goes."""
    assert "engine_hint" in BaseOperation.model_fields


def test_spark_stays_an_enum_member():
    """Removing it would turn a documented input into an import-time ValueError."""
    assert EngineType("spark") is EngineType.SPARK


# ---------------------------------------------------------------------------
# D2 — one identity vocabulary: `name` and `model`, and nothing else
# ---------------------------------------------------------------------------


def test_silver_metadata_stores_name_and_model():
    @silver_metadata(name="Load_Customer_Dim", model="sales")
    class Silver:
        pass

    assert Silver._silver_metadata.name == "Load_Customer_Dim"
    assert Silver._silver_metadata.model == "sales"


def test_silver_metadata_takes_name_and_model_positionally():
    @silver_metadata("Load_Customer_Dim", "sales")
    class Silver:
        pass

    assert Silver._silver_metadata.name == "Load_Customer_Dim"
    assert Silver._silver_metadata.model == "sales"


def test_silver_metadata_requires_model():
    """`model` was optional and back-derived from a filename. It is now declared."""
    with pytest.raises(TypeError, match="model"):
        silver_metadata(name="Load_Customer_Dim")


@pytest.mark.parametrize("parameter", ["sp_name", "group_file_name", "model_name"])
def test_silver_metadata_rejects_the_old_identity_parameters(parameter):
    with pytest.raises(TypeError, match=parameter):
        silver_metadata(name="Load_Customer_Dim", model="sales", **{parameter: "x"})


@pytest.mark.parametrize("field", ["sp_name", "group_file_name", "model_name"])
def test_silver_metadata_model_drops_the_old_identity_fields(field):
    assert field not in SilverMetadata.model_fields


def test_silver_metadata_model_carries_name_and_model():
    assert "name" in SilverMetadata.model_fields
    assert "model" in SilverMetadata.model_fields


def test_gold_metadata_stores_schema():
    @gold_metadata(name="Revenue", schema="gold_sales")
    class Gold:
        pass

    assert Gold._gold_metadata.schema == "gold_sales"


def test_gold_metadata_rejects_schema_name():
    with pytest.raises(TypeError, match="schema_name"):
        gold_metadata(name="Revenue", schema_name="gold")


def test_gold_metadata_model_drops_schema_name():
    from medalflow.types.metadata import GoldMetadata

    assert "schema_name" not in GoldMetadata.model_fields
    assert "schema" in GoldMetadata.model_fields


# ---------------------------------------------------------------------------
# D2 — `name` is the identity in every layer, gold included
# ---------------------------------------------------------------------------


def test_gold_metadata_requires_name():
    """Gold discovery keyed on the *class* name, which is not a declaration.

    Every other layer names its model; gold inferred one. A rename of the class
    silently renamed the model, and two gold classes could not share a name even
    across packages.
    """
    with pytest.raises(TypeError, match="name"):
        gold_metadata(schema="gold")


def test_gold_metadata_stores_name():
    @gold_metadata(name="Revenue", schema="gold")
    class Gold:
        pass

    assert Gold._gold_metadata.name == "Revenue"


def test_gold_metadata_model_carries_name():
    from medalflow.types.metadata import GoldMetadata

    assert "name" in GoldMetadata.model_fields


def test_gold_metadata_takes_name_and_schema_positionally():
    """Same order as bronze: identity first, then the target schema."""

    @gold_metadata("Revenue", "gold")
    class Gold:
        pass

    assert (Gold._gold_metadata.name, Gold._gold_metadata.schema) == ("Revenue", "gold")
