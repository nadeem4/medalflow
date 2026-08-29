"""What `@bronze_metadata` declares (ADR 002, Decisions 2 and 6).

`bronze_metadata` was write-only: it attached `_bronze_metadata` and nothing in
`src/` ever read it, so a declared bronze model could not build a table. It is
load-bearing now — one bronze model is one bronze table — which means it has to
carry the same things every other layer's decorator carries: the model's `name`,
the target `schema`, and `disabled`.

`ingestion_mode` is gone. It had zero consumers, which is the
"declared but nothing acts on it" shape Decision 3 deleted.
"""

import pytest
from medalflow.medallion.bronze.decorators import bronze_metadata
from medalflow.types.metadata import BronzeMetadata


def test_decorated_class_carries_bronze_metadata():
    @bronze_metadata(name="Customers", schema="bronze", source_system="d365")
    class Customers:
        pass

    assert Customers is not None
    assert isinstance(Customers._bronze_metadata, BronzeMetadata)
    assert Customers._bronze_metadata.source_system == "d365"


# --- identity and target ---------------------------------------------------


def test_name_is_the_models_identity():
    @bronze_metadata(name="Customers", schema="bronze", source_system="d365")
    class AnyClassName:
        pass

    assert AnyClassName._bronze_metadata.name == "Customers"


def test_schema_is_the_target_schema():
    """The `"bronze"` literal at sequencer.py:100 was the only target schema
    bronze could ever write to."""

    @bronze_metadata(name="Customers", schema="raw", source_system="d365")
    class Customers:
        pass

    assert Customers._bronze_metadata.schema == "raw"


def test_name_and_schema_are_required():
    with pytest.raises(TypeError):
        bronze_metadata(source_system="d365")


# --- where the rows come from ----------------------------------------------


def test_source_table_defaults_to_the_model_name():
    """One bronze model is one bronze table, and it usually has the same name
    on both sides."""

    @bronze_metadata(name="Customers", schema="bronze", source_system="d365")
    class Customers:
        pass

    assert Customers._bronze_metadata.source_table == "Customers"


def test_source_table_can_differ_from_the_model_name():
    @bronze_metadata(
        name="Customers",
        schema="bronze",
        source_system="d365",
        source_table="CUSTTABLE",
    )
    class Customers:
        pass

    assert Customers._bronze_metadata.source_table == "CUSTTABLE"


def test_source_schema_is_unset_unless_declared():
    """Unset means the sequencer's own `source_schema` stands in; the decorator
    cannot know it."""

    @bronze_metadata(name="Customers", schema="bronze", source_system="d365")
    class Customers:
        pass

    assert Customers._bronze_metadata.source_schema is None


def test_source_schema_can_be_declared_per_model():
    @bronze_metadata(
        name="Customers", schema="bronze", source_system="d365", source_schema="staging"
    )
    class Customers:
        pass

    assert Customers._bronze_metadata.source_schema == "staging"


# --- the uniform fields ----------------------------------------------------


def test_decorator_defaults_match_the_model():
    @bronze_metadata(name="Customers", schema="bronze", source_system="d365")
    class Customers:
        pass

    assert Customers._bronze_metadata.description is None
    assert Customers._bronze_metadata.tags == []
    assert Customers._bronze_metadata.disabled is False


def test_decorator_passes_through_optional_fields():
    @bronze_metadata(
        name="Customers",
        schema="bronze",
        source_system="d365",
        description="Raw customer feed",
        tags=["pii"],
        disabled=True,
    )
    class Customers:
        pass

    metadata = Customers._bronze_metadata
    assert metadata.description == "Raw customer feed"
    assert metadata.tags == ["pii"]
    assert metadata.disabled is True


def test_decorator_returns_the_same_class_object():
    class Customers:
        pass

    decorate = bronze_metadata(name="Customers", schema="bronze", source_system="d365")

    assert decorate(Customers) is Customers


def test_module_does_not_shadow_the_real_metadata_model():
    import medalflow.medallion.bronze.decorators as decorators

    shadow = getattr(decorators, "BronzeMetadata", None)
    assert shadow is None or shadow is BronzeMetadata


# --- the parameter that lied -----------------------------------------------


def test_ingestion_mode_is_rejected():
    """Accepted, stored, and read by nothing — Decision 3's shape."""
    with pytest.raises(TypeError, match="ingestion_mode"):
        bronze_metadata(
            name="Customers",
            schema="bronze",
            source_system="d365",
            ingestion_mode="full",
        )


def test_ingestion_mode_is_off_the_metadata_model():
    assert "ingestion_mode" not in BronzeMetadata.model_fields
