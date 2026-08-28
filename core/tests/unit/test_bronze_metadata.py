"""Regression tests for @bronze_metadata (Phase 1, task 8, Decision D7).

`bronze_metadata` was a stub whose body was `pass`, so it returned None —
meaning `@bronze_metadata(...)` replaced the decorated class with None, and a
bare `@bronze_metadata` did too. The module also defined
`BronzeMetadata = type('BronzeMetadata', (), {})`, an empty class shadowing the
real pydantic model for anything importing it from here.

It is exported public API (`medallion/__init__.py`), so per D7 it is
implemented minimally, mirroring `gold_metadata`.
"""

import pytest
from medalflow.medallion.bronze.decorators import bronze_metadata
from medalflow.types.metadata import BronzeMetadata


def test_decorated_class_carries_bronze_metadata():
    @bronze_metadata(source_system="d365")
    class Customers:
        pass

    assert Customers is not None
    assert isinstance(Customers._bronze_metadata, BronzeMetadata)
    assert Customers._bronze_metadata.source_system == "d365"


def test_decorator_defaults_match_the_model():
    @bronze_metadata(source_system="d365")
    class Customers:
        pass

    assert Customers._bronze_metadata.ingestion_mode == "incremental"
    assert Customers._bronze_metadata.description is None
    assert Customers._bronze_metadata.tags == []


def test_decorator_passes_through_optional_fields():
    @bronze_metadata(
        source_system="d365",
        ingestion_mode="full",
        description="Raw customer feed",
        tags=["pii"],
    )
    class Customers:
        pass

    metadata = Customers._bronze_metadata
    assert metadata.ingestion_mode == "full"
    assert metadata.description == "Raw customer feed"
    assert metadata.tags == ["pii"]


def test_decorator_returns_the_same_class_object():
    class Customers:
        pass

    assert bronze_metadata(source_system="d365")(Customers) is Customers


def test_module_does_not_shadow_the_real_metadata_model():
    import medalflow.medallion.bronze.decorators as decorators

    shadow = getattr(decorators, "BronzeMetadata", None)
    assert shadow is None or shadow is BronzeMetadata


def test_missing_source_system_is_rejected():
    with pytest.raises(TypeError):
        bronze_metadata()
