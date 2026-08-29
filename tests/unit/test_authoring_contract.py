"""ADR 002 — the authoring contract, pinned.

The decorators are the only surface a consuming team writes against, so each
decision in ADR 002 that removes something from that surface gets a test here.
Without them a deleted parameter is one merge away from being reinstated by
someone who reads the old docstring and assumes it was an oversight.
"""

import medalflow
import medalflow.medallion

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
