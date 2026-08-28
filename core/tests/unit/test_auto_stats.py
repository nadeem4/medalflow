"""Regression tests for auto-statistics after CREATE TABLE (Phase 2, batch 10).

`_BasePlatform.execute_operation` creates statistics for a table whose
`metadata.create_stats` is set. Two defects lived in that block:

1. Scope. The `CreateStatistics(...)` constructor sat *outside* the inner
   `try`, and `CreateStatistics` has a model validator that raises ValueError
   when auto-discovery yields no columns. The exception therefore escaped to
   the outer `except Exception` of `execute_operation`, which discarded the
   real (successful) result and returned `success=False`. A CREATE TABLE that
   actually ran was reported as a failure.
2. Fan-out. `StatsProtocol.get_stats_columns` returns a *list*, but
   `BaseQueryBuilder._validate_create_statistics` rejects any operation with
   more than one column ("Synapse only supports single-column statistics").
   One multi-column operation was built, so in the normal configured case the
   statistics were silently swallowed as a warning.

Offline per D6: the platform is built with `__new__` and its engine /
query-builder collaborators are stubs, so nothing touches a warehouse.
"""

import pytest

from medalflow.constants.compute import EngineType
from medalflow.constants.sql import QueryType
from medalflow.compute.platforms.base import _BasePlatform
from medalflow.operations import CreateStatistics, CreateTable
from medalflow.settings.base import CTEBaseSettings
from medalflow.types.metadata import QueryMetadata


class _StubQueryBuilder:
    """Records the operations it is asked to compile."""

    def __init__(self):
        self.operations = []

    def build_query(self, operation):
        self.operations.append(operation)
        return f"-- {operation.operation_type}"


class _StubSQLEngine:
    def __init__(self):
        self.queries = []

    def execute_query(self, query, telemetry=None):
        self.queries.append(query)


class _StubPlatform(_BasePlatform):
    def name(self) -> str:
        return "stub"

    def supported_engines(self):
        return [EngineType.SQL]

    def _initialize_dependencies(self) -> None:
        pass


class _StubStatsManager:
    def __init__(self, columns):
        self._columns = columns

    def get_stats_columns(self, table_name, layer):
        return self._columns


@pytest.fixture
def platform():
    instance = _StubPlatform.__new__(_StubPlatform)
    instance.settings = None
    instance.environment = None
    instance._query_builder = _StubQueryBuilder()
    instance._sql_engine = _StubSQLEngine()
    return instance


@pytest.fixture(autouse=True)
def offline_table_prefix(monkeypatch):
    """`full_object_name` resolves live settings; keep that offline (D6)."""
    import medalflow.settings

    settings = CTEBaseSettings(
        tenant_id="00000000-0000-0000-0000-000000000000",
        source_system="sap",
        ds_env="dev",
        name="fin",
    )
    monkeypatch.setattr(medalflow.settings, "get_settings", lambda: settings)
    return settings


@pytest.fixture
def stats_columns(monkeypatch):
    """Control what auto-discovery finds, without a feature registry."""
    import medalflow.core.features

    def install(columns):
        manager = _StubStatsManager(columns) if columns else None
        monkeypatch.setattr(
            medalflow.core.features,
            "get_feature_manager",
            lambda feature_name: manager if feature_name == "stats" else None,
        )

    return install


def _create_table():
    return CreateTable(
        schema_name="silver",
        object_name="Customers",
        select_query="SELECT 1 AS Id",
        metadata=QueryMetadata(type=QueryType.CREATE_TABLE, create_stats=True),
    )


def test_create_table_succeeds_when_no_stats_columns_discovered(platform, stats_columns):
    """The CreateStatistics validator must not turn a successful CREATE TABLE
    into a failed OperationResult."""
    stats_columns(None)

    result = platform.execute_operation(_create_table())

    assert result.success is True, result.error_message
    assert result.operation_type == QueryType.CREATE_TABLE
    assert result.error_message is None


def test_discovered_stats_columns_fan_out_to_one_operation_each(platform, stats_columns):
    """Synapse only supports single-column statistics, so two discovered
    columns must produce two single-column operations."""
    stats_columns(["col_a", "col_b"])

    result = platform.execute_operation(_create_table())

    assert result.success is True, result.error_message

    stats_ops = [
        op for op in platform._query_builder.operations
        if isinstance(op, CreateStatistics)
    ]
    assert len(stats_ops) == 2
    assert all(len(op.columns) == 1 for op in stats_ops)
    assert {op.columns[0] for op in stats_ops} == {"col_a", "col_b"}
