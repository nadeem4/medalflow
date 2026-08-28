"""Naming conventions MedalFlow applies to your SQL -- none of them by default.

Four behaviours used to be compiled into the sequencers: a soft-delete
predicate added to every bronze SELECT, an enum lookup table addressed by
hardcoded schema, table and column names, a rewrite that moved
``temp.<X>Detail`` to ``silver.<X>`` behind the caller's back, and eleven
name-pattern rules that decided how NULLs were filled. Each encoded the naming
scheme of the one warehouse MedalFlow grew out of.

A framework does not silently rewrite a user's SQL, so each is now an entry
here and each defaults to off. Environment variables are namespaced by the
parent settings object::

    MEDALFLOW_CONVENTIONS__SOFT_DELETE__PREDICATE="IsDelete IS NULL"

Values in this module are configuration, supplied by whoever deploys the
pipeline, and are inlined into generated SQL as written -- the same trust
level as the schema and table names alongside them. Runtime values are not:
those are escaped at the point of use.
"""

from typing import Literal, Optional

from pydantic import BaseModel, Field


class SoftDeleteConvention(BaseModel):
    """Filter deleted rows out of every bronze source SELECT.

    Unset (the default), bronze reads its source tables whole.
    """

    predicate: str = Field(
        ...,
        description="SQL predicate used as the WHERE clause of every bronze "
        "source SELECT, e.g. 'IsDelete IS NULL'.",
    )
    exempt_table_suffixes: list[str] = Field(
        default_factory=list,
        description="Source tables whose name ends with one of these are read "
        "unfiltered, e.g. ['Metadata'] for tables that carry no delete flag.",
    )

    def applies_to(self, table_name: str) -> bool:
        """Whether the predicate should be applied to ``table_name``.

        Args:
            table_name: Unqualified source table name

        Returns:
            False if the name ends with one of the exempt suffixes
        """
        return not any(table_name.endswith(suffix) for suffix in self.exempt_table_suffixes)


class EnumTableConvention(BaseModel):
    """The lookup table behind ``@silver_metadata(filter=...)`` dimensions.

    Every field is required: a half-guessed enum query is a worse outcome than
    no enum query. Unset (the default), filter-based dimensions raise instead
    of generating SQL against a table that may not exist.
    """

    schema_name: str = Field(..., description="Schema holding the enum table, e.g. 'bronze'.")
    table_name: str = Field(..., description="Enum table name, e.g. 'Enumeration'.")
    name_column: str = Field(
        ..., description="Column matched against the decorator's `filter` value."
    )
    value_column: str = Field(..., description="Column holding the enum's display value.")
    value_id_column: str = Field(..., description="Column holding the enum's numeric id.")


class DetailTableConvention(BaseModel):
    """Promote a staged detail table straight into the silver layer.

    When a query's target matches, the schema and table name are rewritten and
    the SQL is wrapped with null handling and a default row. Unset (the
    default), a query lands exactly where its decorator said it would.
    """

    table_suffix: str = Field(
        ...,
        description="Table-name suffix marking a detail table, e.g. 'Detail'. "
        "Stripped from the name on promotion.",
    )
    source_schema: str = Field(default="temp", description="Schema the detail table is staged in.")
    target_schema: str = Field(default="silver", description="Schema it is promoted into.")


class NullHandlingRule(BaseModel):
    """One column-name rule for filling NULLs in a promoted detail table.

    Rules are matched in declared order and the first hit wins, so a narrow
    suffix belongs ahead of a broad one ('KeyID' before 'ID'). Matching is
    case-insensitive; ``default`` is inlined as written, so string defaults
    carry their own quotes ("''", "'1900-01-01'") and expressions do not
    ("0", "CURRENT_TIMESTAMP").
    """

    match: Literal["exact", "suffix", "contains"] = Field(
        default="suffix", description="How `columns` is compared against the column name."
    )
    columns: list[str] = Field(
        ..., min_length=1, description="Names, suffixes or substrings to match."
    )
    default: str = Field(..., description="SQL literal or expression to use in place of NULL.")
    wrap: bool = Field(
        default=True,
        description="Wrap the column in a null-coalescing call. False leaves the "
        "column untouched and uses `default` only for the appended default row.",
    )

    def matches(self, column_name: str) -> bool:
        """Whether this rule governs ``column_name``.

        Args:
            column_name: Output column name or alias

        Returns:
            True if any configured entry matches under this rule's mode
        """
        name = column_name.lower()
        candidates = [entry.lower() for entry in self.columns]

        if self.match == "exact":
            return name in candidates
        if self.match == "suffix":
            return any(name.endswith(entry) for entry in candidates)
        return any(entry in name for entry in candidates)


class ConventionsSettings(BaseModel):
    """Opt-in naming conventions. Everything here is off until configured."""

    soft_delete: Optional[SoftDeleteConvention] = Field(
        default=None, description="Filter applied to bronze source reads."
    )
    enum_table: Optional[EnumTableConvention] = Field(
        default=None, description="Lookup table for filter-based silver dimensions."
    )
    detail_tables: Optional[DetailTableConvention] = Field(
        default=None, description="temp-to-silver promotion of staged detail tables."
    )
    null_handling: list[NullHandlingRule] = Field(
        default_factory=list,
        description="Ordered NULL-filling rules for promoted detail tables. "
        "Empty (the default) means no NULL handling is applied.",
    )
