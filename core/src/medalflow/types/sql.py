"""Marker type for text that is inlined into generated SQL verbatim.

`medalflow.types` has no dependency on `operations` or `query_builder`, so both
can import this without a cycle.

Generated SQL has two kinds of string field. Most carry a *value* -- a location,
a literal, an identifier -- and must be escaped or validated before emission.
A few carry SQL: a WHERE fragment, a CETAS SELECT, an exotic column type. Those
cannot be escaped without destroying them, so the only honest fix is to make
the rawness explicit in the type. `RawSQL` is that marker: whatever it wraps is
emitted exactly as written, and the caller owns the consequences.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Any, Union

from pydantic import BeforeValidator, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


@dataclass(frozen=True)
class RawSQL:
    """SQL text to inline verbatim, bypassing escaping and validation."""

    sql: str

    def __str__(self) -> str:
        return self.sql

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_plain_validator_function(
            cls._validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda value: {"sql": value.sql}
            ),
        )

    @classmethod
    def _validate(cls, value: Any) -> "RawSQL":
        """Accept a RawSQL, or the `{"sql": ...}` shape `model_dump` produces.

        A bare `str` is deliberately refused: raw SQL is always something the
        caller opted into by name, never something a plain string became.
        """
        if isinstance(value, RawSQL):
            return value
        if isinstance(value, Mapping) and set(value) == {"sql"}:
            return cls(str(value["sql"]))
        raise ValueError("RawSQL must be constructed explicitly, e.g. RawSQL('GETDATE()')")


def _check_fragment(value: Any) -> Any:
    """Refuse a bare `str` fragment that is empty or leaves a literal open.

    The quote rule is lexical, not a keyword denylist: an odd number of single
    quotes is exactly the shape that lets appended text escape the string
    literal it was meant to sit inside. Callers who mean it say `RawSQL`.

    (Emptiness is checked here rather than with `min_length`, which pydantic
    cannot apply to a `RawSQL` that has no `len()`.)
    """
    if isinstance(value, str):
        if not value.strip():
            raise ValueError("SQL fragment cannot be empty")
        if value.count("'") % 2:
            raise ValueError(
                "Unbalanced single quote in SQL fragment: "
                f"{value!r}. Wrap it in RawSQL(...) to inline it verbatim."
            )
    return value


#: A field that is inherently raw SQL. A bare `str` still works, provided it is
#: non-empty and lexically closed; `RawSQL` bypasses the check entirely.
SQLFragment = Annotated[Union[str, RawSQL], BeforeValidator(_check_fragment)]
