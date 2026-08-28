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
        # Validation accepts only RawSQL instances: a bare `str` must never be
        # silently promoted to raw SQL. Python-mode `model_dump` leaves the
        # instance intact so operations round-trip through `to_dict`; JSON mode
        # falls back to the wrapped text.
        return core_schema.is_instance_schema(
            cls,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda value: value.sql, when_used="json"
            ),
        )


def _reject_unbalanced_quotes(value: Any) -> Any:
    """Refuse a bare `str` fragment that leaves a string literal open.

    This is a lexical check, not a keyword denylist: an odd number of single
    quotes is exactly the shape that lets appended text escape the literal it
    was meant to sit inside. Callers who mean it say `RawSQL`.
    """
    if isinstance(value, str) and value.count("'") % 2:
        raise ValueError(
            "Unbalanced single quote in SQL fragment: "
            f"{value!r}. Wrap it in RawSQL(...) to inline it verbatim."
        )
    return value


#: A field that is inherently raw SQL. A bare `str` still works, provided it is
#: lexically closed; `RawSQL` bypasses the check entirely.
SQLFragment = Annotated[Union[str, RawSQL], BeforeValidator(_reject_unbalanced_quotes)]
