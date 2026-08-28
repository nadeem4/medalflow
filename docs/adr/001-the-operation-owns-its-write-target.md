# ADR 001: An operation's write target comes from the operation, not from its SQL

- Status: Accepted
- Date: 2026-08-28
- Phase: 3.5 (cross-layer DAG correctness)

## Context

`SQLDependencyAnalyzer.analyze_operations` used to derive both halves of a
dependency — what an operation reads and what it writes — by rendering the
operation with the query builder and parsing the SQL back out again.

Re-deriving the write target that way is a round trip through a parser for
information the operation already holds. `CreateTable(schema_name="bronze",
object_name="Customers")` knows exactly what it produces before any SQL exists.

The round trip broke in CI. `CreateTable.recreate` defaults to True, so the
Synapse builder wraps the CETAS in a guard:

```sql
IF EXISTS (SELECT * FROM sys.external_tables WHERE object_id = OBJECT_ID('[bronze].[fin_Customers]'))
    DROP EXTERNAL TABLE [bronze].[fin_Customers];
CREATE EXTERNAL TABLE ... AS SELECT ...
```

sqlglot 30 degrades that guard to a `Command`. sqlglot 23.17.0 — the version
the lock resolves and CI installs — raises
`ParseError: Required keyword: 'true' missing for <class 'sqlglot.expressions.If'>`
instead, and `sqlglot.parse` is all-or-nothing, so the CETAS after the guard
was lost with it. Every `CREATE TABLE` in the plan lost its sources, the DAG
collapsed from four stages to two, and the same suite passed locally and failed
in CI purely on a patch-level dependency difference.

## Decision

**The operation is authoritative for `writes_to`; SQL is authoritative for
`reads_from`; the two are computed independently.**

- `_declared_target` maps an operation's `schema_name`/`object_name` to a
  logical name through the same `_qualified_name` helper the parsed side uses,
  so both sides stay on one convention (ADR 000).
- `_WRITER_QUERY_TYPES` names, explicitly, which operations write:
  `CREATE_TABLE` and `CREATE_OR_ALTER_VIEW` produce the object; `INSERT`,
  `UPDATE`, `DELETE`, `MERGE` and `COPY` change the rows in it. `DROP_*` is
  excluded for the same reason `exp.Drop` is not a write expression — a DROP
  produces no rows, so naming its target invents an edge. `CREATE_SCHEMA`'s
  `object_name` is a schema, not a table. Statistics operations attach
  optimiser metadata to a table that already exists. `EXECUTE_SQL` carries
  arbitrary SQL, so it is the one operation that does not know its own target
  and still falls back to the parsed one.
- A parse failure therefore costs the reads, never the target.

Two supporting changes keep the reads as well:

- `_parse_statements` parses one statement at a time, so an unreadable guard
  costs only itself and not the payload that follows it. A statement is used
  whole or not at all — nothing is salvaged from a partial parse, so no
  half-built tree can invent a dependency.
- If **no** statement could be read, `extract_dependencies` raises. Returning
  an empty result would be indistinguishable from a real answer of "this SQL
  has no dependencies".

## Consequences

- A sqlglot upgrade, a dialect quirk or a new DDL guard can no longer erase an
  operation from the DAG. At worst it loses that operation's incoming edges.
- Losing edges is loud. `analyze_operations` logs at ERROR with the operation's
  type, schema, object, the SQL, and the fact that edges will be missing.
- It does not raise. A plan is still buildable without an edge the analyzer
  could not see, the operation keeps its declared target so it is still a
  producer, and the DAG builder still rejects cycles. Raising would turn a
  parser limitation on framework-generated guard DDL — sqlglot 23 cannot read
  the `CREATE SCHEMA` guard at all — into a hard planning failure for a correct
  project.
- Tests may no longer assert a write target through the analyzer's SQL parsing
  as a proxy for the DAG. `writes_to` is still never hand-written in a fixture;
  it now comes from the operation under test.

## Rejected: parse leniently instead

`sqlglot.parse(..., error_level=ErrorLevel.IGNORE)` also recovers the CETAS on
sqlglot 23. Rejected because it returns a mangled tree for the statement that
failed — the guard comes back as an `If` with its `DROP` body silently
discarded — and a half-parsed statement is exactly the kind of thing that
reports a dependency that is not there. Whole statements or nothing.
