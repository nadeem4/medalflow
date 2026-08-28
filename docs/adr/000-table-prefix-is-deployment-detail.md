# ADR 000: The table prefix is a deployment detail, not part of a model's name

- Status: Accepted
- Date: 2026-08-28
- Phase: 3.5 (cross-layer DAG correctness)

## Context

MedalFlow prefixes every physical table it creates with `settings.table_prefix`
(`f"{name}_"`, e.g. `fin_`), so one warehouse can host several data sources
without collisions. `BaseQueryBuilder.fully_qualified_name` applies it, so the
SQL that reaches the warehouse says `[silver].[fin_DimCustomer]`.

A model author never sees that. They write ordinary SQL against logical names:

```python
def build_fact_orders(self) -> str:
    return "SELECT ... FROM silver.DimCustomer c ..."
```

The dependency analyzer matches what one operation writes against what another
reads. Those two sides came from different places — one from the query builder
(prefixed), one from the author's SQL (unprefixed) — so `silver.fin_dimcustomer`
was compared against `silver.dimcustomer` and never matched. Combined with the
`recreate` guard hiding write targets entirely, the DAG had never formed a
single cross-layer edge.

## Decision

**The analyzer normalizes both sides to unprefixed logical names before
matching.** Names produced by `SQLDependencyAnalyzer` — both `reads_from` and
`writes_to` — are logical: lowercase `[database.]schema.table` with the
deployment prefix removed.

The prefix is a property of *where the model is deployed*, not of *what the
model is*. The same model deployed under a different `name` is the same node in
the DAG.

## Consequences

- Authoring stays prefix-free. A model's SQL is portable across deployments.
- The DAG is keyed on logical names, so it is stable across data sources.
- Stripping must mirror `fully_qualified_name`, not be blind — see below.

### The `skip_prefix_on_schema` wrinkle

`compute.skip_prefix_on_schema` (default `["dbo", "gold", "snapshot"]`) makes
the prefix *conditional per schema*: nothing in `gold` is prefixed at all. So
the analyzer cannot simply drop a leading `fin_`. It consults the same setting
the query builder does and only strips where the builder would have added
(`SQLDependencyAnalyzer._is_prefixed_schema`). A real `gold` table literally
named `fin_Revenue` therefore keeps its name.

An unqualified reference (`FROM Customers`) has no schema to judge by and is
left untouched.

## Rejected: make authors write the prefix

Authors would write `FROM silver.fin_DimCustomer`, and the two sides would match
without any normalization. Rejected because:

- It puts a deployment concern in source code. The prefix comes from
  `MEDALFLOW_NAME`; hard-coding it makes a model unusable under another name and
  turns a config change into a code change across every model.
- It is unenforceable and silent. Forget the prefix and nothing errors — the
  edge merely fails to form, which is exactly the failure this phase fixes.
- `skip_prefix_on_schema` would have to be reasoned about by hand, per schema,
  in every query.
