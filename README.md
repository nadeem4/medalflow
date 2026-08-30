# MedalFlow

**dbt, but in Python classes and methods.**

> [!WARNING]
> **Work in progress — not ready for production use.**
>
> MedalFlow is pre-release and under active development. It is **not published to PyPI**,
> the API is **not stable**, and breaking changes land regularly — this phase alone renamed
> the identity parameter on every model and deleted four public entry points.
>
> One thing in particular to weigh before adopting it: **executing a plan against a live
> warehouse is unproven.** Plan generation is well covered, but no test opens a database
> connection. See [Status](#status-work-in-progress) for what is and is not proven.

Declare your medallion models — Bronze, Silver, Gold — as plain Python classes with
decorated methods that return SQL. MedalFlow discovers them, works out the dependencies
between them by parsing that SQL, builds one cross-layer DAG, and compiles it into an
execution plan with observability built in.

```python
from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.bronze import BronzeSequencer, bronze_metadata
from medalflow.medallion.silver import SilverTransformationSequencer, silver_metadata


@bronze_metadata(name="Customers", schema="bronze", source_system="d365")
class Customers(BronzeSequencer):
    """dbo.Customers -> bronze.Customers. One model, one table, no SQL to write."""


@silver_metadata(
    name="DimCustomer",
    schema="silver",
    model="sales",
)
class DimCustomer(SilverTransformationSequencer):
    @query_metadata(type=QueryType.CREATE_TABLE, table_name="DimCustomer")
    def build_dim_customer(self) -> str:
        return "SELECT CustomerId, Name FROM bronze.Customers"
```

MedalFlow reads `bronze.Customers` out of that SQL and wires the edge itself. You never
declare dependencies by hand, and you never write orchestration code.

## Quickstart

MedalFlow is **not yet on PyPI**, so install it from a clone. Python 3.13+:

```bash
git clone https://github.com/nadeem4/medalflow.git
cd medalflow

python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install .

cd examples                    # MedalFlow reads `.env` from the working directory
cp .env.example .env
python run.py
```

```
stage 1: Customers, Orders
stage 2: DimCustomer
stage 3: FactOrders
stage 4: vw_Revenue
```

That is a real project — [`examples/`](examples/) — compiling to a real plan, with no
warehouse, no credentials and no network. The four stages were not declared anywhere:
MedalFlow read them out of the models' own `SELECT` statements. The five variables in
`examples/.env.example` are all it needs, and there is no connection string among them.

[`examples/README.md`](examples/README.md) walks through the five models and how the DAG
falls out of them. It is the same project MedalFlow's end-to-end suite compiles and runs,
so it cannot drift from the library.

## Status: work in progress

**This is a work in progress, not a finished library.** It is **not published to PyPI**
and the API is not stable. It is being brought to a `v0.1.0` release through a phased
remediation plan; expect breaking changes between now and then. The package has been
renamed from `core` to `medalflow`; update imports accordingly.

Be aware of what is and is not proven today:

| | State |
|---|---|
| Model discovery, dependency extraction, DAG building, plan generation | **Works, covered by tests** — including an end-to-end suite over [`examples/`](examples/) |
| `compile(selector)`, `run(selector)` and the v0.1 selector grammar | **Works, covered by tests** — `run()` end to end against a stubbed executor |
| SQL generation for Azure Synapse serverless | **Works, asserted against golden strings** |
| Executing a plan against a **live warehouse** | **Not verified.** No test opens a connection; treat it as unproven |
| Microsoft Fabric, Databricks, Snowflake, Spark, DuckDB | **Not built.** See [Not built yet](#not-built-yet) |
| Configuration | **Four environment variables construct settings**, six for a real deployment, plus `MEDALFLOW_MODELS_PACKAGE` to point discovery at your models. All are prefixed `MEDALFLOW_`; see [`.env.example`](.env.example) |

An earlier version of this README claimed four execution platforms and native OpenTelemetry
export. Neither was true. This document now describes only what the code does, and every
command and snippet in it was run before it was written down.

## What works today

- **Authoring by decorator.** One class decorator per layer (`@bronze_metadata`,
  `@silver_metadata`, `@gold_metadata`) plus `@query_metadata` on methods that return SQL.
  Every layer decorator takes the same `name`, `schema`, `description=` and `tags=`, plus
  at most one layer-specific extra (`source_system` for bronze, `model` for silver). A
  model's `schema` is the default target schema for its own `@query_metadata` methods, so
  a method only names one when it writes somewhere else.
- **Discovery by package walk.** Point `MEDALFLOW_MODELS_PACKAGE` at the package holding
  your models and MedalFlow imports each layer's subpackage and collects the decorated
  classes it finds. All three layers are discovered this way, so a plan compiles with no
  warehouse and no credentials. A model marked `disabled=True` is left out of the plan.
- **Bronze models are declared, and one model is one table.** `@bronze_metadata` names the
  table, its target schema, and the source table and schema it lands. Deriving the table
  list from a live `INFORMATION_SCHEMA` query instead is still available behind
  `MEDALFLOW_BRONZE_INTROSPECTION=true`, but it is opt-in and it means compiling needs a
  warehouse.
- **Automatic dependency extraction.** Model SQL is parsed with
  [sqlglot](https://github.com/tobymao/sqlglot); source and target tables become
  fully-qualified `schema.table` names.
- **One cross-layer DAG.** Edges are matched globally, so a Silver model reading
  `bronze.customers` depends on whichever operation writes it — across layers, not just
  within one. Tables with several writers (a `CREATE TABLE` and a later `INSERT`) keep an
  edge to each.
- **Staged execution plans.** The DAG is levelled into stages that can run in parallel
  within a stage, with cycle detection.
- **Fail-fast planning.** An authoring mistake — a model that raises, a module that will not
  import, metadata that cannot be read — fails the whole plan naming the culprit, rather
  than quietly shrinking it.
- **Azure Synapse serverless SQL generation.** The only engine there is; see below.

## The API

Four functions, all on `medalflow.api`.

```python
from medalflow.api import compile, run, execute, test_connection
```

**`compile(selector="*") -> CompileResult`** walks every layer the selector can reach,
narrows what it found, and builds one cross-layer plan. It is offline: bronze models are
declared, so nothing here opens a connection.

```python
result = compile("*")
result.ok           # True when nothing went wrong
result.models       # what the selector kept: name, layer, schema, description, tags
result.plan         # the staged ExecutionPlan
result.errors       # everything wrong with the project, collected
result.to_dict()    # the whole thing, JSON-serialisable
```

Errors are **collected, not raised**. An author with three broken models learns about all
three from one run, and the models that do work still reach the plan. Each is five fields,
machine-readable first — this is the loop a coding agent iterates on:

```json
{
  "file": "/acme/silver/models.py",
  "model": "NotSql",
  "error_type": "UncompilableModel",
  "message": "Failed to build queries for model 'NotSql' (NotSql): Method 'build_not_sql' in NotSql must return either:\n  1. A string containing a SQL query\n  2. None (for filter-based dimensions with auto-generation)\nInstead got: int",
  "suggestion": "Fix NotSql in the file above. Every @query_metadata method must name the table it writes, be callable with no arguments at compile time, and return a SQL string."
}
```

`error_type` is a closed vocabulary, not a Python exception name — the point is that a
caller can switch on it and a doc can list it. There are six:
`UnconfiguredPackage`, `UnimportablePackage`, `UndiscoverableLayer`, `UnreachableSource`,
`UncompilableModel`, `UnbuildablePlan`. The originating exception is diagnostic rather
than something to branch on, so it goes to the DEBUG log with its traceback and stays out
of the field consumers read.

The one thing `compile()` *does* raise on is an unparseable selector, because that is the
caller's mistake rather than the project's: returning an empty result would make a typo
indistinguishable from a project that declares no matching model.

**`run(selector="*") -> RunResult`** compiles, then executes the plan it produced against
the configured warehouse. Stages run in topological order; a failure stops the run, and
everything the run did not reach is reported as skipped rather than silently dropped.

```python
result = run("*")
result.ok               # compiled cleanly and everything planned then ran
result.succeeded        # every operation that completed, in execution order
result.failed           # the operation the run stopped on, or None
result.skipped          # what the plan still held when it stopped
result.compile_result   # the compile it began with, carried whole
```

A project that does not compile is not executed at all, and the compile errors come back in
the `RunResult` rather than as an exception: "what happened" gets one answer in one shape
whether the project was broken or the warehouse was.

**`execute(operation)`** runs one serialized operation, and **`test_connection()`** reports
per-engine connectivity. They are the seam underneath `run()`, and they are what you reach
for when your own orchestrator wants to fan the stages out itself.

### Selectors

Both `compile()` and `run()` take a selector. The v0.1 grammar is four forms:

| Selector | Selects |
|---|---|
| `*` | every model |
| `layer:bronze` `layer:silver` `layer:gold` | one medallion layer |
| `tag:daily` | every model carrying that tag, in any layer |
| `DimCustomer` | one model, by the `name` its decorator declares |

`layer:` is the one form that can be answered before anything is discovered, so it is the
one form that prunes the walk rather than only filtering its results.

`+name` and `name+` — a model and everything upstream or downstream of it — are recognised
and **refused by name**, not silently ignored. They arrive in v0.3.

## Architecture

MedalFlow is designed to be cloud-agnostic through exactly **three seams**:

| Seam | Responsibility | Today |
|---|---|---|
| Compute platform | SQL generation and execution | Azure Synapse serverless |
| Storage client | Reading and writing lake files | ADLS Gen2 |
| Secret provider | Resolving credentials | Azure Key Vault, or a mock in test mode |

Everything outside those seams is cloud-neutral, and that boundary is **enforced**. The
Azure SDKs, `pyodbc`, `pandas` and the `abfs://` plumbing live behind an optional extra:

```bash
pip install .            # planning, DAG building, SQL analysis — no cloud SDK
pip install '.[azure]'   # adds Synapse execution, ADLS Gen2 and Key Vault
```

A `bare-install` CI job installs the package with no extras, imports the public surface and
fails if any `azure`, `pyodbc`, `adlfs`, `pyarrow` or `pandas` import has leaked back into
core. Reaching a cloud path without the extra installed raises a MedalFlow error naming the
extra that provides the missing module, not a bare `ModuleNotFoundError` three frames down.

Deployment is runtime-agnostic by design: a plain Python process or container configured by
environment variables. There is no built-in scheduler, no hosted service, and no UI —
orchestration stays with whatever you already run (Airflow, ADF, Azure Functions, a
Kubernetes CronJob, cron), exactly like dbt.

## Observability

MedalFlow is instrumented with the **OpenTelemetry API only**. It emits spans and metrics
through `opentelemetry-api` and deliberately does **not** depend on the SDK or any exporter
— `opentelemetry-api` is the single OpenTelemetry entry in `pyproject.toml`, and there is
no `opentelemetry-sdk` in the lock file either.

That means **the host application wires up the SDK, the exporter and the collector.** If you
do not configure a `TracerProvider` and `MeterProvider`, the instrumentation is a no-op and
nothing is exported. This is a deliberate choice so the library does not dictate your
telemetry stack — but it does mean MedalFlow does not ship telemetry to a backend on its own.

Structured logging goes through the standard library `logging` module with context fields
attached via `extra`.

## Not built yet

Everything in this section is **absent from the code**, not merely undocumented. There is
exactly one compute platform — `ComputeType` has a single member, `SYNAPSE` — and exactly
one query builder, `SynapseServerlessQueryBuilder`. Asking for any other platform raises
`ValueError: Unsupported compute type`. Nothing here is pluggable today; the seam exists,
but nothing has been plugged into it.

| Not built | Notes |
|---|---|
| **DuckDB** | First adapter candidate: a local, zero-cloud backend for development and CI. It is first precisely because it would let the suite prove execution without a warehouse |
| **Microsoft Fabric** | Code existed, had never run, and was deleted in Phase 2 (`c3302f0`) |
| **Apache Spark** | Same, deleted in Phase 2 (`2d7848a`): removed rather than left standing as a claim |
| **Databricks** | Never existed |
| **Snowflake** | Never existed |

Each returns only when it is built test-first, with its own golden-SQL suite, as an
optional extra: `medalflow[duckdb]`, then `[fabric]`, `[databricks]`, `[snowflake]`.

## Roadmap

The path to a first public release, and beyond:

- **v0.1.0** — the library: correct, tested, documented, installable from PyPI
- **v0.2** — a dbt-style CLI: `init`, `compile`, `run`, `ls`, every command with `--json`.
  The API above is already shaped for it: `CompileResult.to_dict()` and
  `RunResult.to_dict()` are the payloads `--json` will print
- **v0.2.x** — an MCP server, so coding agents can author models and use the compile step as
  their verification loop
- **v0.3** — node selection and state: `+gold.revenue` for upstream closures, `model+` for
  downstream rebuilds, `--retry-failed`. The grammar already refuses these by name, so v0.3
  adds behaviour without changing a call site
- **v0.4+** — the adapters in [Not built yet](#not-built-yet), each as an optional extra

## Development

Requires Python 3.13+ and [Poetry](https://python-poetry.org/).

```bash
poetry install --with dev
poetry run pytest
```

The suite runs entirely offline — no warehouse, no network, no cloud credentials. CI runs
five blocking jobs on every pull request: `lint` (ruff and black over `src`, `tests` and
`examples`), `test`, `import-smoke`, `bare-install`, and `example` — which compiles
[`examples/`](examples/) from a clean checkout with no extras and diffs the output against
the four stages its README documents.

`examples/` is the project the end-to-end suite in `tests/e2e/` runs against. There is no
second copy of it: the example a reader is invited to copy is the one CI compiles, so it
can only rot by turning the build red. The deliberately broken projects under
`tests/fixtures/` — a model that raises, two models sharing a name, a dependency cycle —
are what the error paths are tested against.

## License

MIT. See [LICENSE](LICENSE).
