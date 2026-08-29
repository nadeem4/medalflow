# MedalFlow

**dbt, but in Python classes and methods.**

Declare your medallion models — Bronze, Silver, Gold — as plain Python classes with
decorated methods that return SQL. MedalFlow discovers them, works out the dependencies
between them by parsing that SQL, builds one cross-layer DAG, and compiles it into an
execution plan with observability built in.

```python
from medalflow.constants.sql import QueryType
from medalflow.medallion.base.decorators import query_metadata
from medalflow.medallion.silver import SilverTransformationSequencer, silver_metadata


@silver_metadata(
    name="usp_load_dim_customer",
    model="sales",
)
class DimCustomer(SilverTransformationSequencer):
    @query_metadata(
        type=QueryType.CREATE_TABLE,
        table_name="DimCustomer",
        schema_name="silver",
    )
    def build_dim_customer(self) -> str:
        return "SELECT CustomerId, Name FROM bronze.Customers"
```

MedalFlow reads `bronze.Customers` out of that SQL and wires the edge itself. You never
declare dependencies by hand, and you never write orchestration code.

## Status: pre-release, under active repair

This project is **not yet published to PyPI** and the API is not stable. It is being brought
to a `v0.1.0` release through a phased remediation plan; expect breaking changes. The
package has been renamed from `core` to `medalflow`; update imports accordingly.

Be aware of what is and is not proven today:

| | State |
|---|---|
| Model discovery, dependency extraction, DAG building, plan generation | **Works, covered by tests** — including an end-to-end suite over a sample project |
| SQL generation for Azure Synapse serverless | **Works, asserted against golden strings** |
| Executing a plan against a live warehouse | **Not verified.** No test exercises it; treat it as unproven |
| Microsoft Fabric, Databricks, Snowflake, Spark | **Not supported.** See the roadmap |
| Configuration | **Four environment variables construct settings**, six for a real deployment. All are prefixed `MEDALFLOW_`; see [`.env.example`](.env.example) |

An earlier version of this README claimed four execution platforms and native OpenTelemetry
export. Neither was true. This document now describes only what the code does.

## What works today

- **Authoring by decorator.** One class decorator per layer (`@bronze_metadata`,
  `@silver_metadata`, `@gold_metadata`) plus `@query_metadata` on methods that return SQL.
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
- **Azure Synapse serverless SQL generation.**

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
pip install medalflow            # planning, DAG building, SQL analysis — no cloud SDK
pip install 'medalflow[azure]'   # adds Synapse execution, ADLS Gen2 and Key Vault
```

A `bare-install` CI job installs the package with no extras, imports the public surface and
fails if any `azure`, `pyodbc`, `adlfs`, `pyarrow` or `pandas` import has leaked back into
core. Reaching a cloud path without the extra installed raises a MedalFlow error naming the
install command, not a bare `ModuleNotFoundError`.

Deployment is runtime-agnostic by design: a plain Python process or container configured by
environment variables. There is no built-in scheduler, no hosted service, and no UI —
orchestration stays with whatever you already run (Airflow, ADF, Azure Functions, a
Kubernetes CronJob, cron), exactly like dbt.

## Observability

MedalFlow is instrumented with the **OpenTelemetry API only**. It emits spans and metrics
through `opentelemetry-api` and deliberately does **not** depend on the SDK or any exporter.

That means **the host application wires up the SDK, the exporter and the collector.** If you
do not configure a `TracerProvider` and `MeterProvider`, the instrumentation is a no-op and
nothing is exported. This is a deliberate choice so the library does not dictate your
telemetry stack — but it does mean MedalFlow does not ship telemetry to a backend on its own.

Structured logging goes through the standard library `logging` module with context fields
attached via `extra`.

## Roadmap

The path to a first public release, and beyond:

- **v0.1.0** — the library: correct, tested, documented, installable from PyPI
- **v0.2** — a dbt-style CLI: `init`, `compile`, `run`, `ls`, every command with `--json`
- **v0.2.x** — an MCP server, so coding agents can author models and use the compile step as
  their verification loop
- **v0.3** — node selection and state: `--select +gold.revenue` for upstream closures,
  `model+` for downstream rebuilds, `--retry-failed`
- **v0.4+** — adapters as optional extras, each built test-first with its own golden-SQL
  suite. First candidate is **`medalflow[duckdb]`**, a local zero-cloud backend for
  development and CI; then `[fabric]`, `[databricks]`, `[snowflake]`

Fabric, Databricks, Snowflake and Spark appear here rather than in the feature list on
purpose. Fabric and Spark code did exist, had never run, and is being removed; it returns
only when it is built test-first against a real dialect.

## Development

Requires Python 3.13+ and [Poetry](https://python-poetry.org/).

```bash
poetry install --with dev
poetry run pytest
```

The suite runs entirely offline — no warehouse, no network, no cloud credentials. CI runs
lint, the test suite on Python 3.13, and an import smoke test on every pull request.

`tests/fixtures/sample_project/` is a miniature MedalFlow project — one bronze table,
two Silver models (one reading the other) and a Gold model — used by the end-to-end suite in
`tests/e2e/`. It is the clearest worked example of the authoring model until a proper
`examples/` project lands.

## License

MIT. See [LICENSE](LICENSE).
