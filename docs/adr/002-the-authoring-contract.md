# ADR 002 — The authoring contract

**Status:** accepted
**Date:** 2026-08-29
**Phase:** 4 (Authoring Experience)

## Context

MedalFlow's pitch is "dbt, but in Python classes". A team declares medallion models,
MedalFlow works out the dependencies and compiles them into a staged plan. That only
works if there is *one* way to declare a model.

There isn't. A survey of the current surface found 35 concrete inconsistencies. The
load-bearing ones:

**The four layers disagree on everything.** Only `description` and `tags` are shared
across the four layer decorators. Bronze keys on `source_system`, silver on
`sp_name` + `group_file_name`, gold and snapshot on `schema_name`. Constructors
differ in shape: only `BronzeSequencer` takes `settings`; bronze and gold both have
a selection parameter but with different names *and different types* (`table_names:
str` comma-separated vs `selected_tables: list[str]`); silver's selection lives in
discovery instead; snapshot has no constructor surface at all.

**Two of four layers cannot be discovered.** Only silver has discovery. Gold's public
entry point builds a *bare* `GoldSequencer`, which finds only `@query_metadata`
methods on itself — a user's `@gold_metadata` class is never found, and the
orchestrator then raises on an empty operations list. `gold_package_name` exists in
settings and is referenced zero times. Bronze does not discover classes at all; it
introspects `INFORMATION_SCHEMA` against a live warehouse, so `@bronze_metadata` is
write-only — nothing reads it.

**Identity is derived from a stored-procedure naming convention.** `sp_name` is the
discovery key, the log name, the cache key, and — via
`sp_name.removeprefix("Load_")` — the reported target table. `group_file_name` names
a JSON file that is never opened; it is split on `/` and stripped of a `group_`
prefix purely to derive a model name. In the only worked example in the repo, both
fixtures pass `model_name=` explicitly, so `group_file_name` is pure ceremony
required by a non-optional field.

**Some parameters do nothing.** `silver_metadata(take_snapshot=)` is accepted and
never stored. `query_metadata(query=)` and `query_metadata(name=)` are accepted,
documented, and never read — and the docstring promises a `ValueError` that is never
raised.

**Engine selection is inert.** `_select_engine_for_operation` runs ~30 lines of
decision logic, and then `_execute_with_sql` is called unconditionally. The computed
value only reaches `result.engine_used`, a telemetry label. `"sql"`, `"auto"` and
`"spark"` all execute identically.

## Decisions

### 1. Keep the `*_metadata` decorator names

Rejected renaming to `@bronze_model` / `@silver_model` / `@gold_model`. "Model" is the
better domain noun and matches dbt, but the rename touches every model file a user
writes to buy a word. The decorator attaches metadata to a class; the existing names
say so. Churn without clarity.

### 2. One parameter set across all layers

Every layer decorator takes the same four, plus at most one layer-specific extra:

| Parameter | Meaning |
|---|---|
| `name` | the model's identity — unique within a project, used for discovery keys, logs, caches, and selectors |
| `schema` | target schema for what this model builds |
| `description=` | free text; surfaces in compile output and generated docs |
| `tags=` | free-text labels for selection |

Bronze keeps `source_system`; silver keeps `model` (its grouping concept). Everything
else goes.

`name` replaces `sp_name`. `model` replaces the `group_file_name` parse. **The
`group_file_name` parameter is deleted outright** — it names a file nothing opens.

### 3. Delete parameters that do nothing

`take_snapshot`, `query`, `name` (on `query_metadata` — distinct from the new class-level
`name`), and the `ValueError` the docstring promises. A parameter that is accepted and
discarded is worse than one that does not exist: it tells the author their intent was
recorded.

### 4. Drop `preferred_engine` from the authoring surface

It influences nothing. Deprecating only `EngineType.SPARK` — the Phase 2 debt — would
leave `"sql"` and `"auto"` equally inert while implying the parameter means something.

`EngineType.SPARK` **stays as an enum member** (removing it turns a documented input
into a hard `ValueError` at the user's import time), but the parameter disappears from
both decorators. `engine_hint` remains on operations as internal plumbing. If engine
selection becomes real, it returns as a designed feature rather than a vestige.

### 5. Uniform sequencer constructor: `(settings, selection=None)`

Every layer. `settings` is always injected, never resolved internally via
`get_settings()` — that is what forces the `__new__`-and-hand-set-attributes
gymnastics in the current tests. `selection` is always `list[str] | None`, where
`None` means everything. The comma-separated-string variant is deleted; the API layer
stops joining lists into CSV for bronze alone.

### 6. Every layer is discoverable by package walk

Silver's mechanism generalises: walk a configured package, find classes carrying the
layer's metadata attribute, respect `disabled`. Bronze and gold get the same. The
`gold_package_name` / `snapshot_package_name` settings that exist and are unused
either become live or are deleted.

**Bronze introspection becomes an explicit opt-in mode, not the only mode.** Deriving
bronze tables from a live `INFORMATION_SCHEMA` query means compile requires a
warehouse, which contradicts offline compile (Decision D6) and makes the example
project unrunnable without cloud credentials. A declared bronze model is the default;
introspection is a documented alternative for teams that want it.

### 7. One execution path: `run(selector)`

Discovery over all layers → one cross-layer DAG → apply selector → execute the
subgraph in topological stages. There is never a per-layer runner. The existing
per-layer API functions become thin wrappers over `run()` or are deleted.

Selector grammar v0.1 accepts `*` and `layer:bronze|silver|gold`. The parser also
*recognises* `+name` and `name+` and rejects them with an explicit "not yet
supported" — so v0.3 adds behaviour without changing the grammar or the call sites.

Selectors match on `name`, `layer`, and `tags`. Note `_dag_id` is unsuitable as a
selector key: it carries a positional `_{i}` suffix, so it is not stable across
selections.

### 8. `compile()` is a public step, and its errors are machine-readable first

`compile()` returns a `CompileResult`: the models discovered, the DAG, and a list of
structured errors — `{file, model, error_type, message, suggestion}`, JSON-serialisable.
`run()` refuses to execute when compile reports errors.

Human-readable text is rendered *from* the structured form, never the other way round.
This is the loop a coding agent iterates on (Phase 8), and it costs nothing to decide now.

### 9. Cut the Snapshot layer from the public API (Decision D2)

Its decorator stores `retention_days`, `frequency` and `compression`, none of which is
read by anything. It has no discovery, no API entry point, no orchestrator method, and
is never instantiated anywhere in the codebase or its tests. `Layer.SNAPSHOT` and the
`"snapshot"` schema strings stay as plumbing — they are a schema vocabulary, not the
layer implementation.

## Consequences

**This breaks every existing model file.** `sp_name` → `name`, `group_file_name`
deleted, `preferred_engine` gone. Acceptable: the package is unpublished at
`0.1.0.dev0` with no external users, and the alternative is shipping v0.1 with an
authoring contract we already know is wrong. It must be in the changelog.

**`run()` has to be built, not wired.** `ExecutionPlan` currently has no consumer
anywhere in the codebase — no runner, no executor, no stage loop. The plan→executor
seam that does exist is `get_all_operations(serialize=True)`, which stamps
`_cte_stage` / `_cte_position` onto operation dicts for an external orchestrator to
fan out. `run()` builds on that seam rather than replacing it, so the
"orchestration stays with your existing tools" promise survives.

**Gold and bronze discovery is new code, not a refactor.** Scope Phase 4 accordingly.

**The example project becomes the contract's test.** If the worked example needs a
comment explaining why a layer is different, the contract has failed.

## Alternatives rejected

**A single `@model(layer=...)` decorator.** Fewer names, but the layer-specific
parameters (`source_system` for bronze) would become optional-and-only-valid-for-one-layer,
which is a runtime validation problem in place of a type-level one. Four decorators
with a shared core is the clearer contract.

**Keeping `sp_name` as an alias for `name`.** Every alias is a second thing to
document, discover and eventually delete. There are no users to break.

**Fixing bronze introspection instead of adding declared bronze models.** Introspection
is genuinely useful for a wide landing zone, but making it the *only* option means
compile cannot run offline — which breaks the quickstart, the example project, and the
agent iteration loop.
## Amendment — 2026-08-29

The Context section above describes the tree **before** PR #28. Decisions 3, 4 and 9 are
implemented and pinned by `tests/unit/test_authoring_contract.py`; read the Context as the
historical record that motivated this ADR, not as a description of `main`.

A ground-truth pass against `dcaab21` found six claims in this ADR that are wrong about the
code. Recording them so the next reader does not act on them:

1. **"`snapshot_package_name` … exists and is unused"** (Decision 6) — no such setting ever
   existed. The dead package settings are `gold_package_name`, `dimension_package_name`,
   `silver_proc_mapping_package_name` and `silver_proc_crud_mapping_package_name`
   (`settings/main.py:300-329`) — four of them, all with zero references. Only
   `silver_package_name` is live.

2. **"Only `description` and `tags` are shared across the four layer decorators"** — there are
   three layer decorators, not four.

3. **"gold and snapshot on `schema_name`"** frames the schema problem as a *disagreement*. It
   is an *absence*: `gold_metadata` is the only layer decorator with a schema parameter at all.
   `bronze_metadata` and `silver_metadata` have none, and bronze hardcodes `schema_name="bronze"`
   at `bronze/sequencer.py:100`. The only `schema=` spellings in the codebase
   (`BronzeSequencer`, `LakeDatabase`) mean *source* schema — a different thing.

4. **"`ExecutionPlan` currently has no consumer anywhere"** — overstated.
   `ExecutionPlanBuilder.validate_plan` (`execution_plan_builder.py:85-139`) consumes a plan to
   field-check it, and is exercised by `test_execution_plan.py:121-138`. It is a validator, not
   an executor, so the conclusion holds — but it must keep working.

5. **"The plan→executor seam that does exist is `get_all_operations(serialize=True)`"** —
   that method has zero callers in the entire repo. The receiving end (`api.execute` reading
   `_cte_stage`, `api/platform.py:35`) exists, but nothing produces its input. The seam is
   *designed*, never *invoked*.

6. **"`settings` is always injected … that is what forces the `__new__` gymnastics in the
   current tests"** — half wrong, and the half that is wrong is load-bearing. `BronzeSequencer`
   already takes injected settings (`bronze/sequencer.py:42`), yet the tests still use `__new__`.
   The real cause is `LakeDatabase(settings, schema)` running in `__init__` (`:55`), which needs
   a warehouse. Decision 5 alone does not fix bronze; `lake_db` has to become lazy.

Two further findings that change the work rather than the record:

- **Decision 2's uniform `schema` lands across two PRs, not one.** `GoldMetadata.schema_name` is
  currently *write-only* — nothing reads it. Adding `schema` to bronze and silver before
  discovery consumes it would recreate precisely the parameter-that-lies problem Decision 3
  deleted. The rename lands with this contract change; the new parameters land with discovery.

- **Decision 5 fixes a live bug, not just a signature.** `_BaseSequencer.__init__` sets
  `self.sql_dialect` from `settings.compute.active_config.dialect` (`base/sequencer.py:62`), and
  `SilverTransformationSequencer.__init__` then overwrites it with the hardcoded default
  `"tsql"` — `if sql_dialect:` at `silver/sequencer.py:23` is always true. Any non-T-SQL
  configured dialect was silently discarded. Dropping the parameter restores it.

**`name` is overloaded, deliberately.** `MedalflowSettings.name` (`settings/main.py:63`) is a
required env var meaning the *data source* short name, and it derives `table_prefix` and
`ds_name`. The decorator's `name` means the *model's* identity. Different objects, no code
collision, but the word does two jobs and docs must not blur them.

**`selection` is a base concept, not a per-layer parameter.** Decision 5 fixes the *signature*;
that is not sufficient. Gold owned a private `_get_queries` override filtering methods by target
table, and silver had no equivalent — so a uniform signature would have meant `selection` doing
one thing in gold and nothing at all in silver, which is the Decision 3 shape again. The filter
belongs on `_BaseSequencer`, which also declares `selection`, so the three layers share one
implementation rather than three interpretations. Bronze is the exception on purpose: it
overrides `get_queries` outright and applies `selection` to `INFORMATION_SCHEMA` instead.

**Injection does not reach bottom yet.** Every sequencer is now handed its settings, but
`_BaseSequencer._init_feature_managers` still calls `get_configuration_service()` →
`get_internal_datalake_client()` → `get_settings()`. Constructing a sequencer therefore still
requires a resolvable global environment, which is why several tests set an offline env first.
This is a D6 leftover and needs its own change; Decision 5 does not close it.

## Amendment — Decision 6, part 1 implemented

Gold and the package configuration landed together; bronze is a separate PR.

- The five convention-derived package properties are **deleted**, including the live
  `silver_package_name` recorded above. Where a project's models live is now
  configuration: `models_package` with per-layer `bronze_package` / `silver_package` /
  `gold_package` overrides, resolved by `MedalflowSettings.package_for_layer(layer)`.
  It is optional at boot — the contract stays at four required variables — and an
  unconfigured layer raises at discovery time naming the variables to set.
- The package walk moved out of `SilverMetadataDiscovery` into `_BaseDiscovery`
  (`medallion/base/discovery.py`), parameterised by package and metadata attribute.
- `GoldMetadataDiscovery` walks the gold package and `get_gold_execution_plan`
  instantiates what it finds with `(settings, selection)`. The bare `GoldSequencer` that
  made a user's `@gold_metadata` class undiscoverable is gone.
- `gold_metadata` gained `disabled=`, and gold discovery respects it.
- `settings.is_model_configured` stays **silver-only** by design: it is backed by
  `configured_models`, silver's grouping concept, and gold models declare no `model=`.
- `layer_type` / `LayerType` now has no consumer at all. It is left in place; deleting it
  is its own change.

## Amendment — Decision 6, part 2 implemented

Bronze is declared and discovered now, so no layer derives its models from a live
warehouse by default.

- `bronze_metadata` gained `name`, `schema`, `source_schema`, `source_table` and
  `disabled`, and lost `ingestion_mode` (accepted, stored, read by nothing). It was
  write-only until this change: nothing in `src/` read `_bronze_metadata`.
- **One bronze model is one bronze table.** `name` is both the model's identity and the
  table it creates; `schema` is the target, replacing the hardcoded `"bronze"` literal
  recorded in ground-truth item 3 above. `source_table` defaults to `name` and
  `source_schema` to the sequencer's own.
- Introspection's only contribution was ever a `list[TableInfo]` — the CTAS, the
  soft-delete filter and the statistics are all generated from it. That list is extracted
  as `BronzeSequencer._source_tables()`. `BronzeSequencer` reads it off its own
  declaration; `IntrospectedBronzeSequencer` overrides it with the previous
  `INFORMATION_SCHEMA` query, moved rather than rewritten.
- Two smaller seams come with it, because a declared target can differ from its source:
  `_target_table_name(source)` and the `target_schema` property. `_create_table_op` and
  `get_queries` stay shared.
- **The mode is configuration, never inference.** `bronze_introspection` defaults to
  False. Choosing the mode by whether discovery happened to find models would make a
  mistyped `MEDALFLOW_BRONZE_PACKAGE` fall back to a warehouse silently; it raises and
  names the variable instead.
- `BronzeMetadataDiscovery` is a thin subclass of the shared walk, keyed on the declared
  `name`. `is_model_configured` stays silver-only, for the reason it is off gold.
- `create_plan_for_bronze_layer` takes a list, like silver and gold.
- Two live bugs fell out, neither reachable before because rendering bronze SQL required a
  warehouse: `_create_select_operation` passed `columns=["*"]`, which the builder's
  identifier whitelist rejects (`Invalid identifier name: *`); and
  `LakeDatabase.get_tables([])` treats an empty list as "no filter", so an empty selection
  returned every table instead of none.
- `QueryMetadata.schema_name` receives the target schema string rather than a `Layer`
  member, which only validated because `Layer` subclasses `str`.

Two silent failures in the shared walk were fixed at the same time, because
`compile()` and `run()` are built on top of it and both would have inherited them:

- `_is_model_class` asked `hasattr`, which an *inherited* attribute satisfies, so any
  subclass of a decorated model counted as a model. The `__module__` guard does not
  catch it — a subclass genuinely belongs to its own module. Where `name` comes from
  the metadata (bronze and silver) the subclass inherited the parent's name too and,
  keyed by name, **displaced the model it inherited from**: the plan lost the declared
  model and gained a class nobody declared. The attribute must now be in `cls.__dict__`.
  Re-decorating a subclass still declares a new model.
- Two models sharing a `name` collapsed into whichever the walk reached last. It raises
  now, naming the duplicated name and both classes as `module.QualName`.

This supersedes the note under Decision 5 that "Bronze is the exception on purpose: it
overrides `get_queries` outright and applies `selection` to `INFORMATION_SCHEMA` instead."
Bronze still overrides `get_queries`, but `selection` filters declared models by name in
the default mode, and `[]` means no tables in both.

## Amendment — Decision 2, the parameter set is uniform

The last two gaps are closed, together with the consumer that makes `schema` mean
something. All three layer decorators now take `name`, `schema`, `description=` and
`tags=`, plus at most one layer-specific extra.

- `gold_metadata` gained a required `name`. Gold discovery took the *class* name as the
  model's identity, so renaming a class renamed the model, and the duplicate-name guard
  fired only when two classes happened to share a class name rather than when two models
  declared the same identity. `GoldModelMetadata.name` and `GoldSequencer.get_obj_name()`
  both read the declaration now, so discovery keys, cache keys and log names agree.
- `silver_metadata` gained a required `schema`. Silver's target schema lived only on each
  `@query_metadata` method, which made it the one layer whose declaration did not say
  where the model writes.
- **A class's `schema` is the default `schema_name` for its own `@query_metadata`
  methods.** `query_metadata(schema_name="")` means the method did not say, so it writes
  where its model writes; a method that states one keeps it. Both parameters were being
  stored and read by nothing, which is the D3 shape — they land with their consumer.
- **The default is applied after `_transform_query_result`, not before.** Silver promotes
  a staged detail table by comparing `metadata.schema_name` against
  `conventions.detail_tables.source_schema`, and that comparison runs during discovery.
  Defaulting earlier would feed it an inherited schema no author wrote, so a silver model
  whose declared `schema` happened to equal the configured source schema would have every
  suffix-matching method silently promoted. The convention keys on what the *method*
  declared; inheritance fills in afterwards, and the rewrite's own target schema is
  non-empty so it survives. Pinned by
  `test_the_class_schema_is_applied_after_the_detail_rewrite`.
- A method that omits `schema_name` on a class with no declaration is unchanged: it still
  raises. Inventing a schema — the layer name, `dbo` — would be a worse answer than
  saying so.
- The three per-class `warnings.catch_warnings()` blocks around the metadata models (
  pydantic warns that a `schema` field shadows its deprecated v1 `.schema()` shim, and
  the message names the class) are one `_shadowing_schema_is_intended()` context manager.
  Its filter stays narrow on purpose: an unrelated `UserWarning` raised while a class body
  runs still reaches the author.

One live bug fell out of it. `OperationBuilder.create_operation` reported construction
failures as `f"...{query_type.value}"`, but `QueryMetadata.type` is stored as a plain
`str` (`use_enum_values=True`), so every call from a sequencer passed one and the handler
raised `AttributeError` *on itself* — swallowing the real validation error. Both messages
on that path read the label defensively now.

Still outstanding for Decision 2: nothing. Decisions 7 and 8 — `compile()`,
`CompileResult`, `run(selector)` and the selector grammar — are next.

## Amendment — Decision 8 implemented, and the selector grammar with it

`compile(selector="*")` is `medalflow.compile`. It walks all three layers' configured
packages, applies the selector, and builds one cross-layer plan through the
`ExecutionPlanOrchestrator` that already existed — there is no second DAG mechanism. It
returns a `CompileResult` carrying the models it compiled (`name`, `layer`, `schema`,
`description`, `tags`), the plan, and `errors: list[CompileError]`, with an `ok`
property. `CompileResult.to_dict()` is JSON-serialisable end to end.

**Errors are collected, not raised.** This is the whole of the decision. A
`@query_metadata` method whose own code raises, one that names no table to write, one
returning something that is not SQL — each becomes one `CompileError` and compile keeps
going, so an author fixing three models learns about all three from one run. The models
that do compile still reach the plan; a broken sibling never shrinks it to nothing.
`tests/fixtures/broken_project` is exactly that shape and
`test_every_broken_model_is_reported_in_one_run` pins it.

An unconfigured layer package is one of those errors rather than a crash. Discovery
raises `ValueError` when `package_for_layer` resolves nothing, and compile turns it into
a `CompileError` whose `suggestion` names both `MEDALFLOW_<LAYER>_PACKAGE` and
`MEDALFLOW_MODELS_PACKAGE`. A project with no gold models compiles its bronze and silver.

**Where the line is drawn.** A *project* problem is collected. The *caller's* own input
is not: an unparseable selector raises `SelectorError` before any discovery runs, because
returning an empty result would make a typo indistinguishable from a project that
declares no matching model. Bugs inside MedalFlow itself still propagate — they are not
something an author can act on.

Selector grammar v0.1 is `medalflow.api.selectors.parse_selector`: `*`,
`layer:bronze|silver|gold`, `tag:<value>`, and a bare word matching a model's declared
`name`. It matches on `name`, `layer` and `tags`, never on `_dag_id`. `+name` and `name+`
are recognised and refused by name, so v0.3 adds behaviour without changing the grammar
or a call site. A selector that parses and matches nothing is an empty plan, not an
error.

Three details that had to be decided:

- `CompiledModel.layer` is the layer the model was *discovered* in, not
  `GoldMetadata.layer`. That field is free text an author may set to `gold_ml`, and
  `layer:gold` has to keep working.
- `CompileError.to_dict()` and `CompiledModel.to_dict()` are overridden rather than
  inherited. `CTEBaseModel.to_dict` drops None fields, which would make `model` a key
  that appears only when a model happens to be named — the opposite of a machine-readable
  shape.
- Compile forces discovery to re-walk its packages. It is what an author runs after
  editing their models, so a cached walk would report the project as it used to be.

`ExecutionPlan.get_all_operations(serialize=True)` is deliberately untouched: `run()`
builds on that seam, and it still has no caller.

Two silent behaviours were found and left alone, both older than this change:

- `_walk_package` returns quietly when the configured package itself cannot be imported,
  so a mistyped `MEDALFLOW_MODELS_PACKAGE` compiles to an empty plan rather than an
  error. A missing *layer* package is now reported; a missing *root* package is not.
- `ExecutionPlan.to_dict()` calls `self.metadata.to_dict()`, but
  `create_plan_from_sequencers` passes a plain `dict` as metadata — so serialising a plan
  from the per-layer API functions raises `AttributeError`. `compile()` passes no
  metadata and is unaffected.

Still outstanding: Decision 7's `run(selector)`.
