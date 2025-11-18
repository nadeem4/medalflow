# MedalFlow Overview

MedalFlow is a metadata-driven ETL framework that lets data teams describe their entire medallion architecture declaratively. The platform loads that metadata, generates dependency-aware DAGs, and executes the resulting SQL/Spark plans across any supported compute backend with full observability.

## Core Capabilities

- **Metadata First** – Sequencers, decorators, and registries capture run-books as metadata, enabling zero-code plan generation, automated validation, and consistent governance across Bronze, Silver, Gold, and Snapshot layers.
- **Multi-Platform Execution** – Engines abstract Azure Synapse (SQL/Spark), Microsoft Fabric (Warehouse/Lakehouse), Databricks, and Snowflake so the same transformation description runs anywhere.
- **Cloud Agnostic Deployment** – The worker model is container-friendly, enabling deployments to Azure Functions, AWS Lambda, Kubernetes, or any job runner with consistent configuration through environment variables and secrets managers.
- **DAG-Based Optimization** – The execution planner resolves inter-model dependencies, builds a directed acyclic graph, and batches compatible steps into stages to eliminate redundant reads/writes and maximise parallelism.
- **Automatic Sequencing** – Metadata decorators capture ordering hints so MedalFlow stitches sequences automatically (e.g., Bronze → Silver cleansing → Gold aggregations) without hand-written orchestration.
- **OpenTelemetry Native Logging** – All operations emit correlated traces, logs, and metrics using OpenTelemetry APIs, so any collector/agent can ship data to backends like Azure Monitor, Grafana Cloud, Datadog, Splunk, or OTLP-compatible systems.

All metrics are exported via OpenTelemetry `MeterProvider`, so you can route them to any backend by configuring the OTLP/Prometheus exporter in your deployment environment.


MedalFlow scales from simple warehouses to multi-cloud, multi-engine environments while keeping the same declarative authoring model and observability story.
