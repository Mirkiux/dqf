# dqf — Data Quality Framework

A composable, pipeline-based Python library for data quality validation of analytical datasets.

Inspired by scikit-learn's transformer and pipeline architecture, `dqf` is designed for data engineers and data scientists working with enterprise data warehouses and lakes.

## Key concepts

- **Universe dataset** — defines the population. All quality metrics are measured against it, not against raw dataset sizes.
- **Variables dataset** — the dataset under analysis, always joined to the universe as a left join.
- **Composable test pipelines** — build validation logic by composing reusable test steps, exactly like sklearn pipelines.
- **TestSuiteResolver** — automatically dispatches the right test pipeline to each variable based on its metadata.
- **Multi-engine** — connects to Databricks, Oracle, PostgreSQL, Snowflake, Spark, and any SQLAlchemy-compatible engine.

## Status

Pre-alpha. Under active development. See [docs/design.md](docs/design.md) for the full architecture and implementation roadmap.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
