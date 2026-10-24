# ADR-002: Data Warehouse — Amazon Redshift over Snowflake/BigQuery

**Date**: 2024-10-24
**Status**: Accepted
**Deciders**: UrbanPulse Data Engineering Team

---

## Context

We need a cloud data warehouse for storing the transformed star schema and serving analytical queries from Metabase dashboards and dbt models.

## Decision

Use **Amazon Redshift** as the data warehouse.

## Consequences

### Positive
- **Native AWS integration** — `COPY FROM S3` is extremely fast (parallelised across all compute nodes), eliminating the need for an external loading tool.
- **Columnar MPP** — Massively Parallel Processing across multiple nodes scales analytical queries linearly.
- **dbt native** — dbt's Redshift adapter supports all Redshift-specific features (distribution keys, sort keys, late binding views).
- **Predictable cost** — Reserved instances offer up to 70% savings vs on-demand; single `dc2.large` node costs ~$0.25/hr.
- **Ecosystem** — Metabase, QuickSight, Tableau all have first-class Redshift connectors.

### Negative
- **Always-on cost** — unlike Snowflake/BigQuery, Redshift charges even when idle. Mitigated by pausing the cluster outside working hours using a Terraform scheduled action.
- **Scaling is manual** — adding nodes requires a cluster resize operation (vs Snowflake's elastic scaling).

## Alternatives Considered

| Warehouse | Rejected Because |
|---|---|
| Snowflake | Higher per-credit cost, requires separate SaaS account |
| BigQuery | Not native to AWS; S3 → GCS transfer adds latency and egress cost |
| Athena | Query-only (no storage); poor performance for repeated dashboard queries |
| PostgreSQL | Not MPP; doesn't scale for analytical workloads over 100M rows |
