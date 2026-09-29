# Giga DataOps Platform: Dagster

The Giga DataOps Platform is a data platform developed by Thinking Machines Data Science
in coordination with UNICEF Giga. The objective of the platform is to ingest school data
from various sources, applying concepts from master data management and data governance
in order to produce a single source of truth—the **School Master Data**—which will then
be used by consumers and downstream applications.

This repository contains the code for the **Dagster**, **Spark**, and **Hive Metastore**
services of the Platform, which handles data orchestration, data pipelines, and
distributed compute.

## Table of Contents

1. [Architecture](architecture.md)
2. [Data Flow](dataflow.md)
3. [Development](development.md)
4. [Deployment](deployment.md)
5. [Support](support.md)

## Jump to other platform services

- [Giga Sync](https://github.com/unicef/giga-data-ingestion)
- [Data Sharing](https://github.com/unicef/giga-data-sharing)
- [Datahub](https://github.com/unicef/giga-datahub)
- [Superset](https://github.com/unicef/giga-superset)
- [Trino](https://github.com/unicef/giga-trino)
- [Monitoring](https://github.com/unicef/giga-monitoring)
