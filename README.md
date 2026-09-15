# Giga DataOps Platform — Dagster

The **Giga DataOps Platform** is a data platform developed by
[Thinking Machines Data Science](https://thinkingmachin.es) in coordination with
[UNICEF Giga](https://giga.global). Its objective is to ingest school data from
various sources and — by applying master data management and data governance
principles — produce a single source of truth, the **School Master Data**, which
downstream applications consume.

This repository contains the code for the **Dagster**, **Spark**, and **Hive
Metastore** services of the platform, which together handle orchestration, data
pipelines, and distributed compute. Authentication in front of the Dagster
webserver is provided by **OAuth2 Proxy** backed by Entra ID (Azure AD).

> Full documentation lives in the [`docs/`](./docs) directory. This README is a
> quick-reference index of the repository layout and a self-contained setup
> guide.

---

## Table of Contents

1. [Architecture at a Glance](#architecture-at-a-glance)
2. [Repository Layout](#repository-layout)
   - [Top-level files](#top-level-files)
   - [Top-level directories](#top-level-directories)
   - [Inside `dagster/`](#inside-dagster)
   - [Inside `infra/`](#inside-infra)
3. [Prerequisites](#prerequisites)
4. [Setup — Step by Step](#setup--step-by-step)
5. [Running the Stack](#running-the-stack)
6. [Common Tasks](#common-tasks)
7. [Environment Variables](#environment-variables)
8. [Deployment](#deployment)
9. [Contributing](#contributing)
10. [Support](#support)
11. [Related Repositories](#related-repositories)

---

## Architecture at a Glance

The stack is composed of four containerised services plus supporting infra:

| Service           | Role                                                                     |
| ----------------- | ------------------------------------------------------------------------ |
| **Dagster**       | Orchestrates assets, jobs, sensors, and schedules; exposes a web UI.     |
| **Spark**         | Distributed compute for data transformations and Delta Lake operations.  |
| **Hive Metastore**| Table metadata catalogue used by Spark and downstream query engines.     |
| **OAuth2 Proxy**  | Sits in front of Dagster's web UI, authenticating users via Entra ID.    |
| **PostgreSQL** ×2 | One database for Dagster run metadata, one for Hive Metastore.           |
| **Azurite**       | Local emulator for Azure Blob Storage (production uses real ADLS Gen2).  |

Data flows through the **Medallion Architecture** — `raw → bronze → staging →
silver → gold` — culminating in the School Master and reference tables. See
[`docs/architecture.md`](./docs/architecture.md) and
[`docs/dataflow.md`](./docs/dataflow.md) for the full picture.

---

## Repository Layout

### Top-level files

| File                              | Purpose                                                                                          |
| --------------------------------- | ------------------------------------------------------------------------------------------------ |
| `Taskfile.yml`                    | [Task](https://taskfile.dev) runner definitions — the primary entry point for local development. |
| `docker-compose.yaml`             | Local dev orchestration (Dagster, Spark, Hive, Postgres, Azurite).                               |
| `docker-compose-network.yaml`     | Creates the shared external Docker network `giga-dataops` used across sibling repos.             |
| `.tool-versions`                  | [asdf](https://asdf-vm.com) pinned versions of Python (3.11.7) and Poetry (1.7.1).               |
| `.pre-commit-config.yaml`         | Pre-commit hooks: linting, formatting, commit-message checks.                                    |
| `.dockerignore`                   | Paths excluded when building Docker images.                                                      |
| `.gitignore`                      | Standard Git ignore rules.                                                                       |
| `.gitattributes`                  | Git text/line-ending normalisation.                                                              |
| `.mailmap`                        | Canonical author identities for `git log`.                                                       |
| `.github/pull_request_template.md`| Template applied to every new PR on GitHub.                                                      |
| `.vscode/`                        | Recommended editor settings and a debugpy launch config for Dagster.                             |

### Top-level directories

| Directory       | Purpose                                                                                                          |
| --------------- | ---------------------------------------------------------------------------------------------------------------- |
| `dagster/`      | The main application: Poetry project, Dagster definitions, pipeline assets, models, resources, scripts, tests.   |
| `spark/`        | Docker build context for the custom Spark image (adds Delta Lake and Azure Storage dependencies to Bitnami Spark).|
| `hive/`         | Docker build context and templated `hive-site.xml` / `metastore-site.xml` for the Hive Metastore image.          |
| `oauth2-proxy/` | Docker build context and configuration for the OAuth2 Proxy that fronts the Dagster webserver.                   |
| `infra/`        | Kubernetes manifests and Helm chart overrides for each service.                                                  |
| `azure/`        | Azure DevOps CI/CD pipelines and reusable templates for dev/staging/prod deployments.                            |
| `docs/`         | Long-form documentation (rendered by Backstage TechDocs).                                                        |

### Inside `dagster/`

| Path                          | Purpose                                                                                            |
| ----------------------------- | -------------------------------------------------------------------------------------------------- |
| `pyproject.toml` / `poetry.lock` | Python dependencies grouped as `dagster`, `pipelines`, `spark`, `notebook`, `dev`.              |
| `poetry.toml`                 | Poetry local config (in-project virtualenv).                                                       |
| `dagster.yaml`                | Dagster instance configuration (Postgres storage, run coordinator, run monitoring, compute logs).  |
| `workspace.yaml`              | Declares the code location loaded by the Dagster webserver.                                        |
| `Dockerfile` / `prod.Dockerfile` | Development and production images for Dagster user code.                                        |
| `docker-entrypoint.dev.sh`    | Entrypoint used by the dev container (installs deps, launches webserver with debugpy).             |
| `.env.example`                | Template for the `.env` file consumed by all Dagster containers.                                   |
| `java/FixedSASTokenProvider.java` | Custom Hadoop `SASTokenProvider` shipped with the image for wasbs:// authentication.           |
| `models/`                     | SQLAlchemy models for the ingestion Postgres DB (approvals, deletions, file uploads, users, etc.). |
| `scripts/`                    | Standalone maintenance scripts (`datahub_create_assertions.py`, `rename_files.py`).                |
| `src/`                        | Application source — see below.                                                                    |
| `tests/`                      | Pytest test suite.                                                                                 |

Inside `dagster/src/`:

| Path                    | Purpose                                                                                                          |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `definitions.py`        | The `Definitions` object Dagster loads — wires up assets, jobs, sensors, schedules, and resources.               |
| `settings.py`           | Pydantic settings loaded from environment variables.                                                             |
| `assets/`               | All asset definitions, grouped by domain (`school_geolocation/`, `school_coverage/`, `qos/`, `adhoc/`, `admin/`, `common/`, `datahub_assets/`, `giga_meter/`, `maintenance/`, `migrations/`, `school_connectivity/`, `school_list/`, `unstructured/`, `debug/`). |
| `jobs/`                 | Job definitions grouping assets for scheduled/sensor-triggered execution.                                        |
| `sensors/`              | Sensors that watch external state (ADLS blobs, schema changes) and materialise assets when it changes.           |
| `schedule/`             | Cron-based schedules.                                                                                            |
| `partitions/` / `partitions.py` | Partition definitions shared across assets.                                                              |
| `resources/`            | Dagster resources (Spark, ADLS, Postgres, e-mail sender, DataHub client, …).                                     |
| `schemas/`              | Column schemas for each dataset tier.                                                                            |
| `spark/`                | Spark session factories and Delta Lake helpers.                                                                  |
| `data_quality_checks/`  | Reusable data-quality check definitions applied at the bronze tier.                                              |
| `constants/`            | Constant values (dataset names, tiers, error codes).                                                             |
| `custom/`               | Custom dataset support code.                                                                                     |
| `exceptions/`           | Application-specific exception types.                                                                            |
| `hooks/`                | Dagster success/failure hooks (Slack notifications, cleanup, …).                                                 |
| `internal/`             | Internal utilities not intended to be imported by asset code.                                                    |
| `utils/`                | Shared helpers (`spark`, `load_module`, `sentry`, etc.).                                                         |

### Inside `infra/`

| Path                          | Purpose                                                                    |
| ----------------------------- | -------------------------------------------------------------------------- |
| `helm/dagster/`               | `values.yaml` for the upstream `dagster/dagster` Helm chart.               |
| `helm/spark/`                 | `values.yaml` for the Bitnami Spark chart.                                 |
| `helm/hive-metastore/`        | Custom Helm chart for the Hive Metastore.                                  |
| `helm/oauth2-proxy/`          | Custom Helm chart for OAuth2 Proxy.                                        |
| `k8s/namespace.yaml`          | `giga-dagster` namespace definition.                                       |
| `k8s/configmap.yaml`          | Cluster ConfigMap (ingress deployment).                                    |
| `k8s/configmap.local.yaml`    | Cluster ConfigMap (LoadBalancer deployment on Docker Desktop).             |
| `k8s/service.yaml`            | Additional Service resources.                                              |
| `k8s/dagster-prd.yaml`        | Production overrides applied on top of the Dagster Helm release.           |

---

## Prerequisites

Required for any local work:

- [Docker](https://docs.docker.com/engine/) (Docker Desktop on macOS/Windows)
- [Task](https://taskfile.dev/installation/)
- [asdf](https://asdf-vm.com/guide/getting-started.html)
- Python **3.11.7** (installed via asdf)
- [Poetry](https://python-poetry.org) **1.7.1** (installed via asdf)

Required only for Kubernetes / Helm work:

- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm](https://helm.sh/docs/intro/install/)

On **Windows**, run everything from inside WSL2 (Ubuntu). See
[`docs/development.md`](./docs/development.md#windows-subsystem-for-linux-wsl)
for the recommended `.wslconfig`.

---

## Setup — Step by Step

### 1. Install language runtimes with asdf

```shell
# from the repo root
asdf plugin add python
asdf plugin add poetry
asdf install               # reads .tool-versions
poetry config virtualenvs.in-project true
```

### 2. Clone and enter the repository

```shell
git clone https://github.com/unicef/giga-dagster.git
cd giga-dagster
```

### 3. Create `.env` files

Each service directory that ships an `.env.example` needs its own `.env`
alongside it. Copy the templates and fill in the blanks:

```shell
cp dagster/.env.example       dagster/.env
cp spark/.env.example         spark/.env
cp oauth2-proxy/.env.example  oauth2-proxy/.env
```

Values for shared/secret variables can be requested from the project team
(see [Support](#support)). At minimum, set `WAREHOUSE_USERNAME` and
`LAKEHOUSE_USERNAME` to the same value (typically your first name) so you get
your own dev warehouse and lakehouse — see
[`docs/development.md`](./docs/development.md#setting-up-your-own-warehouse-and-lakehouse).

The repo root also expects a `.env` (read by `Taskfile.yml`) — most projects
symlink or copy `dagster/.env` there.

### 4. First-time bootstrap

```shell
task setup
```

This runs three sub-tasks:

- `setup-pre-commit` — installs pre-commit hooks.
- `setup-helm-repos` — registers the Bitnami and Dagster Helm chart repos and
  updates dependencies of the local `hive-metastore` chart.
- `setup-dagster` — creates the Poetry virtualenv and installs all dependency
  groups.

---

## Running the Stack

```shell
# Build images and start every container in the background
task            # alias for `task up`

# Follow logs
task logs

# Stop containers (keeps volumes)
task stop

# Remove containers + volumes
task clean
```

Services exposed on the host:

| URL / Address                     | Service                     |
| --------------------------------- | --------------------------- |
| <http://localhost:3001>           | Dagster webserver           |
| <http://localhost:8070>           | Spark master UI             |
| <http://localhost:8071>           | Spark worker 1 UI           |
| <http://localhost:8072>           | Spark worker 2 UI           |
| <http://localhost:4040>           | Spark driver UI (per-app)   |
| `thrift://localhost:9083`         | Hive Metastore              |
| <http://localhost:10000>          | Azurite Blob                |
| <http://localhost:10001>          | Azurite Queue               |
| <http://localhost:10002>          | Azurite Table               |
| `localhost:5433`                  | Dagster Postgres            |
| `localhost:5434`                  | Hive Metastore Postgres     |
| `localhost:5678`                  | Dagster debugpy port        |

### Initialising the workspace

After the first successful start-up, the ingestion flow needs some Delta tables
to exist. Open <http://localhost:3001> and:

1. Go to **Jobs** → materialise `admin__create_lakehouse_local_job`. This creates
   your personal lakehouse folder in ADLS dev and seeds `raw/schema`.
2. Go to **Overview → Sensors** → enable `migrations__schema_sensor`. It will
   spawn a batch of runs that populate the schema tables (~7 tables at time of
   writing).
3. Verify by starting a file upload from
   [`giga-data-ingestion`](https://github.com/unicef/giga-data-ingestion); the
   first screen should no longer error out.

---

## Common Tasks

`task -l` lists every task defined in `Taskfile.yml`. The most useful ones:

| Command                          | What it does                                                                |
| -------------------------------- | --------------------------------------------------------------------------- |
| `task` / `task up`               | Build and start all containers.                                             |
| `task logs`                      | Attach to `docker compose logs --follow`.                                   |
| `task stop`                      | Stop containers.                                                            |
| `task restart`                   | Restart containers.                                                         |
| `task ps`                        | Show container status.                                                      |
| `task clean`                     | Remove containers **and** volumes.                                          |
| `task exec -- <args>`            | Run `docker compose exec` inside the compose project.                       |
| `task python -- <module.path>`   | Run a Python module inside the Dagster webserver container.                 |
| `task ipython`                   | Open an IPython shell inside the Dagster container.                         |
| `task beeline`                   | Open a Beeline SQL shell against the Hive Metastore Postgres.               |
| `task build` / `task push`       | Build and push production Docker images.                                    |
| `task helm-up`                   | Install/upgrade every Helm chart to the local K8s cluster (LoadBalancer).   |
| `task helm-upi`                  | Same but with HTTPS ingress configured.                                     |
| `task helm-down`                 | Uninstall all Helm releases.                                                |
| `task helm-clean`                | Delete every K8s resource in the `giga-dagster` namespace.                  |

### Adding a Python dependency

```shell
cd dagster
poetry add <package>     # or `poetry add --group dev <package>`
cd ..
task                     # rebuild the image with the new dep
```

---

## Environment Variables

The full list of variables (and safe defaults where possible) lives in the
`.env.example` files. High-level grouping:

- **`dagster/.env.example`** — Postgres credentials, Azure storage
  (`AZURE_SAS_TOKEN`, container/account names), Spark connection, DataHub
  metadata, e-mail rendering service, Sentry DSN, Slack webhook, per-developer
  `WAREHOUSE_USERNAME` / `LAKEHOUSE_USERNAME`, and dev-only DB connection
  strings.
- **`spark/.env.example`** — Spark image registry/repository, ingress host, and
  worker sizing (`SPARK_WORKER_CORES`, `SPARK_WORKER_MEMORY`).
- **`oauth2-proxy/.env.example`** — Cookie secret, Entra ID (`AZURE_TENANT_ID`,
  `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`), authproxy image repository, and
  GitHub OAuth credentials for alternative auth.

Values for secrets and shared endpoints can be obtained from the project team.

---

## Deployment

CI/CD is defined in [`azure/`](./azure) and executed by Azure DevOps. Deployment
happens automatically on merge:

| Branch        | Environment |
| ------------- | ----------- |
| `main`        | Dev         |
| `staging`     | Staging     |
| `production`  | Prod        |

Manual triggers and pipeline URLs are documented in
[`docs/deployment.md`](./docs/deployment.md).

---

## Contributing

This project follows **Trunk-Based Development**. Branch off `main`, follow
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for
branch names and commit messages, and open a PR using
[`.github/pull_request_template.md`](./.github/pull_request_template.md). See
[`docs/development.md`](./docs/development.md#trunk-based-development) for the
full workflow.

---

## Support

For questions and help, contact the team listed in
[`docs/support.md`](./docs/support.md).

---

## Related Repositories

Other components of the Giga DataOps Platform:

- [Giga Sync (data ingestion portal)](https://github.com/unicef/giga-data-ingestion)
- [Data Sharing](https://github.com/unicef/giga-data-sharing)
- [DataHub](https://github.com/unicef/giga-datahub)
- [Superset](https://github.com/unicef/giga-superset)
- [Trino](https://github.com/unicef/giga-trino)
- [Monitoring](https://github.com/unicef/giga-monitoring)
