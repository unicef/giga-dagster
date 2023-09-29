# Development Lifecycle

## Trunk Based Development

![Trunk-Based Development](images/trunk-dev.png)

The giga-dagster project follows the concept of Trunk-based Development,
wherein User Stories are worked on PRs. PRs then get merged to `main` once approved by
the team.

The main branch serves as the most up-to-date version of the code base.

### Naming Conventions

**Branch Names:** Refer
to [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

**PR Title:** `[<Feature/Fix/Release/Hotfix>](<issue-id>) <Short desc>`

**PR Template:** [pull_request_template.md](../.github/pull_request_template.md)

### Development Workflow

- Branch off from `main` to ensure you get the latest code.
- Name your branch according to the Naming Conventions.
- Keep your commits self-contained and your PRs small and tailored to a specific feature
  as much as possible.
- Push your commits, open a PR and fill in the PR template.
- Request a review from 1 other developer.
- Once approved, rebase/squash your commits into `main`. Rule of thumb:
    - If the PR contains 1 or 2 commits, perform a **Rebase**.
    - If the PR contains several commits that build toward a larger feature, perform a
      **Squash**.
    - If the PR contains several commits that are relatively unrelated (e.g., an
      assortment of bug fixes), perform a **Rebase**.

## Local Development

### File Structure Walkthrough

- `authproxy/` - Contains all custom auth proxy code.
- `azure/` - Contains all configuration for Azure DevOps pipelines.
- `dagster/` - Contains all custom Dagster code.
- `docs/` - This folder contains all Markdown files for creating Backstage TechDocs.
- `infra/` - Contains all Kubernetes & Helm configuration.

### Pre-requisites

- [ ] [Docker](https://docs.docker.com/engine/)
- [ ] [Kubernetes](https://kubernetes.io/docs/tasks/tools/)
- [ ] [Helm](https://helm.sh/docs/intro/install/)
- [ ] [pyenv](https://github.com/pyenv/pyenv)
- [ ] [Poetry](https://python-poetry.org/docs/#installation)
- [ ] [Task](https://taskfile.dev/installation/#install-script)

### Cloning and Installation

1. `git clone` the repository to your workstation.
2. Run initial setup:
    ```shell
    task setup
    ```

### Environment Setup

Get contents of `.env` from Bitwarden for the following:
- Create a `dagster/.env` file.
- Create an `authproxy/.env` file.

Ensure that the Pre-requisites have already been set up and all the necessary
command-line executables are in your `PATH`.

### Running the Application

```shell
# spin up Docker containers
task

# Follow Docker logs
task logs

# List all tasks (inspect Taskfile.yml to see the actual commands being run)
task -l
```
