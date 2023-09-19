# Development Lifecycle

## Trunk Based Development

![Trunk Based Development](https://trunkbaseddevelopment.com/trunk1b.png)

The giga-dagster project follows the concept of Trunk-based Development,
wherein User Stories are worked on PRs. PRs then get merged to `main` once approved by
the team.

The main branch serves as the most up-to-date version of the code base.

### Naming Format

**Branch Names:**

Refer to [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

**PR Title:** `[<Feature/Fix/Release/Hotfix>](<issue-id>) <Short desc>`

**PR Template:** [pull_request_template.md](../.github/pull_request_template.md)

### Development Workflow

- Branch off from `main` to ensure you get the latest code.
- Name your branch according to the Naming Conventions.
- Keep your commits self-contained and your PRs small and tailored to a specific feature
  as much as possible.
- Push your commits, open a PR and fill in the PR template.
- Request a review from 1 other developer.
- Once approved, rebase/squash your commits into `main`.

## Local Development

### Install Prerequisites

- [ ] [Docker](https://docs.docker.com/engine/)
- [ ] [Kubernetes](https://kubernetes.io/docs/tasks/tools/)
- [ ] [Helm](https://helm.sh/docs/intro/install/)
- [ ] [pyenv](https://github.com/pyenv/pyenv)
- [ ] [Poetry](https://python-poetry.org/docs/#installation)
- [ ] [Task](https://taskfile.dev/installation/#install-script)

### Install Python & dependencies

```shell
pyenv install 3.11
poetry install
```

### Install pre-commit

```shell
pip install pre-commit
pre-commit install
```

### File Structure Walkthrough

- `docs/` - This folder contains all Markdown files for creating Backstage TechDocs.

### Pre-requisites

``` @TODO: Fill with pre-reqs such as access to Cloud Platform, Bitwarden Collection, Github etc ```

### Cloning and Installation

``` @TODO: Fill with set-up/installation guide. Feel free to subdivide to sections or multiple MD files through mkdocs.yml ```

### Environment Setup

``` @TODO: Fill with instructions for exporting local env variables. Distinguish variables being used in local vs dev vs prod ```

### Running the Application

``` @TODO: Fill with steps on running the app locally. Feel free to subdivide to sections or multiple MD files through mkdocs.yml ```
