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

Development works best when working on Linux, WSL, or macOS. Please do not try
developing directly on Windows, you will only hurt yourself. You can still follow the
Windows-specific instructions in the docs for the tools below, but bear in mind that
you may get weird errors along the way.

- [ ] [Docker](https://docs.docker.com/engine/)
- [ ] [Kubernetes](https://kubernetes.io/docs/tasks/tools/)
- [ ] [Helm](https://helm.sh/docs/intro/install/)
- [ ] [pyenv](https://github.com/pyenv/pyenv)
- [ ] [Poetry](https://python-poetry.org/docs/#installation)
- [ ] [Task](https://taskfile.dev/installation/#install-script)

#### Windows Subsystem for Linux (WSL)

_Skip this step if you are on Linux or Mac._

1. Open the Company Portal and search for WSL.
2. Install WSL. You may be prompted to restart your device.
3. In a separate Powershell/CMD terminal, run:
    ```shell
    wsl --set-default-version 2
    ```
4. Open the Microsoft Store and search for Ubuntu.
5. Install Ubuntu.
6. In the Powershell/CMD terminal, run:
    ```shell
    wsl --set-default Ubuntu
    ```
7. In the start menu, Ubuntu should show up in the recently added programs. Open it.
8. You will be prompted for a new username and password. Enter any credentials and make
   sure to remember them. You may be prompted to restart again.
9. If you are not prompted to restart, close Ubuntu and open it again. You should now
   have a working WSL installation.

**From this point on, all commands should be run inside the Ubuntu terminal.**

#### Docker

1. Open the file explorer and type the following in the address bar:
   ```shell
   %USERPROFILE%
   ```
2. Create a new file named `.wslconfig` and add the following contents:
   ```text
   [wsl2]
   memory=8GB
   swap=20GB
   ```
3. Open the Company Portal and search for Docker Desktop.
4. Install Docker Desktop. You may be prompted to restart your device.
5. When running Docker Desktop for the first time, you may encounter an error
   like [this](https://wiki.tm8.dev/doc/docker-desktop-access-denied-qxSv4bVrWI).
6. File
   a [Jira ticket](https://thinkdatasci.atlassian.net/servicedesk/customer/portal/45/group/124/create/10261)
   requesting to be added to the `docker-users` group.
7. After you have been added to the group, you should now be able to run Docker Desktop.
8. Open the Docker Desktop app and go to settings.
9. Ensure you have the following settings:
   ![Docker - General](images/docker-general.png)
   ![Docker - Resources](images/docker-resources.png)
   ![Docker - Kubernetes](images/docker-kubernetes.png)
10. Wait for the Kubernetes installation to complete.
11. Inside an Ubuntu terminal, run:
    ```shell
    docker image ls -a
    kubectl get all
    ```
    If you get no errors, you're good to go!

#### Kubernetes

Kubernetes is installed as part of the Docker Desktop installation. You can optionally
install the `kubectx` and `kubens` plugins to make it easier to switch between
contexts/namespaces.

Install [Krew](https://krew.sigs.k8s.io/docs/user-guide/setup/install/):

1. Run the following:
   ```shell
   (
    set -x; cd "$(mktemp -d)" &&
    OS="$(uname | tr '[:upper:]' '[:lower:]')" &&
    ARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/\(arm\)\(64\)\?.*/\1\2/' -e 's/aarch64$/arm64/')" &&
    KREW="krew-${OS}_${ARCH}" &&
    curl -fsSLO "https://github.com/kubernetes-sigs/krew/releases/latest/download/${KREW}.tar.gz" &&
    tar zxvf "${KREW}.tar.gz" &&
    ./"${KREW}" install krew
   )
   ```
2. Add the Krew path to your system `PATH` by appending to your `.bashrc`
   or `.zshrc` (aka run the following):
   ```shell
   echo 'export PATH="${KREW_ROOT:-$HOME/.krew}/bin:$PATH"' >> ~/.bashrc
   ```
3. Load your new shell config:
    ```shell
    source ~/.bashrc
    ```
4. Download the Krew plugin list
    ```shell
    kubectl krew update
    ```
5. Install `kubectx` and `kubens`
    ```shell
    kubectl krew install ctx
    kubectl krew install ns
    ```
6. Test installation:
    ```shell
    kubectl ctx
    kubectl ns
    ```

#### pyenv

1. Install Python build dependencies:
    - **MacOS**
      ```shell
      brew install openssl readline sqlite3 xz zlib tcl-tk
      ```
    - **Linux/WSL**
      ```shell
       sudo apt update
       sudo apt install -y build-essential libssl-dev zlib1g-dev \
        libbz2-dev libreadline-dev libsqlite3-dev curl \
        libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
      ```
2. Install actual `pyenv`:
    ```shell
    curl https://pyenv.run | bash
    ```
3. Add `pyenv` paths to your `.bashrc`/`.zshrc`:
    ```shell
    echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
    echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
    echo 'eval "$(pyenv init -)"' >> ~/.bashrc
    ```
4. Reload shell config:
    ```shell
    source ~/.bashrc
    ```
5. Install Python 3.11:
    ```shell
    pyenv install 3.11
    ```
6. (Optional) Make Python 3.11 your default version:
    ```shell
    pyenv global 3.11
    ```
7. Test installation:
    ```shell
    python -VV
    ```

#### Poetry

1. Install Poetry
    ```shell
    curl -sSL https://install.python-poetry.org | python -
    ```
2. Add Poetry path to your `.bashrc`/`.zshrc`:
    ```shell
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
    ```
3. Reload shell config:
    ```shell
    source ~/.bashrc
    ```
4. Test installation:
    ```shell
    poetry --version
    ```

#### Task

1. Install Task:
    ```shell
    sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b ~/.local/bin
    ```
2. Test installation:
    ```shell
    task --version
    ```

### Cloning and Installation

1. `git clone` the repository to your workstation.
2. Run initial setup:
    ```shell
    task setup
    ```
   
### Environment Setup

`task setup` will copy the contents of `dagster/.env.example`
and `authproxy/.env.example` to new `.env` files in their respective directories.
Get the contents of the `.env` files from the following Bitwarden entries:

- Dagster .env
- Dagster authproxy .env

Ensure that the Pre-requisites have already been set up and all the necessary
command-line executables are in your `PATH`. You can check this by running each of
these:

```shell
docker info
kubectl get all
helm version
pyenv versions
poetry --version
task --version
```

If you don't get any errors, then you're all set!

### Running the Application

```shell
# spin up Docker containers
task

# Follow Docker logs
task logs

# List all tasks (inspect Taskfile.yml to see the actual commands being run)
task -l
```

### Adding dependencies

Example: Adding `dagster-azure`

```shell
# Move to relevant folder
cd dagster

# Add the dependency using poetry
poetry add dagster-azure

# Re-run task
task
```
