#!/bin/bash

poetry install --with dev,dagster,pipelines,spark,notebook
# Use dagster-webserver instead of dagster dev to avoid daemon conflicts
# (dagster-daemon container runs daemons separately)
PYDEVD_DISABLE_FILE_VALIDATION=1 exec poetry run python -m debugpy --listen 0.0.0.0:5678 -m dagster_webserver -h 0.0.0.0 -p 3002
