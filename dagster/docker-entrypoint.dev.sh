#!/bin/bash

poetry install --with dev,dagster,pipelines,spark,notebook
PYDEVD_DISABLE_FILE_VALIDATION=1 exec poetry run python -m debugpy --listen 0.0.0.0:5678 -m dagster dev -h 0.0.0.0 -p 3002
