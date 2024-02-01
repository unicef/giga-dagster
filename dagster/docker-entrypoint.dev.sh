#!/bin/bash

poetry install --with dev,dagster,pipelines,spark,notebook
exec poetry run dagster dev -h 0.0.0.0 -p 3002
