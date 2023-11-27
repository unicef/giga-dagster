# Deployment Procedure

## Dev Deployment

### Pre-requisites

- Access to the Bitwarden - UNICEF GIGA collection
- Access to client Azure DevOps

### How-to-Guide

The CI/CD pipeline has been configured with GitHub and Azure DevOps. To deploy to the
dev environment, simply create a PR and merge to the `main` branch. If you have admin
permissions, you can also push directly to `main`.

To manually trigger a deployment, go to
the [Pipelines](https://unicef.visualstudio.com/OI-GIGA/_build) page and trigger
the `unicef.giga-dagster` pipeline.

## Staging Deployment

TBD

## Production Deployment

TBD
