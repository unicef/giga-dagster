# Project Architecture and Access to Production

``` @TODO: Summary of Architecture and steps to access each component ```

The `giga-dagster` project is meant as a base repository template; it should be
a basis for other projects.

`giga-dagster` is hosted on GitHub, and is available as
a [Template in Backstage]([url](https://catalog.tm8.dev/create?filters%5Bkind%5D=template&filters%5Buser%5D=all)****)

## Architecture Details

```Note: Structure of this document assumes Dev and Prod are in different Cloud Platform projects. You can reduce the sections for architecture if redundant. Just note the datasets, vms, buckets, etc. being used in Dev vs Prod ```

- Provider: Azure
- Dev Environment: https://io-dagster-dev.unitst.org
- Staging Environment: TBD
- Prod Environment: TBD
- Technology: Python / Dagster / Postgres / Spark

### Implementation Notes

``` @TODO: Note down known limitations, possible issues, or known quirks with the project. The following questions might help: ``` <br>
``` 1. Which component needs most attention? ie. Usually the first thing that needs tweaks ``` <br>
``` 2. Are the parts of the project that might break in the future? (eg. Filling up of disk space, memory issues if input data size increases, a web scraper, etc.)``` <br>
``` 3. What are some known limitations for the project? (eg. Input data schema has to remain the same, etc.)```

## Dev Architecture

``` @TODO: Dev architecture diagram and description. Please include any OAuth or similar Applications.```
``` @TODO: List out main components being used and their descriptions.```

### Virtual Machines

N/A

### Datasets

#### School Master

- Description: Existing School Geolocation Master Data
- File Location: `saunigiga` Storage Account / giga-dataops-dev / gold / school-data
- Retention Policy: Indefinite

### Tokens and Accounts

**Dev Azure DevOps Pipeline Variables**

- Location: Bitwarden - UNICEF GIGA Collection
- Person in charge: Sofia Pineda (sofia.pineda@thinkingmachin.es)
- Validity: Indefinite
- Description: This contains all the variables used in the dev deployment pipeline.
- How to Refresh:
    - N/A

## Production Architecture

TBD

## Accessing Cloud Platform Environments

**Get access to Client Azure Portal**

- Person in charge: [Kenneth Domingo](mailto:kenneth@thinkingmachin.es)
  (TM), [Brian Musisi](mailto:bmusisi@unicef.org) (Giga)

**Access Prod App in UI**

Open web browser and navigate to https://io-dagster-dev.unitst.org. Login with your
TM/UNICEF email. 
