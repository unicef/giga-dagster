# Data Flow

The pipeline design is influenced by the data tiers that we establish, inspired by the
Medallion Architecture:

![](./images/medallion.png)

1. All new data coming from the Ingestion Portal starts out as `raw`, which may have:
    - Columns with the wrong data types.
    - Column names that do not match what is in `gold`. Each raw file will have an
      associated column mapping which the uploader should have filled up in Giga Sync,
      which maps the columns into the correct names.
    - Missing columns which are in `gold`, but are nullable.
    - Extra columns which are not in `gold` .
2. At the `bronze` tier:
    - Column mapping is applied.
    - Missing, nullable columns are added.
    - Extra columns are dropped.
    - Data quality checks are performed.
    - The output is split into 2 tables according to rows that `passed` or `failed` the
      data quality checks.
    - In the `passed` table, columns are cast into the correct data types.
    - The data quality report is generated and emailed to the uploader.
3. At the `staging` tier, the passed table from the previous tier undergoes a
   human-in-the-loop review process, where users with the appropriate permissions can
   approve or deny individual rows from proceeding to the next tier. This process takes
   place in Giga Sync. The output is again split into 2 tables according to `approved`
   or
   `rejected` rows.
4. `approved` rows are merged into the `silver` tier.
5. `silver` is merged into `gold`, which is then split into `master` and `reference`
   tables.

In the context of the current implementation of the platform, we are ingesting school
geolocation data and school coverage data separately, so their tiers are separate up
until `silver`. The silver geolocation and silver coverage tables are then joined to
form the `gold` School Master table.


There are also adhoc pipelines which can help with a direct push to the `gold` layer

Adding support for working with custom datasets
