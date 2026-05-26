### Debugging

##### How to get email data from gold

1. Materialize an entire set of assets that update `gold` e.g.`adhoc_master_csv_to_gold` (at the time of writing)
2. Attach i.e. by creating a new asset and setting its dependency to the last asset

```
# assuming last asset is adhoc__publish_master_to_gold
@asset(deps=["adhoc__publish_master_to_gold"])
def send_master_release_email(
    config: FileConfig,
    spark: PySparkResource,
):
    s: SparkSession = spark.spark_session
    country_code = config.country_code

    cdf = (
        s.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .table(f"school_master.{country_code}")
    )
    detail = s.sql(f"DESCRIBE DETAIL school_master.{country_code}")
```

3. Materialize the last asset and select the `Scaffold missing config`. You may use a sample template below and adjust based on your assets' requirements.

```
#sample config
config:
    dataset_type: 'school_master'
    destination_filepath: 'warehouse-local-{your_name}/school_master.db/{iso_country_code_lowercase}'
    file_size_bytes: 0
    filepath: 'warehouse-local-{your_name}/school_master.db/{iso_country_code_lowercase}'
    metastore_schema: 'school_master'
    tier: 'GOLD

```

4. [optional] You can run `task ipython` and manually run SQL queries on the recently updated `cdf` tables

```
from src.utils.spark import get_spark_session

spark = get_spark_session()
detail = spark.sql("DESCRIBE DETAIL school_master.aia")
detail.show(truncate=False)

cdf = (
    spark.read.format("delta")
    b.option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table(f"school_master.aia")
)

cdf.show(truncate=False)

```

### Running a school_master\_\_convert_gold_csv_to_deltatable_job for your country

1. turn on the following sensor: school_master_gold_csv_to_deltatable_sensor
2. wait for sensor to evaluate
3. open hamburger and then admin **terminate_all_runs_job --> materialize admin**terminate_all_runs
4. Goto runs -> Select "In Progress Filter" -> terminate the (1) `school_master__convert_gold_csv_to_deltatable_job` job (~20 seconds)
5. Assert that terminate_all_runs_job shows a success. Assert that all the country runs got cancelled
6. If choosing a country that you nedver run before, re-execute that country (~15 mins on first run)
7. Grab the corresponding file of the contry you chose in `updated_master_schema/master`
8. Modify some rows of the downloaded file and then place it in the corroesponding country folder in `updated_master_schemam/master_updates;`,. Make sure to rename it with a new timestamp so the original file does not get overwritten. Also make sure that the newset file has the latest `modified` timestamp
