from dagster import OpExecutionContext, job, op
from dagster_pyspark import PySparkResource
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

@op
def process_availability(
    context: OpExecutionContext,
    spark: PySparkResource
):
    s: SparkSession = spark.spark_session
    QOS_VCT = "qos_hourly.vct"
    QOS_AVAILABILITY_VCT = "qos_availability.vct"
    DEVICE_METADATA_TABLE = "custom_dataset.device_matched"
    SCHOOL_MASTER_TABLE = "school_master.vct"
    try:
        last_processed_ts = (
            s.read.table(QOS_VCT)
                .agg(F.max("timestamp").alias("last_ts"))
                .collect()[0]["last_ts"]
        )
    except Exception as e:
        last_processed_ts = None
        context.log.warning("no existing aggregated table found so creating it again")

    # get the data from the table which has all the state change saved
    events_df = s.read.table(QOS_AVAILABILITY_VCT).select("device_id", "status", "timestamp")

    if last_processed_ts is not None:
        # Incremental run: include small overlap
        events_df = events_df.filter(F.col("timestamp") > (F.lit(last_processed_ts) - F.expr("INTERVAL 2 HOURS")))

    if last_processed_ts is not None:
        previous_df = (
            s.read.table(QOS_AVAILABILITY_VCT)
                .filter(F.col("timestamp") <= (F.lit(last_processed_ts) - F.expr("INTERVAL 2 HOURS")))
                .withColumn("rn", F.row_number().over(
                    Window.partitionBy("device_id").orderBy(F.col("timestamp").desc())
                ))
                .filter(F.col("rn") == 1)
                .select(
                    "device_id",
                    F.col("status").alias("prev_status"),
                    F.col("timestamp").alias("prev_timestamp")
                )
        )

        combined_df = previous_df.select(
            "device_id",
            F.col("prev_status").alias("status"),
            F.col("prev_timestamp").alias("timestamp")
        ).union(events_df)

    else:
        combined_df = events_df

    last_complete_window = F.date_trunc("hour", F.current_timestamp() - F.expr("INTERVAL 1 HOUR"))
    combined_df = combined_df.filter(F.col("timestamp") < last_complete_window)

    window = Window.partitionBy("device_id").orderBy("timestamp")
    combined_df = combined_df.withColumn("next_ts", F.lead("timestamp").over(window))
    combined_df = combined_df.withColumn("end_ts", F.coalesce(F.col("next_ts"), last_complete_window))

    online_df = combined_df.filter(F.col("status") == "online")

    online_df = online_df.withColumn("start_hour", F.date_trunc("hour", "timestamp")) \
                        .withColumn("end_hour", F.date_trunc("hour", "end_ts"))

    invalid_rows = online_df.filter(F.col("start_hour") > F.col("end_hour"))
    num_invalid = invalid_rows.count()
    context.log.info(f"filtered out {num_invalid} rows where start_hour > end_hour")
    if num_invalid > 0:
        invalid_rows.select("device_id", "timestamp", "end_ts", "start_hour", "end_hour").show(truncate=False)

    online_df = online_df.filter(F.col("start_hour") <= F.col("end_hour"))
    hourly_df = online_df.withColumn(
        "hour_window",
        F.expr("explode(sequence(start_hour, end_hour - interval 1 hour, interval 1 hour))")
    )

    hourly_df = hourly_df.withColumn("hour_start", F.col("hour_window")) \
        .withColumn("hour_end", F.expr("hour_window + INTERVAL 1 HOUR")) \
        .withColumn("interval_start", F.greatest(F.col("timestamp"), F.col("hour_start"))) \
        .withColumn("interval_end", F.least(F.col("end_ts"), F.col("hour_end"))) \
        .withColumn("seconds_online", F.unix_timestamp("interval_end") - F.unix_timestamp("interval_start"))

    hourly_agg = (
        hourly_df.groupBy("device_id", "hour_start")
                .agg(F.sum("seconds_online").alias("total_seconds_online"))
                .withColumn("uptime_percentage", (F.col("total_seconds_online") / 3600) * 100)
                .withColumn("timestamp", F.col("hour_start"))
                .select("device_id", "timestamp", "uptime_percentage")
    )

    device_meta_df = (
        s.read.table(DEVICE_METADATA_TABLE)
            .select(
                F.col("serial").alias("device_id"),
                "meraki_name_room",
                F.col("school_id_govt").cast("long").cast("string").alias("school_id_govt")
            )
    )
    school_master_df = (
        s.read.table(SCHOOL_MASTER_TABLE)
            .select(
                F.col("school_id_govt").cast("long").cast("string").alias("school_id_govt"),
                "school_id_giga"
            )
    )
    hourly_agg_enriched = (
        hourly_agg.join(device_meta_df, on="device_id", how="left")
                .join(school_master_df, on="school_id_govt", how="left")
    )
    hourly_agg_enriched = hourly_agg_enriched.withColumn("date", F.to_date("timestamp"))

    if not DeltaTable.isDeltaTable(s, QOS_VCT):
        s.sql("CREATE SCHEMA IF NOT EXISTS qos_hourly")
        hourly_agg_enriched.write.format("delta").mode("overwrite").partitionBy("date").option("overwriteSchema", "true").saveAsTable(QOS_VCT)
    else:
        delta_table = DeltaTable.forName(s, QOS_VCT)
        (delta_table.alias("target")
            .merge(
                hourly_agg_enriched.alias("source"),
                "target.device_id = source.device_id AND target.timestamp = source.timestamp"
            )
            .whenMatchedUpdate(set={
                "uptime_percentage": "source.uptime_percentage",
                "meraki_name_room": "source.meraki_name_room",
                "school_id_govt": "source.school_id_govt",
                "school_id_giga": "source.school_id_giga"
            })
            .whenNotMatchedInsert(values={
                "device_id": "source.device_id",
                "timestamp": "source.timestamp",
                "uptime_percentage": "source.uptime_percentage",
                "meraki_name_room": "source.meraki_name_room",
                "school_id_govt": "source.school_id_govt",
                "school_id_giga": "source.school_id_giga"
            })
            .execute()
        )

    s.catalog.refreshTable(QOS_VCT)
    context.log.info(f"upserted {hourly_agg_enriched.count()} hourly uptime rows into {QOS_VCT}")

@job
def generate_uptime():
    process_availability()
