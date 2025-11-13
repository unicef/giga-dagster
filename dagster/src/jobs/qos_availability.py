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
        # incremental run including small overlap
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

    # add next timestamp and status for interval calculation
    window = Window.partitionBy("device_id").orderBy("timestamp")
    combined_df = combined_df.withColumn("next_ts", F.lead("timestamp").over(window)) \
                             .withColumn("next_status", F.lead("status").over(window))

    # compute end_ts for each row using last_complete_window
    combined_df = combined_df.withColumn(
        "end_ts",
        F.when(F.col("status") == "online",
               F.when(F.col("next_status") != "offline", F.coalesce(F.col("next_ts"), last_complete_window))
                .otherwise(F.col("next_ts"))
        ).otherwise(F.col("timestamp"))
    )

    online_df = combined_df.filter(F.col("status") == "online")

    # compute start_hour and end_hour for each interval
    online_df = online_df.withColumn("start_hour", F.date_trunc("hour", "timestamp")) \
                         .withColumn("end_hour", F.date_trunc("hour", "end_ts"))

    # split into the hourly sequence
    online_df = online_df.withColumn(
        "hour_window",
        F.when(
            F.col("end_hour") >= F.col("start_hour"),
            F.expr("sequence(start_hour, end_hour, interval 1 hour)")
        ).otherwise(F.array(F.col("start_hour")))
    )

    hourly_df = online_df.withColumn("hour_start", F.explode("hour_window")) \
                         .withColumn("hour_end", F.expr("hour_start + INTERVAL 1 HOUR")) \
                         .withColumn("interval_start", F.greatest(F.col("timestamp"), F.col("hour_start"))) \
                         .withColumn("interval_end", F.least(F.col("end_ts"), F.col("hour_end"))) \
                         .withColumn("seconds_online", F.unix_timestamp("interval_end") - F.unix_timestamp("interval_start"))

    # aggregate per device per hour to generate uptime
    hourly_agg = (
        hourly_df.groupBy("device_id", "hour_start")
                 .agg(F.sum("seconds_online").alias("total_seconds_online"))
                 .withColumn("uptime_percentage", (F.col("total_seconds_online") / 3600) * 100)
                 .withColumnRenamed("hour_start", "timestamp")
                 .select("device_id", "timestamp", "uptime_percentage")
    )

    events_hourly = combined_df.withColumn("hour_start", F.date_trunc("hour", "timestamp"))
    events_count = events_hourly.groupBy("device_id", "hour_start") \
                                .agg(F.count("*").alias("count_state"))

    devices = combined_df.select("device_id").distinct()

    # minimum and maximum hour-aligned timestamps
    min_max = combined_df.select(
        F.date_trunc("hour", F.min("timestamp")).alias("start_ts"),
        F.date_trunc("hour", F.max("timestamp")).alias("end_ts")
    ).collect()[0]

    start_ts = min_max["start_ts"]
    end_ts = min_max["end_ts"]

    device_hours_df = devices.withColumn("start_ts", F.lit(start_ts)) \
                             .withColumn("end_ts", F.lit(end_ts)) \
                             .withColumn(
                                 "timestamp",
                                 F.explode(F.expr("sequence(start_ts, end_ts, interval 1 hour)"))
                             ) \
                             .select("device_id", "timestamp")

    # join online uptime and event counts
    hourly_full = device_hours_df.join(hourly_agg, ["device_id", "timestamp"], "left") \
                                 .join(events_count.select("device_id", F.col("hour_start").alias("timestamp"), "count_state"),
                                       ["device_id", "timestamp"],
                                       "left") \
                                 .fillna({"uptime_percentage": 0, "count_state": 0})

    hourly_full = hourly_full.withColumn(
        "uptime_percentage",
        F.when(
            (F.col("count_state") == 5) & (F.col("uptime_percentage") == 0),
            0
        ).otherwise(F.col("uptime_percentage"))
    )

    # enrich with device and school metadata
    device_meta = s.read.table(DEVICE_METADATA_TABLE).select(
        F.col("serial").alias("device_id"),
        "meraki_name_room",
        F.col("school_id_govt").cast("long").cast("string").alias("school_id_govt")
    )
    school_master = s.read.table(SCHOOL_MASTER_TABLE).select(
        F.col("school_id_govt").cast("long").cast("string").alias("school_id_govt"),
        "school_id_giga"
    )

    enriched = (
        hourly_full.join(device_meta, on="device_id", how="left")
                  .join(school_master, on="school_id_govt", how="left")
                  .withColumn("date", F.to_date("timestamp"))
    )

    # upsert to Delta table
    if not DeltaTable.isDeltaTable(s, QOS_VCT):
        s.sql("CREATE SCHEMA IF NOT EXISTS qos_hourly")
        enriched.write.format("delta").mode("overwrite").partitionBy("date").option("overwriteSchema", "true").saveAsTable(QOS_VCT)
    else:
        delta_table = DeltaTable.forName(s, QOS_VCT)
        (delta_table.alias("target")
            .merge(
                enriched.alias("source"),
                "target.device_id = source.device_id AND target.timestamp = source.timestamp"
            )
            .whenMatchedUpdate(set={
                "uptime_percentage": "source.uptime_percentage",
                "meraki_name_room": "source.meraki_name_room",
                "count_state": "source.count_state",
                "school_id_govt": "source.school_id_govt",
                "school_id_giga": "source.school_id_giga"
            })
            .whenNotMatchedInsert(values={
                "device_id": "source.device_id",
                "timestamp": "source.timestamp",
                "uptime_percentage": "source.uptime_percentage",
                "count_state": "source.count_state",
                "meraki_name_room": "source.meraki_name_room",
                "school_id_govt": "source.school_id_govt",
                "school_id_giga": "source.school_id_giga"
            })
            .execute()
        )

    context.log.info(f"upserted {enriched.count()} hourly uptime rows into {QOS_VCT}")

@job
def generate_uptime():
    process_availability()
