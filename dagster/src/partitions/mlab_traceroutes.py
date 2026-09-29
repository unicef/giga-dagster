from dagster import DailyPartitionsDefinition

mlab_traceroutes_partitions_def = DailyPartitionsDefinition(
    start_date="2025-12-02", timezone="UTC"
)
