from dagster import AssetSelection, define_asset_job

vct_events_daily_job = define_asset_job(
    "vct_events_daily_job",
    selection=AssetSelection.keys("vct_qos_availability"),
)

vct_qos_60min_job = define_asset_job(
    "vct_qos_60min_job",
    selection=AssetSelection.keys("vct_qos_raw"),
)

vct_combined_daily_job = define_asset_job(
    "vct_combined_daily_job",
    selection=AssetSelection.keys("vct_qos"),
)
