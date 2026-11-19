from dagster import AssetSelection, define_asset_job

mongolia_qos_raw_json_job = define_asset_job(
    "mongolia_qos_raw_json_job",
    selection=AssetSelection.keys("mongolia_qos_raw_json"),
)

mongolia_qos_gold_job = define_asset_job(
    "mongolia_qos_gold_job",
    selection=AssetSelection.keys("mongolia_qos_gold"),
)
