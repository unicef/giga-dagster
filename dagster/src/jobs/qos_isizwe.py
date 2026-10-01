from dagster import AssetSelection, define_asset_job

isizwe_qos_job = define_asset_job(
    "isizwe_qos_job",
    selection=AssetSelection.keys("isizwe_qos"),
)
