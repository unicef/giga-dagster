from dagster import AssetSelection, define_asset_job

bra_qos_job = define_asset_job(
    "bra_qos_job",
    selection=AssetSelection.keys("bra_qos"),
)

bra_qos_raw_republish_job = define_asset_job(
    "bra_qos_raw_republish_job",
    selection=AssetSelection.keys("bra_qos_raw_republish"),
)
