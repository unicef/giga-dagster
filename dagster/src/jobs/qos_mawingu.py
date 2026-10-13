from dagster import AssetSelection, define_asset_job

mawingu_qos_job = define_asset_job(
    "mawingu_qos_job",
    selection=AssetSelection.keys("mawingu_qos"),
)
