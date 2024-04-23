from dagster import define_asset_job

qos_school_list__automated_data_checks_job = define_asset_job(
    name="qos_school_list__automated_data_checks_job",
    selection=["qos_school_list_raw*"],
)
