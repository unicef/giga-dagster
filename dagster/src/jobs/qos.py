from dagster import define_asset_job

qos_list__automated_data_checks_job = define_asset_job(
    name="qos_list__automated_data_checks_job",
    selection=[
        "list_raw",
        "list_bronze",
        "list_dq_results",
        "list_dq_summary_statistics",
        "list_dq_passed_rows",
        "list_dq_failed_rows",
        # "list_staging",
    ],
)
