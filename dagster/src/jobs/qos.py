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


school_list__successful_manual_checks_job = define_asset_job(
    name="school_list__successful_manual_checks_job",
    selection=[
        "manual_review_passed_rows",
        "silver",
        "master",
        "reference",
    ],
)


qos_connectivity__automated_data_checks_job = define_asset_job(
    name="qos_connectivity__automated_data_checks_job",
    selection=[
        "connectivity_raw",
        "connectivity_bronze",
        "connectivity_dq_results",
        "connectivity_dq_summary_statistics",
        "connectivity_dq_passed_rows",
        "connectivity_dq_failed_rows",
        "connectivity_silver",
        "connectivity_gold",
    ],
)
