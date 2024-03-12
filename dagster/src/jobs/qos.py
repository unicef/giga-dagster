from dagster import define_asset_job

qos_school_list__automated_data_checks_job = define_asset_job(
    name="qos_school_list__automated_data_checks_job",
    selection=[
        "qos_school_list_raw",
        "qos_school_list_bronze",
        "qos_school_list_dq_results",
        "qos_school_list_dq_summary_statistics",
        "qos_school_list_dq_passed_rows",
        "qos_school_list_dq_failed_rows",
        "qos_school_list_staging",
    ],
)


qos_school_list__successful_manual_checks_job = define_asset_job(
    name="qos_school_list__successful_manual_checks_job",
    selection=[
        "manual_review_passed_rows",
        "silver",
        "master",
        "reference",
    ],
)


qos_school_connectivity__automated_data_checks_job = define_asset_job(
    name="qos_connectivity__automated_data_checks_job",
    selection=[
        "qos_school_connectivity_raw",
        "qos_school_connectivity_bronze",
        "qos_school_connectivity_dq_results",
        "qos_school_connectivity_dq_summary_statistics",
        "qos_school_connectivity_dq_passed_rows",
        "qos_school_connectivity_dq_failed_rows",
        "qos_school_connectivity_silver",
        "qos_school_connectivity_gold",
    ],
)
