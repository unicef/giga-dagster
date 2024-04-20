from dagster import define_asset_job

qos_school_list__automated_data_checks_job = define_asset_job(
    name="qos_school_list__automated_data_checks_job",
    selection=[
        "qos_school_list_raw",
        "qos_school_list_bronze",
        "qos_school_list_data_quality_results",
        "qos_school_list_data_quality_results_summary",
        "qos_school_list_dq_passed_rows",
        "qos_school_list_dq_failed_rows",
        "qos_school_list_staging",
    ],
)
