from dagster import define_asset_job

school_master__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_master__convert_gold_csv_to_deltatable_job",
    selection=[
        "adhoc__load_master_csv",
        "adhoc__master_data_quality_checks",
        "adhoc__master_dq_checks_passed",
        "adhoc__master_dq_checks_failed",
        "adhoc__publish_master_to_gold",
        "adhoc__df_duplicates",
        "adhoc__master_data_transforms",
    ],
)

school_reference__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_reference__convert_gold_csv_to_deltatable_job",
    selection=[
        "adhoc__load_reference_csv",
        "adhoc__reference_data_quality_checks",
        "adhoc__reference_dq_checks_passed",
        "adhoc__reference_dq_checks_failed",
        "adhoc__publish_reference_to_gold",
    ],
)

school_qos_bra__convert_csv_to_deltatable_job = define_asset_job(
    name="qos__convert_csv_to_deltatable_job",
    selection=[
        "adhoc__publish_qos_to_gold",
    ],
)
