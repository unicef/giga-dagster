from dagster import define_asset_job

school_master__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_master__convert_gold_csv_to_deltatable_job",
    selection=[
        "adhoc__load_master_csv*",
        "adhoc__load_reference_csv*",
    ],
)

school_qos__convert_csv_to_deltatable_job = define_asset_job(
    name="school_qos__convert_csv_to_deltatable_job",
    selection="adhoc__load_qos_csv*",
)


school_master__generate_mock_table_cdf_job = define_asset_job(
    name="school_master__generate_mock_table_cdf_job",
    selection="*adhoc__generate_v3",
)

school_master__generate_silver_tables_job = define_asset_job(
    name="school_master__generate_silver_tables_job",
    selection=[
        "adhoc__generate_silver_geolocation_from_gold",
        "adhoc__generate_silver_coverage_from_gold",
    ],
)
