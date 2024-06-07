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


emit_metadata_to_datahub_job = define_asset_job(
    name="emit_metadata_to_datahub_job",
    selection="emit_metadata_to_datahub",
)
