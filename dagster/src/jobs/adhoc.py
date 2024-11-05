from dagster import define_asset_job
from src.settings import settings

school_master__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_master__convert_gold_csv_to_deltatable_job",
    selection=[
        "adhoc__load_master_csv*",
        "adhoc__load_reference_csv*",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

school_qos__convert_csv_to_deltatable_job = define_asset_job(
    name="school_qos__convert_csv_to_deltatable_job",
    selection="adhoc__load_qos_csv*",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

school_qos_raw__convert_csv_to_deltatable_job = define_asset_job(
    name="school_qos_raw__convert_csv_to_deltatable_job",
    selection="adhoc__load_qos_raw_csv*",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)


school_master__generate_mock_table_cdf_job = define_asset_job(
    name="school_master__generate_mock_table_cdf_job",
    selection="*adhoc__generate_v3",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

school_master__generate_silver_tables_job = define_asset_job(
    name="school_master__generate_silver_tables_job",
    selection=[
        "adhoc__generate_silver_geolocation_from_gold",
        "adhoc__generate_silver_coverage_from_gold",
    ],
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)

school_master__dq_checks_job = define_asset_job(
    name="school_master__dq_checks_job",
    selection="adhoc__standalone_master_data_quality_checks",
    tags={"dagster/max_runtime": settings.DEFAULT_MAX_RUNTIME},
)
