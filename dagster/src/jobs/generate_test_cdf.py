from dagster import define_asset_job

school_master__generate_test_cdf = define_asset_job(
    name="school_master__generate_test_cdf",
    selection=[
        "clone_table",
        "update_values_u1",
        "update_values_u2",
    ],
)
