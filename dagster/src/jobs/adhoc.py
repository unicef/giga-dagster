from dagster import define_asset_job

school_master__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_master__convert_gold_csv_to_deltatable_job",
    selection=[
        "master_csv_to_gold",
    ],
)

school_reference__convert_gold_csv_to_deltatable_job = define_asset_job(
    name="school_reference__convert_gold_csv_to_deltatable_job",
    selection=[
        "reference_csv_to_gold",
    ],
)

qos__convert_csv_to_deltatable_job = define_asset_job(
    name="qos__convert_csv_to_deltatable_job",
    selection=[
        "qos_csv_to_gold",
    ],
)
