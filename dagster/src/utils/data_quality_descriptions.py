from pathlib import Path

import numpy as np
import pandas as pd
from pyspark import sql

from dagster import OpExecutionContext
from src.utils.adls import ADLSFileClient
from src.utils.op_config import FileConfig


def human_readable_standard_checks(columns: list[str]) -> dict[str, str]:
    return {}


def human_readable_geolocation_checks() -> dict[str, str]:
    return {}


def human_readable_coverage_coverage_itu_checks() -> dict[str, str]:
    return {}


def human_readable_coverage_fb_checks() -> dict[str, str]:
    return {}


def convert_dq_checks_to_human_readeable_descriptions_and_upload(
    dq_results: sql.DataFrame,
    dataset_type: str,
    bronze: sql.DataFrame,
    config: FileConfig,
    context: OpExecutionContext,
):
    adls_client = ADLSFileClient()

    if dq_results is None or len(dq_results.columns) == 0:
        context.log.warning(
            f"Empty DQ results for {dataset_type}, creating empty output"
        )
        dq_with_renamed_headers_pandas = pd.DataFrame()
    else:
        dq_with_renamed_headers_pandas = dq_results.toPandas()
        context.log.info(
            f"Original column count: {len(dq_with_renamed_headers_pandas.columns)}"
        )

        columns_to_keep = [
            col
            for col in dq_with_renamed_headers_pandas.columns
            if not col.startswith("dq_") or col == "dq_has_critical_error"
        ]
        dq_with_renamed_headers_pandas = dq_with_renamed_headers_pandas[columns_to_keep]

        empty_values = ["", "None", "null", "NaN", "nan"]
        dq_with_renamed_headers_pandas.replace(empty_values, np.nan, inplace=True)

        dq_with_renamed_headers_pandas.dropna(axis=1, how="all", inplace=True)

        threshold = 0.89
        missing_ratio = dq_with_renamed_headers_pandas.isna().mean()
        columns_to_drop = missing_ratio[missing_ratio > threshold].index.tolist()

        if columns_to_drop:
            dq_with_renamed_headers_pandas.drop(columns=columns_to_drop, inplace=True)

        if "dq_has_critical_error" in dq_with_renamed_headers_pandas.columns:
            if dq_with_renamed_headers_pandas["dq_has_critical_error"].isna().all():
                context.log.info(
                    "Removing 'dq_has_critical_error' column as it is completely empty"
                )
                dq_with_renamed_headers_pandas.drop(
                    columns=["dq_has_critical_error"], inplace=True
                )

    upload_path = Path(config.destination_filepath)
    dataset = upload_path.parts[1]
    country_code = upload_path.parts[3]
    file_name = upload_path.name

    temp_filepath = f"data-quality-results/{dataset}/dq-human-readable-descriptions/{country_code}/{file_name}"

    adls_client.upload_pandas_dataframe_as_file(
        context=context, data=dq_with_renamed_headers_pandas, filepath=temp_filepath
    )


def handle_rename_dq_has_critical_error_column(
    null_mandatory_columns: list[str],
) -> dict[str, str]:
    return {}
