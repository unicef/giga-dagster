from dagster import OpExecutionContext
from src.constants import constants


def get_destination_filepath(source_path, dataset_type, step):
    filename = source_path.split("/")[-1]

    step_destination_folder_map = {
        "raw": f"{constants.raw_folder}/{dataset_type}",
        "bronze": f"bronze/{dataset_type}",
        "dq_passed_rows": f"staging/pending-review/{dataset_type}",
        "dq_failed_rows": "archive/gx-tests-failed",
        "manual_review_passed_rows": (
            f"{constants.staging_approved_folder}/{dataset_type}"
        ),
        "manual_review_failed_rows": (
            f"{constants.archive_manual_review_rejected_folder}"
        ),
        "silver": f"silver/{dataset_type}",
        "gold": "gold",
    }

    destination_folder = step_destination_folder_map[step]

    if not destination_folder:
        raise ValueError(f"Unknown filepath: {source_path}")

    destination_filepath = f"{destination_folder}/{filename}"

    return destination_filepath


def get_output_filepath(context: OpExecutionContext):
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    source_path = context.get_step_execution_context().op_config["filepath"]
    step = context.asset_key.to_user_string()

    destination_filepath = get_destination_filepath(source_path, dataset_type, step)

    return destination_filepath


def get_input_filepath(context: OpExecutionContext, upstream_step):
    dataset_type = context.get_step_execution_context().op_config["dataset_type"]
    source_path = context.get_step_execution_context().op_config["filepath"]
    filename = source_path.split("/")[-1]

    upstream_path = upstream_step + "/" + dataset_type + "/" + filename

    return upstream_path
