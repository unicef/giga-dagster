def get_destination_filepath(source_path, dataset_type, step):
    filename = source_path.split("/")[-1]

    step_destination_folder_map = {
        "raw": "adls-testing-raw",
        "bronze": f"bronze/{dataset_type}",
        "dq_passed_rows": f"staging/pending-review/{dataset_type}",
        "dq_failed_rows": "archive/gx-tests-failed",
        "manual_review_passed_rows": f"staging/approved/{dataset_type}",
        "manual_review_failed_rows": "archive/manual-review-rejected",
        "silver": f"silver/{dataset_type}",
        "gold": "gold",
    }

    destination_folder = step_destination_folder_map[step]

    if not destination_folder:
        raise ValueError(f"Unknown filepath: {source_path}")

    destination_filepath = f"{destination_folder}/{filename}"

    return destination_filepath
