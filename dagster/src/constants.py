from functools import lru_cache

from pydantic import BaseSettings


class Constants(BaseSettings):
    raw_folder = "raw_dev"
    staging_approved_folder = "staging/approved"
    archive_manual_review_rejected_folder = "archive/manual-review-rejected"
    gold_folder = "updated_master_schema"

    step_origin_folder_map = {
        "bronze": "raw",
        "data_quality_results": "bronze",
        "dq_split_rows": "bronze",
        "dq_passed_rows": "bronze",
        "dq_failed_rows": "bronze",
        "manual_review_passed_rows": "bronze",
        "manual_review_failed_rows": "bronze",
        "silver": "manual_review_passed",
        "gold": "silver",
    }

    def step_destination_folder_map(self, dataset_type: str):
        return {
            "raw": f"{self.raw_folder}/{dataset_type}",
            "bronze": f"bronze/{dataset_type}",
            "data_quality_results": "logs-gx",
            "dq_split_rows": "bronze/split-rows",
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
            "master_csv_to_gold": "gold/delta-tables-v1/school-master",
            "reference_csv_to_gold": "gold/delta-tables-v1/school-reference",
        }


@lru_cache
def get_constants():
    return Constants()


constants = get_constants()
