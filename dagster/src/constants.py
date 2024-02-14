from functools import lru_cache

from pydantic import BaseSettings


class Constants(BaseSettings):
    raw_folder = "adls-testing-raw"
    staging_approved_folder = "staging/approved"
    archive_manual_review_rejected_folder = "archive/manual-review-rejected"
    gold_folder = "updated_master_schema"

    step_origin_folder_map: dict[str, str] = {
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

    def step_destination_folder_map(self, dataset_type: str) -> dict[str, str]:
        return {
            "geolocation_raw": f"{self.raw_folder}/school-{dataset_type}-data",
            "geolocation_bronze": f"bronze/school-{dataset_type}-data",
            "geolocation_data_quality_results": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_passed_rows": f"staging/pending-review/school-{dataset_type}-data",
            "geolocation_dq_failed_rows": f"archive/gx-tests-failed/school-{dataset_type}-data",
            "geolocation_staging": f"staging/pending-review/school-{dataset_type}-data",
            "coverage_raw": f"{self.raw_folder}/school-{dataset_type}-data",
            "coverage_data_quality_results": f"logs-gx/school-{dataset_type}-data",
            "coverage_bronze": f"bronze/school-{dataset_type}-data",
            "coverage_dq_passed_rows": f"staging/pending-review/school-{dataset_type}-data",
            "coverage_dq_failed_rows": "archive/gx-tests-failed/school-{dataset_type}-data",
            "coverage_staging": f"staging/pending-review/school-{dataset_type}-data",
            "manual_review_passed_rows": (
                f"{constants.staging_approved_folder}/school-{dataset_type}-data"
            ),
            "manual_review_failed_rows": (
                f"{constants.archive_manual_review_rejected_folder}"
            ),
            "silver": f"silver/school-{dataset_type}-data",
            "gold": "gold",
            "master_csv_to_gold": "gold/delta-tables/school-{dataset_type}",
            "reference_csv_to_gold": "gold/delta-tables/school-{dataset_type}",
            "qos_csv_to_gold": "gold/delta-tables/qos",
        }


@lru_cache
def get_constants():
    return Constants()


constants = get_constants()
