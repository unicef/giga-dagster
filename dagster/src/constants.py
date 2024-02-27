from pydantic import BaseSettings


class Constants(BaseSettings):
    raw_folder = "adls-testing-raw"
    dq_passed_folder = "staging/pending-review"
    staging_approved_folder = "staging/approved"
    archive_manual_review_rejected_folder = "archive/manual-review-rejected"
    gold_folder = "updated_master_schema"

    def step_origin_map(self) -> dict[str, str]:
        return {
            "geolocation_raw": "",
            "geolocation_bronze": "geolocation_raw",
            "geolocation_data_quality_results": "geolocation_bronze",
            "geolocation_dq_passed_rows": "geolocation_data_quality_results",
            "geolocation_dq_failed_rows": "geolocation_data_quality_results",
            "geolocation_staging": "geolocation_dq_passed_rows",
            "coverage_raw": "",
            "coverage_data_quality_results": "coverage_raw",
            "coverage_dq_passed_rows": "coverage_data_quality_results",
            "coverage_dq_failed_rows": "coverage_data_quality_results",
            "coverage_bronze": "coverage_dq_passed_rows",
            "coverage_staging": "coverage_bronze",
        }

    def step_folder_map(self, dataset_type: str) -> dict[str, str]:
        return {
            "geolocation_raw": f"{self.raw_folder}/school-{dataset_type}-data",
            "geolocation_bronze": f"bronze/school-{dataset_type}-data",
            "geolocation_data_quality_results": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_results": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_summary_statistics": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_checks": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_passed_rows": f"staging/pending-review/school-{dataset_type}-data",
            "geolocation_dq_failed_rows": f"archive/gx-tests-failed/school-{dataset_type}-data",
            "geolocation_staging": f"staging/pending-review/school-{dataset_type}-data",
            "coverage_raw": f"{self.raw_folder}/school-{dataset_type}-data",
            "coverage_data_quality_results": f"logs-gx/school-{dataset_type}-data",
            "coverage_dq_results": f"logs-gx/school-{dataset_type}-data",
            "coverage_dq_summary_statistics": f"logs-gx/school-{dataset_type}-data",
            "coverage_dq_checks": f"logs-gx/school-{dataset_type}-data",
            "coverage_bronze": f"bronze/school-{dataset_type}-data",
            "coverage_dq_passed_rows": f"staging/pending-review/school-{dataset_type}-data",
            "coverage_dq_failed_rows": f"archive/gx-tests-failed/school-{dataset_type}-data",
            "coverage_staging": f"staging/pending-review/school-{dataset_type}-data",
            "manual_review_passed_rows": (
                f"{constants.staging_approved_folder}/school-{dataset_type}-data"
            ),
            "manual_review_failed_rows": (
                f"{constants.archive_manual_review_rejected_folder}"
            ),
            "silver": f"silver/school-{dataset_type}-data",
            "gold_master": "gold/school-master",
            "gold_reference": "gold/school-reference",
            "master_csv_to_gold": f"gold/delta-tables/school-{dataset_type}",
            "reference_csv_to_gold": f"gold/delta-tables/school-{dataset_type}",
            "qos_csv_to_gold": "gold/delta-tables/qos",
        }


constants = Constants()
