from datetime import datetime

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NumberTypeClass,
    StringTypeClass,
)
from models import TypeMapping, TypeMappings
from pydantic import BaseSettings
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


class Constants(BaseSettings):
    raw_folder = "adls-testing-raw"
    raw_schema_folder = "raw_schema"
    dq_passed_folder = "staging/pending-review"
    staging_approved_folder = "staging/approved"
    archive_manual_review_rejected_folder = "archive/manual-review-rejected"
    gold_source_folder = "updated_master_schema"

    step_origin_map: dict[str, str] = {
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

    def step_folder_map(self, dataset_type: str) -> dict[str, str]:
        return {
            # geolocation
            "geolocation_raw": f"{self.raw_folder}/school-{dataset_type}-data",
            "geolocation_bronze": f"bronze/school-{dataset_type}-data",
            "geolocation_data_quality_results": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_results": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_summary_statistics": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_checks": f"logs-gx/school-{dataset_type}-data",
            "geolocation_dq_passed_rows": f"staging/pending-review/school-{dataset_type}-data",
            "geolocation_dq_failed_rows": f"archive/gx-tests-failed/school-{dataset_type}-data",
            "geolocation_staging": f"staging/pending-review/school-{dataset_type}-data",
            #
            # coverage
            "coverage_raw": f"{self.raw_folder}/school-{dataset_type}-data",
            "coverage_data_quality_results": f"logs-gx/school-{dataset_type}-data",
            "coverage_dq_results": f"logs-gx/school-{dataset_type}-data",
            "coverage_dq_summary_statistics": f"logs-gx/school-{dataset_type}-data",
            "coverage_dq_checks": f"logs-gx/school-{dataset_type}-data",
            "coverage_bronze": f"bronze/school-{dataset_type}-data",
            "coverage_dq_passed_rows": f"staging/pending-review/school-{dataset_type}-data",
            "coverage_dq_failed_rows": f"archive/gx-tests-failed/school-{dataset_type}-data",
            "coverage_staging": f"staging/pending-review/school-{dataset_type}-data",
            #
            # common
            "manual_review_passed_rows": (
                f"{self.staging_approved_folder}/school-{dataset_type}-data"
            ),
            "manual_review_failed_rows": (
                f"{self.archive_manual_review_rejected_folder}"
            ),
            "silver": f"silver/school-{dataset_type}-data",
            "gold_master": "gold/school-master",
            "gold_reference": "gold/school-reference",
            #
            # adhoc
            "adhoc__load_master_csv": f"updated_master_schema/{dataset_type}",
            "adhoc__load_reference_csv": f"updated_master_schema/{dataset_type}",
            "adhoc__master_data_quality_checks": f"gold/dq-results/school-{dataset_type}/full",
            "adhoc__reference_data_quality_checks": f"gold/dq-results/school-{dataset_type}/full",
            "adhoc__master_dq_checks_passed": f"gold/dq-results/school-{dataset_type}/passed",
            "adhoc__reference_dq_checks_passed": f"gold/dq-results/school-{dataset_type}/passed",
            "adhoc__master_dq_checks_failed": f"gold/dq-results/school-{dataset_type}/failed",
            "adhoc__reference_dq_checks_failed": f"gold/dq-results/school-{dataset_type}/failed",
            "adhoc__publish_master_to_gold": f"gold/delta-tables/school-{dataset_type}",
            "adhoc__publish_reference_to_gold": f"gold/delta-tables/school-{dataset_type}",
            "adhoc__publish_qos_to_gold": "gold/delta-tables/qos",
        }

    TYPE_MAPPINGS: TypeMappings = TypeMappings(
        string=TypeMapping(
            native=str,
            pyspark=StringType,
            datahub=StringTypeClass,
        ),
        integer=TypeMapping(
            native=int,
            pyspark=IntegerType,
            datahub=NumberTypeClass,
        ),
        long=TypeMapping(
            native=int,
            pyspark=LongType,
            datahub=NumberTypeClass,
        ),
        float=TypeMapping(
            native=float,
            pyspark=FloatType,
            datahub=NumberTypeClass,
        ),
        double=TypeMapping(
            native=float,
            pyspark=DoubleType,
            datahub=NumberTypeClass,
        ),
        timestamp=TypeMapping(
            native=datetime,
            pyspark=TimestampType,
            datahub=DateTypeClass,
        ),
        boolean=TypeMapping(
            native=bool,
            pyspark=BooleanType,
            datahub=BooleanTypeClass,
        ),
    )


constants = Constants()
