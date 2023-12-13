from functools import lru_cache

from pydantic import BaseSettings


class Constants(BaseSettings):
    raw_folder = "raw/school_geolocation_coverage_data/bronze/school_data"
    staging_approved_folder = "staging/approved"
    archive_manual_review_rejected_folder = "archive/manual-review-rejected"
    gold_folder = "raw/school_geolocation_coverage_data/gold/school_data"


@lru_cache
def get_constants():
    return Constants()


constants = get_constants()
