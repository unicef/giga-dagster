from enum import StrEnum


class DataTier(StrEnum):
    RAW = "raw"
    BRONZE = "bronze"
    TRANSFORMS = "transforms"
    DATA_QUALITY_CHECKS = "data_quality_checks"
    STAGING = "staging"
    MANUAL_REJECTED = "rejected"
    SILVER = "silver"
    GOLD = "gold"
