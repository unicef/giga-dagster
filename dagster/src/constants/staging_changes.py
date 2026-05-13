from enum import StrEnum


class StagingStatus(StrEnum):
    PENDING = "PENDING"
    REJECTED = "REJECTED"
    PROCESSED = "PROCESSED"
    PROCESSED_UNCHANGED = "PROCESSED_UNCHANGED"


class StagingChangeType(StrEnum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    UNCHANGED = "UNCHANGED"
