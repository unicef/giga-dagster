from dataclasses import dataclass
from enum import Enum


class DQMode(str, Enum):  # noqa: UP042
    UPLOADED = "uploaded"
    MASTER = "master"


@dataclass(frozen=True)
class DQContext:
    dq_mode: DQMode
    dataset_type: str
    country_code_iso3: str
    upload_id: int | None = None
    upload_mode: str | None = None
    dq_config_table: str = "SchoolGeolocationMasterDQChecks"
    is_new_column: str = "is_new_school"
    check_column_aliases: tuple[tuple[str, str], ...] = ()
    values_domain: tuple[tuple[str, tuple[object, ...]], ...] | None = None
    values_range: tuple[tuple[str, float, float], ...] | None = None
    duplicate_all_except_columns: tuple[str, ...] | None = None
    unique_set_columns: tuple[tuple[str, ...], ...] | None = None

    @property
    def mode(self) -> DQMode:
        return self.dq_mode
