from dataclasses import dataclass
from enum import Enum
from typing import Optional


class DQMode(str, Enum):
    UPLOADED = "uploaded"
    MASTER = "master"


@dataclass(frozen=True)
class DQContext:
    dq_mode: DQMode
    dataset_type: str
    country_code_iso3: str
    upload_id: Optional[int] = None
    upload_mode: Optional[str] = None

    @property
    def mode(self) -> DQMode:
        return self.dq_mode
