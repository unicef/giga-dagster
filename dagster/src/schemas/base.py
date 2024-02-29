from abc import ABC, abstractmethod

from pyspark.sql.types import StructField, StructType


class BaseSchema(ABC):
    @property
    @abstractmethod
    def columns(self) -> list[StructField]:
        raise NotImplementedError

    @property
    @abstractmethod
    def schema(self) -> StructType:
        raise NotImplementedError
