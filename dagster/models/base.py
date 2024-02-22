from abc import abstractmethod

from pyspark.sql.types import StructField, StructType


class BaseModel:
    __schema_name__: str = "schemas"

    @property
    @abstractmethod
    def __table_name__(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def fields(self) -> list[StructField]:
        raise NotImplementedError

    @property
    @abstractmethod
    def schema(self) -> StructType:
        raise NotImplementedError
