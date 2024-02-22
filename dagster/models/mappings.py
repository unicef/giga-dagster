from typing import Literal, TypedDict

from avrogen.dict_wrapper import DictWrapper
from pyspark.sql.types import DataType

VALID_PRIMITIVES = Literal["string", "integer", "float", "timestamp"]


class TypeMappings(TypedDict):
    native: type
    pyspark: type[DataType]
    datahub: type[DictWrapper]


class FullTypeMappings(TypedDict):
    string: TypeMappings
    integer: TypeMappings
    float: TypeMappings
    timestamp: TypeMappings
