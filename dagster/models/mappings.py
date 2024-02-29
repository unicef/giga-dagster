from avrogen.dict_wrapper import DictWrapper
from pydantic import BaseModel
from pyspark.sql.types import DataType

VALID_PRIMITIVES = [
    "string",
    "integer",
    "long",
    "float",
    "double",
    "timestamp",
    "boolean",
]


class TypeMapping(BaseModel):
    native: type
    pyspark: type[DataType]
    datahub: type[DictWrapper]


class TypeMappings(BaseModel):
    string: TypeMapping
    integer: TypeMapping
    long: TypeMapping
    float: TypeMapping
    double: TypeMapping
    timestamp: TypeMapping
    boolean: TypeMapping
