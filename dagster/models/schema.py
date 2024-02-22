from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from .base import BaseModel


class SchemaModel(BaseModel):
    __table_name__ = "metaschema"

    @property
    def fields(self) -> list[StructField]:
        return [
            StructField("name", StringType(), False),
            StructField("data_type", StringType(), False),
            StructField("is_nullable", BooleanType(), False),
            StructField("description", StringType(), True),
        ]

    @property
    def schema(self) -> StructType:
        return StructType(self.fields)


Schema = SchemaModel()
