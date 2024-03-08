from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    TimestampType,
)

from .base import BaseSchema


class QosSchema(BaseSchema):
    @property
    def columns(self) -> list[StructField]:
        return [
            StructField("gigasync_id", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("date", StringType(), False),
            StructField("country_id", StringType(), False),
            StructField("school_id_govt", StringType(), False),
            StructField("school_id_giga", StringType(), False),
            StructField("speed_download", FloatType(), True),
            StructField("speed_upload", FloatType(), True),
            StructField("roundtrip_time", FloatType(), True),
            StructField("jitter_download", FloatType(), True),
            StructField("jitter_upload", FloatType(), True),
            StructField("rtt_packet_loss_pct", FloatType(), True),
            StructField("latency", FloatType(), True),
            StructField("provider", StringType(), True),
            StructField("ip_family", IntegerType(), True),
            StructField("report_id", StringType(), True),
            StructField("agent_id", StringType(), True),
        ]


qos = QosSchema()
