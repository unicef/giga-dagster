from datetime import datetime

from dagster import (
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)
from src.constants import constants
from src.utils.country import get_country_codes_list

adhoc_qos_partitions_def = MultiPartitionsDefinition(
    {
        "country": StaticPartitionsDefinition(get_country_codes_list()),
        "datetime": TimeWindowPartitionsDefinition(
            start=datetime(2024, 1, 1),
            fmt=constants.datetime_partition_key_format,
            cron_schedule="*/15 * * * *",
        ),
    },
)

adhoc_qos_country_partitions_def = DynamicPartitionsDefinition(name="country")
adhoc_qos_datetime_partitions_def = DynamicPartitionsDefinition(name="datetime")

adhoc_qos_dynamic_partitions_def = MultiPartitionsDefinition(
    {
        "country": adhoc_qos_country_partitions_def,
        "datetime": adhoc_qos_datetime_partitions_def,
    },
)
