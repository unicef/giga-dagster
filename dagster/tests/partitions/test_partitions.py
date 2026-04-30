from datetime import UTC, datetime

from src.partitions.qos import adhoc_qos_partitions_def

from dagster import (
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    TimeWindowPartitionsDefinition,
)


def test_qos_partitions_structure():
    assert isinstance(adhoc_qos_partitions_def, MultiPartitionsDefinition)

    dims = {pid.name: pid for pid in adhoc_qos_partitions_def.partitions_defs}
    assert "country" in dims
    assert "datetime" in dims

    country_def = dims["country"]
    dt_def = dims["datetime"]

    assert country_def.name == "country"
    assert dt_def.name == "datetime"


def test_qos_partitions_country():
    dims = {pid.name: pid for pid in adhoc_qos_partitions_def.partitions_defs}
    country_def = dims["country"]

    assert isinstance(country_def.partitions_def, StaticPartitionsDefinition)
    assert len(country_def.partitions_def.get_partition_keys()) > 0


def test_qos_partitions_datetime():
    dims = {pid.name: pid for pid in adhoc_qos_partitions_def.partitions_defs}
    dt_def = dims["datetime"]

    assert isinstance(dt_def.partitions_def, TimeWindowPartitionsDefinition)
    assert dt_def.partitions_def.start == datetime(2024, 1, 1, tzinfo=UTC)
    assert dt_def.partitions_def.cron_schedule == "*/15 * * * *"
