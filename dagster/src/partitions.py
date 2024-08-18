from country_converter import CountryConverter

from dagster import StaticPartitionsDefinition

countries_partitions_def = StaticPartitionsDefinition(
    CountryConverter().data["ISO3"].to_list()
)
