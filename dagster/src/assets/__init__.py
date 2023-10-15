from dagster import load_assets_from_package_module

from . import bank_loans, great_expectations, poc

ALL_ASSETS = [
    *load_assets_from_package_module(package_module=poc, group_name="poc"),
    *load_assets_from_package_module(
        package_module=bank_loans, group_name="bank_loans"
    ),
    *load_assets_from_package_module(
        package_module=great_expectations, group_name="great_expectations"
    ),
]
