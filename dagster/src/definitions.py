# import os

# from dagster_snowflake_pandas import SnowflakePandasIOManager

from dagster import Definitions

# EnvVar, fs_io_manager
# from src._utils import StagingS3IOManager, StagingS3IOManagerPropertySplit

# ### BC
# from src.bc.assets.chart_of_accounts import (
#     chart_of_accounts,
#     raw_bc_chart_of_accounts_cfcm,
# )
# from src.bc.assets.dimension_set_entries import (
#     dimension_set_entries,
#     raw_bc_dimension_set_entries_cfcm,
# )
# from src.bc.assets.ehsicas_employees import (
#     ehsicas_employees,
#     raw_bc_ehsicas_employees_cfcm,
# )
# from src.bc.assets.gl_entries import gl_entries, raw_bc_gl_entries_cfcm
# from src.bc.assets.item_ledger_entries import (
#     item_ledger_entries,
#     raw_bc_item_ledger_entries_cfcm,
# )
# from src.bc.assets.locations import locations, raw_bc_locations_cfcm
# from src.bc.assets.msf_dimension_values import (
#     msf_dimension_values,
#     raw_bc_msf_dimension_values_cfcm,
# )
# from src.bc.jobs import (
#     get_chart_of_accounts_job,
#     get_dimension_set_entries_job,
#     get_ehsicas_employees_job,
#     get_gl_entries_job,
#     get_item_ledger_entries_job,
#     get_locations_job,
#     get_msf_dimension_values_job,
# )
# from src.bc.resources.apis import (
#     ChartOfAccountsAPI,
#     DimensionSetEntriesAPI,
#     EhsicasEmployeesAPI,
#     GLEntriesAPI,
#     ItemLedgerEntriesAPI,
#     LocationsAPI,
#     MSFDimensionValuesAPI,
# )
# from src.bc.resources.auth import bc_auth_token_API
# from src.bc.schedules import (
#     chart_of_accounts_schedule,
#     dimension_set_entries_schedule,
#     ehsicas_employees_schedule,
#     gl_entries_schedule,
#     item_ledger_entries_schedule,
#     locations_schedule,
#     msf_dimension_values_schedule,
# )

# ### RMS
# from src.rms.assets.areas import (
#     areas,
#     areas_by_property,
#     raw_rms_areas_cfcm,
#     raw_rms_areas_crsb,
#     raw_rms_areas_crsm,
#     raw_rms_areas_qhcc,
#     raw_rms_areas_qhtc,
#     raw_rms_areas_qpcc,
# )
# from src.rms.assets.categories import (
#     categories,
#     categories_by_property,
#     raw_rms_categories_cfcm,
#     raw_rms_categories_crsb,
#     raw_rms_categories_crsm,
#     raw_rms_categories_qhcc,
#     raw_rms_categories_qhtc,
#     raw_rms_categories_qpcc,
# )
# from src.rms.assets.companies import companies, raw_rms_companies
# from src.rms.assets.gl_account_code_groupings import (
#     gl_account_code_groupings,
#     raw_rms_gl_account_code_groupings,
# )
# from src.rms.assets.gl_account_codes import gl_account_codes, raw_rms_gl_account_codes
# from src.rms.assets.guests import guests, raw_rms_guests
# from src.rms.assets.market_segments import (
#     market_segments,
#     raw_rms_market_segments,
#     raw_rms_sub_market_segments,
#     sub_market_segments,
# )
# from src.rms.assets.properties import properties, raw_rms_properties
# from src.rms.assets.reasons import raw_rms_reasons, reasons
# from src.rms.assets.reservations import (
#     raw_rms_reservations_cfcm,
#     raw_rms_reservations_crsb,
#     raw_rms_reservations_crsm,
#     raw_rms_reservations_qhcc,
#     raw_rms_reservations_qhtc,
#     raw_rms_reservations_qpcc,
#     reservations,
#     reservations_by_property,
# )
# from src.rms.assets.sundries import (
#     raw_rms_sundries_cfcm,
#     raw_rms_sundries_crsb,
#     raw_rms_sundries_crsm,
#     raw_rms_sundries_qhcc,
#     raw_rms_sundries_qhtc,
#     raw_rms_sundries_qpcc,
#     sundries_by_property,
# )
# from src.rms.assets.transactions import (
#     raw_rms_transactions_cfcm,
#     raw_rms_transactions_crsb,
#     raw_rms_transactions_crsm,
#     raw_rms_transactions_qhcc,
#     raw_rms_transactions_qhtc,
#     raw_rms_transactions_qpcc,
#     transactions,
#     transactions_by_property,
# )
# from src.rms.jobs import (
#     get_areas_job,
#     get_categories_job,
#     get_companies_job,
#     get_gl_account_code_groupings_job,
#     get_gl_account_codes_job,
#     get_guests_job,
#     get_market_segments_job,
#     get_properties_job,
#     get_reasons_job,
#     get_reservations_job,
#     get_sundries_job,
#     get_transactions_job,
# )
# from src.rms.resources.apis import (
#     AreasAPI,
#     CategoriesAPI,
#     CompaniesAPI,
#     GLAccountCodeGroupingsAPI,
#     GLAccountCodesAPI,
#     GuestsAPI,
#     MarketSegmentsAPI,
#     PropertiesAPI,
#     ReasonsAPI,
#     ReservationsAPI,
#     SubMarketSegmentsAPI,
#     SundriesAPI,
#     TransactionsAPI,
# )
# from src.rms.resources.auth import rms_auth_token_API
# from src.rms.schedules import (
#     areas_schedule,
#     categories_schedule,
#     companies_schedule,
#     gl_account_code_groupings_schedule,
#     gl_account_codes_schedule,
#     guests_schedule,
#     market_segments_schedule,
#     properties_schedule,
#     reasons_schedule,
#     reservations_schedule,
#     sundries_schedule,
#     transactions_schedule,
# )

# bc_config_auth = {
#     "base_api_url": {"env": "BC_CLIENT_URL"},
#     "username": {"env": "BC_USERNAME"},
#     "password": {"env": "BC_PASSWORD"},
# }

# rms_config_auth = {
#     "base_api_url": {"env": "RMS_CLIENT_URL"},
#     "agent_id": {"env": "RMS_AGENT_ID"},
#     "agent_password": {"env": "RMS_AGENT_PASSWORD"},
#     "client_id": {"env": "RMS_CLIENT_ID"},
#     "client_password": {"env": "RMS_CLIENT_PASSWORD"},
#     "module_types": ["datawarehouse"],
#     "use_training_database": {"env": "RMS_USE_TRAINING_DATABASE"},
# }

# io_managers = {
#     "dev": fs_io_manager.configured({"base_dir": "/tmp/io_manager_storage"}),
#     "bc_s3_staging": StagingS3IOManager(
#         bucket="tm-fhc-ap-southeast-1-dwh-s3-bucket-staging", data_source="bc"
#     ),
#     "rms_s3_staging": StagingS3IOManager(
#         bucket="tm-fhc-ap-southeast-1-dwh-s3-bucket-staging", data_source="rms"
#     ),
#     "rms_s3_staging_property_split": StagingS3IOManagerPropertySplit(
#         bucket="tm-fhc-ap-southeast-1-dwh-s3-bucket-staging", data_source="rms"
#     ),
#     "snowflake_staging": SnowflakePandasIOManager(
#         account=EnvVar("SNOWFLAKE_ACCOUNT"),
#         user=EnvVar("SNOWFLAKE_USER"),
#         password=EnvVar("SNOWFLAKE_PASSWORD"),
#         role=EnvVar("SNOWFLAKE_ROLE"),
#         database=EnvVar("SNOWFLAKE_DATABASE"),
#         warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
#         schema=EnvVar("SNOWFLAKE_SCHEMA"),
#     ),
# }

defs = Definitions(
    # assets=[
    #     # BC assets
    #     chart_of_accounts,
    #     raw_bc_chart_of_accounts_cfcm,
    #     dimension_set_entries,
    #     raw_bc_dimension_set_entries_cfcm,
    #     ehsicas_employees,
    #     raw_bc_ehsicas_employees_cfcm,
    #     gl_entries,
    #     raw_bc_gl_entries_cfcm,
    #     item_ledger_entries,
    #     raw_bc_item_ledger_entries_cfcm,
    #     locations,
    #     raw_bc_locations_cfcm,
    #     msf_dimension_values,
    #     raw_bc_msf_dimension_values_cfcm,
    #     # RMS assets
    #     areas,
    #     areas_by_property,
    #     raw_rms_areas_cfcm,
    #     raw_rms_areas_crsb,
    #     raw_rms_areas_crsm,
    #     raw_rms_areas_qhcc,
    #     raw_rms_areas_qhtc,
    #     raw_rms_areas_qpcc,
    #     categories,
    #     categories_by_property,
    #     raw_rms_categories_cfcm,
    #     raw_rms_categories_crsb,
    #     raw_rms_categories_crsm,
    #     raw_rms_categories_qhcc,
    #     raw_rms_categories_qhtc,
    #     raw_rms_categories_qpcc,
    #     companies,
    #     raw_rms_companies,
    #     gl_account_code_groupings,
    #     raw_rms_gl_account_code_groupings,
    #     gl_account_codes,
    #     raw_rms_gl_account_codes,
    #     guests,
    #     raw_rms_guests,
    #     market_segments,
    #     raw_rms_market_segments,
    #     properties,
    #     raw_rms_properties,
    #     reasons,
    #     raw_rms_reasons,
    #     reservations,
    #     reservations_by_property,
    #     raw_rms_reservations_cfcm,
    #     raw_rms_reservations_crsb,
    #     raw_rms_reservations_crsm,
    #     raw_rms_reservations_qhcc,
    #     raw_rms_reservations_qhtc,
    #     raw_rms_reservations_qpcc,
    #     sub_market_segments,
    #     raw_rms_sub_market_segments,
    #     sundries_by_property,
    #     raw_rms_sundries_cfcm,
    #     raw_rms_sundries_crsb,
    #     raw_rms_sundries_crsm,
    #     raw_rms_sundries_qhcc,
    #     raw_rms_sundries_qhtc,
    #     raw_rms_sundries_qpcc,
    #     transactions,
    #     transactions_by_property,
    #     raw_rms_transactions_cfcm,
    #     raw_rms_transactions_crsb,
    #     raw_rms_transactions_crsm,
    #     raw_rms_transactions_qhcc,
    #     raw_rms_transactions_qhtc,
    #     raw_rms_transactions_qpcc,
    # ],
    # resources={
    #     "bc_auth_token_API": bc_auth_token_API.configured(
    #         {
    #             "username": bc_config_auth["username"],
    #             "password": bc_config_auth["password"],
    #         }
    #     ),
    #     "rms_auth_token_API": rms_auth_token_API.configured(rms_config_auth),
    #     # BC APIs
    #     "chart_of_accounts_API": ChartOfAccountsAPI.configured(
    #         {"base_api_url": bc_config_auth["base_api_url"]}
    #     ),
    #     "dimension_set_entries_API": DimensionSetEntriesAPI.configured(
    #         {"base_api_url": bc_config_auth["base_api_url"]}
    #     ),
    #     "ehsicas_employees_API": EhsicasEmployeesAPI.configured(
    #         {"base_api_url": bc_config_auth["base_api_url"]}
    #     ),
    #     "gl_entries_API": GLEntriesAPI.configured(
    #         {"base_api_url": bc_config_auth["base_api_url"]}
    #     ),
    #     "item_ledger_entries_API": ItemLedgerEntriesAPI.configured(
    #         {"base_api_url": bc_config_auth["base_api_url"]}
    #     ),
    #     "locations_API": LocationsAPI.configured(
    #         {"base_api_url": bc_config_auth["base_api_url"]}
    #     ),
    #     "msf_dimension_values_API": MSFDimensionValuesAPI.configured(
    #         {"base_api_url": bc_config_auth["base_api_url"]}
    #     ),
    #     # RMS APIs
    #     "areas_API": AreasAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "categories_API": CategoriesAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "companies_API": CompaniesAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "gl_account_code_groupings_API": GLAccountCodeGroupingsAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "gl_account_codes_API": GLAccountCodesAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "guests_API": GuestsAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "market_segments_API": MarketSegmentsAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "properties_API": PropertiesAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "reasons_API": ReasonsAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "reservations_API": ReservationsAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "sundries_API": SundriesAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "sub_market_segments_API": SubMarketSegmentsAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "transactions_API": TransactionsAPI.configured(
    #         {"base_api_url": rms_config_auth["base_api_url"]}
    #     ),
    #     "bc_s3_io_manager": io_managers.get(f"bc_s3_{os.getenv('ENVIRONMENT')}"),
    #     "rms_s3_io_manager": io_managers.get(f"rms_s3_{os.getenv('ENVIRONMENT')}"),
    #     "rms_s3_property_split_io_manager": io_managers.get(
    #         f"rms_s3_{os.getenv('ENVIRONMENT')}_property_split"
    #     ),
    #     "snowflake_io_manager": io_managers.get(
    #         f"snowflake_{os.getenv('ENVIRONMENT')}"
    #     ),
    # },
    # jobs=[
    #     # BC jobs
    #     get_chart_of_accounts_job,
    #     get_dimension_set_entries_job,
    #     get_ehsicas_employees_job,
    #     get_gl_entries_job,
    #     get_item_ledger_entries_job,
    #     get_locations_job,
    #     get_msf_dimension_values_job,
    #     # RMS jobs
    #     get_areas_job,
    #     get_categories_job,
    #     get_companies_job,
    #     get_gl_account_code_groupings_job,
    #     get_gl_account_codes_job,
    #     get_guests_job,
    #     get_market_segments_job,
    #     get_properties_job,
    #     get_reasons_job,
    #     get_reservations_job,
    #     get_sundries_job,
    #     get_transactions_job,
    # ],
    # schedules=[
    #     # BC schedules
    #     chart_of_accounts_schedule,
    #     dimension_set_entries_schedule,
    #     ehsicas_employees_schedule,
    #     gl_entries_schedule,
    #     item_ledger_entries_schedule,
    #     locations_schedule,
    #     msf_dimension_values_schedule,
    #     # RMS schedules
    #     areas_schedule,
    #     categories_schedule,
    #     companies_schedule,
    #     gl_account_code_groupings_schedule,
    #     gl_account_codes_schedule,
    #     guests_schedule,
    #     market_segments_schedule,
    #     properties_schedule,
    #     reasons_schedule,
    #     reservations_schedule,
    #     sundries_schedule,
    #     transactions_schedule,
    # ],
)
