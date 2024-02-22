from src.utils.sentry import capture_op_exceptions

from dagster import OpExecutionContext, asset


@asset
@capture_op_exceptions
def migrate_metaschema(_: OpExecutionContext):
    pass
