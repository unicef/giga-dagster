from dagster import job
from src.ops.delta_poc_op import write_delta_lake_poc


@job
def delta_poc_job():
    write_delta_lake_poc()
