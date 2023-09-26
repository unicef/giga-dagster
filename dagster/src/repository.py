from dagster import repository
from src.jobs.delta_poc import delta_poc_job


@repository
def giga_dataops_platform():
    jobs = [delta_poc_job]
    return jobs
