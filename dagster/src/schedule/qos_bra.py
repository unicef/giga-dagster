from src.jobs.qos_bra import bra_qos_job, bra_qos_raw_republish_job

from dagster import RunRequest, ScheduleEvaluationContext, build_schedule_from_partitioned_job, schedule


@schedule(cron_schedule="10 3,7,11,15,19,23 * * *", job=bra_qos_job)
def bra_qos_schedule(context: ScheduleEvaluationContext):
    """bra_qos always targets today's own (still-accumulating) partition - see the
    asset's docstring for why this can't use build_schedule_from_partitioned_job,
    which targets the most recently *completed* partition instead."""
    partition_key = context.scheduled_execution_time.date().isoformat()
    yield RunRequest(partition_key=partition_key)


bra_qos_raw_republish_schedule = build_schedule_from_partitioned_job(
    bra_qos_raw_republish_job, hour_of_day=1, minute_of_hour=45
)
