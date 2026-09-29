from src.jobs.qos_mawingu import mawingu_qos_job

from dagster import build_schedule_from_partitioned_job

mawingu_qos_schedule = build_schedule_from_partitioned_job(
    mawingu_qos_job, cron_schedule="10 2 * * *"
)
