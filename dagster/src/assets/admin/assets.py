from dagster import DagsterRunStatus, OpExecutionContext, RunsFilter, asset


@asset
def admin__terminate_all_runs(context: OpExecutionContext):
    runs = [
        r
        for r in context.instance.get_runs(
            filters=RunsFilter(
                statuses=[
                    DagsterRunStatus.STARTED,
                    DagsterRunStatus.STARTING,
                    DagsterRunStatus.QUEUED,
                    DagsterRunStatus.NOT_STARTED,
                ]
            )
        )
        if r.run_id != context.run_id or not r.job_name.startswith("admin__")
    ]

    for run in runs:
        context.instance.report_run_canceled(run)
        context.log.info(f"Terminated run {run.job_name} ({run.run_id})")

    context.log.info(f"Terminated {len(runs)} run(s)")
