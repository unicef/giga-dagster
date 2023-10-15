from dagster import Config, OpExecutionContext, job, op


class FileConfig(Config):
    filename: str


@op
def read_filename(context: OpExecutionContext, config: FileConfig):
    context.log.info(f"{config.filename=}")


@job
def log_file_job():
    read_filename()
