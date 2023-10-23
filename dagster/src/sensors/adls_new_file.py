from dagster import RunRequest, sensor
from src.io_managers.common import get_fs_client
from src.jobs.data_passthrough import move_to_gold_job
from src.settings import settings


@sensor(
    job=move_to_gold_job,
    minimum_interval_seconds=30,
)
def adls_new_file():
    fs_client = get_fs_client()
    for path in fs_client.get_paths(path=settings.ADLS_SENSE_DIRECTORY, recursive=True):
        if path["is_directory"]:
            continue

        *path_prefix, filename = path["name"].split("/")
        no_root_path_prefix = path_prefix[1:]
        chroot_path_prefix = ["fake-gold", *no_root_path_prefix]
        config = dict(
            source_path_prefix="/".join(path_prefix),
            destination_path_prefix="/".join(chroot_path_prefix),
            filename=filename,
        )

        yield RunRequest(
            run_key=path["name"],
            run_config={
                "ops": {
                    "move_to_gold_as_delta_table": dict(config=config),
                },
            },
        )
