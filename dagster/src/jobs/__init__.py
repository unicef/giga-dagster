from .data_passthrough import raw_to_delta
from .log_added_file import log_file_job


ALL_JOBS = [
    raw_to_delta,
    log_file_job,
]
