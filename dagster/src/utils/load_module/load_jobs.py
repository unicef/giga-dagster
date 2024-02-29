from collections.abc import Sequence
from types import ModuleType

from dagster import JobDefinition
from dagster._core.definitions.load_assets_from_modules import _find_modules_in_package
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)

from .base import _find_definition_in_module


def load_jobs_from_package_module(
    package_module: ModuleType,
) -> Sequence[JobDefinition | UnresolvedAssetJobDefinition]:
    job_ids: set[int] = set()
    jobs: Sequence[JobDefinition | UnresolvedAssetJobDefinition] = []
    for module in _find_modules_in_package(package_module):
        for job in _find_definition_in_module(
            module, (JobDefinition, UnresolvedAssetJobDefinition)
        ):
            if job_id := id(job) not in job_ids:
                job_ids.add(job_id)
                jobs.append(job)
    return jobs
