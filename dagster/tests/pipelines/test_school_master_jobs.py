import pytest
from src.assets.common import GROUP_NAME as COMMON_GROUP_NAME
from src.jobs.school_master import (
    school_master_coverage__admin_delete_rows_job,
    school_master_coverage__automated_data_checks_job,
    school_master_coverage__post_manual_checks_job,
    school_master_geolocation__admin_delete_rows_job,
    school_master_geolocation__automated_data_checks_job,
    school_master_geolocation__post_manual_checks_job,
)

from dagster import AssetSelection
from dagster._core.instance import DagsterInstance


@pytest.mark.parametrize(
    "job, expected_prefix",
    [
        (school_master_geolocation__automated_data_checks_job, "geolocation_raw"),
        (school_master_coverage__automated_data_checks_job, "coverage_raw"),
    ],
)
def test_automated_jobs_have_correct_selection(job, expected_prefix):
    selection = job.selection
    assert isinstance(selection, AssetSelection)
    assert expected_prefix in str(selection)


@pytest.mark.parametrize(
    "job",
    [
        school_master_geolocation__automated_data_checks_job,
        school_master_coverage__automated_data_checks_job,
    ],
)
def test_automated_jobs_have_hooks(job):
    assert job.hooks
    assert len(job.hooks) == 3


@pytest.mark.parametrize(
    "job",
    [
        school_master_geolocation__post_manual_checks_job,
        school_master_coverage__post_manual_checks_job,
    ],
)
def test_post_manual_jobs_use_common_group(job):
    assert isinstance(job.selection, AssetSelection)
    assert COMMON_GROUP_NAME in str(job.selection)


@pytest.mark.parametrize(
    "job, expected_target",
    [
        (
            school_master_geolocation__admin_delete_rows_job,
            "geolocation_delete_staging",
        ),
        (school_master_coverage__admin_delete_rows_job, "coverage_delete_staging"),
    ],
)
def test_admin_delete_jobs_target_correct(job, expected_target):
    assert expected_target in str(job.selection)


@pytest.mark.parametrize(
    "job",
    [
        school_master_geolocation__automated_data_checks_job,
        school_master_coverage__automated_data_checks_job,
        school_master_geolocation__post_manual_checks_job,
        school_master_coverage__post_manual_checks_job,
        school_master_geolocation__admin_delete_rows_job,
        school_master_coverage__admin_delete_rows_job,
    ],
)
def test_job_can_be_executed(job, defs_builder, dagster_instance):
    defs = defs_builder(job)
    resolved_job = defs.get_job_def(job.name)
    result = resolved_job.execute_in_process(instance=DagsterInstance.ephemeral())
    assert result.success
