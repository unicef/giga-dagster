import pytest
from src.jobs.school_connectivity import (
    school_connectivity__get_new_realtime_schools_job,
    school_connectivity__update_master_realtime_schools_job,
)

from dagster import AssetSelection


@pytest.mark.parametrize(
    "job, expected_selection",
    [
        (
            school_connectivity__get_new_realtime_schools_job,
            "school_connectivity_realtime_schools",
        ),
        (
            school_connectivity__update_master_realtime_schools_job,
            "school_connectivity_realtime_silver",
        ),
    ],
)
def test_job_has_correct_selection(job, expected_selection):
    selection = job.selection
    assert isinstance(selection, AssetSelection | list)
    assert expected_selection in str(selection)


@pytest.mark.parametrize(
    "job",
    [
        school_connectivity__get_new_realtime_schools_job,
        school_connectivity__update_master_realtime_schools_job,
    ],
)
def test_job_has_max_runtime_tag(job):
    assert "dagster/max_runtime" in job.tags
    assert job.tags["dagster/max_runtime"] is not None


@pytest.mark.parametrize(
    "job",
    [
        school_connectivity__get_new_realtime_schools_job,
        school_connectivity__update_master_realtime_schools_job,
    ],
)
def test_job_name_matches_expected_convention(job):
    assert job.name.startswith("school_connectivity__")
    assert "__" in job.name
