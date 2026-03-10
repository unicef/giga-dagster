import pytest
from src.jobs.qos import (
    qos_availability__convert_gold_csv_to_deltatable_job,
    qos_school_connectivity__automated_data_checks_job,
    qos_school_list__automated_data_checks_job,
)

from dagster import AssetSelection


@pytest.mark.parametrize(
    "job, expected_group",
    [
        (qos_school_list__automated_data_checks_job, "school_list"),
        (qos_school_connectivity__automated_data_checks_job, "school_connectivity"),
    ],
)
def test_automated_jobs_select_correct_group(job, expected_group):
    selection = job.selection
    assert isinstance(selection, AssetSelection)
    assert expected_group in str(selection) or "groups" in str(selection)


def test_qos_availability_job_selects_correct_assets():
    job = qos_availability__convert_gold_csv_to_deltatable_job
    selection = job.selection

    selection_str = str(selection)
    assert "qos_availability" in selection_str


@pytest.mark.parametrize(
    "job",
    [
        qos_school_list__automated_data_checks_job,
        qos_school_connectivity__automated_data_checks_job,
        qos_availability__convert_gold_csv_to_deltatable_job,
    ],
)
def test_qos_job_has_max_runtime_tag(job):
    assert "dagster/max_runtime" in job.tags
    assert job.tags["dagster/max_runtime"] is not None


@pytest.mark.parametrize(
    "job",
    [
        qos_school_list__automated_data_checks_job,
        qos_school_connectivity__automated_data_checks_job,
        qos_availability__convert_gold_csv_to_deltatable_job,
    ],
)
def test_qos_job_name_follows_convention(job):
    assert job.name.startswith("qos")
    assert "__" in job.name or "_convert_" in job.name
