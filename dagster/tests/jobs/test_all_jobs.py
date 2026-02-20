from src.jobs import (
    adhoc,
    admin,
    datahub,
    debug,
    migrations,
    qos,
    qos_availability,
    school_connectivity,
    school_master,
    superset,
    unstructured,
)

from dagster import JobDefinition


def _test_job_module(module, module_name):
    jobs_found = 0
    for name in dir(module):
        if name.startswith("_"):
            continue
        obj = getattr(module, name)

        type_name = type(obj).__name__
        if (
            isinstance(obj, JobDefinition)
            or type_name == "UnresolvedAssetJobDefinition"
        ):
            jobs_found += 1
            assert obj.name is not None
            assert len(obj.name) > 0

    return jobs_found


def test_adhoc_jobs():
    assert _test_job_module(adhoc, "adhoc") > 0


def test_admin_jobs():
    assert _test_job_module(admin, "admin") > 0


def test_datahub_jobs():
    assert _test_job_module(datahub, "datahub") > 0


def test_debug_jobs():
    assert _test_job_module(debug, "debug") > 0


def test_migrations_jobs():
    assert _test_job_module(migrations, "migrations") > 0


def test_qos_jobs():
    assert _test_job_module(qos, "qos") > 0


def test_qos_availability_jobs():
    assert _test_job_module(qos_availability, "qos_availability") > 0


def test_school_connectivity_jobs():
    assert _test_job_module(school_connectivity, "school_connectivity") > 0


def test_school_master_jobs():
    assert _test_job_module(school_master, "school_master") > 0


def test_superset_jobs():
    assert _test_job_module(superset, "superset") > 0


def test_unstructured_jobs():
    assert _test_job_module(unstructured, "unstructured") > 0
