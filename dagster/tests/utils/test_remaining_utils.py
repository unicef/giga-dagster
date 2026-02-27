from src.utils import external_db, github_api_calls
from src.utils.email import send_email_base
from src.utils.load_module import base, load_jobs, load_schedules, load_sensors
from src.utils.nocodb import get_nocodb_data
from src.utils.qos_apis import common, school_connectivity, school_list
from src.utils.slack import send_slack_base


def test_external_db():
    assert len(dir(external_db)) > 3


def test_github_api():
    assert len(dir(github_api_calls)) > 3


def test_nocodb():
    assert len(dir(get_nocodb_data)) > 3


def test_load_jobs():
    assert len(dir(load_jobs)) > 3


def test_load_schedules():
    assert len(dir(load_schedules)) > 3


def test_load_sensors():
    assert len(dir(load_sensors)) > 3


def test_load_base():
    assert len(dir(base)) > 3


def test_email_base():
    assert len(dir(send_email_base)) > 3


def test_slack_base():
    assert len(dir(send_slack_base)) > 3


def test_qos_apis_common():
    assert len(dir(common)) > 3


def test_qos_apis_connectivity():
    assert len(dir(school_connectivity)) > 3


def test_qos_apis_school_list():
    assert len(dir(school_list)) > 3
