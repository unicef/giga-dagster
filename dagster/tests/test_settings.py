from src.settings import settings


def test_settings_deploy_env_is_string():
    env = settings.DEPLOY_ENV
    assert isinstance(env, str)
    assert len(env) > 0
    assert env in ["dev", "stage", "prod", "local", "test"]


def test_settings_in_production_is_bool():
    in_prod = settings.IN_PRODUCTION
    assert isinstance(in_prod, bool)


def test_settings_commit_sha_exists():
    sha = settings.COMMIT_SHA
    assert sha is None or isinstance(sha, str)


def test_settings_has_multiple_configs():
    public_attrs = [a for a in dir(settings) if not a.startswith("_")]
    assert len(public_attrs) >= 10


def test_settings_sentry_dsn_type():
    dsn = settings.SENTRY_DSN
    assert dsn is None or isinstance(dsn, str)
    if dsn:
        assert len(dsn) > 0
