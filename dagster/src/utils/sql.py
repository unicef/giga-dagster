from mako.template import Template

from src.settings import settings


def load_sql_template(sql_file: str, **kwargs) -> str:
    with open(settings.BASE_DIR / "sql" / f"{sql_file}.sql.mako", "r") as f:
        template = f.read()

    text = Template(text=template, default_filters=["h"])
    return text.render(**kwargs)
