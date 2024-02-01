from mako.template import Template

from src.settings import settings


def load_sql_template(sql_file: str, **kwargs) -> str:
    """
    Load the SQL Mako template from /dagster/sql

    :param sql_file: The name of the SQL file, without the `.sql.mako` extension.
    :param kwargs: The keyword arguments to be passed to the template.
    :return: The rendered SQL statement.
    """
    with open(settings.BASE_DIR / "sql" / f"{sql_file}.sql.mako") as f:
        template = f.read()

    text = Template(text=template, default_filters=["h"])
    return text.render(**kwargs)
