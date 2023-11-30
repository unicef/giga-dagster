from pathlib import Path

from mako.template import Template


def load_sql_template(filename: Path | str, *args) -> str:
    with open(filename, "r") as f:
        template = f.read()

    text = Template(text=template, default_filters=["h"])
    return text.render(*args)
