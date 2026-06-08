from pyspark.sql.types import DoubleType, StringType, StructField
from src.assets.common import assets as common_assets


def test_silver_null_defaults_target_required_string_columns(monkeypatch):
    monkeypatch.setattr(common_assets.f, "col", lambda name: ("col", name))
    monkeypatch.setattr(common_assets.f, "lit", lambda value: ("lit", value))
    monkeypatch.setattr(
        common_assets.f,
        "coalesce",
        lambda column, default: ("coalesce", column, default),
    )

    schema_columns = [
        StructField("school_id_govt", StringType(), nullable=False),
        StructField("school_id_govt_type", StringType(), nullable=False),
        StructField("school_name", StringType(), nullable=False),
        StructField("latitude", DoubleType(), nullable=False),
        StructField("nullable_string", StringType(), nullable=True),
    ]

    actions = common_assets._handle_null_string_columns(
        schema_columns, primary_key="school_id_govt"
    )

    assert actions == {
        "school_id_govt_type": (
            "coalesce",
            ("col", "school_id_govt_type"),
            ("lit", "Unknown"),
        ),
        "school_name": ("coalesce", ("col", "school_name"), ("lit", "Unknown")),
    }
