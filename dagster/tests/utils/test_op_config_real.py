from unittest.mock import MagicMock, patch

with patch.dict("sys.modules", {"src.utils.datahub.builders": MagicMock()}):
    from src.utils.op_config import FileConfig, OpDestinationMapping, generate_run_ops
from src.constants import DataTier


def test_file_config_properties():
    config = FileConfig(
        filepath="gold/test.csv",
        dataset_type="test",
        country_code="BRA",
        file_size_bytes=100,
        destination_filepath="gold/dest.csv",
        metastore_schema="schema",
        tier=DataTier.GOLD,
        metadata={"k": "v"},
    )

    assert config.filepath_object.name == "test.csv"
    assert config.destination_filepath_object.name == "dest.csv"

    prop = FileConfig.datahub_destination_dataset_urn
    urn_func = prop.fget.__globals__["build_dataset_urn"]

    if hasattr(urn_func, "return_value"):
        urn_func.return_value = "urn:li:dataset:(deltaLake,test,PROD)"
        assert "deltaLake" in config.datahub_destination_dataset_urn
    else:
        import sys

        mod = sys.modules[FileConfig.__module__]
        with patch.object(
            mod,
            "build_dataset_urn",
            return_value="urn:li:dataset:(deltaLake,test,PROD)",
        ):
            assert "deltaLake" in config.datahub_destination_dataset_urn

    prop_fn = FileConfig.filename_components
    globals_dict = prop_fn.fget.__globals__

    with patch.dict(
        globals_dict,
        {"deconstruct_qos_filename_components": MagicMock(return_value=MagicMock())},
    ):
        qos_config = FileConfig(
            filepath="raw/qos/BRA/test.csv",
            dataset_type="qos",
            country_code="BRA",
            file_size_bytes=100,
            destination_filepath="raw/qos/BRA/dest.csv",
            metastore_schema="schema",
            tier=DataTier.RAW,
        )

        assert qos_config.filename_components is not None


def test_generate_run_ops():
    mappings = {
        "asset1": OpDestinationMapping(
            source_filepath="src1.csv",
            destination_filepath="dst1.csv",
            metastore_schema="schema1",
            tier=DataTier.RAW,
        )
    }

    with patch("src.utils.adls.ADLSFileClient") as mock_adls:
        mock_adls.return_value.exists.return_value = False

        ops = generate_run_ops(
            ops_destination_mapping=mappings,
            dataset_type="test",
            metadata={"metadata_json": "meta.json", "default": "value"},
            file_size_bytes=1000,
            domain="domain",
            country_code="BRA",
        )

        assert "asset1" in ops
        config = ops["asset1"]
        assert config.filepath == "src1.csv"
        assert config.destination_filepath == "dst1.csv"
        assert config.file_size_bytes == 1000
        assert config.country_code == "BRA"
