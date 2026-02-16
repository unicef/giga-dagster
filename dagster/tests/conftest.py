import gc
import os
import sys
import types
from pathlib import Path

# Ensure Spark workers use the same Python interpreter as the driver (the venv)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Ensure Trino provider can initialize without error during imports
os.environ["TRINO_CONNECTION_STRING"] = "trino://user@localhost:8080/catalog"

from unittest.mock import MagicMock, patch

from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(ENV_PATH, override=True)

import json
import tempfile
from io import BytesIO
from typing import Any, Optional

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from src.constants.constants_class import constants as project_constants

from dagster import Definitions, IOManager, asset, build_op_context, io_manager
from dagster._core.instance import DagsterInstance


@pytest.fixture(scope="session", autouse=True)
def set_dagster_home(tmp_path_factory):
    """
    Sets a unique DAGSTER_HOME for the test session to avoid
    sqlite database locking/closure issues with the default home.
    """
    home = tmp_path_factory.mktemp("dagster_home")
    os.environ["DAGSTER_HOME"] = str(home)
    yield
    gc.collect()  # Force cleanup of DagsterInstances holding DB locks
    if "DAGSTER_HOME" in os.environ:
        del os.environ["DAGSTER_HOME"]


@pytest.fixture(autouse=True)
def mock_trino_module(monkeypatch):
    fake_trino = types.ModuleType("src.utils.db.trino")

    class DummyTrinoProvider:
        def __init__(self, *_, **__):
            pass

        def get_db(self):
            yield None

        def get_db_context(self):
            class DummyCtx:
                def __enter__(self):
                    return None

                def __exit__(self, *args):
                    pass

            return DummyCtx()

    fake_trino.TrinoDatabaseProvider = DummyTrinoProvider
    fake_trino._trino = DummyTrinoProvider()
    fake_trino.get_db = fake_trino._trino.get_db
    fake_trino.get_db_context = fake_trino._trino.get_db_context

    monkeypatch.setitem(sys.modules, "src.utils.db.trino", fake_trino)
    yield


@asset(name="geolocation_raw")
def mock_geolocation_raw():
    return 1


@asset(name="coverage_raw")
def mock_coverage_raw():
    return 1


@asset(name="geolocation_delete_staging")
def mock_geolocation_delete_staging():
    return 1


@asset(name="coverage_delete_staging")
def mock_coverage_delete_staging():
    return 1


@pytest.fixture
def dagster_instance():
    """
    Provides an ephemeral in-memory Dagster instance for running jobs in tests.
    """
    with DagsterInstance.ephemeral() as instance:
        yield instance


@pytest.fixture
def fake_assets():
    """
    Returns the minimal set of fake assets the jobs select.
    """
    return [
        mock_geolocation_raw,
        mock_coverage_raw,
        mock_geolocation_delete_staging,
        mock_coverage_delete_staging,
    ]


@pytest.fixture
def defs_for_job(fake_assets):
    def _make(job, resources=None):
        return Definitions(
            assets=fake_assets,
            jobs=[job],
            resources=resources or {},
        )

    return _make


@pytest.fixture
def defs_builder(fake_assets):
    """
    Factory that builds Definitions containing the given job and the fake assets.

    Usage:
        defs = defs_builder(job)
        resolved_job = defs.get_job_def(job.name)
    """

    def _builder(job):
        return Definitions(assets=fake_assets, jobs=[job])

    return _builder


class FakeADLSFileClient:
    def __init__(self) -> None:
        self._tmpdir = Path(tempfile.mkdtemp(prefix="fake_adls_"))

        self._store: dict[str, bytes] = {}

        self._metadata: dict[str, dict[str, str]] = {}

    def upload_data(
        self, data: bytes, overwrite: bool = False, path: Optional[str] = None
    ) -> None:
        """
        Emulates DataLakeFileClient.upload_data. If path provided, use it, else data must include filename context.
        """
        if path:
            key = path
        else:
            raise RuntimeError("upload_data requires explicit path in our fake client")
        self._store[key] = data

    def download_raw(self, path: str) -> bytes:
        if path not in self._store:
            raise FileNotFoundError(path)
        return self._store[path]

    def download_json(self, path: str) -> dict | list:
        data = self.download_raw(path)
        return json.loads(data.decode("utf-8"))

    def upload_bytes_at(self, path: str, data: bytes) -> None:
        self._store[path] = data

    def exists(self, path: str) -> bool:
        return path in self._store

    def get_file_properties(self, path: str) -> dict[str, Any]:
        return {"metadata": self._metadata.get(path, {})}

    def set_metadata(self, path: str, metadata: dict[str, str]) -> None:
        self._metadata[path] = metadata

    def list_paths(self, path_prefix: str) -> list[str]:
        return [k for k in self._store.keys() if k.startswith(path_prefix)]

    def upload_json(self, path: str, data: dict | list) -> None:
        self._store[path] = json.dumps(data, indent=2).encode()

    def put_file_from_bytes(self, path: str, data: bytes) -> None:
        self._store[path] = data

    def read_text(self, path: str) -> str:
        return self._store[path].decode("utf-8")

    def list_files(self, prefix: str) -> list[str]:
        return [path for path in self._store if path.startswith(prefix)]

    def fetch_metadata_for_blob(self, path):
        return {"country": "BRA"}


class FakeProps:
    def __init__(self, size):
        self.size = size


def get_file_metadata(self, filepath):
    data = self._store.get(filepath, b"")
    return FakeProps(size=len(data))


class FakeSparkDataFrame:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def withColumnsRenamed(self, cols_map: dict[str, str]):
        return FakeSparkDataFrame(self._df.rename(columns=cols_map))

    def select(self, *cols):
        # Flatten list if cols passed as list
        flat_cols = []
        for c in cols:
            if isinstance(c, list):
                flat_cols.extend(c)
            else:
                flat_cols.append(c)
        return FakeSparkDataFrame(self._df[flat_cols])

    @property
    def write(self):
        class FakeWriter:
            def format(self, *args, **kwargs):
                return self

            def mode(self, *args, **kwargs):
                return self

            def saveAsTable(self, *args, **kwargs):
                pass

        return FakeWriter()

    @property
    def schema(self):
        class FakeField:
            def __init__(self, name):
                self.name = name

            self.nullable = True

        class FakeSchema:
            def __init__(self, columns):
                self.fields = [FakeField(c) for c in columns]

        return FakeSchema(self._df.columns)

    def toPandas(self):
        return self._df

    def count(self):
        return len(self._df)

    @property
    def columns(self):
        return list(self._df.columns)

    def collect(self):
        return list(self._df.itertuples(index=False))

    def withColumn(self, name, col):
        return self

    def withColumns(self, colsMap):
        # Naive implementation: just add columns with None values
        new_df = self._df.copy()
        for col_name in colsMap:
            if col_name not in new_df.columns:
                new_df[col_name] = None
        return FakeSparkDataFrame(new_df)


class FakeSpark:
    """
    Very small shim that supports read.csv(...) and createDataFrame from pandas.
    Use only to return a DataFrame-like object that your pipeline ops can use.
    """

    class Reader:
        def __init__(self, parent: "FakeSpark"):
            self.parent = parent
            self._options = {}

        def csv(self, path: str, header: bool = True, multiLine: bool = True, **kwargs):
            if (
                path.startswith("abfss://")
                or path.startswith("wasbs://")
                or "raw/uploads" in path
            ):
                filename = Path(path).name

                for key in self.parent._client._store:
                    if key.endswith(filename):
                        data = self.parent._client._store[key]
                        df = pd.read_csv(BytesIO(data))
                        return FakeSparkDataFrame(df)
            raise FileNotFoundError(path)

        def csv_with_schema(self, path: str, schema=None, **kwargs):
            return self.csv(path, **kwargs)

    def __init__(self, adls_client: FakeADLSFileClient):
        self._client = adls_client
        self.read = self.Reader(self)
        self.spark_session = self

    @property
    def sparkContext(self):
        """Provide a fake spark context for emptyRDD() calls."""
        mock_sc = MagicMock()
        mock_sc.emptyRDD.return_value = []
        return mock_sc

    def sql(self, query):
        # Return empty DF for any SQL query
        return FakeSparkDataFrame(pd.DataFrame())

    def createDataFrame(self, data, schema=None):
        if isinstance(data, pd.DataFrame):
            return FakeSparkDataFrame(data)
        return FakeSparkDataFrame(pd.DataFrame(data))

    @property
    def catalog(self):
        class FakeCatalog:
            def tableExists(self, *args, **kwargs):
                return False

            def refreshTable(self, *args, **kwargs):
                pass

        return FakeCatalog()


@pytest.fixture
def fake_spark(fake_adls):
    with (
        patch("src.utils.schema.DeltaTable") as mock_dt_schema,
        patch("src.utils.delta.DeltaTable") as mock_dt_delta,
    ):
        # Mock DeltaTable.forName().toDF() to return a FakeSparkDataFrame with basic columns
        mock_dt_instance = MagicMock()
        # Return an empty DF or one with some generic columns?
        # For safety, let's return an empty DF so no new columns are forcibly added.
        mock_dt_instance.toDF.return_value = FakeSparkDataFrame(pd.DataFrame())

        mock_dt_schema.forName.return_value = mock_dt_instance
        mock_dt_delta.forName.return_value = mock_dt_instance

        # Mock createIfNotExists builder
        mock_builder = MagicMock()
        mock_builder.tableName.return_value = mock_builder
        mock_builder.addColumns.return_value = mock_builder
        mock_builder.partitionedBy.return_value = mock_builder
        mock_builder.location.return_value = mock_builder
        mock_builder.property.return_value = mock_builder
        mock_builder.comment.return_value = mock_builder
        mock_builder.execute.return_value = None

        mock_dt_schema.createIfNotExists.return_value = mock_builder
        mock_dt_delta.createIfNotExists.return_value = mock_builder

        yield FakeSpark(fake_adls)


@pytest.fixture
def fake_adls() -> FakeADLSFileClient:
    """
    Provide a FakeADLSFileClient to simulate blob storage.
    """
    client = FakeADLSFileClient()
    with patch(
        "src.utils.data_quality_descriptions.ADLSFileClient", return_value=client
    ):
        yield client


@pytest.fixture
def sample_school_geolocation_csv(tmp_path: Path) -> Path:
    """
    Create a small school_geolocation CSV sample and return its path.
    """
    content = """school_id_giga,school_id_govt,name,latitude,longitude
    S1,0001,Alpha School,12.9716,77.5946
    S2,0002,Beta School,13.0827,80.2707
    """
    p = tmp_path / "school_geolocation_sample.csv"
    p.write_text(content)
    return p


@pytest.fixture
def upload_sample_to_adls(
    fake_adls: FakeADLSFileClient, sample_school_geolocation_csv: Path
) -> str:
    """
    Upload the sample file to fake ADLS under the path the pipeline expects and return the key.
    """

    country = "BRA"
    dataset = "school-geolocation"
    filename = "school_geolocation_sample.csv"
    upload_path = (
        f"{project_constants.UPLOAD_PATH_PREFIX}/{dataset}/{country}/{filename}"
    )

    data = sample_school_geolocation_csv.read_bytes()
    fake_adls.put_file_from_bytes(upload_path, data)

    metadata_path = f"{upload_path}.metadata.json"
    fake_adls.upload_json(
        metadata_path, {"uploader_email": "tester@example.com", "country": country}
    )

    return upload_path


@pytest.fixture
def resources_override(fake_adls: FakeADLSFileClient, fake_spark: FakeSpark) -> dict:
    """
    Returns resource mapping that can be passed to Definitions or execute_in_process run config.
    Key names must match the resources used by your pipeline.
    """

    return {
        "adls_file_client": fake_adls,
        "spark": fake_spark,
    }


@pytest.fixture
def fake_adls_passthrough_io_manager(fake_adls):
    @io_manager
    def _fake_manager(_context):
        class FakePassthroughIOManager(IOManager):
            def handle_output(self, context, obj):
                key = context.asset_key.to_string()
                fake_adls.put_file_from_bytes(key, obj)

            def load_input(self, context):
                key = context.asset_key.to_string()
                return fake_adls._store.get(key)

        return FakePassthroughIOManager()

    return _fake_manager


@pytest.fixture(scope="session")
def spark_session():
    """
    Creates a local SparkSession for testing.
    """
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("pytest-spark")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def mock_adls_client():
    return MagicMock()


@pytest.fixture
def mock_context():
    context = MagicMock()
    context.log = MagicMock()
    context.cursor = None
    return context


@pytest.fixture
def op_context():
    return build_op_context()


@pytest.fixture
def mock_file_config():
    from src.constants import DataTier
    from src.utils.op_config import FileConfig

    return FileConfig(
        filepath="123_BRA_school-coverage_fb_20230101-120000.csv",
        dataset_type="coverage",
        country_code="BRA",
        destination_filepath="test/dest/file.csv",
        metastore_schema="schema",
        tier=DataTier.RAW,
        file_size_bytes=100,
        metadata={},
        database_data="{}",
        dq_target_filepath="test/dq",
        domain="School",
        table_name="table",
    )


@pytest.fixture
def mock_spark_resource(spark_session):
    spark = MagicMock()
    spark.spark_session = spark_session
    return spark


@pytest.fixture(scope="module", autouse=True)
def patch_settings():
    from src.settings import settings

    settings.GIGAMAPS_DB_CONNECTION_STRING = "postgresql://dummy:5432/db"
    settings.GIGAMETER_DB_CONNECTION_STRING = "postgresql://dummy:5432/db"
    yield
