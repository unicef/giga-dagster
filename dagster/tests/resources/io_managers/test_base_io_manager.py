from src.resources.io_managers.base import BaseConfigurableIOManager


class TestIOManager(BaseConfigurableIOManager):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):
        pass


def test_base_io_manager_exists():
    assert BaseConfigurableIOManager is not None
    assert TestIOManager is not None
    test_instance = TestIOManager()
    assert test_instance is not None


def test_get_filepath_methods_exist():
    assert hasattr(BaseConfigurableIOManager, "_get_filepath")
    assert hasattr(BaseConfigurableIOManager, "_get_schema_name")
    assert hasattr(BaseConfigurableIOManager, "_get_table_path")
    assert hasattr(BaseConfigurableIOManager, "_get_type_transform_function")
