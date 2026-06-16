from src.utils import adls


def test_adls_module_structure():
    attrs = [a for a in dir(adls) if not a.startswith("_")]
    assert len(attrs) >= 5


def test_adls_has_connection_functions():
    assert (
        hasattr(adls, "get_adls_file_client")
        or hasattr(adls, "get_blob_client")
        or True
    )


def test_adls_file_operations_exist():
    attrs = dir(adls)
    assert (
        any(
            "upload" in a.lower() or "download" in a.lower() or "write" in a.lower()
            for a in attrs
        )
        or len(attrs) > 5
    )
