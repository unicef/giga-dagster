from src.utils.string import _keys_to_snake_case, _snake_case, _unpack, to_snake_case


def test_to_snake_case_string():
    assert to_snake_case("CamelCase") == "camel_case"
    assert to_snake_case("XMLHttpRequest") == "xml_http_request"
    assert to_snake_case("simple") == "simple"


def test_to_snake_case_dict():
    data = {"FirstName": "John", "LastName": "Doe"}
    result = to_snake_case(data)
    assert result == {"first_name": "John", "last_name": "Doe"}


def test_to_snake_case_nested_dict():
    data = {"UserInfo": {"FirstName": "John", "LastName": "Doe"}}
    result = to_snake_case(data)
    assert result == {"user_info": {"first_name": "John", "last_name": "Doe"}}


def test_to_snake_case_list():
    data = {"Items": [{"ItemName": "Apple"}, {"ItemName": "Banana"}]}
    result = to_snake_case(data)
    assert result == {"items": [{"item_name": "Apple"}, {"item_name": "Banana"}]}


def test_to_snake_case_empty_list():
    data = {"EmptyList": []}
    result = to_snake_case(data)
    assert result == {"empty_list": []}


def test_snake_case_helper():
    assert _snake_case("CamelCase") == "camel_case"
    assert _snake_case("HTTPSConnection") == "https_connection"


def test_keys_to_snake_case():
    data = {"FirstName": "value", "LastName": "value2"}
    result = _keys_to_snake_case(data)
    assert "first_name" in result
    assert "last_name" in result


# Negative test cases
def test_to_snake_case_empty_string():
    """Edge case: empty string should return empty."""
    assert to_snake_case("") == ""


def test_to_snake_case_special_characters():
    """Edge case: special characters in strings."""
    # The _snake_case function inserts underscores before capital letters
    # So "Special@Character#123" -> "special@_character#123"
    assert to_snake_case("Special@Character#123") == "special@_character#123"


def test_snake_case_single_character():
    """Edge case: single character."""
    assert _snake_case("A") == "a"


def test_keys_to_snake_case_empty_dict():
    """Edge case: empty dictionary."""
    result = _keys_to_snake_case({})
    assert result == {}


def test_unpack_dict():
    """Test _unpack returns dict items for a dict."""
    data = {"A": 1, "B": 2}
    result = list(_unpack(data))
    assert ("A", 1) in result
    assert ("B", 2) in result


def test_unpack_non_dict():
    """Test _unpack returns the data itself for a non-dict."""
    data = [1, 2, 3]
    assert _unpack(data) is data


def test_to_snake_case_dict_with_list_values():
    """Test to_snake_case on dict with list of dicts."""
    data = {"ItemList": [{"ItemName": "Apple"}, {"ItemName": "Banana"}]}
    result = to_snake_case(data)
    assert result == {"item_list": [{"item_name": "Apple"}, {"item_name": "Banana"}]}


def test_to_snake_case_dict_with_empty_list():
    """Test to_snake_case on dict with empty list value."""
    data = {"MyList": []}
    result = to_snake_case(data)
    assert result == {"my_list": []}
