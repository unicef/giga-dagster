from src.utils.string import _keys_to_snake_case, _snake_case, to_snake_case


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
