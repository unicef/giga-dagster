from unittest.mock import patch


def test_get_country_codes_list():
    from src.utils.country import get_country_codes_list

    result = get_country_codes_list()
    assert isinstance(result, list)
    assert len(result) > 0
    assert "USA" in result
    assert "IND" in result


def test_string_unpack_list():
    from src.utils.string import (
        _keys_to_snake_case,
        _snake_case,
        _unpack,
        to_snake_case,
    )

    # Test _unpack with a list (covers line 7)
    data = [("a", 1), ("b", 2)]
    assert _unpack(data) == data

    # Test _unpack with a dict
    assert list(_unpack({"a": 1})) == [("a", 1)]

    # Test _snake_case
    assert _snake_case("camelCase") == "camel_case"
    assert _snake_case("PascalCase") == "pascal_case"

    # Test _keys_to_snake_case
    assert _keys_to_snake_case({"camelCase": 1}) == {"camel_case": 1}

    # Test to_snake_case with string
    assert to_snake_case("camelCase") == "camel_case"

    # Test to_snake_case with nested dict
    result = to_snake_case({"camelCase": {"nestedKey": "value"}})
    assert result == {"camel_case": {"nested_key": "value"}}

    # Test to_snake_case with list values
    result = to_snake_case({"myList": [{"innerKey": "val"}]})
    assert result == {"my_list": [{"inner_key": "val"}]}

    # Test to_snake_case with empty list
    result = to_snake_case({"emptyList": []})
    assert result == {"empty_list": []}


def test_format_changes_for_slack_message():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame(
        [("col1", "added", 5), ("col2", "modified", 3)],
        ["column_name", "operation", "change_count"],
    )
    from src.utils.send_slack_master_release_notification import (
        format_changes_for_slack_message,
    )

    result = format_changes_for_slack_message(df)
    assert "col1" in result
    assert "col2" in result
    assert "added" in result
    assert "```" in result


def test_slack_props():
    from src.utils.send_slack_master_release_notification import SlackProps

    props = SlackProps(
        country="TestCountry",
        added=10,
        modified=5,
        deleted=2,
        updateDate="2024-01-01",
        version=1,
        rows=100,
        column_changes="test",
    )
    assert props.country == "TestCountry"
    assert props.added == 10


async def test_send_slack_master_release_notification():
    from src.utils.send_slack_master_release_notification import (
        SlackProps,
        send_slack_master_release_notification,
    )

    props = SlackProps(
        country="TestCountry",
        added=10,
        modified=5,
        deleted=2,
        updateDate="2024-01-01",
        version=1,
        rows=100,
        column_changes="test_changes",
    )

    with patch(
        "src.utils.send_slack_master_release_notification.send_slack_base"
    ) as mock_slack:
        await send_slack_master_release_notification(props)
        mock_slack.assert_called_once()
        call_text = mock_slack.call_args[0][0]
        assert "TESTCOUNTRY" in call_text
        assert "10" in call_text
        assert "5" in call_text
        assert "2" in call_text
