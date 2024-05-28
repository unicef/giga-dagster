import re


def _unpack(data):
    if isinstance(data, dict):
        return data.items()
    return data


def _snake_case(value):
    first_underscore = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", first_underscore).lower()


def _keys_to_snake_case(content):
    return {_snake_case(key): value for key, value in content.items()}


def to_snake_case(data):
    formatted = {}

    if isinstance(data, str):
        return _snake_case(data)

    for key, value in _unpack(_keys_to_snake_case(data)):
        if isinstance(value, dict):
            formatted[key] = to_snake_case(value)
        elif isinstance(value, list) and len(value) > 0:
            formatted[key] = []
            for _, val in enumerate(value):
                formatted[key].append(to_snake_case(val))
        else:
            formatted[key] = value
    return formatted
