from collections.abc import Generator
from types import ModuleType
from typing import TypeVar

T = TypeVar("T")


def _find_definition_in_module(
    module: ModuleType, definition: T | tuple[T]
) -> Generator[T, None, None]:
    for attr in dir(module):
        value = getattr(module, attr)
        if isinstance(value, definition):
            yield value
        elif isinstance(value, list) and all(
            isinstance(el, definition) for el in value
        ):
            yield from value
