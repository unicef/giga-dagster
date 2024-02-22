from collections.abc import Generator
from types import ModuleType
from typing import TypeVar

T = TypeVar("T")


def _find_definition_in_module(
    module: ModuleType, definition: T | tuple[T]
) -> Generator[T, None, None]:
    for leaf in dir(module):
        submodule = getattr(module, leaf)
        if isinstance(submodule, definition):
            yield submodule
        elif isinstance(submodule, list) and all(
            isinstance(sub, definition) for sub in submodule
        ):
            yield from submodule
