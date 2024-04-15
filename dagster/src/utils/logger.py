from typing import TypeVar

import loguru

from dagster import DagsterLogManager, OpExecutionContext


def get_context_with_fallback_logger(
    context: OpExecutionContext = None,
) -> DagsterLogManager | type(loguru.logger):
    if context is None:
        return loguru.logger
    return context.log


T = TypeVar("T")


class ContextLoggerWithLoguruFallback:
    def __init__(self, context: OpExecutionContext = None, group: str = None) -> None:
        self.context = context
        self.group = group

    @property
    def log(self) -> DagsterLogManager | type(loguru.logger):
        return get_context_with_fallback_logger(self.context)

    def passthrough(self, expr: T, message: str) -> T:
        if self.group is None:
            self.log.info(message)
        else:
            self.log.info(f"[{self.group}] {message}")
        return expr
