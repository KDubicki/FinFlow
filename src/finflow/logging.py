"""Structured logging setup.

Console rendering when attached to a terminal, JSON otherwise, so local runs stay
readable while orchestrated runs stay parseable. Call :func:`configure_logging`
once at process start; use :func:`get_logger` everywhere else.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from finflow.config import Settings, get_settings


def configure_logging(settings: Settings | None = None) -> None:
    """Configure structlog and the stdlib logging bridge.

    Idempotent: calling it more than once reconfigures rather than stacking
    processors, which matters because orchestrators may import entrypoints twice.
    """
    settings = settings or get_settings()
    use_json = settings.log_json if settings.log_json is not None else not sys.stderr.isatty()

    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    structlog.configure(
        processors=[*shared, structlog.processors.format_exc_info, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[settings.log_level.value]
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # Route stdlib loggers (httpx, dbt, pyspark) through the same handler so
    # third-party output does not bypass our formatting.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=settings.log_level.value,
        force=True,
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound logger, optionally named after the calling module.

    The name is bound as an ordinary event key rather than through
    ``stdlib.add_logger_name``, because the configured logger factory writes
    directly to stderr and has no stdlib ``name`` attribute for that processor
    to read.
    """
    logger: structlog.stdlib.BoundLogger = structlog.get_logger()
    if name is not None:
        logger = logger.bind(logger=name)
    return logger
