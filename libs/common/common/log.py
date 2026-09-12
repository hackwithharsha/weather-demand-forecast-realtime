"""
Structured JSON logging via structlog.

Usage
-----
Call ``configure_logging()`` once at service startup (before any log output),
then obtain loggers with ``get_logger``:

    from common.log import configure_logging, get_logger

    configure_logging(level="INFO")
    log = get_logger(__name__)
    log.info("server_started", port=8000)
"""

import logging
import sys

import structlog


def configure_logging(level: str = "INFO") -> None:
    """Wire structlog to stdlib and emit JSON to stdout."""
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


# Drop-in replacement for logging.getLogger / structlog.get_logger.
get_logger = structlog.get_logger
