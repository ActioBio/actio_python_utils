"""
Logging-related functionality.
"""
import _io
import logging
from contextlib import contextmanager
from typing import Any, Optional
from .utils import cfg


class LazyLogger(logging.Logger):
    """
    Wraps a Logger to accept either a message or a function to evaluate to
    produce the desired message.  If a function is provided it is evaluated
    lazily, i.e. only if the logger is enabled for the appropriate level.
    """

    def _get_msg(self, msg_or_func: Any) -> Any:
        """
        Evaluates and returns msg_or_func() if it is callable, otherwise just
        returns it

        :param msg_or_func: Any
        :return: The logging message or callable to evaluate
        :rtype: Any
        """
        if callable(msg_or_func):
            return msg_or_func()
        else:
            return msg_or_func

    def debug(self, msg_or_func: Any, *args, **kwargs) -> None:
        if self.isEnabledFor(logging.DEBUG):
            super().debug(self._get_msg(msg_or_func), *args, **kwargs)

    def info(self, msg_or_func: Any, *args, **kwargs) -> None:
        if self.isEnabledFor(logging.INFO):
            super().info(self._get_msg(msg_or_func), *args, **kwargs)

    def warning(self, msg_or_func: Any, *args, **kwargs) -> None:
        if self.isEnabledFor(logging.WARNING):
            super().warning(self._get_msg(msg_or_func), *args, **kwargs)

    def warn(self, msg_or_func: Any, *args, **kwargs) -> None:
        if self.isEnabledFor(logging.WARNING):
            super().warn(self._get_msg(msg_or_func), *args, **kwargs)

    def error(self, msg_or_func: Any, *args, **kwargs) -> None:
        if self.isEnabledFor(logging.ERROR):
            super().error(self._get_msg(msg_or_func), *args, **kwargs)

    def exception(self, msg_or_func: Any, *args, exc_info=True, **kwargs) -> None:
        super().exception(
            self._get_msg(msg_or_func), *args, exc_info=exc_info, **kwargs
        )

    def critical(self, msg_or_func: Any, *args, **kwargs) -> None:
        if self.isEnabledFor(logging.CRITICAL):
            super().critical(self._get_msg(msg_or_func), *args, **kwargs)

    def fatal(self, msg_or_func: Any, *args, **kwargs) -> None:
        if self.isEnabledFor(logging.CRITICAL):
            super().fatal(self._get_msg(msg_or_func), *args, **kwargs)


logging.setLoggerClass(
    LazyLogger
)  # enables new non-root loggers to use this functionality


@contextmanager
def log(level: int | str) -> None:
    """
    Used as a context manager to log at the specified level temporarily
    and return to the previous level after exiting

    :param level: The logging level to use temporarily
    :type level: int or str
    """
    logger = logging.getLogger()
    current_level = logger.getEffectiveLevel()
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(current_level)


def setup_logging(
    logging_level: int | str = cfg["logging"]["level"],
    name: str = "root",
    stream: Optional[_io.TextIOWrapper] = None,
    stream_handler_logging_level: Optional[int | str] = None,
    format_string: str = cfg["logging"]["format"],
    loggers_to_ignore: Optional[list[str]] = cfg["logging"]["loggers_to_ignore"],
) -> None:
    """
    Set up the logger given by name, attach a stream handler, set the format,
    and log levels as specified.  For logger names in loggers_to_ignore, their
    levels are instead set to logging.CRITICAL

    :param logging_level: The logging level to use, defaults to
        cfg["logging"]["level"]
    :type logging_level: int or str
    :param str name: The name of the logger to configure, defaults to "root"
    :param stream: An optional output stream to log to, defaults to None
    :type stream: _io.TextIOWrapper or None
    :param stream_handler_logging_level: The logging level to use for the
        handler; uses logging_level if not specified, defaults to None
    :type stream_handler_logging_level: int or str or None
    :param str format_string: How to format log messages, defaults to
        cfg["logging"]["format"]
    :param loggers_to_ignore: A list of logger names for which to set their
        logging levels to logging.CRITICAL, defaults to
        cfg["logging"]["loggers_to_ignore"]
    :type loggers_to_ignore: list or None
    """
    logger_to_configure = logging.getLogger(name)
    if logger_to_configure.hasHandlers():
        # don't add another handler if defined
        return
    logger_to_configure.setLevel(logging_level)
    ch = logging.StreamHandler(stream)
    if stream_handler_logging_level is None:
        stream_handler_logging_level = logging_level
    ch.setLevel(stream_handler_logging_level)
    formatter = logging.Formatter(format_string)
    ch.setFormatter(formatter)
    logger_to_configure.addHandler(ch)
    if loggers_to_ignore:
        for logger in loggers_to_ignore:
            logging.getLogger(logger).setLevel(logging.CRITICAL)
