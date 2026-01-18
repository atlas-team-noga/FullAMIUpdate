"""
Utilities for logging and duration formatting.

This module provides utilities to set up and configure loggers, log messages
with optional error contexts, and format durations in a human-readable format.
It includes pre-configured loggers suitable for general and error-specific
logging purposes.
"""

import logging
import json
from datetime import datetime


def setup_logger(log_name, log_file, level=logging.INFO, clear_on_start=False):
    """
    Sets up and configures a logger instance with the specified name, log file, and log
    level. Optionally clears the log file's contents at the start.

    :param log_name: The name of the logger to configure.
    :type log_name: str
    :param log_file: The path to the log file where log messages will be written.
    :type log_file: str
    :param level: The logging level for filtering log messages. Defaults to `logging.INFO`.
    :type level: int
    :param clear_on_start: If `True`, clears the log file contents before logging starts.
        Defaults to `False`.
    :type clear_on_start: bool
    :return: Configured logger instance.
    :rtype: logging.Logger
    """
    logger = logging.getLogger(log_name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers
    if not logger.handlers:
        # Clear log file if requested
        if clear_on_start:
            open(log_file, 'w', encoding='utf-8').close()

        handler = logging.FileHandler(log_file, encoding='utf-8')
        handler.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Pre-configured loggers (cleared at each program start)
main_logger = setup_logger('main', 'application.log', logging.INFO, clear_on_start=True)
error_logger = setup_logger('errors', 'errors.log', logging.ERROR, clear_on_start=True)


def log_and_print(message, level="info", logger=None):
    """
    Logs and prints a message while supporting different log levels and a custom logger. If no
    logger is provided, the default logger will be used. Messages with severity levels 'error'
    and 'warning' are additionally logged to a dedicated error logger.

    :param message: The message to be logged and printed.
    :type message: str
    :param level: The severity level of the message. Defaults to "info".
    :type level: str
    :param logger: Optional custom logger object. Defaults to None, which uses the main logger.
    :type logger: logging.Logger, optional
    :return: None
    """
    print(message)

    if logger is None:
        logger = main_logger

    getattr(logger, level)(message)

    # Also log to error logger for errors and warnings
    if level in ["error", "warning"]:
        getattr(error_logger, level)(message)


def log_error_with_context(context, error_message, details=None, logger=None):
    """
    Logs an error message along with its context and optional details, and returns a dictionary representation of the
    logged error entry in JSON format. The error message will be logged to a specified logger if provided, and a formatted
    error message will be printed and logged.

    :param context: The contextual information about where the error occurred.
    :type context: str
    :param error_message: A brief descriptive message explaining the error.
    :type error_message: str
    :param details: Optional additional key-value details about the error. Default is None.
    :type details: dict, optional
    :param logger: Optional logger instance to use for logging the error. If not provided, default logging is used.
                   Default is None.
    :type logger: logging.Logger, optional
    :return: A dictionary representation of the error entry containing the timestamp, context, error message,
             and additional details if provided.
    :rtype: dict
    """
    error_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "context": context,
        "error": error_message
    }

    if details:
        error_entry.update(details)

    error_json = json.dumps(error_entry, ensure_ascii=False, indent=2)

    log_and_print(f"ERREUR [{context}]: {error_message}", "error", logger)
    error_logger.error(f"CONTEXT_ERROR: {error_json}")

    return error_entry


def format_duration(seconds):
    """
    Format a given duration in seconds into a human-readable string.

    This function converts a numeric duration in seconds into an appropriate
    string representation, breaking it down into hours, minutes, and seconds
    as needed. It ensures readability by formatting the output according to
    the range within which the input ``seconds`` falls.

    :param seconds: Duration in seconds to be formatted. Must be a non-negative value.
    :type seconds: float
    :return: A formatted duration string. The format will be:
             - "{seconds:.1f}s" for durations less than 60 seconds,
             - "{minutes}m {remaining_seconds:.1f}s" for durations between 60 and 3600 seconds,
             - "{hours}h {minutes}m {remaining_seconds:.1f}s" for durations of 3600 seconds or more.
    :rtype: str
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.1f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        remaining_seconds = seconds % 60
        return f"{hours}h {remaining_minutes}m {remaining_seconds:.1f}s"