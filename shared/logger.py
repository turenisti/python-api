"""
Structured logging configuration for Python API
Supports both JSON and Console formats, matching Go API's zerolog style
"""

import logging
import json
import sys
import os
from datetime import datetime
from typing import Any, Dict


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "level": record.levelname.lower(),
            "time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z"),
            "message": record.getMessage(),
        }

        # Add extra fields
        if hasattr(record, 'execution_id'):
            log_data['execution_id'] = record.execution_id
        if hasattr(record, 'config_id'):
            log_data['config_id'] = record.config_id
        if hasattr(record, 'duration_ms'):
            log_data['duration_ms'] = record.duration_ms
        if hasattr(record, 'rows'):
            log_data['rows'] = record.rows
        if hasattr(record, 'stage'):
            log_data['stage'] = record.stage
        if hasattr(record, 'query'):
            log_data['query'] = record.query

        # Add exception info if present
        if record.exc_info:
            log_data['error'] = self.formatException(record.exc_info)

        return json.dumps(log_data)


class ConsoleFormatter(logging.Formatter):
    """Console formatter matching Go API's zerolog console format"""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[90m',    # Gray
        'INFO': '\033[32m',     # Green
        'WARNING': '\033[33m',  # Yellow
        'ERROR': '\033[31m',    # Red
        'CRITICAL': '\033[31m\033[1m',  # Bold Red
    }
    RESET = '\033[0m'
    GRAY = '\033[90m'
    CYAN = '\033[36m'

    def format(self, record: logging.LogRecord) -> str:
        # Timestamp in gray
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
        timestamp_colored = f"{self.GRAY}{timestamp}{self.RESET}"

        # Level with color
        level = record.levelname[:3].upper()
        level_colored = f"{self.COLORS.get(record.levelname, '')}{level}{self.RESET}"

        # Message
        message = record.getMessage()

        # Build log line
        log_parts = [timestamp_colored, level_colored, message]

        # Add structured fields in cyan
        if hasattr(record, 'execution_id'):
            log_parts.append(f"{self.CYAN}execution_id={self.RESET}{record.execution_id}")
        if hasattr(record, 'config_id'):
            log_parts.append(f"{self.CYAN}config_id={self.RESET}{record.config_id}")
        if hasattr(record, 'duration_ms'):
            log_parts.append(f"{self.CYAN}duration_ms={self.RESET}{record.duration_ms}")
        if hasattr(record, 'rows'):
            log_parts.append(f"{self.CYAN}rows={self.RESET}{record.rows}")
        if hasattr(record, 'stage'):
            log_parts.append(f"{self.CYAN}stage={self.RESET}{record.stage}")
        if hasattr(record, 'query'):
            log_parts.append(f"{self.CYAN}query={self.RESET}{record.query}")

        # Add exception if present
        if record.exc_info:
            log_parts.append(f"\n{self.formatException(record.exc_info)}")

        return " ".join(log_parts)


def setup_logger(name: str = None, log_format: str = None, log_level: str = None) -> logging.Logger:
    """
    Setup structured logger

    Args:
        name: Logger name (defaults to __name__)
        log_format: 'json' or 'console' (defaults to env LOG_FORMAT or 'console')
        log_level: 'debug', 'info', 'warning', 'error' (defaults to env LOG_LEVEL or 'info')

    Returns:
        Configured logger instance
    """

    # Get configuration from environment or defaults
    log_format = log_format or os.getenv('LOG_FORMAT', 'console')
    log_level = log_level or os.getenv('LOG_LEVEL', 'info')

    # Create logger
    logger = logging.getLogger(name or __name__)
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.handlers.clear()  # Remove existing handlers

    # Create handler
    handler = logging.StreamHandler(sys.stdout)

    # Set formatter based on format
    if log_format.lower() == 'json':
        formatter = StructuredFormatter()
    else:
        formatter = ConsoleFormatter()

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# Helper function to log with structured fields
def log_with_context(logger: logging.Logger, level: str, message: str, **kwargs):
    """
    Log message with additional context fields

    Example:
        log_with_context(logger, 'info', 'Query executed',
                        execution_id='123', duration_ms=500, rows=10)
    """
    extra = kwargs
    log_method = getattr(logger, level.lower())
    log_method(message, extra=extra)
