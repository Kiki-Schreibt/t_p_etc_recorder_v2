# logger.py

import os
import logging
import atexit
from datetime import datetime

from src.infrastructure.utils.standard_paths import standard_log_dir
from src.infrastructure.core.global_vars import log_file_name

"""
Custom logger module that mimics the standard logging interface.
"""

LOG_DIRECTORY = standard_log_dir
LOG_FILE_NAME = log_file_name
LOG_FILE = None
_logger_initialized = False


def basicConfig(**kwargs):
    logging.basicConfig(**kwargs)


def getLogger(name=None):
    return logging.getLogger(name)


def shutdown():
    logging.shutdown()


def init_logging():
    global _logger_initialized, LOG_FILE
    if not _logger_initialized:
        create_log_dir()

        # Set up logging with FileHandler
        LOG_FILE = os.path.join(
            LOG_DIRECTORY,
            datetime.now().strftime('%Y-%m-%d_%H') + '_' + LOG_FILE_NAME
        )

        setup_logger()
        atexit.register(close_logging_handlers, logging.getLogger())
        _logger_initialized = True


def setup_logger():
    # Clear existing handlers if re-running this setup
    logger = logging.getLogger()
    if logger.handlers:
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)

    # Configure new logging handlers
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a'),
            logging.StreamHandler()  # Optional: also log to stderr
        ]
    )


def create_log_dir():
    try:
        if not os.path.exists(LOG_DIRECTORY):
            os.makedirs(LOG_DIRECTORY)
    except Exception as e:
        print(f"Failed to create log directory {LOG_DIRECTORY}: {e}")


def close_logging_handlers(logger):
    """
    Close all handlers of the specified logger to ensure all resources are properly released.

    Parameters:
        logger (logging.Logger): The logger whose handlers should be closed.
    """
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

# Initialize logging when the module is imported
init_logging()
