import os
import logging
import atexit
from datetime import datetime

from src.standard_paths import standard_log_dir


class AppLogger:
    """
    A class for setting up and providing access to a logger.

    This class configures a logger using settings from a configuration file. It sets up
    logging to a specified file and optionally to the standard error stream.

    Attributes:
        LOG_DIRECTORY (str): Directory where the log files are stored.
        LOG_FILE_NAME (str): Name of the log file.
        LOG_FILE (str): Full path of the log file.

    Methods:
        setup_logger(): Configures the logger with a FileHandler and optional StreamHandler.
        get_logger(logger_name): Retrieves a logger with the specified name.
        create_log_dir(): Creates the log directory if it does not exist.
    """
    LOG_DIRECTORY   = standard_log_dir
    LOG_FILE_NAME        = 'Application_Log.log'
    _logger_initialized = False

    def __init__(self):
        if not AppLogger._logger_initialized:

            self.create_log_dir()

            # Set up logging with FileHandler
            self.LOG_FILE = os.path.join(
                                            self.LOG_DIRECTORY,
                                            datetime.now().strftime('%Y-%m-%d_%H') + '_' + self.LOG_FILE_NAME
                                        )

            self.setup_logger()
            atexit.register(self.close_logging_handlers, logging.getLogger())
        AppLogger._logger_initialized = True

    def setup_logger(self):
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
                logging.FileHandler(self.LOG_FILE, mode='a'),
                logging.StreamHandler()  # Optional: also log to stderr
            ]
        )

    def get_logger(self, logger_name):
        return logging.getLogger(logger_name)

    def create_log_dir(self):
        try:
            if not os.path.exists(self.LOG_DIRECTORY):
                os.makedirs(self.LOG_DIRECTORY)
        except Exception as e:
            print(f"Failed to create log directory {self.LOG_DIRECTORY}: {e}")

    def close_logging_handlers(self, logger):
        """
        Close all handlers of the specified logger to ensure all resources are properly released.

        Parameters:
            logger (logging.Logger): The logger whose handlers should be closed.
        """
        # Loop over a copy of the handlers list to avoid modifying the list during iteration
        for handler in logger.handlers[:]:  # Copy the list to avoid modification issues
            handler.close()                 # Close each handler to flush and release resources
            logger.removeHandler(handler)   # Remove the handler from the logger
