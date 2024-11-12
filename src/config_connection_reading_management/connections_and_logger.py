import os
import logging
import time
import atexit
import pyodbc
from datetime import datetime

from pymodbus.client import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ModbusException, ConnectionException
from psycopg2.extras import RealDictCursor  # Optional: for dictionary-like cursor
import psycopg2

from src.config_connection_reading_management.config_reader import GetConfig



# Load the configuration
config = GetConfig()
# Accessing the variables
LOG_DIRECTORY   = config.LOG_DIRECTORY
LOG_FILE        = config.LOG_FILE
DB_SERVER       = config.DB_SERVER
DB_DATABASE     = config.DB_DATABASE
DB_USERNAME     = config.DB_USERNAME
DB_PASSWORD     = config.DB_PASSWORD
DB_PORT         = config.DB_PORT
MODBUS_HOST     = config.MODBUS_HOST
MODBUS_PORT     = config.MODBUS_PORT
REGS_OF_INTEREST = config.REGS_OF_INTEREST
START_REG       = config.START_REG
END_REG         = config.END_REG
SLEEP_INTERVAL  = config.SLEEP_INTERVAL

TP_DATA_TABLE_NAME = config.TP_DATA_TABLE_NAME
THERMAL_CONDUCTIVITY_DATA_TABLE_NAME = config.THERMAL_CONDUCTIVITY_DATA_TABLE_NAME
THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME = config.THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME



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

    _logger_initialized = False

    def __init__(self):
        if not AppLogger._logger_initialized:
            self.LOG_DIRECTORY = config.LOG_DIRECTORY
            self.LOG_FILE_NAME = config.LOG_FILE
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


class DatabaseConnection:
    """
    Class for managing database connections using psycopg2.

    This class is intended to be used as a context manager that facilitates opening and
    closing PostgreSQL database connections.

    Attributes:
        conn (psycopg2.Connection): Database connection object.
        cursor (psycopg2.Cursor): Cursor object for database operations.
        logger (Logger): Logger for logging messages.
        auto_close (bool): Flag to indicate automatic closing of the connection.
    """

    def __init__(self):
        self.conn = None
        self.cursor = None
        self.logger = AppLogger().get_logger(__name__)
        self.auto_close = True

    def __enter__(self):
        self.open_connection(auto_close=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            # An exception occurred, rollback the transaction
            self.conn.rollback()
            print("Transaction rolled back due to an exception.")
        else:
            # No exception, commit the transaction
            self.conn.commit()
            #print("Transaction committed successfully.")

        if self.auto_close:
            self.close_connection()

    def open_connection(self, auto_close=False):
        if self.conn is None:
            conn_str = f"dbname={DB_DATABASE} user={DB_USERNAME} password={DB_PASSWORD} host={DB_SERVER} port={DB_PORT}"
            try:
                self.conn = psycopg2.connect(conn_str)
                self.cursor = self.conn.cursor()  # Optional
                self.logger.info("Database connection opened.")
                self.auto_close = auto_close
            except Exception as e:
                self.logger.error("Database connection error: %s", str(e))
                raise

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Database connection closed.")
        self.conn = None
        self.cursor = None


class ModbusConnection:
    """
    Class for managing Modbus TCP connections.

    This class is designed to handle connections with a Modbus TCP server, using the pymodbus
    library. It can be used as a context manager to ensure that connections are properly
    opened and closed.

    Attributes:
        client (ModbusClient): Modbus TCP client object.
        logger (Logger): Logger for logging messages.

    Methods:
        connect(): Establishes a connection with the Modbus TCP server.
        close(): Closes the Modbus TCP connection.
        is_connected(): Checks if the Modbus TCP connection is still active.
        reconnect(): Re-establishes the Modbus TCP connection.
    """

    def __init__(self, mb_host=MODBUS_HOST, mb_port=MODBUS_PORT):
        self.client = None
        self.logger = AppLogger().get_logger(__name__)
        self.mb_host = mb_host
        self.mb_port = mb_port

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        try:
            self.client = ModbusClient(host=self.mb_host, port=self.mb_port)
            if not self.client.connect():
                self.logger.error("Failed to connect to Modbus server at %s:%s", self.mb_host, self.mb_port)
                raise ConnectionException(f"Unable to connect to {self.mb_host}:{self.mb_port}")
            self.logger.info("Connected to Modbus server at %s:%s.", self.mb_host, self.mb_port)
        except ConnectionException as e:
            self.logger.error("ConnectionException: %s", e)
            raise
        except Exception as e:
            self.logger.error("Unexpected error during Modbus connection: %s", e)
            raise

    def close(self):
        if self.client:
            self.client.close()
            self.logger.info("Modbus connection closed.")

    def is_connected(self):
        # Implement logic to check if the client is still connected
        # This depends on the capabilities of your ModbusClient library
        return self.client and self.client.is_socket_open()

    def reconnect(self, retries=3, delay=5):
        self.logger.info("Attempting to reconnect to Modbus server...")
        self.close()
        for attempt in range(1, retries + 1):
            try:
                self.connect()
                if self.is_connected():
                    self.logger.info("Reconnected to Modbus server.")
                    return True
            except Exception as e:
                self.logger.error(f"Reconnect attempt {attempt} failed: {e}")
                time.sleep(delay)
        self.logger.error("Failed to reconnect after %s attempts.", retries)
        return False


def test_functionality():
    # Test the AppLogger
    logger = AppLogger().get_logger("TestApp")
    logger.info("Testing AppLogger functionality.")

    # Test the DatabaseConnection
    try:
        with DatabaseConnection() as db_conn:
            logger.info("Testing DatabaseConnection functionality.")
            # Perform a simple database operation, e.g., fetch version
            db_conn.cursor.execute("SELECT version();")
            version = db_conn.cursor.fetchone()
            logger.info(f"Database version: {version}")
    except Exception as e:
        logger.error(f"DatabaseConnection test failed: {e}")

    # Test the ModbusConnection
    try:
        with ModbusConnection() as modbus_connection:
            logger.info("Testing ModbusConnection functionality.")
            # Perform a simple Modbus operation, e.g., read a register
            if END_REG-START_REG % 2 == 0:
                final_reg = END_REG-START_REG
            else:
                final_reg = END_REG-START_REG+1

            result = modbus_connection.client.read_holding_registers(START_REG, final_reg, 255)
            logger.info(f"Modbus register read result: {result}")
            logger.info(f"Alles Chacha hier")
    except Exception as e:
        logger.error(f"ModbusConnection test failed: {e}")

    logger.info("Functionality test completed.")


def test_connection_simulated_modbus():
    from pymodbus.pdu import ExceptionResponse
    with ModbusConnection(mb_host="127.0.0.1", mb_port=5020) as modbus_connection:
        try:
            # Example: Reading holding registers starting at address 0, quantity of 2
            address = 0
            count = 2
            result = modbus_connection.client.read_holding_registers(address, count)
            if isinstance(result, ExceptionResponse):
                print("Error reading registers")
            else:
                print("Register values:", result.registers)
        except Exception as e:
            print("An error occurred:", e)

def test_mb_connection():

    logger = AppLogger().get_logger("TestApp")
    with ModbusConnection() as connection:
        try:
            logger.info("Testing ModbusConnection functionality.")
            if END_REG-START_REG % 2 == 0:
                final_reg = END_REG-START_REG
            else:
                final_reg = END_REG-START_REG+1
            result = connection.client.read_holding_registers(START_REG, final_reg, 255)
            if result.isError():
                logger.error(f"Error reading registers: {result}")
            else:

                logger.info(f"Modbus register read result: {result}")
        except ModbusException as e:
            logger.error(f"Modbus communication error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
    logger.info("Functionality test completed.")

# Run the test function
if __name__ == "__main__":
    test_functionality()
    #test_mb_connection()
    #test_connection_simulated_modbus()
