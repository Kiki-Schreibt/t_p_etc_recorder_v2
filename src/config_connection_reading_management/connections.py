#connections.py
import time
import socket

from pymodbus.client import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ModbusException, ConnectionException
import psycopg2

try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging


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

    Usage Example:
    with DatabaseConnection(**db_conn_params) as db_conn:
        db_conn.cursor.executemany(query, values)
        records = db_conn.cursor.fetchall()

    """

    def __init__(self, DB_SERVER=None, DB_DATABASE=None, DB_USERNAME=None, DB_PASSWORD=None, DB_PORT=None):
        """
        Initialises empty attributes for later use and sets up logging for DatabaseConnection
        """
        self.conn = None
        self.cursor = None
        self.logger = logging.getLogger(__name__)
        self.auto_close = True
        self.DB_SERVER       = DB_SERVER or 'localhost'
        self.DB_DATABASE     = DB_DATABASE or 'postgres'
        self.DB_USERNAME     = DB_USERNAME or 'postgres'
        self.DB_PASSWORD     = DB_PASSWORD or "pw12345"
        self.DB_PORT         = DB_PORT or 502

    def __enter__(self):
        """Entry for context manager
        :returns DatabaseConnection, self
        """
        self.open_connection(auto_close=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit point for context manager. Closes connection, rolls back current transactions."""

        if exc_type is not None:
            # An exception occurred, rollback the transaction
            self.conn.rollback()
            self.logger.error("Transaction rolled back due to an exception.")
        else:
            # No exception, commit the transaction
            self.conn.commit()
            #print("Transaction committed successfully.")

        if self.auto_close:
            self.close_connection()

    def open_connection(self, auto_close=False):
        """Can be used in case manually opening a connection is wanted.
        Arguments:
             auto_close (bool): Dont change. Just important for context manager. If you open manually, also close manually.
        """
        if self.conn is None:
            conn_str = f"dbname={self.DB_DATABASE} user={self.DB_USERNAME} password={self.DB_PASSWORD} host={self.DB_SERVER} port={self.DB_PORT}"
            try:
                self.conn = psycopg2.connect(conn_str)
                self.cursor = self.conn.cursor()  # Optional
                self.logger.debug("Database connection opened.")
                self.auto_close = auto_close
            except Exception as e:
                self.logger.error("Database connection error: %s", str(e))
                raise

    def close_connection(self):
        """can be used for closing connection manually"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        self.logger.debug("Database connection closed.")
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

    Usage Example:
    with ModbusConnection(**mb_conn_params) as mb_conn:
        mb_conn.client.read_holding_registers(START_REG, FINAL_REG, MODBUS_SLAVE_ID)
    """

    def __init__(self, MB_HOST=None, MB_PORT=None):
        self.client = None
        self.logger = logging.getLogger(__name__)
        self.MB_HOST = MB_HOST or "192.168.178.1"
        self.MB_PORT = MB_PORT or 502

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        try:
            self.client = ModbusClient(host=self.MB_HOST, port=self.MB_PORT)
            if not self.client.connect():
                self.logger.error("Failed to connect to Modbus server at %s:%s", self.MB_HOST, self.MB_PORT)
                raise ConnectionException(f"Unable to connect to {self.MB_HOST}:{self.MB_PORT}")
            self.logger.info("Connected to Modbus server at %s:%s.", self.MB_HOST, self.MB_PORT)
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
                delay *= 2
        self.logger.error("Failed to reconnect after %s attempts.", retries)
        return False


class HotDiskConnection:
    """
    Class for managing HotDisk Constants Analyzer TCP/IP connections.

    This class is designed to handle connections with the constants analyzer thermal conductivity measurement software which acts as server, using the socket
    library. It can be used as a context manager to ensure that connections are properly
    opened and closed.

    Attributes:
        HD_HOST (str): Host address.
        HD_PORT (int): Host port
        logger (Logger): Logger for logging messages.
        sock (socket): socket to connect

    Methods:
        connect(): Establishes a connection with the TCP server.
        disconnect(): Closes the TCP/IP connection.
        send_command(str): Sends command to TCP server
        receive_response(): Receives response from TCP server
        send_command_receive_response: Sends a command to TCP server and returns the response

    Usage Example:
    with HotDiskConnection() as hd_conn:
        hd_conn.send_command_receive_response(f"*IDN?")  #returns hot disk id
    """

    def __init__(self, HD_HOST=None, HD_PORT=None):
        self.logger = logging.getLogger(__name__)
        self.HD_HOST = HD_HOST or 'localhost'
        self.HD_PORT = HD_PORT or 50009
        self.sock = None

    def __enter__(self):
        self.connect()
        #self.send_command("CONFIRM ON")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        # Return False to propagate exceptions
        return False

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(10)  # Set timeout [s]
            self.logger.info(f"Connecting to {self.HD_HOST}:{self.HD_PORT}...")
            self.sock.connect((self.HD_HOST, self.HD_PORT))
            self.logger.info("Connected.")
            return self
        except Exception as e:
            self.logger.error(f"An error occurred during connection: {e}")
            raise

    def send_command(self, command):
        """sends command to hot disk constants analyzer
        :argument
        command (str): the command that should be sent
        """
        if not self.sock:
            self.logger.error("Not connected to the server.")
            return
        try:
            self.sock.sendall(command.encode('utf-8') + b'\r\n')
            self.logger.info(f"Sent command: {command}")
        except Exception as e:
            self.logger.error(f"An error occurred while sending command: {e}")

    def receive_response(self):
        try:
            response = self.sock.recv(4096)
            decoded_response = response.decode('utf-8')
            self.logger.debug(f"Received response: {decoded_response}")
            return decoded_response
        except socket.timeout:
            self.logger.warning("No response received within timeout period.")
        except Exception as e:
            self.logger.error(f"An error occurred while receiving response: {e}")

    def send_command_receive_response(self, command):
        self.send_command(command=command)
        return self.receive_response()

    def disconnect(self):
        if self.sock:
            try:
                self.sock.close()
                self.sock = None
                self.logger.info("Disconnected from the server.")
            except Exception as e:
                self.logger.error(f"Couldn't disconnect from server: {e}")


def test_functionality(config):
    test_db_connection(config=config)
    test_mb_connection(config=config)


def test_db_connection(config):
    # Test the DatabaseConnection
    # Test the AppLogger
    logger = logging.getLogger("TestApp")
    logger.info("Testing AppLogger functionality.")

    try:
        with DatabaseConnection(**config.db_conn_params) as db_conn:
            logger.info("Testing DatabaseConnection functionality.")
            # Perform a simple database operation, e.g., fetch version
            db_conn.cursor.execute("SELECT version();")
            version = db_conn.cursor.fetchone()
            logger.info(f"Database version: {version}")
    except Exception as e:
        logger.error(f"DatabaseConnection test failed: {e}")


def test_mb_connection(config):
    # Test the ModbusConnection
    # Test the AppLogger
    logger = logging.getLogger("TestApp")
    logger.info("Testing AppLogger functionality.")
    END_REG = config.mb_reading_params["END_REG"]
    START_REG = config.mb_reading_params["START_REG"]
    REGS_OF_INTEREST = config.mb_reading_params["REGS_OF_INTEREST"]
    SLEEP_INTERVAL = config.mb_reading_params["SLEEP_INTERVAL"]
    try:
        with ModbusConnection(**config.mb_conn_params) as modbus_connection:

            logger.info("Testing ModbusConnection functionality.")
            # Perform a simple Modbus operation, e.g., read a register
            if (END_REG-START_REG) % 2 == 0:
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


def test_hot_disk_connection():
    try:
        with HotDiskConnection() as client:
            #client.send_command(f"SCHED:INIT {file}")
            client.send_command(f"*IDN?")
            client.receive_response()
    except KeyboardInterrupt:
        print("Program interrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Run the test function

if __name__ == "__main__":
    from src.config_connection_reading_management.config_reader import GetConfig

    # Load the configuration
    config = GetConfig()


    file = r"C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\Scripts\temp_for_hotdisk\waittenminutes.hseq"

    test_functionality(config)
    #test_hot_disk_connection()
