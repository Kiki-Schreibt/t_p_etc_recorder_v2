import time
import socket

from pymodbus.client import ModbusTcpClient as ModbusClient
from pymodbus.exceptions import ModbusException, ConnectionException
import psycopg2

from src.config_connection_reading_management.logger import AppLogger
from src.config_connection_reading_management.config_reader import GetConfig


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
    # Load the configuration
    config = GetConfig()
    # Accessing the variables
    DB_SERVER       = config.DB_SERVER
    DB_DATABASE     = config.DB_DATABASE
    DB_USERNAME     = config.DB_USERNAME
    DB_PASSWORD     = config.DB_PASSWORD
    DB_PORT         = config.DB_PORT

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
            conn_str = f"dbname={self.DB_DATABASE} user={self.DB_USERNAME} password={self.DB_PASSWORD} host={self.DB_SERVER} port={self.DB_PORT}"
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
    config = GetConfig()
    MODBUS_HOST     = config.MODBUS_HOST
    MODBUS_PORT     = config.MODBUS_PORT
    REGS_OF_INTEREST = config.REGS_OF_INTEREST
    START_REG       = config.START_REG
    END_REG         = config.END_REG
    SLEEP_INTERVAL  = config.SLEEP_INTERVAL



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


class HotDiskConnection:
    HOT_DISK_HOST = 'localhost'
    HOT_DISK_PORT = 50009

    def __init__(self, host=HOT_DISK_HOST, port=HOT_DISK_PORT):
        self.logger = AppLogger().get_logger(__name__)
        self.host = host
        self.port = port
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
            self.logger.info(f"Connecting to {self.host}:{self.port}...")
            self.sock.connect((self.host, self.port))
            self.logger.info("Connected.")
            return self
        except Exception as e:
            self.logger.error(f"An error occurred during connection: {e}")

    def send_command(self, command):
        if not self.sock:
            self.logger.error("Not connected to the server.")
            return
        try:
            self.sock.sendall(command.encode('ascii') + b'\r\n')
            self.logger.info(f"Sent command: {command}")
        except Exception as e:
            self.logger.error(f"An error occurred while sending command: {e}")

    def receive_response(self):
        try:
            response = self.sock.recv(4096)
            self.logger.info(f"Received response: {response.decode('ascii')}")
            return response.decode('ascii')
        except socket.timeout:
            self.logger.warning("No response received within timeout period.")
        except Exception as e:
            self.logger.error(f"An error occurred while receiving response: {e}")

    def disconnect(self):
        if self.sock:
            try:
                self.sock.close()
                self.sock = None
                self.logger.info("Disconnected from the server.")
            except Exception as e:
                self.logger.error(f"Couldn't disconnect from server: {e}")


if __name__ == "__main__":
    file = r"C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\Scripts\temp_for_hotdisk\waittenminutes.hseq"
    try:
        with HotDiskConnection() as client:
            #client.send_command(f"SCHED:INIT {file}")
            client.send_command(f"STAT?")
            client.receive_response()
    except KeyboardInterrupt:
        print("Program interrupted by user.")
    except Exception as e:
        print(f"An error occurred: {e}")



def test_functionality():
    config = GetConfig()
    END_REG = config.END_REG
    START_REG = config.START_REG

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
    config = GetConfig()
    END_REG = config.END_REG
    START_REG = config.START_REG
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
    pass
    #test_functionality()
    #test_mb_connection()
    #test_connection_simulated_modbus()
