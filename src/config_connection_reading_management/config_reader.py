import json
from datetime import datetime, timedelta
import sys
import os
import logging

import psycopg2

from src.standard_paths import standard_config_file_path as config_file_path
from src.standard_paths import standard_log_dir
from src.table_data import TableConfig


current_dir = os.path.dirname(__file__)


class GetConfig:
    """
    A class to load and provide access to configuration settings from a JSON file.

    The configuration settings include database connection parameters, logging details,
    Modbus connection settings, and other application-specific configurations.

    Attributes:
        LOG_DIRECTORY (str): Directory path where logs are stored.
        LOG_FILE (str): File name for logging.
        DB_SERVER (str): Database server address.
        DB_DATABASE (str): Database name.
        DB_USERNAME (str): Database username.
        DB_PASSWORD (str): Database password.
        DB_PORT (int): Database port number.
        MODBUS_HOST (str): Modbus server address.
        MODBUS_PORT (int): Modbus port number.
        REGS_OF_INTEREST (list): List of Modbus registers of interest.
        START_REG (int): Starting register for Modbus reads.
        END_REG (int): Ending register for Modbus reads.
        SLEEP_INTERVAL (int): Sleep interval for loops in seconds.
        HOT_DISK_LOG_FILE_PATH (str): Path for hot disk log files.
        TP_DATA_COLUMN_NAMES (list): Column names for temperature and pressure data.
        TP_DATA_TABLE_NAME (str): Table name for temperature and pressure data.
        THERMAL_CONDUCTIVITY_DATA_TABLE_NAME (str): Table name for thermal conductivity data.
        THERMAL_CONDUCTIVITY_COLUMN_NAMES (list): Column names for thermal conductivity data.
        THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME (str): Table name for thermal conductivity XY data.
        THERMAL_CONDUCTIVITY_XY_COLUMN_NAMES (list): Column names for thermal conductivity XY data.
        TP_DATA_INSERT_QUERY (str): SQL insert query for temperature and pressure data.
        THERMAL_CONDUCTIVITY_DATA_INSERT_QUERY (str): SQL insert query for thermal conductivity data.
        THERMAL_CONDUCTIVITY_XY_DATA_INSERT_QUERY (str): SQL insert query for thermal conductivity XY data.

    Methods:
        create_insert_query(table_name, column_names): Creates an SQL insert query for the given table and column names.
    """

    def __init__(self, config_file_path=config_file_path):
        try:
            with open(config_file_path, 'r') as file:
                config = json.load(file)

            # Accessing the variables
            self.LOG_DIRECTORY   = standard_log_dir
            self.LOG_FILE        = config['LOG_FILE']


            self.DB_SERVER       = config['DB_SERVER']
            self.DB_DATABASE     = config['DB_DATABASE']
            self.DB_USERNAME     = config['DB_USERNAME']
            self.DB_PASSWORD     = config['DB_PASSWORD']
            self.DB_PORT         = config['DB_PORT']

            self.MODBUS_HOST     = config['MODBUS_HOST']
            self.MODBUS_PORT     = config['MODBUS_PORT']
            self.REGS_OF_INTEREST = config['REGS_OF_INTEREST']
            self.START_REG       = config['START_REG']
            self.END_REG         = config['END_REG']
            self.SLEEP_INTERVAL  = config['SLEEP_INTERVAL']

            self.HOT_DISK_LOG_FILE_PATH = config['HOT_DISK_LOG_FILE_PATH']
            self.TP_DATA_TABLE_NAME = TableConfig().TPDataTable.table_name
            self.META_DATA_TABLE_NAME = TableConfig().MetaDataTable.table_name
            self.CYCLE_DATA_TABLE_NAME = TableConfig().CycleDataTable.table_name

            self.THERMAL_CONDUCTIVITY_DATA_TABLE_NAME = config['THERMAL_CONDUCTIVITY_DATA_TABLE_NAME']
            self.THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME = config['THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME']
            self.MINIMUM_TEMPERATURE_INCREASE = config["MINIMUM_TEMPERATURE_INCREASE"]
            self.MAXIMUM_TEMPERATURE_INCREASE = config["MAXIMUM_TEMPERATURE_INCREASE"]
            self.MINIMUM_TOTAL_TO_CHARACTERISTIC_TIME = config["MINIMUM_TOTAL_TO_CHARACTERISTIC_TIME"]
            self.MAXIMUM_TOTAL_TO_CHARACTERISTIC_TIME = config["MAXIMUM_TOTAL_TO_CHARACTERISTIC_TIME"]

            self.connection_string = {
                                        'host': self.DB_SERVER,
                                        'dbname': self.DB_DATABASE,
                                        'user': self.DB_USERNAME,
                                        'password': self.DB_PASSWORD,
                                        'port': self.DB_PORT
                                      }

        except Exception:

            config_prompter = ConfigPrompter()
            temp_file_name = "config_logging_modbus_database_temp.json"

            temp_file_path = os.path.join(config_prompter.get_config_dir(), temp_file_name)
            if not os.path.exists(temp_file_path):
                print("No config file yet. Please create. Restart after creation. Template will be used for now")
                config_prompter.config = config_prompter.config_template
                config_prompter.save_to_file(temp_file_name)
            self.__init__(config_file_path=temp_file_path)

        try:
            self.TP_DATA_COLUMN_NAMES = self.get_column_names(self.TP_DATA_TABLE_NAME)
            self.THERMAL_CONDUCTIVITY_COLUMN_NAMES = self.get_column_names(self.THERMAL_CONDUCTIVITY_DATA_TABLE_NAME)
            self.THERMAL_CONDUCTIVITY_XY_COLUMN_NAMES = self.get_column_names(self.THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME)
            self.META_DATA_COLUMN_NAMES = self.get_column_names(self.META_DATA_TABLE_NAME)
            self.CYCLE_DATA_COLUMN_NAMES = self.get_column_names(self.CYCLE_DATA_TABLE_NAME)

        except Exception as e:
            print("No tables yet. Will be created")


    def get_column_names(self, table_name):
        column_names = []
        try:
            # Open a new connection using psycopg2
            connection = psycopg2.connect(**self.connection_string)
            cursor = connection.cursor()

            # Execute the query to fetch column names
            cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s ORDER BY ordinal_position", (table_name,))

            # Fetch all rows
            rows = cursor.fetchall()

            # Iterate over rows and extract column names
            for row in rows:
                column_name = row[0]
                if self._has_uppercase(column_name):
                    column_name = f"\"{column_name}\""
                column_names.append(column_name)

        except Exception as e:
            print("Error while fetching column names:", e)
        finally:
            # Close the cursor and connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        return column_names

    @staticmethod
    def _has_uppercase(string):
        for char in string:
            if char.isupper():
                return True
        return False

    @staticmethod
    def create_insert_query(table_name, column_names):
        # Create the column part of the query
        column_names_str = ", ".join(column_names)
        # Create the placeholders for values
        placeholders = ", ".join(["%s"] * len(column_names))
        # Create the full insert query
        return f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders})"

"""
ConfigPrompter: A class to prompt user input for configuration settings and save them to a JSON file.

Attributes:
    config_template (dict): A dictionary containing default configuration values.
    config (dict): A dictionary to store user-provided configuration values.

Methods:
    __init__(config_template=None): Initializes the ConfigPrompter with a default or provided configuration template.
    prompt(): Prompts the user for configuration values and stores them in the config attribute.
    _prompt_for_value(key, default_value): Prompts the user for a specific configuration value.
    _convert_value(value): Attempts to convert a value to an integer or float, otherwise returns it as a string.
    get_base_dir(): Returns the base directory of the script or the PyInstaller bundle.
    get_config_dir(): Returns the path to the configuration directory, creating it if it doesn't exist.
    save_to_file(filename): Saves the configuration to a JSON file in the configuration directory.
    resolve_path(path): Resolves a relative path to an absolute path based on the base directory.

Usage Example:
    prompter = ConfigPrompter()
    prompter.prompt()
    prompter.save_to_file("config.json")
"""

class ConfigPrompter:

    def __init__(self, config_template=None):
        """
        Initializes the ConfigPrompter with a default or provided configuration template.

        Args:
            config_template (dict, optional): A dictionary of default configuration values.
                                              If not provided, a default template is used.
        """
        if config_template:
            self.config_template = config_template
        else:
            self.config_template = {
                                    "LOG_DIRECTORY": "C:\\Daten\\Kiki\\ProgrammingStuff\\kikis_hot_v5\\Log",
                                    "LOG_FILE": "Application_Log.log",
                                    "DB_SERVER": "localhost",
                                    "DB_DATABASE": "postgres",
                                    "DB_USERNAME": "postgres",
                                    "DB_PASSWORD": "Bananensalat1!",
                                    "DB_PORT": "5432",
                                    "MODBUS_HOST": "192.168.178.1",
                                    "MODBUS_PORT": 502,
                                    "REGS_OF_INTEREST": [4605, 4653, 4655, 4669, 4671],
                                    "START_REG": 4585,
                                    "END_REG": 4672,
                                    "SLEEP_INTERVAL": 0.5,
                                    "HOT_DISK_LOG_FILE_PATH": "C:\\HotDiskTPS_7\\data\\Log",
                                    "TP_DATA_TABLE_NAME": "t_p_data",
                                    "THERMAL_CONDUCTIVITY_DATA_TABLE_NAME": "thermal_conductivity_data",
                                    "THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME": "thermal_conductivity_xy_data",
                                    "MINIMUM_TEMPERATURE_INCREASE": 2,
                                    "MAXIMUM_TEMPERATURE_INCREASE": 5,
                                    "MINIMUM_TOTAL_TO_CHARACTERISTIC_TIME": 0.33,
                                    "MAXIMUM_TOTAL_TO_CHARACTERISTIC_TIME": 1,
                                    "META_DATA_TABLE_NAME": "meta_data",
                                    "CYCLE_DATA_TABLE_NAME": "cycle_data"
                                   }

        self.config = {}

    def prompt(self):
        """
        Prompts the user for configuration values and stores them in the config attribute.
        """
        print("Please enter the following configuration values:")
        for key, value in self.config_template.items():
            self.config[key] = self._prompt_for_value(key, value)

    def _prompt_for_value(self, key, default_value):
        """
        Prompts the user for a specific configuration value.

        Args:
            key (str): The name of the configuration setting.
            default_value (any): The default value for the configuration setting.

        Returns:
            any: The value entered by the user, or the default value if no input is provided.
        """
        if isinstance(default_value, list):
            # Handle list input
            value = input(f"Enter values for {key} separated by commas (default: {default_value}): ")
            if value:
                return [self._convert_value(v.strip()) for v in value.split(',')]
            else:
                return default_value
        else:
            # Handle single value input
            value = input(f"Enter value for {key} (default: {default_value}): ")
            if value:
                return self._convert_value(value)
            else:
                return default_value

    def _convert_value(self, value):
        """
        Attempts to convert a value to an integer or float, otherwise returns it as a string.

        Args:
            value (str): The value to convert.

        Returns:
            int, float, or str: The converted value.
        """
        try:
            # Try to convert to int
            return int(value)
        except ValueError:
            try:
                # Try to convert to float
                return float(value)
            except ValueError:
                # Return as string if neither int nor float
                return value

    def get_base_dir(self):
        """
        Returns the base directory of the script or the PyInstaller bundle.

        Returns:
            str: The base directory path.
        """
        if hasattr(sys, '_MEIPASS'):
            # Running in a PyInstaller bundle
            base_path = sys._MEIPASS
        else:
            # Running in a regular Python environment
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', ".."))
        return base_path

    def get_config_dir(self):
        """
        Returns the path to the configuration directory, creating it if it doesn't exist.

        Returns:
            str: The configuration directory path.
        """
        base_path = self.get_base_dir()
        config_path = os.path.join(base_path, 'config')

        # Create the directory if it doesn't exist
        if not os.path.exists(config_path):
            os.makedirs(config_path)

        return config_path

    def save_to_file(self, filename):
        """
        Saves the configuration to a JSON file in the configuration directory.

        Args:
            filename (str): The name of the file to save the configuration to.

        Returns:
            str: The path to the saved configuration file.
        """
        config_dir = self.get_config_dir()
        file_path = os.path.join(config_dir, filename)
        with open(file_path, 'w') as file:
            json.dump(self.config, file, indent=4)
        print(f"Configuration saved to {file_path}")
        return file_path

    def resolve_path(self, path):
        """
        Resolves a relative path to an absolute path based on the base directory.

        Args:
            path (str): The relative path to resolve.

        Returns:
            str: The absolute path.
        """
        base_path = self.get_base_dir()
        return os.path.abspath(os.path.join(base_path, path))
# Template JSON structure



def test_get_column_names_from_postgres():
    """
    Test the get_column_names_from_postgres method.
    """
    # Replace 'your_table_name' with the actual name of your table
    get_config = GetConfig()
    table_name = get_config.META_DATA_COLUMN_NAMES
    print("Column names retrieved successfully:", get_config.CYCLE_DATA_COLUMN_NAMES)
 #   print("Old Column names =:", get_config.THERMAL_CONDUCTIVITY_COLUMN_NAMES_OLD)


if __name__ == "__main__":

    config = GetConfig()
    print(config.LOG_FILE)
    print(config.TP_DATA_COLUMN_NAMES)



