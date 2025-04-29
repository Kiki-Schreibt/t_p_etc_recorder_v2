#config_reader.py
import json
import sys
import os
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging

from src.standard_paths import standard_config_file_path as config_file_path
from src.standard_paths import standard_log_dir

current_dir = os.path.dirname(__file__)

###uncomment to use test database
#test_database_config_file_path = r'C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\config\config_logging_modbus_database _testing.json'
#config_file_path = test_database_config_file_path

class GetConfig:
    """
    A class to load and provide access to configuration settings from a JSON file.

    The configuration settings include database connection parameters, logging details,
    Modbus connection settings, and other application-specific configurations.
    """

    def __init__(self, config_file_path=config_file_path):
        self.logger = logging.getLogger(__name__)
        try:
            self._load_config(config_file_path)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error("Error loading config file '%s': %s", config_file_path, e)
            # Use the fallback mechanism with ConfigPrompter
            config_prompter = ConfigPrompter()
            temp_file_name = "config_logging_modbus_database_temp.json"
            temp_file_path = os.path.join(config_prompter.get_config_dir(), temp_file_name)
            if not os.path.exists(temp_file_path):
                self.logger.info("No valid config file found. Creating default config file at '%s'.", temp_file_path)
                config_prompter.config = config_prompter.config_template
                config_prompter.save_to_file(temp_file_name)
            try:
                self._load_config(temp_file_path)
            except Exception as fallback_exception:
                self.logger.critical("Failed to load fallback config file: %s", fallback_exception)
                sys.exit(1)

    def _load_config(self, config_file_path):
        with open(config_file_path, 'r') as file:
            config = json.load(file)

        self.db_conn_params = {
                        "DB_SERVER": config['DB_SERVER'],
                        "DB_DATABASE": config['DB_DATABASE'],
                        "DB_USERNAME": config['DB_USERNAME'],
                        "DB_PASSWORD": config['DB_PASSWORD'],
                        "DB_PORT": config['DB_PORT'],
                    }

        self.mb_conn_params = {
                            "MB_HOST": config['MODBUS_HOST'],
                            "MB_PORT": config['MODBUS_PORT']
                        }

        self.mb_reading_params = {
                            "START_REG": int(config['START_REG']),
                            "END_REG": int(config['END_REG']),
                            "REGS_OF_INTEREST": [int(x) for x in config['REGS_OF_INTEREST']],
                            "SLEEP_INTERVAL": int(config['SLEEP_INTERVAL'])
                                }

        self.app_logger_params = {
                            "LOG_DIRECTORY": standard_log_dir,
                            "LOG_FILE": config['LOG_FILE']
                                    }

        self.hd_log_file_tracker_params = {"HOT_DISK_LOG_FILE_PATH": config['HOT_DISK_LOG_FILE_PATH']}

        self.HOT_DISK_LOG_FILE_PATH = config['HOT_DISK_LOG_FILE_PATH']

    @staticmethod
    def _has_uppercase(string):
        return any(char.isupper() for char in string)

    @staticmethod
    def create_insert_query(table_name, column_names):
        """
        Creates an SQL insert query for the given table and column names.
        """
        column_names_str = ", ".join(column_names)
        placeholders = ", ".join(["%s"] * len(column_names))
        return f"INSERT INTO {table_name} ({column_names_str}) VALUES ({placeholders})"


class ConfigPrompter:
    """
    A class to prompt user input for configuration settings and save them to a JSON file.
    """

    def __init__(self, config_template=None):
        if config_template:
            self.config_template = config_template
        else:
            self.config_template = {
                "LOG_FILE": "Application_Log.log",
                "DB_SERVER": "localhost",
                "DB_DATABASE": "postgres",
                "DB_USERNAME": "postgres",
                "DB_PASSWORD": "Bananensalat1!",
                "DB_PORT": 5432,  # Use an integer for the port
                "MODBUS_HOST": "192.168.178.1",
                "MODBUS_PORT": 502,
                "REGS_OF_INTEREST": [4605, 4653, 4655, 4669, 4671],
                "START_REG": 4585,
                "END_REG": 4672,
                "SLEEP_INTERVAL": 0.5,
                "HOT_DISK_LOG_FILE_PATH": "C:\\HotDiskTPS_7\\data\\Log"
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
        """
        if isinstance(default_value, list):
            value = input(f"Enter values for {key} separated by commas (default: {default_value}): ")
            if value:
                return [self._convert_value(v.strip()) for v in value.split(',')]
            else:
                return default_value
        else:
            value = input(f"Enter value for {key} (default: {default_value}): ")
            if value:
                return self._convert_value(value)
            else:
                return default_value

    @staticmethod
    def _convert_value(value):
        """
        Attempts to convert a value to an integer or float, otherwise returns it as a string.
        """
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    @staticmethod
    def get_base_dir():
        """
        Returns the base directory of the script or the PyInstaller bundle.
        """
        if hasattr(sys, '_MEIPASS'):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', ".."))
        return base_path

    def get_config_dir(self):
        """
        Returns the path to the configuration directory, creating it if it doesn't exist.
        """
        base_path = self.get_base_dir()
        config_path = os.path.join(base_path, 'config')
        if not os.path.exists(config_path):
            os.makedirs(config_path)
        return config_path

    def save_to_file(self, filename):
        """
        Saves the configuration to a JSON file in the configuration directory.
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
        """
        base_path = self.get_base_dir()
        return os.path.abspath(os.path.join(base_path, path))

    def delete_file(self, filename):
        """
        Deletes the specified configuration file.
        """
        config_dir = self.get_config_dir()
        file_path = os.path.join(config_dir, filename)
        if os.path.exists(file_path):
            os.remove(file_path)


if __name__ == "__main__":
    # For testing whether the configuration reader works.
    config = GetConfig()
    print("LOG_FILE from configuration:", config.db_conn_params)
