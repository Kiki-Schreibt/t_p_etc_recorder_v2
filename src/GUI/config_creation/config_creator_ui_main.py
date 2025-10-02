import sys
import json
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout,
    QLabel, QLineEdit, QPushButton,
    QMessageBox, QGroupBox,
    QGridLayout, QHBoxLayout
)
from PySide6.QtCore import Signal

from src.infrastructure.utils.standard_paths import standard_config_file_path
import src.infrastructure.core.global_vars as global_vars

STYLE_SHEET = global_vars.style


class ConfigWindow(QWidget):
    config_saved_sig = Signal()

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Connection and Database Settings")
        self.setStyleSheet(STYLE_SHEET)
        self.config_template = {
            "LOG_DIRECTORY": "Log",
            "LOG_FILE": "Application_Log.log",
            "DB_SERVER": "localhost",
            "DB_DATABASE": "postgres",
            "DB_USERNAME": "postgres",
            "DB_PASSWORD": "It's a secret you must not tell anyone",
            "DB_PORT": "5432",
            "MODBUS_HOST": "192.168.178.1",
            "MODBUS_PORT": 502,
            "REGS_OF_INTEREST": [4605, 4653, 4655, 4669, 4671],
            "START_REG": 4585,
            "END_REG": 4672,
            "SLEEP_INTERVAL": 0.5,
            "HOT_DISK_LOG_FILE_PATH": r"C:\HotDiskTPS_7\data\Log",
            "MINIMUM_TEMPERATURE_INCREASE": 2,
            "MAXIMUM_TEMPERATURE_INCREASE": 5,
            "MINIMUM_TOTAL_TO_CHARACTERISTIC_TIME": 0.33,
            "MAXIMUM_TOTAL_TO_CHARACTERISTIC_TIME": 1,
        }

        self.non_editable_keys = [
            "TP_DATA_TABLE_NAME",
            "THERMAL_CONDUCTIVITY_DATA_TABLE_NAME",
            "THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME",
            "META_DATA_TABLE_NAME",
            "CYCLE_DATA_TABLE_NAME"
        ]

        self.config_values = {}
        self.initUI()

    def initUI(self):
        self.setWindowTitle('Configuration Input')
        layout = QVBoxLayout()

        db_group = self.create_group_box("Database Settings", [
            "DB_SERVER", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD", "DB_PORT"
        ])

        modbus_group = self.create_group_box("Modbus Settings", [
            "MODBUS_HOST", "MODBUS_PORT", "REGS_OF_INTEREST", "START_REG", "END_REG", "SLEEP_INTERVAL"
        ])

        log_group = self.create_group_box("Log Settings", [
            "LOG_DIRECTORY", "LOG_FILE"
        ])

        file_paths_group = self.create_group_box("File Paths", [
            "HOT_DISK_LOG_FILE_PATH"
        ])


        other_settings_group = self.create_group_box("Other Settings", [
            "MINIMUM_TEMPERATURE_INCREASE", "MAXIMUM_TEMPERATURE_INCREASE", "MINIMUM_TOTAL_TO_CHARACTERISTIC_TIME",
            "MAXIMUM_TOTAL_TO_CHARACTERISTIC_TIME"
        ])

        layout.addWidget(db_group)
        layout.addWidget(modbus_group)
        layout.addWidget(log_group)
        layout.addWidget(file_paths_group)
        layout.addWidget(other_settings_group)

        # Create a horizontal layout for the buttons
        button_layout = QHBoxLayout()

        # Add the Test Connection button
        test_connection_button = QPushButton('Test Connection')
        test_connection_button.clicked.connect(self.test_connection)
        button_layout.addWidget(test_connection_button)

        # Add the Save Configuration button
        save_button = QPushButton('Save Configuration')
        save_button.clicked.connect(self.save_config)
        button_layout.addWidget(save_button)

        # Add the button layout to the main layout
        layout.addLayout(button_layout)

        self.setLayout(layout)

    def create_group_box(self, title, keys):
        group_box = QGroupBox(title)
        form_layout = QGridLayout()

        self.inputs = getattr(self, "inputs", {})
        row = 0
        for key in keys:
            default_value = self.config_template[key]
            label = QLabel(key)
            if isinstance(default_value, list):
                input_field = QLineEdit(','.join(map(str, default_value)))
            else:
                input_field = QLineEdit(str(default_value))

            if key in self.non_editable_keys:
                input_field.setReadOnly(True)

            self.inputs[key] = input_field
            form_layout.addWidget(label, row // 2, (row % 2) * 2)
            form_layout.addWidget(input_field, row // 2, (row % 2) * 2 + 1)
            row += 1

        group_box.setLayout(form_layout)
        return group_box

    def save_config(self):
        config_values = {}
        for key, input_field in self.inputs.items():
            value_str = input_field.text()
            default_value = self.config_template[key]

            try:
                # Convert the value to the appropriate type based on the default value
                if isinstance(default_value, int):
                    value = int(value_str)
                elif isinstance(default_value, float):
                    value = float(value_str)
                elif isinstance(default_value, list):
                    # For lists, we assume a comma-separated string of integers
                    value = [int(item.strip()) for item in value_str.split(',')]
                else:
                    # Treat as string
                    value = value_str

                config_values[key] = value
            except ValueError:
                QMessageBox.warning(self, "Invalid Value", f"Invalid value for {key}: {value_str}")
                return

        # Update the config_values dictionary
        self.config_values.update(config_values)

        # Save the configuration to a JSON file
        try:
            with open(standard_config_file_path, "w") as config_file:
                json.dump(self.config_values, config_file, indent=4)
            QMessageBox.information(self, "Success", "Configuration saved successfully.")
            db_conn_params = {'DB_SERVER': self.inputs['DB_SERVER'].text(),
                              'DB_DATABASE':   self.inputs['DB_DATABASE'].text(),
                              'DB_USERNAME':   self.inputs['DB_USERNAME'].text(),
                              'DB_PASSWORD':   self.inputs['DB_PASSWORD'].text(),
                              'DB_PORT':       self.inputs['DB_PORT'].text()}
            self._create_db_and_tables(db_conn_params)
            self.config_saved_sig.emit()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save configuration: {str(e)}")

    def test_connection(self):
        db_connection_successful, db_error_message = self._test_database_connection()
        modbus_connection_successful, modbus_error_message = self._test_modbus_connections()

        # Display the results
        if db_connection_successful and modbus_connection_successful:
            QMessageBox.information(self, "Success", "Successfully connected to the database and Modbus server.")
        else:
            error_message = ""
            if not db_connection_successful:
                error_message += f"Database Connection Error:\n{db_error_message}\n\n"
            if not modbus_connection_successful:
                error_message += f"Modbus Connection Error:\n{modbus_error_message}"
            QMessageBox.critical(self, "Connection Error", error_message)

    def _test_database_connection(self):
         # Test Database Connection
        db_server = self.inputs['DB_SERVER'].text()
        db_database = self.inputs['DB_DATABASE'].text()
        db_username = self.inputs['DB_USERNAME'].text()
        db_password = self.inputs['DB_PASSWORD'].text()
        db_port = self.inputs['DB_PORT'].text()

        try:
            db_port = int(db_port)
        except ValueError:
            QMessageBox.warning(self, "Invalid Port", "Please enter a valid integer for the database port.")
            return

        # Attempt to connect to the database
        db_connection_successful = False
        db_error_message = ""
        try:
            import psycopg2
            connection = psycopg2.connect(
                host=db_server,
                port=db_port,
                database=db_database,
                user=db_username,
                password=db_password
            )
            connection.close()

            return True, None
        except ImportError:
            db_error_message = "psycopg2 module is not installed. Please install it to test the database connection."
            return False, db_error_message
        except Exception as e:
            db_error_message = str(e)
            return False, db_error_message

    def _test_modbus_connections(self):
        # Test Modbus Connection
        modbus_host = self.inputs['MODBUS_HOST'].text()
        modbus_port = self.inputs['MODBUS_PORT'].text()

        try:
            modbus_port = int(modbus_port)
        except ValueError:
            QMessageBox.warning(self, "Invalid Port", "Please enter a valid integer for the Modbus port.")
            return

        # Attempt to connect to the Modbus server
        modbus_connection_successful = False
        modbus_error_message = ""
        try:
            from pymodbus.client import ModbusTcpClient
            client = ModbusTcpClient(modbus_host, port=modbus_port)
            connection = client.connect()
            if connection:
                client.close()
                modbus_connection_successful = True
                return modbus_connection_successful, modbus_error_message
            else:
                client.close()
                modbus_error_message = "Failed to connect to the Modbus server."
                return modbus_connection_successful, modbus_error_message
        except ImportError:
            modbus_error_message = "pymodbus module is not installed. Please install it to test the Modbus connection."
            return modbus_connection_successful, modbus_error_message
        except Exception as e:
            modbus_error_message = str(e)
            return modbus_connection_successful, modbus_error_message

    @staticmethod
    def _convert_value(value):
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    @staticmethod
    def _create_db_and_tables(db_conn_params):
        from src.table_creator import TableCreator, create_database

        create_database(db_conn_params=db_conn_params)
        TableCreator(db_conn_params=db_conn_params).create_all_tables()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ConfigWindow()
    window.show()
    sys.exit(app.exec())
