from __future__ import annotations
from typing import Any, Dict, Tuple

import sys
import json
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout,
    QLabel, QLineEdit, QPushButton,
    QMessageBox, QGroupBox,
    QGridLayout, QHBoxLayout
)
from PySide6.QtCore import Signal

from recorder_app.infrastructure.utils.standard_paths import standard_config_file_path
from recorder_app.infrastructure.core import global_vars

STYLE_SHEET = global_vars.style



class ConfigWindow(QWidget):
    """
    UI-only class. Renders the window and delegates all logic to ConfigService.
    """
    config_saved_sig = Signal()

    def __init__(self, service: ConfigService | None = None):
        super().__init__()
        self.service = service or ConfigService()
        self.inputs: Dict[str, QLineEdit] = {}

        self.setWindowTitle("Connection and Database Settings")
        self.setStyleSheet(STYLE_SHEET)
        self._init_ui()

    # ---------- UI construction ----------

    def _init_ui(self) -> None:
        self.setWindowTitle("Configuration Input")
        layout = QVBoxLayout()

        # Groupings
        db_group = self._create_group_box(
            "Database Settings",
            ["DB_SERVER", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD", "DB_PORT"],
        )

        modbus_group = self._create_group_box(
            "Modbus Settings",
            ["MODBUS_HOST", "MODBUS_PORT", "REGS_OF_INTEREST", "START_REG", "END_REG", "SLEEP_INTERVAL"],
        )

        log_group = self._create_group_box(
            "Log Settings",
            ["LOG_DIRECTORY", "LOG_FILE"],
        )

        file_paths_group = self._create_group_box(
            "File Paths",
            ["HOT_DISK_LOG_FILE_PATH"],
        )

        other_settings_group = self._create_group_box(
            "Other Settings",
            [
                "MINIMUM_TEMPERATURE_INCREASE",
                "MAXIMUM_TEMPERATURE_INCREASE",
                "MINIMUM_TOTAL_TO_CHARACTERISTIC_TIME",
                "MAXIMUM_TOTAL_TO_CHARACTERISTIC_TIME",
            ],
        )

        for grp in (db_group, modbus_group, log_group, file_paths_group, other_settings_group):
            layout.addWidget(grp)

        # Buttons
        button_layout = QHBoxLayout()
        test_btn = QPushButton("Test Connection")
        test_btn.clicked.connect(self._on_test_connection)
        button_layout.addWidget(test_btn)

        save_btn = QPushButton("Save Configuration")
        save_btn.clicked.connect(self._on_save_config)
        button_layout.addWidget(save_btn)

        layout.addLayout(button_layout)
        self.setLayout(layout)

    def _create_group_box(self, title: str, keys: list[str]) -> QGroupBox:
        group_box = QGroupBox(title)
        grid = QGridLayout()

        row = 0
        for key in keys:
            default_value = self.service.config_template[key]
            label = QLabel(key)
            if isinstance(default_value, list):
                line = QLineEdit(",".join(map(str, default_value)))
            else:
                line = QLineEdit(str(default_value))

            if key == "DB_PASSWORD":
                line.setEchoMode(QLineEdit.Password)

            if key in self.service.non_editable_keys:
                line.setReadOnly(True)

            self.inputs[key] = line
            # two columns (label, input) per row, but arrange in a 2-column grid
            grid.addWidget(label, row // 2, (row % 2) * 2)
            grid.addWidget(line, row // 2, (row % 2) * 2 + 1)
            row += 1

        group_box.setLayout(grid)
        return group_box

    # ---------- UI handlers ----------

    def _on_save_config(self) -> None:
        # Gather raw strings from inputs keyed by config keys
        raw: Dict[str, str] = {k: w.text() for k, w in self.inputs.items()}

        # Parse + persist via service
        try:
            parsed = self.service.parse_values_from_strings(raw)
            self.service.save_config(parsed)
        except ValueError as e:
            QMessageBox.warning(self, "Invalid Value", str(e))
            return
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save configuration: {e}")
            return

        # Initialize DB after successful save
        try:
            db_conn_params = {
                "DB_SERVER": self.inputs["DB_SERVER"].text(),
                "DB_DATABASE": self.inputs["DB_DATABASE"].text(),
                "DB_USERNAME": self.inputs["DB_USERNAME"].text(),
                "DB_PASSWORD": self.inputs["DB_PASSWORD"].text(),
                "DB_PORT": self.inputs["DB_PORT"].text(),
            }
            self.service.create_db_and_tables(db_conn_params)
        except Exception as e:
            QMessageBox.critical(self, "DB Init Error", f"Failed to create DB/tables: {e}")
            return

        QMessageBox.information(self, "Success", "Configuration saved successfully.")
        self.config_saved_sig.emit()

    def _on_test_connection(self) -> None:
        # DB test
        ok_db, err_db = self.service.test_database_connection(
            host=self.inputs["DB_SERVER"].text(),
            port=self.inputs["DB_PORT"].text(),
            database=self.inputs["DB_DATABASE"].text(),
            user=self.inputs["DB_USERNAME"].text(),
            password=self.inputs["DB_PASSWORD"].text(),
        )

        # Modbus test
        ok_mb, err_mb = self.service.test_modbus_connection(
            host=self.inputs["MODBUS_HOST"].text(),
            port=self.inputs["MODBUS_PORT"].text(),
        )

        if ok_db and ok_mb:
            QMessageBox.information(self, "Success", "Successfully connected to the database and Modbus server.")
            return

        parts = []
        if not ok_db:
            parts.append(f"Database Connection Error:\n{err_db or 'Unknown error'}")
        if not ok_mb:
            parts.append(f"Modbus Connection Error:\n{err_mb or 'Unknown error'}")
        QMessageBox.critical(self, "Connection Error", "\n\n".join(parts))


class ConfigService:
    """
    Business logic for reading/writing config, validating & parsing values,
    testing external connections, and initializing the database/tables.
    """

    def __init__(self) -> None:
        # Defaults + types are defined here so the UI can render correct inputs.
        self.config_template: Dict[str, Any] = {
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
            "CYCLE_DATA_TABLE_NAME",
        ]

        # In-memory store of last-saved/parsed values
        self.config_values: Dict[str, Any] = {}

    # ---------- Parsing & persistence ----------

    def parse_values_from_strings(self, raw: Dict[str, str]) -> Dict[str, Any]:
        """
        Parse a dict of string inputs into typed values using self.config_template
        as the type source of truth. Raises ValueError with a useful message if
        any field fails parsing.
        """
        parsed: Dict[str, Any] = {}
        for key, default in self.config_template.items():
            if key not in raw:
                raise ValueError(f"Missing required config key: {key}")

            value_str = raw[key]

            try:
                if isinstance(default, int):
                    parsed[key] = int(value_str)
                elif isinstance(default, float):
                    parsed[key] = float(value_str)
                elif isinstance(default, list):
                    # Comma-separated integers
                    parsed[key] = [int(item.strip()) for item in value_str.split(",") if item.strip() != ""]
                else:
                    parsed[key] = value_str
            except ValueError as e:
                raise ValueError(f"Invalid value for {key}: {value_str}") from e

        return parsed

    def save_config(self, values: Dict[str, Any]) -> None:
        """
        Save the provided config values to the standard JSON file.
        """
        self.config_values.update(values)
        with open(standard_config_file_path, "w") as f:
            json.dump(self.config_values, f, indent=4)

    # ---------- Connectivity tests ----------

    def test_database_connection(
        self,
        host: str,
        port: str | int,
        database: str,
        user: str,
        password: str,
    ) -> Tuple[bool, str | None]:
        """
        Return (ok, error_message). error_message is None if ok is True.
        """
        try:
            port = int(port)
        except ValueError:
            return False, "Invalid database port (must be an integer)."

        try:
            import psycopg2  # type: ignore
        except ImportError:
            return False, "psycopg2 is not installed. Please install it to test the database connection."

        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )
            conn.close()
            return True, None
        except Exception as e:
            return False, str(e)

    def test_modbus_connection(self, host: str, port: str | int) -> Tuple[bool, str | None]:
        """
        Return (ok, error_message). error_message is None if ok is True.
        """
        try:
            port = int(port)
        except ValueError:
            return False, "Invalid Modbus port (must be an integer)."

        try:
            from pymodbus.client import ModbusTcpClient  # type: ignore
        except ImportError:
            return False, "pymodbus is not installed. Please install it to test the Modbus connection."

        try:
            client = ModbusTcpClient(host, port=port)
            connected = client.connect()
            client.close()
            if connected:
                return True, None
            return False, "Failed to connect to the Modbus server."
        except Exception as e:
            return False, str(e)

    # ---------- DB bootstrap ----------

    def create_db_and_tables(self, db_conn_params: Dict[str, Any]) -> None:
        """
        Create database (if needed) and all application tables.
        """
        from recorder_app.table_creator import TableCreator, create_database
        create_database(db_conn_params=db_conn_params)
        TableCreator(db_conn_params=db_conn_params).create_all_tables()


# ---------- Entrypoints ----------

def main():
    app = QApplication(sys.argv)
    window = ConfigWindow()
    window.show()
    sys.exit(app.exec())

def test_db_and_table_creation():
    from recorder_app.infrastructure.core.config_reader import config

    db_conn_params = config.db_conn_params
    db_conn_params["DB_DATABASE"] = "BaseTestBase"
    print(config.db_conn_params)
    ConfigService().create_db_and_tables(db_conn_params)

if __name__ == "__main__":
    main()
