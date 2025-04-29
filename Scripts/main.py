#main.py

import os
import threading
import sys

from PySide6.QtWidgets import QApplication
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile

from src.GUI.config_creation.config_creator_ui_main import ConfigWindow
from src.standard_paths import standard_config_file_path
from src.GUI.recording_gui.recording_main_v3 import MainWindow as RecordingMainWindow, local_tz
from src.simulation.simulator_gui import ModbusServerControlGUI
from test_planner import TestPlannerMain
from src.GUI.side_operations.h2_uptake_correction_gui import UptakeCorrectionWindow
from src.GUI.hot_disk_sequenzer.suquenzer_gui import SequenzerMainWindow
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging

logger = logging.getLogger()

#main_ui_file_path = r"..\src\GUI\recording_gui\recording_ui_design_works.ui"
def load_ui_file(ui_file_path):
    """
    Loads a GUI's UI file using PySide6 QUiLoader.

    Args:
        ui_file_path (str): The full file path to the UI file.

    Returns:
        QWidget: The loaded GUI object if successful.
        None: If the UI file could not be loaded.

    Raises:
        Exception: If there is an issue during the loading process.

    Example:
         ui = load_ui_file("path/to/ui_file.ui")
         if ui:
            ui.show()
    """
    try:
        loader = QUiLoader()
        ui_file = QFile(ui_file_path)

        ui_file.open(QFile.ReadOnly)
        ui = loader.load(ui_file)
        ui_file.close()
        if not ui:
            logger.error("Failed to load UI file")
            return
        return ui

    except Exception as e:
        logger.exception("Unable to load ui file %s", e)


class MainProgram(RecordingMainWindow):
    """
    The main application window for data recording and related tools.

    Extends RecordingMainWindow to add menu actions for:
      - Test Planner
      - Configuration Settings
      - Quick Export
      - DICON Simulator
      - Schedule Creator
      - Uptake Correction

    Args:
        config (ConfigReader): Loaded configuration object providing DB and other params.
    """

    def __init__(self, config):
        """
        Initialize the main program and connect all menu actions.

        Args:
            config (ConfigReader): The application configuration.
        """
        super().__init__(config=config)

        self.ui.actionTest_Planner.triggered.connect(self.open_test_planner)
        self.ui.actionConfig_Settings.triggered.connect(self.open_config_settings)
        self.ui.actionQuick_Export.triggered.connect(self._quick_export)
        self.ui.actionDicon_Simulator.triggered.connect(self._open_dicon_simulator)
        self.ui.actionSchedule_Creator.triggered.connect(self._open_schedule_creator)
        self.ui.actionUptake_Correction.triggered.connect(self._open_uptake_correction)

    def open_test_planner(self):
        """
        Launch the Test Planner module in its own window.
        """
        self.planner = TestPlannerMain()
        self.planner.show()

    def open_config_settings(self):
        """
        Open the configuration settings dialog for editing and saving.
        """
        self.config_settings = ConfigWindow()
        self.config_settings.show()

    def _quick_export(self):
        """
        Perform a quick export using the current constraints, in a background thread.
        """
        from src.export_methods import QuickExport
        exporter = QuickExport(meta_data=self.meta_data, db_conn_params=self.config.db_conn_params)
        constraints = self.controller.constraints_dict
        export_thread = threading.Thread(target=exporter.export_all, args=(constraints), daemon=True)
        export_thread.start()

    def _open_dicon_simulator(self):
        """
        Open the DICON Modbus simulator GUI and update host/port when started or stopped.
        """
        self.prev_mb_conn_params = None
        self.dicon_simulator = ModbusServerControlGUI()
        self.dicon_simulator.show()
        self.dicon_simulator.server_started.connect(self.change_modbus_host_ip)
        self.dicon_simulator.server_stopped.connect(self.change_modbus_host_ip)

    def _open_schedule_creator(self):
        """
        Launch the Schedule Creator module in its own window.
        """
        self.schedule_creator = SequenzerMainWindow()
        self.schedule_creator.show()

    def _open_uptake_correction(self):
        """
        Launch the Uptake Correction window, seeded with the current plot time range.
        """
        from datetime import datetime
        view_range = self.plot_manager.top_plot.viewRange()[0]
        dt0 = datetime.fromtimestamp(view_range[0], tz=local_tz)
        dt1 = datetime.fromtimestamp(view_range[1], tz=local_tz)
        time_range = [dt0, dt1]

        self.uptake_corrector = UptakeCorrectionWindow(meta_data=self.meta_data,
                                                       time_range_to_read=time_range,
                                                       config=self.config)
        self.uptake_corrector.show()

    def change_modbus_host_ip(self, host_ip=None, port=None):
        """
        Callback for DICON simulator start/stop; reconfigures the Modbus host/port.

        Args:
            host_ip (str, optional): New Modbus server host.
            port (int or str, optional): New Modbus server port.
        """
        recorder_was_running = self.controller.is_tp_recording_running()
        if recorder_was_running:
            self.controller.stop_tp_recording()

        if host_ip and port:
            self.prev_mb_conn_params = self.controller.recorder.mb_processor.mb_conn_params

            self.controller.recorder.mb_processor.mb_conn_params["MB_PORT"] = str(port)
            self.controller.recorder.mb_processor.mb_conn_params["MB_HOST"] = str(host_ip)

        elif self.prev_mb_conn_params:
            self.controller.recorder.mb_processor.mb_conn_params = self.prev_mb_conn_params
        if recorder_was_running:
            self.controller.start_tp_recording()

    def closeEvent(self, event):
        """
        Clean up any running simulators before the main window closes.

        Overrides the base closeEvent to stop the DICON simulator.
        """
        super().closeEvent(event)
        if hasattr(self, "dicon_simulator"):
            self.dicon_simulator.business.stop_server()


def main():
    """
    Application entry point.
    If no config file exists, open the ConfigWindow first; otherwise launch MainProgram.

    This function starts the Qt event loop.
    """
    app = QApplication(sys.argv)

    #standard_config_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'config', 'config.json')

    if not os.path.exists(standard_config_file_path):

        #print(standard_config_file_path)
        config_window = ConfigWindow()
        config_window.config_saved_sig.connect(lambda: launch_main_program(app))
        config_window.config_saved_sig.connect(lambda: config_window.close())
        config_window.show()
    else:
        launch_main_program(app)

    sys.exit(app.exec())

def launch_main_program(app):
    """
    Instantiate and show the MainProgram window.

    Args:
        app (QApplication): The running QApplication instance.
    """
    try:
        from src.config_connection_reading_management.config_reader import GetConfig
        config = GetConfig()
        main_program = MainProgram(config=config)
        main_program.show()
    except Exception as e:
        logger.error(f"Uhpsi daisy: {e}")


if __name__ == '__main__':
    main()















