#main.py

import os
import threading
import sys

from PySide6.QtWidgets import QApplication
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile

from src.GUI.config_creation.config_creator_ui_main import ConfigWindow
from src.standard_paths import standard_config_file_path
from src.GUI.recording_gui.recording_main_v3 import MainWindow as RecordingMainWindow
from src.simulation.simulator_gui import ModbusServerControlGUI
from test_planner import TestPlannerMain
from src.GUI.hot_disk_sequenzer.suquenzer_gui import ScheduleGeneratorMain
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
    def __init__(self, config):
        super().__init__(config=config)

        self.ui.actionTest_Planner.triggered.connect(self.open_test_planner)
        self.ui.actionConfig_Settings.triggered.connect(self.open_config_settings)
        self.ui.actionQuick_Export.triggered.connect(self._quick_export)
        self.ui.actionDicon_Simulator.triggered.connect(self._open_dicon_simulator)
        self.ui.actionSchedule_Creator.triggered.connect(self._open_schedule_creator)

    def open_test_planner(self):
        self.planner = TestPlannerMain()
        self.planner.show()

    def open_config_settings(self):
        self.config_settings = ConfigWindow()
        self.config_settings.show()

    def _quick_export(self):
        from src.export_methods import QuickExport
        exporter = QuickExport(meta_data=self.meta_data, db_conn_params=self.config.db_conn_params)
        constraints = self.controller.constraints_dict
        export_thread = threading.Thread(target=exporter.export_all, args=(constraints), daemon=True)
        export_thread.start()

    def _open_dicon_simulator(self):
        self.dicon_simulator = ModbusServerControlGUI()
        self.dicon_simulator.show()
        self.dicon_simulator.server_started.connect(self.change_modbus_host_ip)
        self.dicon_simulator.server_stopped.connect(self.change_modbus_host_ip)

    def _open_schedule_creator(self):
        self.schedule_creator = ScheduleGeneratorMain()
        self.schedule_creator.show()

    def change_modbus_host_ip(self, host_ip=None, port=None):
        recorder_was_running = self.controller.is_tp_recording_running()
        if self.controller.is_tp_recording_running:
                self.controller.stop_tp_recording()

        if host_ip and port:
            self.prev_port = self.controller.recorder.mb_processor.MB_PORT
            self.prev_host = self.controller.recorder.mb_processor.MB_HOST
            self.controller.recorder.mb_processor.MB_PORT = port
            self.controller.recorder.mb_processor.MB_HOST = host_ip

        elif self.prev_host and self.prev_port:
            self.controller.recorder.mb_processor.MB_PORT = self.prev_port
            self.controller.recorder.mb_processor.MB_HOST = self.prev_host

        if recorder_was_running:
            self.controller.start_tp_recording()

    def closeEvent(self, event):
        super().closeEvent(event)
        if hasattr(self, "dicon_simulator"):
            self.dicon_simulator.business.stop_server()


def main():
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
    try:
        from src.config_connection_reading_management.config_reader import GetConfig
        config = GetConfig()
        main_program = MainProgram(config=config)
        main_program.show()
    except Exception as e:
        logger.error(f"Uhpsi daisy: {e}")


if __name__ == '__main__':
    main()















