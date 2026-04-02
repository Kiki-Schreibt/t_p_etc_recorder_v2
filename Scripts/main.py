#main.py

import os
import threading
import sys

from PySide6.QtWidgets import QApplication
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile

import global_vars
from src.GUI.config_creation.config_creator_ui_main import ConfigWindow
from src.infrastructure.utils.standard_paths import standard_config_file_path
from src.GUI.recording_gui.recording_main_v3 import MainWindow as RecordingMainWindow, local_tz
from src.GUI.simulation.simulator_gui import ModbusServerControlGUI
from src.GUI.planner_gui.test_planner import TestPlannerMain
from src.GUI.side_operations.h2_uptake_correction_gui import UptakeCorrectionWindow
from src.GUI.hot_disk_sequenzer.suquenzer_gui import SequenzerMainWindow
from src.GUI.etc_measurement_starter.start_etc_measurement_gui import StartETCMeasurementGUI
from src.GUI.plot_individualizer.plot_individualizer import PlotIndividualizerMainWindow
from src.GUI.hydride_handler_gui.hydride_handler_main import HydrideHandlerMainWindow
from src.GUI.meta_data_gui.meta_data_gui import MetaDataGUI

try:
    import src.infrastructure.core.logger as logging
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
        self.ui.actionDatabase_Maintenance.triggered.connect(self._open_database_maintainer)
        self.ui.actionETC_Measurement_Starter.triggered.connect(self._open_etc_measurement_performer)
        self.ui.actionPlot_Individualizer.triggered.connect(self._open_plot_individualizer)
        self.ui.actionHydride_Handler.triggered.connect(self._open_hydride_handler)
        self.ui.actionMeta_Data.triggered.connect(self._open_meta_data_handler)


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
        constraints = self.controller.constraints
        export_thread = threading.Thread(target=exporter.export_all, args=(constraints,), daemon=True)
        self.logger.info(f"Start exporting {self.meta_data.sample_id}")
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
        self.schedule_creator = SequenzerMainWindow(config=self.config)
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

    def change_modbus_host_ip(self, host_ip=None, port=None, switch_off=False):
        """
        Callback for DICON simulator start/stop; reconfigures the Modbus host/port.

        Args:
            host_ip (str, optional): New Modbus server host.
            port (int or str, optional): New Modbus server port.
        """
        recorder_was_running = self.controller.is_tp_recording_running()
        if recorder_was_running:
            self._disconnect_set_state_signals()
            self.controller.stop_t_p_recording()

        if host_ip and port:
            self.prev_mb_conn_params = self.controller.recorder.mb_processor.mb_conn_params

            self.controller.recorder.mb_processor.mb_conn_params["MB_PORT"] = str(port)
            self.controller.recorder.mb_processor.mb_conn_params["MB_HOST"] = str(host_ip)

        elif self.prev_mb_conn_params:
            self.controller.recorder.mb_processor.mb_conn_params = self.prev_mb_conn_params
        if recorder_was_running and not switch_off:
            self.controller.start_t_p_recording()
            self._connect_set_state_signals()

    def _open_database_maintainer(self):
        from src.GUI.database_maintenance.database_maintainer import MaintenanceWindow
        self.db_maintainer = MaintenanceWindow(db_conn_params=self.db_conn_params)
        self.db_maintainer.show()
        self.db_maintainer.started.connect(self.controller.stop_t_p_recording)

    def _open_etc_measurement_performer(self):
        try:
            self.etc_ms = StartETCMeasurementGUI(config=self.config,
                                            standard_etc_folder_path=global_vars.standard_etc_folder_path,
                                            meta_data=self.meta_data)
            self.etc_ms.show()
        except Exception as e:
            self.logger.error(f"Error opening measurement performer: {e}")
        #etc_ms.new_etc_data_written.connect(self.controller.recorder._emit_etc_data)

    def _open_plot_individualizer(self):
        try:
            self.plot_individualizer = PlotIndividualizerMainWindow(config=self.config,
                                                                meta_data=self.meta_data)
            self.plot_individualizer.show()
        except Exception as e:
            self.logger.error(f"Error opening plot individualizer: {e}")

    def _open_hydride_handler(self):
        try:
            self.hydride_handler = HydrideHandlerMainWindow()
            self.hydride_handler.show()

        except Exception as e:
            self.logger.error(f"Error opening hydride handler {e}")

    def _open_meta_data_handler(self):
        try:
            self.meta_gui = MetaDataGUI()
            self.meta_gui.show()

        except Exception as e:
            self.logger.error(f"Error opening meta data GUI {e}")

    def closeEvent(self, event):
        """
        Clean up any running simulators before the main window closes.

        Overrides the base closeEvent to stop the DICON simulator.
        """
        if hasattr(self, "dicon_simulator"):
            self.change_modbus_host_ip(switch_off=True)  #switch host and ip back to standard
            self.dicon_simulator.business.stop_server()
        super().closeEvent(event)


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
        from src.infrastructure.core.config_reader import config
        main_program = MainProgram(config=config)
        main_program.show()
    except Exception as e:
        logger.error(f"Uhpsi daisy: {e}")


if __name__ == '__main__':
    main()















