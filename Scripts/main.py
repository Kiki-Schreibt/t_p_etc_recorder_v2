import os
import threading
import sys

from PySide6.QtWidgets import QApplication
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile

from src.GUI.config_creation.config_creator_ui_main import ConfigWindow
from src.standard_paths import standard_config_file_path
from src.GUI.recording_gui.recording_main_v2 import RecordingMainWindow
from src.simulation.simulator_gui import ModbusServerControlGUI
from test_planner import TestPlannerMain
from src.GUI.hot_disk_sequenzer.suquenzer_gui import ScheduleGeneratorMain


#main_ui_file_path = r"..\src\GUI\recording_gui\recording_ui_design_works.ui"
def load_ui_file(ui_file_path):
        try:
            loader = QUiLoader()
            ui_file = QFile(ui_file_path)

            ui_file.open(QFile.ReadOnly)
            ui = loader.load(ui_file)
            ui_file.close()
            if not ui:
                print("Failed to load UI file")
                return
            return ui

        except Exception as e:
            print("Unable to load ui file %s", e)


class MainProgram(RecordingMainWindow):
    def __init__(self):
        super().__init__()
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
        exporter = QuickExport()
        sample_id = self.meta_data.sample_id
        export_thread = threading.Thread(target=exporter.export_all, args=(sample_id,), daemon=True)
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
        if host_ip and port:
            if self.controller.recorder.mb_processor_thread:
                self.controller.recorder.stop_t_p_recording_thread()
                recorder_was_running = True
            else:
                recorder_was_running = False

            self.prev_port = self.controller.recorder.mb_processor.mb_port
            self.prev_host = self.controller.recorder.mb_processor.mb_host
            print(host_ip)
            print(port)
            self.controller.recorder.mb_processor.mb_port = port
            self.controller.recorder.mb_processor.mb_host = host_ip
            if recorder_was_running:
                self.controller.recorder.start_t_p_recording_thread()

        elif self.prev_host and self.prev_port:
            if self.controller.recorder.mb_processor_thread:
                self.controller.recorder.stop_t_p_recording_thread()
                recorder_was_running = True
            else:
                recorder_was_running = False
            self.controller.recorder.stop_t_p_recording_thread()
            self.controller.recorder.mb_processor.mb_port = self.prev_port
            self.controller.recorder.mb_processor.mb_host = self.prev_host

            if recorder_was_running:
                self.controller.recorder.start_t_p_recording_thread()

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
    main_program = MainProgram()
    main_program.show()


if __name__ == '__main__':
    main()















