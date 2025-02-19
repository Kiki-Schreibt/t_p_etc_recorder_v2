#recording_main_v3.py
# ===============================
#         UI CONTROLLERS
# ===============================

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QMessageBox
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile, Qt
from PySide6.QtGui import QDoubleValidator, QFont

from src.meta_data.meta_data_handler import MetaData
from src.GUI.recording_gui.recording_business_v2 import DataRecorder
from src.standard_paths import recording_ui_file_path

try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging
# Constants
FONT = QFont("Arial", 8)
local_tz = ZoneInfo("Europe/Berlin")
STANDARD_CONSTRAINTS = {
    "min_TotalCharTime": 0.33,
    "max_TotalCharTime": 1,
    "min_TotalTempIncr": 2,
    "max_TotalTempIncr": 5
}


class MetaDataManager:
    """
    Synchronizes UI fields with the MetaData object.
    """
    def __init__(self, meta_data: MetaData):
        self.meta_data = meta_data

    def update_from_ui(self, ui):
        self.meta_data.sample_id = ui.sample_id_edit_field.text()
        self.meta_data.sample_mass = self._to_float(ui.sample_mass_edit_field.text())
        self.meta_data.sample_material = ui.material_composition_edit_field.text()
        self.meta_data.measurement_cell = ui.cell_type_edit_field.text()
        self.meta_data.volume_measurement_cell = self._to_float(ui.cell_volume_edit_field.text())
        self.meta_data.first_hydrogenation = self._parse_date(ui.first_hyd_edit_field.text())
        self.meta_data.start_time = self._parse_date(ui.test_start_edit_field.text())
        self.meta_data.end_time = self._parse_date(ui.test_end_edit_field.text())
        self.meta_data.max_pressure_cycling = self._to_float(ui.max_pressure_cycling_edit_field.text())
        self.meta_data.min_temperature_cycling = self._to_float(ui.min_temperature_cycling_edit_field.text())
        self.meta_data.average_cycle_duration = self._parse_duration(ui.cycle_duration_edit_field.text())
        self.meta_data.write()

    def update_ui(self, ui):
        ui.sample_id_edit_field.setText(self.meta_data.sample_id or "")
        ui.sample_mass_edit_field.setText(str(self.meta_data.sample_mass) if self.meta_data.sample_mass is not None else "")
        ui.material_composition_edit_field.setText(self.meta_data.sample_material or "")
        ui.cell_volume_edit_field.setText(str(self.meta_data.volume_measurement_cell) if self.meta_data.volume_measurement_cell is not None else "")
        ui.cell_type_edit_field.setText(self.meta_data.measurement_cell or "")
        ui.first_hyd_edit_field.setText(self.meta_data.first_hydrogenation.strftime("%Y-%m-%d %H:%M:%S") if self.meta_data.first_hydrogenation else "")
        ui.test_start_edit_field.setText(self.meta_data.start_time.strftime("%Y-%m-%d %H:%M:%S") if self.meta_data.start_time else "")
        ui.test_end_edit_field.setText(self.meta_data.end_time.strftime("%Y-%m-%d %H:%M:%S") if self.meta_data.end_time else "")
        ui.max_pressure_cycling_edit_field.setText(str(self.meta_data.max_pressure_cycling) if self.meta_data.max_pressure_cycling is not None else "")
        ui.min_temperature_cycling_edit_field.setText(str(self.meta_data.min_temperature_cycling) if self.meta_data.min_temperature_cycling is not None else "")
        ui.cycle_duration_edit_field.setText(str(self.meta_data.average_cycle_duration) if self.meta_data.average_cycle_duration else "")

    @staticmethod
    def _to_float(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_date(date_str):
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    @staticmethod
    def _parse_duration(duration_str):
        if not duration_str:
            return None
        try:
            h, m, s = map(int, duration_str.split(':'))
            return timedelta(hours=h, minutes=m, seconds=s)
        except ValueError:
            return None


class PlotManager:
    """
    Manages creation, clearing, and arrangement of plot widgets.
    """
    def __init__(self, ui, meta_data: MetaData, logger):
        self.ui = ui
        self.meta_data = meta_data
        self.logger = logger
        self.top_plot = None
        self.bottom_plot = None
        self.right_plot = None
        self.current_xy_dropdown_text = ''

    def clear_plots(self):
        for widget in [self.top_plot, self.bottom_plot, self.right_plot]:
            if widget:
                widget.setParent(None)
                widget.deleteLater()
        self.top_plot = self.bottom_plot = self.right_plot = None

    def init_continuous_plots(self):
        from src.GUI.recording_gui.recording_business_v2 import ContinuousPlotWindow
        self.clear_plots()
        self.top_plot = ContinuousPlotWindow(y_axis="temperature", meta_data=self.meta_data)
        self.bottom_plot = ContinuousPlotWindow(y_axis="pressure", meta_data=self.meta_data)
        self._ensure_layout(self.ui.ExpMetaPlotLeft)
        self._ensure_layout(self.ui.ExpMetaPlotLeftLower)
        self.ui.ExpMetaPlotLeft.layout().addWidget(self.top_plot)
        self.ui.ExpMetaPlotLeftLower.layout().addWidget(self.bottom_plot)
        self.bottom_plot.setXLink(self.top_plot)
        return self.top_plot, self.bottom_plot

    def init_static_plots(self):
        from src.GUI.recording_gui.recording_business_v2 import StaticPlotWindow
        self.clear_plots()
        self.top_plot = StaticPlotWindow(y_axis="temperature", meta_data=self.meta_data)
        self.bottom_plot = StaticPlotWindow(y_axis="pressure", meta_data=self.meta_data)
        self._ensure_layout(self.ui.ExpMetaPlotLeft)
        self._ensure_layout(self.ui.ExpMetaPlotLeftLower)
        self.ui.ExpMetaPlotLeft.layout().addWidget(self.top_plot)
        self.ui.ExpMetaPlotLeftLower.layout().addWidget(self.bottom_plot)
        self.bottom_plot.setXLink(self.top_plot)
        return self.top_plot, self.bottom_plot

    def init_right_plot_xy(self, dropdown_text):

        self.current_xy_dropdown_text = dropdown_text
        self.top_plot._reset_point_colors()
        self.bottom_plot._reset_point_colors()

        if self.right_plot:
            self.right_plot.setParent(None)
            self.right_plot.deleteLater()
        from src.GUI.recording_gui.recording_business_v2 import XYPlot
        self.right_plot = XYPlot()
        self._ensure_layout(self.ui.xy_plot_window)
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)
        self.top_plot.point_clicked_time_received.connect(lambda time, color: self.right_plot.add_curve_to_plot(time_value=time, color=color, xy_data_to_load=dropdown_text))
        return self.right_plot

    def init_uptake_plot(self, time_range):
        from src.GUI.recording_gui.recording_business_v2 import UptakePlot
        if self.right_plot:
            self.right_plot.setParent(None)
            self.right_plot.deleteLater()
        self.right_plot = UptakePlot()
        self.right_plot.load_data(meta_data=self.meta_data, time_range=time_range)
        self._ensure_layout(self.ui.xy_plot_window)
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)
        return self.right_plot

    def init_tp_dependent_plot(self, x_col, time_range):
        if self.right_plot:
            self.right_plot.setParent(None)
            self.right_plot.deleteLater()
        from src.GUI.recording_gui.recording_business_v2 import ReadPlotTpDependent
        self.right_plot = ReadPlotTpDependent.PlotTpDependent()
        self.right_plot.load_data(time_range=time_range, x_col=x_col)
        self._ensure_layout(self.ui.xy_plot_window)
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)
        return self.right_plot

    def _ensure_layout(self, widget):
        if widget.layout() is None:
            widget.setLayout(QVBoxLayout())


class MainController:
    """
    Coordinates business logic (DataRecorder) and UI plot management.
    """
    def __init__(self, meta_data: MetaData, plot_manager: PlotManager, logger):
        self.meta_data = meta_data
        self.plot_manager = plot_manager
        self.logger = logger
        self.recorder = DataRecorder(meta_data=self.meta_data)
        self.constraints = STANDARD_CONSTRAINTS.copy()
        self.reservoir_volume = 1

    def set_reservoir_volume(self, volume):
        self.reservoir_volume = volume
        self.meta_data.reservoir_volume = volume
        self.recorder.update_reservoir_volume(volume)

    def start_tp_recording(self):
        top, bottom = self.plot_manager.init_continuous_plots()
        self.recorder.start_tp_recording()
        top.reader.start()
        bottom.reader.start()
        self.recorder.newEtcDataWritten.connect(top.update_plot_right)
        self.recorder.newEtcDataWritten.connect(bottom.update_plot_right)
        return top, bottom

    def stop_tp_recording(self):
        self.recorder.stop_all_recording()
        if self.plot_manager.top_plot:
            self.plot_manager.top_plot.reader.stop()
        if self.plot_manager.bottom_plot:
            self.plot_manager.bottom_plot.reader.stop()

    def start_log_tracking(self):
        self.recorder.start_etc_recording()

    def stop_log_tracking(self):
        self.recorder.stop_etc_recording()

    def set_constraints(self, constraints):
        self.constraints = constraints
        if self.plot_manager.top_plot and self.plot_manager.bottom_plot:
            self.plot_manager.top_plot.reader.update_constraints_etc(constraints)
            self.plot_manager.bottom_plot.reader.update_constraints_etc(constraints)

    def update_sample_id(self, sample_id):
        self.meta_data.sample_id = sample_id
        self.meta_data.read()
        self.recorder.update_sample_id(sample_id)
        if self.plot_manager.top_plot:
            self.plot_manager.top_plot.reader.on_meta_data_changed(new_meta_data=self.meta_data)
        if self.plot_manager.bottom_plot:
            self.plot_manager.bottom_plot.reader.on_meta_data_changed(new_meta_data=self.meta_data)

    def plot_full_test(self):
        self.recorder.stop_all_recording()
        if self.plot_manager.top_plot:
            self.plot_manager.top_plot.reader.stop()
        if self.plot_manager.bottom_plot:
            self.plot_manager.bottom_plot.reader.stop()
        self.plot_manager.top_plot.update_on_data = True
        self.plot_manager.top_plot.update_on_record = False
        self.plot_manager.top_plot.read_on_change = True
        self.plot_manager.top_plot.reader.reading_mode = "full_test"
        self.plot_manager.top_plot.reader.p_data_sig.connect(self.plot_manager.bottom_plot.update_plot_left)
        self.plot_manager.top_plot.reader.etc_data_sig.connect(self.plot_manager.bottom_plot.update_plot_right)
        self.plot_manager.top_plot.reader.cycles_full_test_sig.connect(self.plot_manager.bottom_plot.update_min_max_plot)
        self.plot_manager.top_plot.reader.start()

    def toggle_h2_uptake(self, enabled):
        self.recorder.update_h2_uptake_flag(enabled)

    def toggle_cycling(self, enabled):
        self.recorder.update_cycling_flag(enabled)


# ===============================
#         MAIN WINDOW
# ===============================
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("T-p ETC Recorder")
        from src.GUI.qt_styles import aqua as style
        self.setStyleSheet(style)
        self.setFont(FONT)
        self.logger = logging.getLogger(__name__)
        self.ui = self._load_ui(recording_ui_file_path)
        self.setCentralWidget(self.ui)
        self.meta_data = MetaData()
        self.meta_manager = MetaDataManager(self.meta_data)
        self.plot_manager = PlotManager(self.ui, self.meta_data, self.logger)
        self.controller = MainController(self.meta_data, self.plot_manager, self.logger)
        self._init_ui()
        self._init_connections()
        self._setup_validators()
        self._load_default_constraints()
        self._sync_constraints_to_ui()
        self.plot_manager.init_continuous_plots()

    def _load_ui(self, ui_path):
        from PySide6.QtUiTools import QUiLoader
        from PySide6.QtCore import QFile
        loader = QUiLoader()
        ui_file = QFile(ui_path)
        if not ui_file.exists():
            self.logger.error(f"UI file {ui_path} does not exist.")
            return None
        ui_file.open(QFile.ReadOnly)
        ui = loader.load(ui_file)
        ui_file.close()
        if ui is None:
            self.logger.error("Failed to load UI file.")
        return ui

    def _init_ui(self):
        self.ui.xy_plot_window.setLayout(QVBoxLayout())
        self.setMinimumSize(1600, 900)
        self.ui.reservoir_volume_edit_field.setText(str(self.controller.reservoir_volume))
        self.ui.sample_id_edit_field.setText(self.meta_data.sample_id or "")

    def _init_connections(self):
        self.ui.update_meta_data_button.clicked.connect(self._update_meta_data)
        self.ui.start_stop_tp_recording_button.setCheckable(True)
        self.ui.start_stop_tp_recording_button.clicked.connect(self._toggle_tp_recording)
        self.ui.start_stop_log_file_tracker_button.setCheckable(True)
        self.ui.start_stop_log_file_tracker_button.clicked.connect(self._toggle_log_tracking)
        self.ui.start_stop_static_plot_button.setCheckable(True)
        self.ui.start_stop_static_plot_button.clicked.connect(self._toggle_plotting_mode)
        self.ui.plot_uptake_button.clicked.connect(self._init_uptake_plot)
        self.ui.XyDataSelectDropDown.highlighted.connect(self._init_right_plot_xy)

        self.ui.T_p_dependent_drop_down.currentIndexChanged.connect(self._init_tp_dependent_plot)
        self.ui.h2_uptake_check_box.clicked.connect(lambda: self.controller.toggle_h2_uptake(
            self.ui.h2_uptake_check_box.isChecked()))
        self.ui.cycle_test_check_box.clicked.connect(lambda: self.controller.toggle_cycling(
            self.ui.cycle_test_check_box.isChecked()))
        self.ui.sample_id_edit_field.editingFinished.connect(self._on_sample_id_changed)
        self.ui.sample_mass_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.material_composition_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.cell_volume_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.cell_type_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.first_hyd_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.test_start_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.test_end_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.max_pressure_cycling_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.min_temperature_cycling_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.cycle_duration_edit_field.editingFinished.connect(self._on_meta_data_field_changed)
        self.ui.reservoir_volume_edit_field.editingFinished.connect(self._on_reservoir_volume_changed)
        self.ui.MinCharTimeEditField.editingFinished.connect(self._on_constraints_changed)
        self.ui.MaxCharTimeEditField.editingFinished.connect(self._on_constraints_changed)
        self.ui.MinTempIncEditField.editingFinished.connect(self._on_constraints_changed)
        self.ui.MaxTempIncEditField.editingFinished.connect(self._on_constraints_changed)

    def _setup_validators(self):
        validator = QDoubleValidator()
        self.ui.sample_mass_edit_field.setValidator(validator)
        self.ui.cell_volume_edit_field.setValidator(validator)
        self.ui.reservoir_volume_edit_field.setValidator(validator)
        self.ui.max_pressure_cycling_edit_field.setValidator(validator)
        self.ui.min_temperature_cycling_edit_field.setValidator(validator)

    def _load_default_constraints(self):
        self.controller.set_constraints(STANDARD_CONSTRAINTS.copy())

    def _sync_constraints_to_ui(self):
        cons = self.controller.constraints
        self.ui.MinCharTimeEditField.setText(str(cons["min_TotalCharTime"]))
        self.ui.MaxCharTimeEditField.setText(str(cons["max_TotalCharTime"]))
        self.ui.MinTempIncEditField.setText(str(cons["min_TotalTempIncr"]))
        self.ui.MaxTempIncEditField.setText(str(cons["max_TotalTempIncr"]))

    def _update_meta_data(self):
        self.meta_manager.update_from_ui(self.ui)
        self.meta_manager.update_ui(self.ui)
        self.controller.update_sample_id(self.meta_data.sample_id)

    def _toggle_tp_recording(self):
        is_on = self.ui.start_stop_tp_recording_button.isChecked()
        self._toggle_button(self.ui.start_stop_tp_recording_button,
                            "Start T-p Recording", "Stop T-p Recording", is_on)
        if is_on:
            self.controller.start_tp_recording()
            self.controller.plot_manager.top_plot.current_cycle_sig.connect(
                lambda n: self._set_current_state("number", n))
            self.controller.plot_manager.top_plot.current_state_sig.connect(
                lambda s: self._set_current_state("state", s))
            self.controller.plot_manager.top_plot.current_uptake_sig.connect(
                lambda u: self._set_current_state("uptake", u))
        else:
            self.controller.stop_tp_recording()

    def _toggle_log_tracking(self):
        is_on = self.ui.start_stop_log_file_tracker_button.isChecked()
        self._toggle_button(self.ui.start_stop_log_file_tracker_button,
                            "Start Log File Tracking", "Stop Log File Tracking", is_on)
        if is_on:
            self.controller.start_log_tracking()
        else:
            self.controller.stop_log_tracking()

    def _toggle_plotting_mode(self):
        is_on = self.ui.start_stop_static_plot_button.isChecked()
        self._toggle_button(self.ui.start_stop_static_plot_button,
                            "Start Static Plot", "Stop Static Plot", is_on)
        if is_on:
            top, bottom = self.plot_manager.init_static_plots()
            top.reader.meta_data_sig.connect(self._meta_data_received)
            if self.meta_data.sample_id:
                self.controller.plot_full_test()
        else:
            self.plot_manager.clear_plots()
            self.plot_manager.init_continuous_plots()

    def _init_right_plot_xy(self):
        self.plot_manager.init_right_plot_xy(self.ui.XyDataSelectDropDown.currentText())

    def _init_uptake_plot(self):
        view_range = self.plot_manager.top_plot.plotItem.viewRange()[0] if self.plot_manager.top_plot else None
        time_range = [datetime.fromtimestamp(ts, tz=local_tz) for ts in view_range] if view_range else None
        self.plot_manager.init_uptake_plot(time_range)

    def _init_tp_dependent_plot(self):
        drop_val = self.ui.T_p_dependent_drop_down.currentText().lower()
        x_col = 'pressure' if 'pressure' in drop_val else 'temperature' if 'temperature' in drop_val else None
        if self.plot_manager.bottom_plot:
            stamps = self.plot_manager.bottom_plot.plotItem.viewRange()[0]
            time_range = [datetime.fromtimestamp(ts, tz=local_tz) for ts in stamps]
            self.plot_manager.init_tp_dependent_plot(x_col, time_range)

    def _on_sample_id_changed(self):
        sample_id = self.ui.sample_id_edit_field.text()
        self.controller.update_sample_id(sample_id)
        self.meta_manager.update_ui(self.ui)
        if self.ui.start_stop_static_plot_button.isChecked():
            self.controller.plot_full_test()

    def _on_reservoir_volume_changed(self):
        vol = self._to_float(self.ui.reservoir_volume_edit_field.text())
        if vol is None:
            QMessageBox.warning(self, "Invalid Input", "Please enter a numeric value for reservoir volume.")
        else:
            self.controller.set_reservoir_volume(vol)

    def _on_meta_data_field_changed(self):
        self.meta_manager.update_from_ui(self.ui)
        self.meta_manager.update_ui(self.ui)
        self.controller.update_sample_id(self.meta_data.sample_id)

    def _on_constraints_changed(self):
        fields = [self.ui.MinCharTimeEditField.text(), self.ui.MaxCharTimeEditField.text(),
                  self.ui.MinTempIncEditField.text(), self.ui.MaxTempIncEditField.text()]
        if all(self._is_numeric(val) for val in fields):
            cons = {
                "min_TotalCharTime": float(self.ui.MinCharTimeEditField.text()),
                "max_TotalCharTime": float(self.ui.MaxCharTimeEditField.text()),
                "min_TotalTempIncr": float(self.ui.MinTempIncEditField.text()),
                "max_TotalTempIncr": float(self.ui.MaxTempIncEditField.text())
            }
            self.controller.set_constraints(cons)
        else:
            QMessageBox.warning(self, "Invalid Input", "Please enter numeric values for constraints.")

    def _meta_data_received(self, meta_data):
        self.meta_data = meta_data
        self.meta_manager.meta_data = meta_data
        self.meta_manager.update_ui(self.ui)
        self.controller.update_sample_id(meta_data.sample_id)

    def _toggle_button(self, button, start_text, stop_text, is_on):
        if is_on:
            button.setText(stop_text)
            button.setStyleSheet("background-color: darkgreen")
        else:
            button.setText(start_text)
            button.setStyleSheet("background-color: lightgray")

    @staticmethod
    def _to_float(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _is_numeric(value):
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _set_current_state(self, key, value):
        if key.lower() == "number":
            self.ui.current_cycle_edit_field.setText(str(value))
        elif key.lower() == "state":
            self.ui.current_de_hyd_state_edit_field.setText(str(value))
        elif key.lower() == "uptake":
            self.ui.current_uptake_edit_field.setText(str(value))

    def closeEvent(self, event):
        self.controller.stop_tp_recording()
        super().closeEvent(event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self.plot_manager.top_plot and hasattr(self.plot_manager.top_plot, 'adjust_plot_size'):
            self.plot_manager.top_plot.adjust_plot_size()
        if self.plot_manager.bottom_plot and hasattr(self.plot_manager.bottom_plot, 'adjust_plot_size'):
            self.plot_manager.bottom_plot.adjust_plot_size()
        if self.plot_manager.right_plot and hasattr(self.plot_manager.right_plot, 'adjust_plot_size'):
            self.plot_manager.right_plot.adjust_plot_size()


def main():
    app = QApplication([])
    main_window = MainWindow()
    main_window.show()
    app.exec()


if __name__ == "__main__":
    main()
