from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QMessageBox
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile, Qt
from PySide6.QtGui import QDoubleValidator
from PySide6 import QtGui

# Assume these are unchanged imports
from src.GUI.recording_gui.recording_business_v2 import (
    PlotContinuousWindow,
    PlotStaticWindow,
    Record,
    ReadPlotUptake,
    ReadPlotTpDependent,
    ReadPlotXY
)
from src.meta_data.meta_data_handler import MetaData
from src.standard_paths import (
    recording_ui_file_path,
    standard_t_p_test_data_folder_path,
)
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging

from src.GUI.qt_styles import aqua as style

# Constants
FONT = QtGui.QFont("Arial", 8)
local_tz_reg = ZoneInfo("Europe/Berlin")
standard_constraints_dict = {
    "min_TotalCharTime": 0.33,
    "max_TotalCharTime": 1,
    "min_TotalTempIncr": 2,
    "max_TotalTempIncr": 5
}
READING_MODE_FULL_TEST = 'full_test'
READING_MODE_BY_TIME = 'by_time'


class MetaDataManager:
    """
    Handles reading and writing meta-data fields to and from the MetaData object.
    Provides methods to sync between the UI and MetaData.
    """
    def __init__(self, meta_data: MetaData):
        self.meta_data = meta_data

    def update_from_ui_fields(self, ui):
        # Convert inputs and assign to meta_data
        self.meta_data.sample_id = ui.sample_id_edit_field.text()
        self.meta_data.sample_mass = self._convert_to_numeric(ui.sample_mass_edit_field.text())
        self.meta_data.sample_material = ui.material_composition_edit_field.text()
        self.meta_data.measurement_cell = ui.cell_type_edit_field.text()
        self.meta_data.volume_measurement_cell = self._convert_to_numeric(ui.cell_volume_edit_field.text())
        self.meta_data.first_hydrogenation = self._parse_date(ui.first_hyd_edit_field.text())
        self.meta_data.start_time = self._parse_date(ui.test_start_edit_field.text())
        self.meta_data.end_time = self._parse_date(ui.test_end_edit_field.text())
        self.meta_data.max_pressure_cycling = self._convert_to_numeric(ui.max_pressure_cycling_edit_field.text())
        self.meta_data.min_temperature_cycling = self._convert_to_numeric(ui.min_temperature_cycling_edit_field.text())
        self.meta_data.average_cycle_duration = self._parse_duration(ui.cycle_duration_edit_field.text())
        self.meta_data.write()

    def update_ui_fields(self, ui):
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
    def _convert_to_numeric(value):
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
            hours, minutes, seconds = map(int, duration_str.split(':'))
            return timedelta(hours=hours, minutes=minutes, seconds=seconds)
        except ValueError:
            return None


class PlotManager:
    """
    Manages the creation, switching, and cleanup of plot widgets.
    Handles continuous/static modes and right-side plots (XY, Uptake, T-p dependent).
    """
    def __init__(self, ui, meta_data, logger):
        self.ui = ui
        self.meta_data = meta_data
        self.logger = logger
        self.left_upper_plot = None
        self.left_lower_plot = None
        self.right_plot = None

    def initialize_continuous_plots(self):
        self.clear_plots()
        self.left_upper_plot = PlotContinuousWindow(y_axis="temperature", meta_data=self.meta_data)
        self.left_lower_plot = PlotContinuousWindow(y_axis="pressure", meta_data=self.meta_data)

        # Setup layouts
        self._ensure_layout(self.ui.ExpMetaPlotLeft)
        self._ensure_layout(self.ui.ExpMetaPlotLeftLower)

        self.ui.ExpMetaPlotLeft.layout().addWidget(self.left_upper_plot)
        self.ui.ExpMetaPlotLeftLower.layout().addWidget(self.left_lower_plot)

        self.left_lower_plot.setXLink(self.left_upper_plot)
        return self.left_upper_plot, self.left_lower_plot

    def initialize_static_plots(self):
        self.clear_plots()
        self.left_upper_plot = PlotStaticWindow(y_axis="temperature", meta_data=self.meta_data)
        self.left_lower_plot = PlotStaticWindow(y_axis="pressure", meta_data=self.meta_data)

        self._ensure_layout(self.ui.ExpMetaPlotLeft)
        self._ensure_layout(self.ui.ExpMetaPlotLeftLower)

        self.ui.ExpMetaPlotLeft.layout().addWidget(self.left_upper_plot)
        self.ui.ExpMetaPlotLeftLower.layout().addWidget(self.left_lower_plot)
        self.left_lower_plot.setXLink(self.left_upper_plot)
        return self.left_upper_plot, self.left_lower_plot

    def init_right_plot_xy(self, current_text):
        self.remove_right_plot()
        self.right_plot = ReadPlotXY()
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)

        self.ui.XyDataSelectDropDown.currentIndexChanged.connect(self.right_plot.clear_plot)
        self.ui.XyDataSelectDropDown.currentIndexChanged.connect(self.left_upper_plot._reset_point_colors)
        self.ui.XyDataSelectDropDown.currentIndexChanged.connect(self.left_lower_plot._reset_point_colors)

        self.left_upper_plot.point_clicked_time_received.connect(
            lambda d1, d2: self.right_plot.add_curve_to_plot(d1, d2, self.ui.XyDataSelectDropDown.currentText())
        )

        return self.right_plot

    def init_plot_uptake(self, time_range):
        self.remove_right_plot()
        self.right_plot = ReadPlotUptake()
        self.right_plot.load_data(meta_data=self.meta_data, time_range=time_range)
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)
        return self.right_plot

    def init_plot_tp_dependent(self, x_col, time_range):
        self.remove_right_plot()
        if x_col not in ['pressure', 'temperature']:
            self.logger.error("Invalid selection for T-p dependent plot.")
            return None
        self.right_plot = ReadPlotTpDependent.PlotTpDependent()
        self.right_plot.load_data(time_range=time_range, x_col=x_col)
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)
        return self.right_plot

    def clear_plots(self):
        # Stop and remove left plots
        if self.left_upper_plot and hasattr(self.left_upper_plot.reader, 'stop'):
            self.left_upper_plot.reader.stop()
        if self.left_lower_plot and hasattr(self.left_lower_plot.reader, 'stop'):
            self.left_lower_plot.reader.stop()

        self._remove_widget(self.left_upper_plot, self.ui.ExpMetaPlotLeft)
        self._remove_widget(self.left_lower_plot, self.ui.ExpMetaPlotLeftLower)
        self.left_upper_plot = None
        self.left_lower_plot = None

        self.remove_right_plot()

    def remove_right_plot(self):
        if self.right_plot:
            layout = self.ui.xy_plot_window.layout()
            if layout:
                layout.removeWidget(self.right_plot)
            self.right_plot.deleteLater()
            self.right_plot = None

    def _ensure_layout(self, widget):
        if widget.layout() is None:
            widget.setLayout(QVBoxLayout())

    def _remove_widget(self, widget, container):
        if widget:
            layout = container.layout()
            if layout:
                layout.removeWidget(widget)
            widget.deleteLater()


class MainController:
    """
    The main application logic controller.
    Coordinates MetaDataManager, Record (data recording), PlotManager, and constraints.
    """
    def __init__(self, meta_data: MetaData, plot_manager: PlotManager, logger):
        self.meta_data = meta_data
        self.plot_manager = plot_manager
        self.logger = logger
        self.recorder = Record(meta_data=self.meta_data)

        # Default constraints
        self.constraints_dict = standard_constraints_dict.copy()
        self.reservoir_volume = 1

    def set_reservoir_volume(self, volume):
        if volume is not None:
            self.reservoir_volume = volume
            self.meta_data.reservoir_volume = self.reservoir_volume
            self.recorder.update_reservoir_volume(self.reservoir_volume)

    def start_tp_recording(self):
        # Continuous plots
        upper_plot, lower_plot = self.plot_manager.initialize_continuous_plots()
        self.recorder.start_t_p_recording_thread()
        lower_plot.reader.start()
        upper_plot.reader.start()

        # Connect signals
        self.recorder.new_etc_data_written_to_database.connect(lower_plot.update_plot_right)
        self.recorder.new_etc_data_written_to_database.connect(upper_plot.update_plot_right)
        return upper_plot, lower_plot

    def stop_tp_recording(self):
        self.recorder.stop_recording_all()
        if self.plot_manager.left_lower_plot:
            self.plot_manager.left_lower_plot.reader.stop()
        if self.plot_manager.left_upper_plot:
            self.plot_manager.left_upper_plot.reader.stop()

    def start_log_file_tracking(self):
        self.recorder.start_etc_recording_thread()

    def stop_log_file_tracking(self):
        self.recorder.stop_etc_recording_thread()

    def set_constraints(self, constraints):
        self.constraints_dict = constraints
        # Update readers if available
        if self.plot_manager.left_upper_plot and self.plot_manager.left_lower_plot:
            self.plot_manager.left_upper_plot.reader.update_constraints_etc(self.constraints_dict)
            self.plot_manager.left_lower_plot.reader.update_constraints_etc(self.constraints_dict)

    def update_sample_id(self, sample_id):
        self.meta_data.sample_id = sample_id
        self.meta_data.read()
        self.recorder.update_sample_id(sample_id)
        # Update plot readers if plots exist
        if self.plot_manager.left_upper_plot:
            self.plot_manager.left_upper_plot.reader.on_meta_data_changed(new_meta_data=self.meta_data)
        if self.plot_manager.left_lower_plot:
            self.plot_manager.left_lower_plot.reader.on_meta_data_changed(new_meta_data=self.meta_data)

    def plot_full_test(self):
        self.recorder.stop_recording_all()
        if self.plot_manager.left_lower_plot:
            self.plot_manager.left_lower_plot.reader.stop()
        if self.plot_manager.left_upper_plot:
            self.plot_manager.left_upper_plot.reader.stop()

        self.plot_manager.left_upper_plot.update_on_data = True
        self.plot_manager.left_upper_plot.update_on_record = False
        self.plot_manager.left_upper_plot.read_on_change = True
        self.plot_manager.left_upper_plot.reader.reading_mode = READING_MODE_FULL_TEST
        self.plot_manager.left_upper_plot.reader.p_data_sig.connect(self.plot_manager.left_lower_plot.update_plot_left)
        self.plot_manager.left_upper_plot.reader.etc_data_sig.connect(self.plot_manager.left_lower_plot.update_plot_right)
        self.plot_manager.left_upper_plot.reader.cycles_full_test_sig.connect(self.plot_manager.left_lower_plot.update_min_max_plot)
        self.plot_manager.left_upper_plot.reader.start()

    def toggle_h2_uptake_flag(self, enabled):
        self.recorder.update_h2_uptake_flag(enabled)

    def toggle_cycling_flag(self, enabled):
        self.recorder.update_cycling_flag(enabled)


class RecordingMainWindow(QMainWindow):
    """
    The main application window that delegates logic to the MainController and PlotManager.
    Handles all UI interactions, input validation, and updates to the interface.
    """
    def __init__(self):
        super().__init__()
        self.setWindowTitle("T-p ETC Recorder")
        self.setStyleSheet(style)
        self.setFont(FONT)
        self.logger = logging.getLogger(__name__)

        # Load UI
        self.ui = self.load_ui(recording_ui_file_path)

        # Initialize data and managers
        self.meta_data = MetaData()
        self.meta_data_manager = MetaDataManager(self.meta_data)
        self.plot_manager = PlotManager(self.ui, self.meta_data, self.logger)
        self.controller = MainController(self.meta_data, self.plot_manager, self.logger)

        self.actual_button_color = "lightgray"
        self.activated_button_color = "darkgreen"

        self.init_ui()
        self.init_connections()
        self.setup_validators()

        # Initialize constraints from standard and update UI
        self.load_default_constraints()
        self.sync_constraints_to_ui()

        # Start with continuous plots by default
        self.plot_manager.initialize_continuous_plots()

    def load_ui(self, ui_file_path):
        loader = QUiLoader()
        ui_file = QFile(ui_file_path)
        if not ui_file.exists():
            self.logger.error(f"UI file {ui_file_path} does not exist.")
            return
        ui_file.open(QFile.ReadOnly)
        ui = loader.load(ui_file)
        ui_file.close()
        if not ui:
            self.logger.error("Failed to load UI file.")
        self.setCentralWidget(ui)
        return ui

    def init_ui(self):
        self.ui.xy_plot_window.setLayout(QVBoxLayout())
        self.setMinimumSize(1600, 900)
        # Default reservoir volume
        self.ui.reservoir_volume_edit_field.setText(str(self.controller.reservoir_volume))
        self.ui.sample_id_edit_field.setText(self.meta_data.sample_id or "")

    def init_connections(self):
        # Buttons
        self.ui.update_meta_data_button.clicked.connect(self.update_meta_data_from_ui)

        self.ui.start_stop_tp_recording_button.setCheckable(True)
        self.ui.start_stop_tp_recording_button.clicked.connect(self.on_toggle_tp_recording)

        self.ui.start_stop_log_file_tracker_button.setCheckable(True)
        self.ui.start_stop_log_file_tracker_button.clicked.connect(self.on_toggle_log_file_tracking)

        self.ui.start_stop_static_plot_button.setCheckable(True)
        self.ui.start_stop_static_plot_button.clicked.connect(self.on_toggle_plotting_mode)

        self.ui.plot_uptake_button.clicked.connect(self.init_plot_uptake_from_ui)

        self.ui.XyDataSelectDropDown.highlighted.connect(self.init_right_plot_xy_from_ui)
        self.ui.T_p_dependent_drop_down.currentIndexChanged.connect(self.init_plot_tp_dependent_from_ui)

        # Check boxes
        self.ui.h2_uptake_check_box.clicked.connect(lambda: self.controller.toggle_h2_uptake_flag(self.ui.h2_uptake_check_box.isChecked()))
        self.ui.cycle_test_check_box.clicked.connect(lambda: self.controller.toggle_cycling_flag(self.ui.cycle_test_check_box.isChecked()))

        # Meta data fields
        self.ui.sample_id_edit_field.editingFinished.connect(self.on_sample_id_changed)
        self.ui.sample_mass_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.material_composition_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.cell_volume_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.cell_type_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.first_hyd_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.test_start_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.test_end_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.max_pressure_cycling_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.min_temperature_cycling_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.cycle_duration_edit_field.editingFinished.connect(lambda: self.on_meta_data_field_changed())
        self.ui.reservoir_volume_edit_field.editingFinished.connect(self.on_reservoir_volume_changed)

        # Constraints fields
        self.ui.MinCharTimeEditField.editingFinished.connect(self.on_constraints_changed)
        self.ui.MaxCharTimeEditField.editingFinished.connect(self.on_constraints_changed)
        self.ui.MinTempIncEditField.editingFinished.connect(self.on_constraints_changed)
        self.ui.MaxTempIncEditField.editingFinished.connect(self.on_constraints_changed)

    def setup_validators(self):
        double_validator = QDoubleValidator()
        self.ui.sample_mass_edit_field.setValidator(double_validator)
        self.ui.cell_volume_edit_field.setValidator(double_validator)
        self.ui.reservoir_volume_edit_field.setValidator(double_validator)
        self.ui.max_pressure_cycling_edit_field.setValidator(double_validator)
        self.ui.min_temperature_cycling_edit_field.setValidator(double_validator)

    def load_default_constraints(self):
        self.controller.set_constraints(standard_constraints_dict.copy())

    def sync_constraints_to_ui(self):
        constraints = self.controller.constraints_dict
        self.ui.MinCharTimeEditField.setText(str(constraints["min_TotalCharTime"]))
        self.ui.MaxCharTimeEditField.setText(str(constraints["max_TotalCharTime"]))
        self.ui.MinTempIncEditField.setText(str(constraints["min_TotalTempIncr"]))
        self.ui.MaxTempIncEditField.setText(str(constraints["max_TotalTempIncr"]))

    def on_toggle_tp_recording(self):
        is_checked = self.ui.start_stop_tp_recording_button.isChecked()
        self.toggle_button(self.ui.start_stop_tp_recording_button, "Start T-p Recording", "Stop T-p Recording", is_checked)
        if is_checked:
            left_upper_plot, __ = self.controller.start_tp_recording()
            left_upper_plot.current_cycle_sig.connect(lambda number: self.set_text_current_state(arg="number", value=number))
            left_upper_plot.current_state_sig.connect(lambda state: self.set_text_current_state(arg="state", value=state))
            left_upper_plot.current_uptake_sig.connect(lambda uptake: self.set_text_current_state(arg="uptake", value=uptake))
        else:
            self.controller.stop_tp_recording()

    def on_toggle_log_file_tracking(self):
        is_checked = self.ui.start_stop_log_file_tracker_button.isChecked()
        self.toggle_button(self.ui.start_stop_log_file_tracker_button, "Start Log File Tracking", "Stop Log File Tracking", is_checked)
        if is_checked:
            self.controller.start_log_file_tracking()
        else:
            self.controller.stop_log_file_tracking()

    def on_toggle_plotting_mode(self):
        is_checked = self.ui.start_stop_static_plot_button.isChecked()
        self.toggle_button(self.ui.start_stop_static_plot_button, "Start Static Plot", "Stop Static Plot", is_checked)
        if is_checked:
            # Switch to static plotting
            upper_plot, lower_plot = self.plot_manager.initialize_static_plots()
            # Connect meta_data updates
            upper_plot.reader.meta_data_sig.connect(self.meta_data_received)
            # After switching, if we have a sample ID, plot full test
            if self.meta_data.sample_id:
                self.controller.plot_full_test()
        else:
            # Switch back to continuous plotting
            self.plot_manager.clear_plots()
            self.plot_manager.initialize_continuous_plots()

    def on_sample_id_changed(self):
        sample_id = self.ui.sample_id_edit_field.text()
        self.controller.update_sample_id(sample_id)
        self.meta_data_manager.update_ui_fields(self.ui)
        is_checked = self.ui.start_stop_static_plot_button.isChecked()
        if is_checked:
            self.controller.plot_full_test()

    def on_reservoir_volume_changed(self):
        val = self.ui.reservoir_volume_edit_field.text()
        volume = self._convert_to_numeric(val)
        if volume is None:
            QMessageBox.warning(self, "Invalid Input", "Please enter a numeric value for reservoir volume.")
        else:
            self.controller.set_reservoir_volume(volume)

    def on_meta_data_field_changed(self):
        self.meta_data_manager.update_from_ui_fields(self.ui)
        self.meta_data_manager.update_ui_fields(self.ui)
        # Update recorders/plots if needed
        self.controller.update_sample_id(self.meta_data.sample_id)

    def on_constraints_changed(self):
        fields = [
            self.ui.MinCharTimeEditField.text(),
            self.ui.MaxCharTimeEditField.text(),
            self.ui.MinTempIncEditField.text(),
            self.ui.MaxTempIncEditField.text(),
        ]
        if all(self._is_numeric(val) for val in fields):
            constraints = {
                "min_TotalCharTime": float(self.ui.MinCharTimeEditField.text()),
                "max_TotalCharTime": float(self.ui.MaxCharTimeEditField.text()),
                "min_TotalTempIncr": float(self.ui.MinTempIncEditField.text()),
                "max_TotalTempIncr": float(self.ui.MaxTempIncEditField.text())
            }
            self.controller.set_constraints(constraints)
        else:
            QMessageBox.warning(self, "Invalid Input", "Please enter numeric values for constraints.")
            return

    def meta_data_received(self, meta_data):
        # Received updated meta_data from a plot
        self.meta_data = meta_data
        self.meta_data_manager.meta_data = self.meta_data
        self.meta_data_manager.update_ui_fields(self.ui)
        self.controller.update_sample_id(self.meta_data.sample_id)

    def init_right_plot_xy_from_ui(self):
        self.plot_manager.init_right_plot_xy(self.ui.XyDataSelectDropDown.currentText())
        # Now that right_plot is initialized, connect the cycle_number_sig
        if self.plot_manager.right_plot:
            self.plot_manager.right_plot.cycle_number_sig.connect(
                lambda cycle_number: self.set_text_current_state(cycle_number, 'number')
            )

    def init_plot_uptake_from_ui(self):
        if self.plot_manager.left_upper_plot:
            view_range = self.plot_manager.left_upper_plot.plotItem.viewRange()[0]
            current_x_range = [datetime.fromtimestamp(ts, tz=local_tz_reg) for ts in view_range]
        else:
            current_x_range = None
        self.plot_manager.init_plot_uptake(current_x_range)

    def init_plot_tp_dependent_from_ui(self):
        drop_down_val = self.ui.T_p_dependent_drop_down.currentText().lower()
        x_col = None
        if 'pressure' in drop_down_val:
            x_col = 'pressure'
        elif 'temperature' in drop_down_val:
            x_col = 'temperature'
        if self.plot_manager.left_lower_plot:
            time_range_stamps = self.plot_manager.left_lower_plot.plotItem.viewRange()[0]
            time_range = [datetime.fromtimestamp(ts, local_tz_reg) for ts in time_range_stamps]
            self.plot_manager.init_plot_tp_dependent(x_col, time_range)

    def update_meta_data_from_ui(self):
        self.meta_data_manager.update_from_ui_fields(self.ui)
        self.meta_data_manager.update_ui_fields(self.ui)
        self.controller.update_sample_id(self.meta_data.sample_id)

    def toggle_button(self, button, start_text, stop_text, is_checked):
        if is_checked:
            button.setText(stop_text)
            button.setStyleSheet(f"background-color: {self.activated_button_color}")
        else:
            button.setText(start_text)
            button.setStyleSheet(f"background-color: {self.actual_button_color}")

    def closeEvent(self, event):
        self.controller.stop_tp_recording()
        super().closeEvent(event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        # Adjust plots if necessary
        if self.plot_manager.left_upper_plot and hasattr(self.plot_manager.left_upper_plot, 'adjust_plot_size'):
            self.plot_manager.left_upper_plot.adjust_plot_size()
        if self.plot_manager.left_lower_plot and hasattr(self.plot_manager.left_lower_plot, 'adjust_plot_size'):
            self.plot_manager.left_lower_plot.adjust_plot_size()
        if self.plot_manager.right_plot and hasattr(self.plot_manager.right_plot, 'adjust_plot_size'):
            self.plot_manager.right_plot.adjust_plot_size()

    @staticmethod
    def _convert_to_numeric(value):
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

    def set_text_current_state(self, value, arg):
        """
        Update UI fields based on the current state.
        """
        if 'number' in arg.lower():
            self.ui.current_cycle_edit_field.setText(str(value))
        elif 'state' in arg.lower():
            self.ui.current_de_hyd_state_edit_field.setText(str(value))
        elif 'uptake' in arg.lower():
            self.ui.current_uptake_edit_field.setText(str(value))


def main():
    app = QApplication([])
    main_window = RecordingMainWindow()
    main_window.show()
    app.exec()


if __name__ == "__main__":
    main()
