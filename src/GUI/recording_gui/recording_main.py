from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QMessageBox
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile
from PySide6 import QtGui

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
STANDARD_T_P_TEST_DATA_FOLDER_PATH = standard_t_p_test_data_folder_path
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


class RecordingMainWindow(QMainWindow):
    """
    Main application window for recording and plotting T-p ETC data.
    """

    def __init__(self):
        """
        Initialize the RecordingMainWindow, setup UI components and connections.
        """
        super().__init__()
        self.setWindowTitle("T-p ETC Recorder")
        self.setStyleSheet(style)
        self.setFont(FONT)
        self.logger = logging.getLogger(__name__)
        self.load_ui_file(recording_ui_file_path)

        self.meta_data = MetaData()
        self.reservoir_volume = 1
        self.actual_button_color = "lightgray"
        self.activated_button_color = "darkgreen"
        self.constraints_dict = {}

        self.init_ui()
        self.init_constants()
        self.init_connections()
        self.setup_continuous_plotting()
        self.setup_recording()

    def load_ui_file(self, ui_file_path):
        """
        Load the UI file and set it as the central widget.
        """
        try:
            loader = QUiLoader()
            ui_file = QFile(ui_file_path)
            if not ui_file.exists():
                self.logger.error(f"UI file {ui_file_path} does not exist.")
                return
            ui_file.open(QFile.ReadOnly)
            self.ui = loader.load(ui_file)
            ui_file.close()
            if not self.ui:
                self.logger.error("Failed to load UI file.")
                return
            self.setCentralWidget(self.ui)
        except Exception as e:
            self.logger.error(f"Unable to load UI file: {e}")

    def init_ui(self):
        """
        Initialize UI components and layouts.
        """
        self.right_plot = None
        self.ui.xy_plot_window.setLayout(QVBoxLayout())
        self.ui.setWindowTitle("Recording Main")
        self.setMinimumSize(1600, 900)
        self.ui.reservoir_volume_edit_field.setText(str(self.reservoir_volume))
        self.ui.sample_id_edit_field.setText(self.meta_data.sample_id)

    def init_constants(self):
        """
        Initialize constants and default values.
        """
        self.constraints_dict = standard_constraints_dict
        self.ui.MinCharTimeEditField.setText(str(self.constraints_dict["min_TotalCharTime"]))
        self.ui.MaxCharTimeEditField.setText(str(self.constraints_dict["max_TotalCharTime"]))
        self.ui.MinTempIncEditField.setText(str(self.constraints_dict["min_TotalTempIncr"]))
        self.ui.MaxTempIncEditField.setText(str(self.constraints_dict["max_TotalTempIncr"]))

        self.update_constraints()

    def init_connections(self):
        """
        Initialize UI connections and event handlers.
        """
        self.ui.update_meta_data_button.clicked.connect(self.update_meta_data_from_ui)
        self.init_text_box_connections()
        self.ui.MinCharTimeEditField.editingFinished.connect(self.update_constraints)
        self.ui.MaxCharTimeEditField.editingFinished.connect(self.update_constraints)
        self.ui.MinTempIncEditField.editingFinished.connect(self.update_constraints)
        self.ui.MaxTempIncEditField.editingFinished.connect(self.update_constraints)

        # Button connections
        self.ui.start_stop_tp_recording_button.setCheckable(True)
        self.ui.start_stop_tp_recording_button.clicked.connect(self.toggle_tp_recording)
        self.ui.start_stop_log_file_tracker_button.setCheckable(True)
        self.ui.start_stop_log_file_tracker_button.clicked.connect(self.toggle_log_file_tracking)
        self.ui.start_stop_static_plot_button.setCheckable(True)
        self.ui.start_stop_static_plot_button.clicked.connect(self.toggle_plotting)

        # Plotting connections
        self.ui.XyDataSelectDropDown.highlighted.connect(self.init_right_plot_xy)
        self.ui.T_p_dependent_drop_down.currentIndexChanged.connect(self.init_plot_tp_dependent)
        self.ui.plot_uptake_button.clicked.connect(self.init_plot_uptake)

    def init_text_box_connections(self):
        """
        Connect text boxes to their respective handlers.
        """
        self.ui.sample_id_edit_field.editingFinished.connect(self.sample_id_changed)
        self.ui.sample_mass_edit_field.editingFinished.connect(lambda: self.edit_field_changed('sample_mass'))
        self.ui.material_composition_edit_field.editingFinished.connect(lambda: self.edit_field_changed('sample_material'))
        self.ui.reservoir_volume_edit_field.editingFinished.connect(lambda: self.edit_field_changed('reservoir_volume'))
        self.ui.cell_volume_edit_field.editingFinished.connect(lambda: self.edit_field_changed('cell_volume'))
        self.ui.cell_type_edit_field.editingFinished.connect(lambda: self.edit_field_changed('cell_type'))
        self.ui.first_hyd_edit_field.editingFinished.connect(lambda: self.edit_field_changed('first_hydrogenation'))
        self.ui.max_pressure_cycling_edit_field.editingFinished.connect(lambda: self.edit_field_changed('max_pressure_cycling'))
        self.ui.min_temperature_cycling_edit_field.editingFinished.connect(lambda: self.edit_field_changed('min_temperature_cycling'))
        self.ui.cycle_duration_edit_field.editingFinished.connect(lambda: self.edit_field_changed('average_cycle_duration'))

    def edit_field_changed(self, field):
        """
        Handle changes to editable fields and update meta-data accordingly.
        """
        if field == 'sample_mass':
            value = self.ui.sample_mass_edit_field.text()
            self.meta_data.sample_mass = self._convert_to_numeric(value)
        elif field == 'sample_material':
            value = self.ui.material_composition_edit_field.text()
            self.meta_data.sample_material = value
        elif field == 'reservoir_volume':
            value = self.ui.reservoir_volume_edit_field.text()
            self.update_reservoir_volume(value)
        elif field == 'cell_volume':
            value = self.ui.cell_volume_edit_field.text()
            self.meta_data.volume_measurement_cell = self._convert_to_numeric(value)
        elif field == 'cell_type':
            value = self.ui.cell_type_edit_field.text()
            self.meta_data.measurement_cell = value
        elif field == 'first_hydrogenation':
            value = self.ui.first_hyd_edit_field.text()
            self.meta_data.first_hydrogenation = self._parse_date(value)
        elif field == 'max_pressure_cycling':
            value = self.ui.max_pressure_cycling_edit_field.text()
            self.meta_data.max_pressure_cycling = self._convert_to_numeric(value)
        elif field == 'min_temperature_cycling':
            value = self.ui.min_temperature_cycling_edit_field.text()
            self.meta_data.min_temperature_cycling = self._convert_to_numeric(value)
        elif field == 'average_cycle_duration':
            value = self.ui.cycle_duration_edit_field.text()
            self.meta_data.average_cycle_duration = self._parse_duration(value)
        # Write updated meta-data to storage
        self.meta_data.write()
        # Update the UI to reflect changes
        self.update_meta_data_in_ui()

    def _parse_date(self, date_str):
        """
        Parse a date string into a datetime object.
        """
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    def _parse_duration(self, duration_str):
        """
        Parse a duration string in HH:MM:SS format into a timedelta object.
        """
        try:
            hours, minutes, seconds = map(int, duration_str.split(':'))
            return timedelta(hours=hours, minutes=minutes, seconds=seconds)
        except ValueError:
            return None

    def setup_continuous_plotting(self):
        """
        Initialize continuous plotting components.
        """
        self._delete_plots()
        self.left_upper_plot = PlotContinuousWindow(y_axis="temperature", meta_data=self.meta_data)
        self.left_lower_plot = PlotContinuousWindow(y_axis="pressure", meta_data=self.meta_data)
        self.ui.ExpMetaPlotLeft.setLayout(QVBoxLayout())
        self.ui.ExpMetaPlotLeft.layout().addWidget(self.left_upper_plot)
        self.ui.ExpMetaPlotLeftLower.setLayout(QVBoxLayout())
        self.ui.ExpMetaPlotLeftLower.layout().addWidget(self.left_lower_plot)
        self.left_lower_plot.setXLink(self.left_upper_plot)

        # Connect signals and slots
        self.left_upper_plot.reader.meta_data_sig.connect(self.meta_data_received)
        self.left_upper_plot.current_cycle_sig.connect(lambda cycle_number: self.set_text_current_state(cycle_number, "number"))
        self.left_upper_plot.current_state_sig.connect(lambda state: self.set_text_current_state(state, "state"))
        self.left_upper_plot.current_uptake_sig.connect(lambda uptake: self.set_text_current_state(uptake, "uptake"))
        self.left_upper_plot.current_uptake_sig.connect(lambda _: self.init_plot_uptake())

    def setup_static_plotting(self):
        self._delete_plots()
        self.left_upper_plot = PlotStaticWindow(y_axis="temperature", meta_data=self.meta_data)
        self.left_lower_plot = PlotStaticWindow(y_axis="pressure", meta_data=self.meta_data)
        self.ui.ExpMetaPlotLeft.layout().addWidget(self.left_upper_plot)
        self.ui.ExpMetaPlotLeftLower.layout().addWidget(self.left_lower_plot)
        # Set up connections
        self.left_upper_plot.reader.meta_data_sig.connect(self.meta_data_received)
        self.left_upper_plot.current_cycle_sig.connect(lambda cycle_number: self.set_text_current_state(cycle_number, "number"))
        self.left_upper_plot.current_state_sig.connect(lambda state: self.set_text_current_state(state, "state"))
        self.left_upper_plot.current_uptake_sig.connect(lambda uptake: self.set_text_current_state(uptake, "uptake"))
        self.left_upper_plot.current_uptake_sig.connect(lambda _: self.init_plot_uptake())
        self.left_lower_plot.setXLink(self.left_upper_plot)
        self.ui.sample_id_edit_field.editingFinished.connect(self.plot_full_test)

    def setup_recording(self):
        """
        Initialize recording components and buttons.
        """
        self.recorder = Record(meta_data=self.meta_data)

        # Connect signals
        self.recorder.new_etc_data_written_to_database.connect(self.left_lower_plot.update_plot_right)
        self.recorder.new_etc_data_written_to_database.connect(self.left_upper_plot.update_plot_right)
        self.ui.h2_uptake_check_box.clicked.connect(self.recorder.update_h2_uptake_flag)
        self.ui.cycle_test_check_box.clicked.connect(self.recorder.update_cycling_flag)

    def toggle_tp_recording(self):
        """
        Start or stop T-p recording based on the button state.
        """
        is_checked = self.ui.start_stop_tp_recording_button.isChecked()
        self.toggle_button(
            self.ui.start_stop_tp_recording_button,
            "Start T-p Recording",
            "Stop T-p Recording",
            is_checked
        )
        if is_checked:
            self.setup_continuous_plotting()
            self.recorder.start_t_p_recording_thread()
            self.left_lower_plot.reader.start()
            self.left_upper_plot.reader.start()
        else:
            self.recorder.stop_recording_all()
            self.left_lower_plot.reader.stop()
            self.left_upper_plot.reader.stop()

    def toggle_log_file_tracking(self):
        """
        Start or stop log file tracking based on the button state.
        """
        is_checked = self.ui.start_stop_log_file_tracker_button.isChecked()
        self.toggle_button(
            self.ui.start_stop_log_file_tracker_button,
            "Start Log File Tracking",
            "Stop Log File Tracking",
            is_checked
        )
        if is_checked:
            self.recorder.start_etc_recording_thread()
        else:
            self.recorder.stop_etc_recording_thread()

    def _delete_plots(self):

        if hasattr(self, 'lef_lower_plot'):
            self.left_lower_plot.reader.stop()
            self.left_lower_plot.deleteLater()
        if hasattr(self, 'lef_upper_plot'):
            self.left_upper_plot.reader.stop()
            self.left_upper_plot.deleteLater()

        layout = self.ui.ExpMetaPlotLeft.layout()
        if layout is not None:
            # Remove continuous plots
            self.ui.ExpMetaPlotLeft.layout().removeWidget(self.left_upper_plot)
        layout = self.ui.ExpMetaPlotLeftLower.layout()
        if layout is not None:
            self.ui.ExpMetaPlotLeftLower.layout().removeWidget(self.left_lower_plot)
        layout = self.ui.xy_plot_window.layout()
        if layout is not None:
            pass
            #if hasattr(self, 'right_plot'):
                #self.ui.xy_plot_window.layout().removeWidget(self.right_plot)

    def toggle_plotting(self):
        is_checked = self.ui.start_stop_static_plot_button.isChecked()
        self.toggle_button(
            self.ui.start_stop_static_plot_button,
            "Start Static Plot",
            "Stop Static Plot",
            is_checked
        )
        if is_checked:
            self.setup_static_plotting()
            if self.meta_data.sample_id:
                self.plot_full_test()

        else:
            # Revert to continuous plotting
            # Stop static plotting
            self.left_upper_plot.reader.stop()
            self.left_lower_plot.reader.stop()
            self.ui.sample_id_edit_field.editingFinished.disconnect(self.plot_full_test)
            # Remove static plots
            # Recreate continuous plots
            self.setup_continuous_plotting()

    def plot_full_test(self):
        """
        Plot the full test data based on the sample ID.
        """
        self.recorder.stop_recording_all()
        self.left_lower_plot.reader.stop()
        self.left_upper_plot.reader.stop()
        self.left_upper_plot.update_on_data = True
        self.left_upper_plot.update_on_record = False
        self.left_upper_plot.read_on_change = True
        self.left_upper_plot.reader.reading_mode = READING_MODE_FULL_TEST
        self.left_upper_plot.reader.p_data_sig.connect(self.left_lower_plot.update_plot_left)
        self.left_upper_plot.reader.etc_data_sig.connect(self.left_lower_plot.update_plot_right)
        self.left_upper_plot.reader.cycles_full_test_sig.connect(self.left_lower_plot.update_min_max_plot)
        self.left_upper_plot.reader.start()

    def sample_id_changed(self):
        """
        Handle changes to the sample ID.
        """
        self.meta_data.sample_id = self.ui.sample_id_edit_field.text()
        self.meta_data.read()
        self.update_meta_data_in_ui()
        # Update recorder and plot readers with new meta-data
        self.recorder.update_sample_id(self.meta_data.sample_id)
        self.left_upper_plot.reader.on_meta_data_changed(new_meta_data=self.meta_data)
        self.left_lower_plot.reader.on_meta_data_changed(new_meta_data=self.meta_data)
        # Clear plots
        self.left_upper_plot.plotItem.clear()
        self.left_upper_plot.rightViewBox.clear()
        self.left_lower_plot.plotItem.clear()
        self.left_lower_plot.rightViewBox.clear()

    def update_reservoir_volume(self, value=None):
        """
        Update the reservoir volume based on user input.
        """
        if value is None:
            value = self.ui.reservoir_volume_edit_field.text()
        volume = self._convert_to_numeric(value)
        if volume is not None:
            self.reservoir_volume = round(volume, 15)
            self.meta_data.reservoir_volume = self.reservoir_volume
            self.recorder.update_reservoir_volume(self.reservoir_volume)
        else:
            QMessageBox.warning(self, "Invalid Input", "Please enter a numeric value for reservoir volume.")

    def update_meta_data_from_ui(self):
        """
        Update meta-data based on user input in the UI.
        """
        self.meta_data.sample_id = self.ui.sample_id_edit_field.text()
        self.meta_data.sample_mass = self._convert_to_numeric(self.ui.sample_mass_edit_field.text())
        self.meta_data.sample_material = self.ui.material_composition_edit_field.text()
        self.meta_data.measurement_cell = self.ui.cell_type_edit_field.text()
        self.meta_data.volume_measurement_cell = self._convert_to_numeric(self.ui.cell_volume_edit_field.text())
        self.meta_data.first_hydrogenation = self._parse_date(self.ui.first_hyd_edit_field.text())
        self.meta_data.start_time = self._parse_date(self.ui.test_start_edit_field.text())
        self.meta_data.end_time = self._parse_date(self.ui.test_end_edit_field.text())
        self.meta_data.max_pressure_cycling = self._convert_to_numeric(self.ui.max_pressure_cycling_edit_field.text())
        self.meta_data.min_temperature_cycling = self._convert_to_numeric(self.ui.min_temperature_cycling_edit_field.text())
        self.meta_data.average_cycle_duration = self._parse_duration(self.ui.cycle_duration_edit_field.text())
        # Save updated meta-data
        self.meta_data.write()
        # Update the UI
        self.update_meta_data_in_ui()

    def update_meta_data_in_ui(self):
        """
        Update the meta-data fields in the UI.
        """
        self.ui.sample_id_edit_field.setText(self.meta_data.sample_id or "")
        self.ui.sample_mass_edit_field.setText(str(self.meta_data.sample_mass) if self.meta_data.sample_mass is not None else "")
        self.ui.material_composition_edit_field.setText(self.meta_data.sample_material or "")
        self.ui.reservoir_volume_edit_field.setText(str(self.reservoir_volume) if self.reservoir_volume is not None else "")
        self.ui.cell_volume_edit_field.setText(str(self.meta_data.volume_measurement_cell) if self.meta_data.volume_measurement_cell is not None else "")
        self.ui.cell_type_edit_field.setText(self.meta_data.measurement_cell or "")
        self.ui.first_hyd_edit_field.setText(self.meta_data.first_hydrogenation.strftime("%Y-%m-%d %H:%M:%S") if self.meta_data.first_hydrogenation else "")
        self.ui.test_start_edit_field.setText( self.meta_data.start_time.strftime("%Y-%m-%d %H:%M:%S") if self.meta_data.start_time else "")
        self.ui.test_end_edit_field.setText( self.meta_data.end_time.strftime("%Y-%m-%d %H:%M:%S") if self.meta_data.end_time else "")
        self.ui.max_pressure_cycling_edit_field.setText(str(self.meta_data.max_pressure_cycling) if self.meta_data.max_pressure_cycling is not None else "")
        self.ui.min_temperature_cycling_edit_field.setText(str(self.meta_data.min_temperature_cycling) if self.meta_data.min_temperature_cycling is not None else "")
        self.ui.cycle_duration_edit_field.setText(str(self.meta_data.average_cycle_duration) if self.meta_data.average_cycle_duration else "")
        # Update other fields as needed

    def update_constraints(self):
        """
        Update the constraints dictionary based on UI input.
        """
        min_char_time = self.ui.MinCharTimeEditField.text()
        max_char_time = self.ui.MaxCharTimeEditField.text()
        min_temp_incr = self.ui.MinTempIncEditField.text()
        max_temp_incr = self.ui.MaxTempIncEditField.text()
        # Check if the values are numeric
        if self._is_numeric(min_char_time) and self._is_numeric(max_char_time) \
                and self._is_numeric(min_temp_incr) and self._is_numeric(max_temp_incr):
            # Update the constraints dictionary with the numeric values
            self.constraints_dict["min_TotalCharTime"] = float(min_char_time)
            self.constraints_dict["max_TotalCharTime"] = float(max_char_time)
            self.constraints_dict["min_TotalTempIncr"] = float(min_temp_incr)
            self.constraints_dict["max_TotalTempIncr"] = float(max_temp_incr)
        else:
            QMessageBox.warning(self, "Invalid Input", "Please enter numeric values for constraints.")

        if hasattr(self, 'left_upper_plot'):
            # Update constraints in the plots
            self.left_upper_plot.reader.update_constraints_etc(self.constraints_dict)
            self.left_lower_plot.reader.update_constraints_etc(self.constraints_dict)

    def init_right_plot_xy(self):
        """
        Initialize the right plot with X-Y data based on user selection.
        """
        if self.right_plot:
            self.ui.xy_plot_window.layout().removeWidget(self.right_plot)
            self.right_plot.deleteLater()
            self.right_plot = None

        self.right_plot = ReadPlotXY()
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)

        self.ui.XyDataSelectDropDown.currentIndexChanged.connect(self.right_plot.clear_plot)
        self.ui.XyDataSelectDropDown.currentIndexChanged.connect(self.left_upper_plot._reset_point_colors)
        self.ui.XyDataSelectDropDown.currentIndexChanged.connect(self.left_lower_plot._reset_point_colors)

        self.left_upper_plot.point_clicked_time_received.connect(
            lambda d1, d2: self.right_plot.add_curve_to_plot(d1, d2, self.ui.XyDataSelectDropDown.currentText())
        )
        self.right_plot.cycle_number_sig.connect(
            lambda cycle_number: self.set_text_current_state(cycle_number, 'number')
        )

    def init_plot_uptake(self):
        """
        Initialize the plot for hydrogen uptake.
        """
        if self.right_plot:
            self.ui.xy_plot_window.layout().removeWidget(self.right_plot)
            self.right_plot.deleteLater()
            self.right_plot = None
        if hasattr(self, 'left_upper_plot'):
            view_range = self.left_upper_plot.plotItem.viewRange()[0]
            current_x_range = [datetime.fromtimestamp(ts, tz=local_tz_reg) for ts in view_range]

        self.right_plot = ReadPlotUptake()
        self.right_plot.load_data(meta_data=self.meta_data, time_range=current_x_range)
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)

    def init_plot_tp_dependent(self):
        """
        Initialize the plot for T-p dependent data.
        """
        if self.right_plot:
            self.ui.xy_plot_window.layout().removeWidget(self.right_plot)
            self.right_plot.deleteLater()
            self.right_plot = None

        drop_down_val = self.ui.T_p_dependent_drop_down.currentText()
        x_col = 'pressure' if 'pressure' in drop_down_val.lower() else 'temperature' if 'temperature' in drop_down_val.lower() else None

        if x_col is None:
            self.logger.error("Invalid selection for T-p dependent plot.")
            return

        self.right_plot = ReadPlotTpDependent.PlotTpDependent(constraints=self.constraints_dict)
        time_range_stamps = self.left_lower_plot.plotItem.viewRange()[0]
        time_range = [datetime.fromtimestamp(ts, local_tz_reg) for ts in time_range_stamps]

        self.right_plot.load_data(time_range=time_range, x_col=x_col)
        self.ui.xy_plot_window.layout().addWidget(self.right_plot)

    def meta_data_received(self, meta_data):
        """
        Handle the reception of new meta data.
        """
        self.meta_data = meta_data
        self.ui.sample_id_edit_field.setText(self.meta_data.sample_id)
        self.update_meta_data_in_ui()
        self.sample_id_changed()

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

    def toggle_button(self, button, start_text, stop_text, is_checked):
        """
        Toggle the appearance and text of a button.
        """
        if is_checked:
            button.setText(stop_text)
            button.setStyleSheet(f"background-color: {self.activated_button_color}")
        else:
            button.setText(start_text)
            button.setStyleSheet(f"background-color: {self.actual_button_color}")

    def closeEvent(self, event):
        """
        Handle the application close event, ensuring proper resource cleanup.
        """
        self.recorder.stop_recording_all()
        if hasattr(self.left_lower_plot, 'closeEvent'):
            self.left_lower_plot.closeEvent(event)
        if hasattr(self.left_upper_plot, 'closeEvent'):
            self.left_upper_plot.closeEvent(event)
        if hasattr(self.right_plot, 'closeEvent'):
            self.right_plot.closeEvent(event)
        super().closeEvent(event)

    def resizeEvent(self, event):
        """
        Handle the resize event to adjust plot sizes.
        """
        super().resizeEvent(event)
        if hasattr(self.left_upper_plot, 'adjust_plot_size'):
            self.left_upper_plot.adjust_plot_size()
        if hasattr(self.left_lower_plot, 'adjust_plot_size'):
            self.left_lower_plot.adjust_plot_size()
        if hasattr(self, 'right_plot'):
            if hasattr(self.right_plot, 'adjust_plot_size'):
                self.right_plot.adjust_plot_size()

    @staticmethod
    def _convert_to_numeric(value):
        """
        Try to convert a value to a float.
        """
        try:
            return float(value)
        except ValueError:
            return None

    @staticmethod
    def _is_numeric(value):
        """
        Check if a value can be converted to a float.
        """
        try:
            float(value)
            return True
        except ValueError:
            return False


def main():
    # Create the Qt Application
    app = QApplication([])
    # Create and show the main application window
    main_window = RecordingMainWindow()
    main_window.show()
    # Run the event loop
    app.exec()


if __name__ == "__main__":
    main()


