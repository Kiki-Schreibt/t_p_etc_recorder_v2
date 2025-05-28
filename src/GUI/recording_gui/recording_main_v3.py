#recording_main_v3.py
# ===============================
#         UI CONTROLLERS
# ===============================
"""
Main UI controller module with enhanced error handling and documentation.

This module includes:
    - MetaDataManager: Synchronizes UI fields with the MetaData object.
    - PlotManager: Manages the creation and layout of plot widgets.
    - MainController: Coordinates business logic (DataRecorder) with UI plot management.
    - MainWindow: The primary application window with UI elements and event connections.

All methods are wrapped with try/except blocks for robust error handling.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import warnings

from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QMessageBox
from PySide6.QtGui import QDoubleValidator, QFont

from src.infrastructure.meta_data.meta_data_handler import MetaData
from src.GUI.recording_gui.recording_business_v2 import DataRecorder
from src.infrastructure.utils.standard_paths import recording_ui_file_path
try:
    import src.infrastructure.core.logger as logging
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

    Methods:
        update_from_ui(): Read values from UI and update the MetaData instance.
        update_ui(): Set UI fields based on the current MetaData instance.
    """
    def __init__(self, meta_data: MetaData):
        self.meta_data = meta_data

    def update_from_ui(self, ui, update_meta_button_pushed=True):
        """
        Update the MetaData object from the UI fields.
        """
        try:
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
            if update_meta_button_pushed:
                self.meta_data.write()
                self.update_ui(ui=ui)
                logging.getLogger(__name__).info("MetaData updated")
        except Exception as e:
            logging.getLogger(__name__).exception("Error updating MetaData from UI:")

    def update_ui(self, ui):
        """
        Update the UI fields based on the current MetaData values.
        """
        try:
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
        except Exception as e:
            logging.getLogger(__name__).exception("Error updating UI from MetaData:")

    @staticmethod
    def _to_float(value):
        """
        Convert a value to float if possible.
        """
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_date(date_str):
        """
        Parse a date string in the format "%Y-%m-%d %H:%M:%S".
        """
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    @staticmethod
    def _parse_duration(duration_str):
        """
        Parse a duration string in the format "H:M:S" into a timedelta.
        """
        if not duration_str:
            return None
        try:
            h, m, s = map(int, duration_str.split(':'))
            return timedelta(hours=h, minutes=m, seconds=s)
        except ValueError:
            return None


class PlotManager:
    """
    Manages creation, clearing, and arrangement of plot widgets within the UI.

    Methods:
        clear_plots(): Remove existing plot widgets.
        init_continuous_plots(): Initialize continuous plotting widgets.
        init_static_plots(): Initialize static plotting widgets.
        init_right_plot_xy(): Initialize an XY plot in the right panel.
        init_uptake_plot(): Initialize the hydrogen uptake plot.
        init_tp_dependent_plot(): Initialize the TP-dependent ETC plot.
    """
    def __init__(self, ui, meta_data: MetaData, logger, db_conn_params):
        self.ui = ui
        self.db_conn_params = db_conn_params
        self.meta_data = meta_data
        self.logger = logger
        self.top_plot = None
        self.bottom_plot = None
        self.right_plot = None
        self.current_xy_dropdown_text = ''

    def clear_plots(self):
        """
        Remove and clean up any existing plot widgets.
        """
        self.disconnect_emits_top_to_bottom_plot()
        try:
            for widget in [self.top_plot, self.bottom_plot, self.right_plot]:
                if not widget:
                    continue

                widget.close()
                widget.setParent(None)
                widget.deleteLater()
            self.top_plot = self.bottom_plot = self.right_plot = None
        except Exception as e:
            self.logger.exception("Error clearing plots in PlotManager:")

    def init_continuous_plots(self):
        """
        Initialize continuous plots for temperature and pressure.
        """
        try:
            from src.GUI.recording_gui.recording_business_v2 import ContinuousPlotWindow
            self.clear_plots()
            self.top_plot = ContinuousPlotWindow(y_axis="temperature", meta_data=self.meta_data, db_conn_params=self.db_conn_params)
            self.bottom_plot = ContinuousPlotWindow(y_axis="pressure", meta_data=self.meta_data, db_conn_params=self.db_conn_params)
            self._ensure_layout(self.ui.ExpMetaPlotLeft)
            self._ensure_layout(self.ui.ExpMetaPlotLeftLower)
            self.ui.ExpMetaPlotLeft.layout().addWidget(self.top_plot)
            self.ui.ExpMetaPlotLeftLower.layout().addWidget(self.bottom_plot)
            self.bottom_plot.setXLink(self.top_plot)
            self.connect_emits_top_to_bottom_plot()
            return self.top_plot, self.bottom_plot
        except Exception as e:
            self.logger.exception("Error initializing continuous plots:")
            return None, None

    def init_static_plots(self):
        """
        Initialize static plots for temperature and pressure.
        """
        try:
            from src.GUI.recording_gui.recording_business_v2 import StaticPlotWindow
            self.clear_plots()
            self.top_plot = StaticPlotWindow(y_axis="temperature", meta_data=self.meta_data, db_conn_params=self.db_conn_params)
            self.bottom_plot = StaticPlotWindow(y_axis="pressure", meta_data=self.meta_data, db_conn_params=self.db_conn_params, passive_window=True)
            self._ensure_layout(self.ui.ExpMetaPlotLeft)
            self._ensure_layout(self.ui.ExpMetaPlotLeftLower)
            self.ui.ExpMetaPlotLeft.layout().addWidget(self.top_plot)
            self.ui.ExpMetaPlotLeftLower.layout().addWidget(self.bottom_plot)
            self.bottom_plot.setXLink(self.top_plot)
            self.connect_emits_top_to_bottom_plot()
            return self.top_plot, self.bottom_plot
        except Exception as e:
            self.logger.exception("Error initializing static plots:")
            return None, None

    def init_right_plot_xy(self, dropdown_text):
        """
        Initialize the right-hand side XY plot based on a dropdown selection.
        """
        try:
            self.current_xy_dropdown_text = dropdown_text
            if self.top_plot and hasattr(self.top_plot, '_reset_point_colors'):
                self.top_plot._reset_point_colors()
            if self.bottom_plot and hasattr(self.bottom_plot, '_reset_point_colors'):
                self.bottom_plot._reset_point_colors()

            if self.right_plot:
                self.right_plot.setParent(None)
                self.right_plot.deleteLater()

            from src.GUI.recording_gui.recording_business_v2 import XYPlot
            self.right_plot = XYPlot(db_conn_params=self.db_conn_params)
            self._ensure_layout(self.ui.xy_plot_window)
            self.ui.xy_plot_window.layout().addWidget(self.right_plot)
            if self.top_plot:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=RuntimeWarning)
                    self.top_plot.point_clicked_time_received.disconnect()

                self.top_plot.point_clicked_time_received.connect(
                    lambda time, color: self.right_plot.add_curve_to_plot(time_value=time, color=color, xy_data_to_load=dropdown_text))

            return self.right_plot
        except Exception as e:
            self.logger.exception("Error initializing right XY plot:")
            return None

    def init_uptake_plot(self, time_range):
        """
        Initialize the uptake plot for hydrogen uptake data.
        """
        try:
            from src.GUI.recording_gui.recording_business_v2 import UptakePlot
            if self.right_plot:
                self.right_plot.setParent(None)
                self.right_plot.deleteLater()
            self.right_plot = UptakePlot(db_conn_params=self.db_conn_params)
            self.right_plot.load_data(meta_data=self.meta_data, time_range=time_range)
            self._ensure_layout(self.ui.xy_plot_window)
            self.ui.xy_plot_window.layout().addWidget(self.right_plot)
            return self.right_plot
        except Exception as e:
            self.logger.exception("Error initializing uptake plot:")
            return None

    def init_tp_dependent_plot(self, x_col, time_range):
        """
        Initialize the TP-dependent plot.
        """
        try:
            if self.right_plot:
                self.right_plot.setParent(None)
                self.right_plot.deleteLater()
            from src.GUI.recording_gui.recording_business_v2 import ReadPlotTpDependent
            self.right_plot = ReadPlotTpDependent.PlotTpDependent(db_conn_params=self.db_conn_params)
            self.right_plot.load_data(time_range=time_range, x_col=x_col)
            self._ensure_layout(self.ui.xy_plot_window)
            self.ui.xy_plot_window.layout().addWidget(self.right_plot)
            return self.right_plot
        except Exception as e:
            self.logger.exception("Error initializing TP dependent plot:")
            return None

    def _ensure_layout(self, widget):
        """
        Ensure the widget has a layout; if not, set a QVBoxLayout.
        """
        try:
            if widget.layout() is None:
                widget.setLayout(QVBoxLayout())
        except Exception as e:
            self.logger.exception("Error ensuring layout in PlotManager:")

    def connect_emits_top_to_bottom_plot(self):
        if not self.top_plot.reader.p_data_sig_connected:
            self.top_plot.reader.p_data_sig.connect(self.bottom_plot.on_tp_data)
            self.top_plot.reader.p_data_sig_connected = True
        if not self.top_plot.reader.etc_data_sig_connected:
            self.top_plot.reader.etc_data_sig.connect(self.bottom_plot.on_etc_data)
            self.top_plot.reader.etc_data_sig_connected = True
        if not self.top_plot.reader.cycle_data_sig_connected:
            self.top_plot.reader.cycle_data_sig.connect(self.bottom_plot.on_min_max_data)
            self.top_plot.reader.cycle_data_sig_connected = True

    def disconnect_emits_top_to_bottom_plot(self):
        if not self.top_plot or not self.bottom_plot:
            return
        if self.top_plot.reader.p_data_sig_connected:
            self.top_plot.reader.p_data_sig.connect(self.bottom_plot.on_tp_data)
            self.top_plot.reader.p_data_sig_connected = False
        if self.top_plot.reader.etc_data_sig_connected:
            self.top_plot.reader.etc_data_sig.connect(self.bottom_plot.on_etc_data)
            self.top_plot.reader.etc_data_sig_connected = False
        if self.top_plot.reader.cycle_data_sig_connected:
            self.top_plot.reader.cycle_data_sig.connect(self.bottom_plot.on_min_max_data)
            self.top_plot.reader.cycle_data_sig_connected = False


class MainController:
    """
    Coordinates business logic (DataRecorder) and UI plot management.

    Methods:
        start_t_p_recording(): Start T-p recording and continuous plots.
        stop_t_p_recording(): Stop T-p recording.
        start_log_tracking(): Start ETC log tracking.
        stop_log_tracking(): Stop ETC log tracking.
        set_constraints(): Update measurement constraints.
        update_sample_id(): Update the sample ID.
        plot_full_test(): Plot the full test data.
        toggle_h2_uptake(): Enable or disable H2 uptake measurements.
        toggle_cycling(): Enable or disable cycling.
    """
    def __init__(self, meta_data: MetaData,
                 plot_manager: PlotManager,
                 logger,
                 config):

        self.newETCDataWritten_connected = False
        self.meta_data = meta_data
        self.config = config
        self.plot_manager = plot_manager
        self.logger = logger

        try:
            self.recorder = DataRecorder(meta_data=self.meta_data,
                                         config=self.config
                                         )

        except Exception as e:
            self.logger.exception("Error initializing DataRecorder in MainController:")
            self.recorder = None
        self.constraints = STANDARD_CONSTRAINTS.copy()
        self.reservoir_volume = 1

    def set_reservoir_volume(self, volume):
        """
        Set and update the reservoir volume.
        """
        try:
            self.reservoir_volume = volume
            self.meta_data.reservoir_volume = volume
            if self.recorder:
                self.recorder.update_reservoir_volume(volume)
        except Exception as e:
            self.logger.exception("Error setting reservoir volume in MainController:")

    def start_t_p_recording(self):
        """
        Start T-p recording and connect the data signals to the continuous plots.
        """
        try:
            #init the plot windows
            self.plot_manager.init_continuous_plots()
            #start tp recorder
            if self.recorder:
                self.recorder.start_tp_recording()
            #start the live plotting
            if self.plot_manager.top_plot:
                self.plot_manager.top_plot.reader.start()

            if self.recorder:
                self.recorder.newEtcDataWritten.connect(self.plot_manager.top_plot.on_etc_data)
                self.recorder.newEtcDataWritten.connect(self.plot_manager.bottom_plot.on_etc_data)
                self.newETCDataWritten_connected = True
            return self.plot_manager.top_plot, self.plot_manager.bottom_plot
        except Exception as e:
            self.logger.exception("Error starting T-p recording in MainController:")
            return None, None

    def stop_t_p_recording(self):
        """
        Stop T-p recording and halt data updates.
        """
        try:
            if self.recorder:
                if self.newETCDataWritten_connected:
                    self.recorder.newEtcDataWritten.disconnect()
                    self.newETCDataWritten_connected = False
                self.recorder.stop_all_recording()
        except Exception as e:
            self.logger.exception("Error stopping T-p recording in MainController:")

    def start_log_tracking(self):
        """
        Start ETC log file tracking.
        """
        try:
            if self.recorder:
                self.recorder.start_etc_recording()
        except Exception as e:
            self.logger.exception("Error starting log tracking in MainController:")

    def stop_log_tracking(self):
        """
        Stop ETC log file tracking.
        """
        try:
            if self.recorder:
                self.recorder.stop_etc_recording()
        except Exception as e:
            self.logger.exception("Error stopping log tracking in MainController:")

    def set_constraints(self, constraints):
        """
        Set measurement constraints and update plot readers.
        """
        try:
            self.constraints = constraints
            if self.plot_manager.top_plot and hasattr(self.plot_manager.top_plot.reader, 'update_constraints_etc'):
                self.plot_manager.top_plot.reader.update_constraints_etc(constraints)
            if self.plot_manager.bottom_plot and hasattr(self.plot_manager.bottom_plot.reader, 'update_constraints_etc'):
                self.plot_manager.bottom_plot.reader.update_constraints_etc(constraints)
        except Exception as e:
            self.logger.exception("Error setting constraints in MainController:")

    def update_sample_id(self, sample_id):
        """
        Update the sample ID in metadata and notify plot readers.
        """
        try:
            self.meta_data.sample_id = sample_id
            self.meta_data.read()
            was_log_tracker_running = False
            was_tp_recording_running = False
            #todo: start recorder if was running afterwards. implement stop_tp_recording maybe for that
            if self.recorder:
                if self.is_tp_recording_running():
                    was_tp_recording_running = True
                    self.stop_t_p_recording()
                if self.is_log_file_tracker_running():
                    was_log_tracker_running = True
                    self.stop_log_tracking()
            self.recorder = DataRecorder(meta_data=self.meta_data,
                                         config=self.config
                                         )

            if self.plot_manager.top_plot and hasattr(self.plot_manager.top_plot.reader, 'on_meta_data_changed'):
                self.plot_manager.top_plot.reader.on_meta_data_changed(new_meta_data=self.meta_data)
            if self.plot_manager.bottom_plot and hasattr(self.plot_manager.bottom_plot.reader, 'on_meta_data_changed'):
                self.plot_manager.bottom_plot.reader.on_meta_data_changed(new_meta_data=self.meta_data)

            #restart recording if it was running
            if was_log_tracker_running:
                self.start_log_tracking()
            if was_tp_recording_running:
                self.start_t_p_recording()
        except Exception as e:
            self.logger.exception("Error updating sample ID in MainController:")

    def plot_full_test(self):
        """
        Plot the full test data by stopping continuous recording and configuring full test mode.
        """
        is_recorder_running = False
        is_log_tracker_running = False
        try:
            if self.recorder:
                is_recorder_running = self.is_tp_recording_running()
                is_log_tracker_running = self.is_log_file_tracker_running()
                self.recorder.stop_all_recording()

            self.plot_manager.init_static_plots()


            #if not self.plot_manager.top_plot.reader.p_data_sig_connected:
            #    self.plot_manager.top_plot.reader.p_data_sig.connect(self.plot_manager.bottom_plot.on_tp_data)
            #    self.plot_manager.top_plot.reader.p_data_sig_connected = True
            #if not self.plot_manager.top_plot.reader.etc_data_sig_connected:
            #    self.plot_manager.top_plot.reader.etc_data_sig.connect(self.plot_manager.bottom_plot.on_etc_data)
            #    self.plot_manager.top_plot.reader.etc_data_sig_connected = True
            #if not self.plot_manager.top_plot.reader.cycle_data_sig_connected:
             #       self.plot_manager.top_plot.reader.cycle_data_sig.connect(self.plot_manager.bottom_plot.on_min_max_data)
             #       self.plot_manager.top_plot.reader.cycle_data_sig_connected = True


            if is_recorder_running:
                self.recorder.start_tp_recording()
            if is_log_tracker_running:
                self.recorder.start_etc_recording()

        except Exception as e:
            self.logger.exception("Error plotting full test in MainController: %s", e)

    def toggle_h2_uptake_flag(self, enabled):
        """
        Enable or disable hydrogen uptake measurements.
        """
        try:
            if self.recorder:
                self.recorder.update_h2_uptake_flag(enabled)
        except Exception as e:
            self.logger.exception("Error toggling H2 uptake in MainController:")

    def toggle_cycling_flag(self, enabled):
        """
        Enable or disable cycling flag. Cycles will only be counted if set to True
        """
        try:
            if self.recorder:
                self.recorder.update_cycling_flag(enabled)
        except Exception as e:
            self.logger.exception("Error toggling cycling in MainController:")

    def toggle_is_isotherm_flag(self, enabled):
        """
        Enable or disable hydrogen uptake measurements.
        """
        try:
            if self.recorder:
                self.recorder.update_is_isotherm_flag(enabled)
        except Exception as e:
            self.logger.exception("Error toggling Is Isotherm Flag in MainController:")

    def is_tp_recording_running(self):
        return self.recorder.is_tp_thread_running()

    def is_log_file_tracker_running(self):
        return self.recorder.is_log_thread_running()


# -------------------------------
# Main Window
# -------------------------------
class MainWindow(QMainWindow):
    """
    The main application window. Sets up the UI, connects signals and slots,
    and handles user interactions.
    """
    def __init__(self, config):
        try:
            super().__init__()

            self.config = config
            self.setWindowTitle("T-p ETC Recorder")
            from src.GUI.qt_styles import aqua as style
            self.setStyleSheet(style)
            self.setFont(FONT)
            self.logger = logging.getLogger(__name__)

            ##QTimer for memory logging. Can be deleted
            from src.infrastructure.utils.memory_logger import log_memory
            from PySide6.QtCore import QTimer
            self.memory_timer = QTimer()
            self.memory_timer.setInterval(10*1e3*60)  # 1 second in milliseconds
            self.memory_timer.timeout.connect(lambda: log_memory(logger=self.logger,
                                                                 message="Memory after QTimer timeout"))
            self.memory_timer.start()
            log_memory(logger=self.logger,
                       message="Memory after QTimer timeout")

            ## End of QTimer implementation



            try:
                self.db_conn_params = config.db_conn_params
                self.mb_conn_params = config.mb_conn_params
                self.mb_reading_params = config.mb_reading_params
                self.hd_log_file_tracker_params = config.hd_log_file_tracker_params
            except Exception as e:
                self.logger.error(f"No proper config provided: {e}")
            self.ui = self._load_ui(recording_ui_file_path)
            self.setCentralWidget(self.ui)
            self.meta_data = MetaData(db_conn_params=self.db_conn_params)
            self.meta_manager = MetaDataManager(self.meta_data)
            self.plot_manager = PlotManager(self.ui, self.meta_data, self.logger, db_conn_params=self.db_conn_params)
            self.controller = MainController(self.meta_data, self.plot_manager, self.logger, config)
            self._init_ui()
            self._init_connections()
            self._setup_validators()
            self._load_default_constraints()
            self._sync_constraints_to_ui()
            self.plot_manager.init_continuous_plots()
        except Exception as e:
            logging.getLogger(__name__).exception("Error initializing MainWindow:")

    def _load_ui(self, ui_path):
        """
        Load the UI file using QUiLoader.
        """
        try:
            from PySide6.QtUiTools import QUiLoader
            from PySide6.QtCore import QFile
            loader = QUiLoader()
            ui_file = QFile(ui_path)
            if not ui_file.exists():
                self.logger.error(f"UI file {ui_path} does not exist.")
                return None
            if not ui_file.open(QFile.ReadOnly):
                self.logger.error(f"Unable to open UI file: {ui_path}")
                return None
            ui = loader.load(ui_file)
            ui_file.close()
            if ui is None:
                self.logger.error("Failed to load UI file.")
            return ui
        except Exception as e:
            self.logger.exception("Exception occurred while loading UI:")
            return None

    def _init_ui(self):
        """
        Perform additional UI initialization.
        """
        try:
            self.ui.xy_plot_window.setLayout(QVBoxLayout())
            self.setMinimumSize(1600, 900)
            self.ui.reservoir_volume_edit_field.setText(str(self.controller.reservoir_volume))
            self.ui.sample_id_edit_field.setText(self.meta_data.sample_id or "")
        except Exception as e:
            self.logger.exception("Error initializing UI in MainWindow:")

    def _init_connections(self):
        """
        Connect UI widgets to their corresponding event handlers.
        """
        try:
            self.ui.update_meta_data_button.clicked.connect(self._update_meta_data)
            self.ui.start_stop_tp_recording_button.setCheckable(True)
            self.ui.start_stop_tp_recording_button.clicked.connect(self._toggle_tp_recording)
            self.ui.start_stop_log_file_tracker_button.setCheckable(True)
            self.ui.start_stop_log_file_tracker_button.clicked.connect(self._toggle_log_tracking)
            self.ui.start_stop_static_plot_button.setCheckable(True)
            self.ui.start_stop_static_plot_button.clicked.connect(self._toggle_plotting_mode)
            self.ui.plot_uptake_button.clicked.connect(self._init_uptake_plot)
            self.ui.XyDataSelectDropDown.activated.connect(self._init_right_plot_xy)
            self.ui.T_p_dependent_drop_down.currentIndexChanged.connect(self._init_tp_dependent_plot)

            #
            self.ui.h2_uptake_check_box.clicked.connect(lambda: self.controller.toggle_h2_uptake_flag(
                self.ui.h2_uptake_check_box.isChecked()))
            self.ui.cycle_test_check_box.clicked.connect(lambda: self.controller.toggle_cycling_flag(
                self.ui.cycle_test_check_box.isChecked()))
            self.ui.isotherm_check_box.clicked.connect(lambda: self.controller.toggle_is_isotherm_flag(
                self.ui.isotherm_check_box.isChecked()))

            self.ui.sample_id_edit_field.editingFinished.connect(self._on_sample_id_changed)
            self.ui.MinCharTimeEditField.editingFinished.connect(self._on_constraints_changed)
            self.ui.MaxCharTimeEditField.editingFinished.connect(self._on_constraints_changed)
            self.ui.MinTempIncEditField.editingFinished.connect(self._on_constraints_changed)
            self.ui.MaxTempIncEditField.editingFinished.connect(self._on_constraints_changed)

        except Exception as e:
            self.logger.exception("Error initializing connections in MainWindow:")

    def _setup_validators(self):
        """
        Setup input validators for numerical fields.
        """
        try:
            validator = QDoubleValidator()
            self.ui.sample_mass_edit_field.setValidator(validator)
            self.ui.cell_volume_edit_field.setValidator(validator)
            self.ui.reservoir_volume_edit_field.setValidator(validator)
            self.ui.max_pressure_cycling_edit_field.setValidator(validator)
            self.ui.min_temperature_cycling_edit_field.setValidator(validator)
        except Exception as e:
            self.logger.exception("Error setting up validators in MainWindow:")

    def _load_default_constraints(self):
        """
        Load default measurement constraints.
        """
        try:
            self.controller.set_constraints(STANDARD_CONSTRAINTS.copy())
        except Exception as e:
            self.logger.exception("Error loading default constraints in MainWindow:")

    def _sync_constraints_to_ui(self):
        """
        Synchronize the constraint values to the UI fields.
        """
        try:
            cons = self.controller.constraints
            self.ui.MinCharTimeEditField.setText(str(cons["min_TotalCharTime"]))
            self.ui.MaxCharTimeEditField.setText(str(cons["max_TotalCharTime"]))
            self.ui.MinTempIncEditField.setText(str(cons["min_TotalTempIncr"]))
            self.ui.MaxTempIncEditField.setText(str(cons["max_TotalTempIncr"]))
        except Exception as e:
            self.logger.exception("Error syncing constraints to UI in MainWindow:")

    def _update_meta_data(self):
        """
        Update metadata from UI inputs and refresh UI fields.
        """
        try:
            self.meta_manager.update_from_ui(self.ui)
            self.meta_manager.update_ui(self.ui)
            self.controller.update_sample_id(self.meta_data.sample_id)
        except Exception as e:
            self.logger.exception("Error updating meta data in MainWindow:")

    def _toggle_tp_recording(self):
        """
        Start or stop T-p recording based on button state.
        """
        try:
            is_on = self.ui.start_stop_tp_recording_button.isChecked()
            self._toggle_button(self.ui.start_stop_tp_recording_button,
                                "Start T-p Recording", "Stop T-p Recording", is_on)
            if is_on:
                self._set_flags()
                self.controller.start_t_p_recording()
                if self.plot_manager.top_plot:
                    self._init_continuous_plotting_connections()
            else:
                self._disconnect_continuous_plotting_signals()
                self.controller.stop_t_p_recording()
        except Exception as e:
            self.logger.exception("Error toggling T-p recording in MainWindow:")

    def _init_continuous_plotting_connections(self):
        if not self.plot_manager.top_plot.reader.current_cycle_sig_connected:
            self.plot_manager.top_plot.current_cycle_sig.connect(
                lambda n: self._set_current_state("number", n))
            self.plot_manager.top_plot.reader.current_cycle_sig_connected = True

        if not self.plot_manager.top_plot.reader.current_state_sig_connected:
            self.plot_manager.top_plot.current_state_sig.connect(
                lambda s: self._set_current_state("state", s))
            self.plot_manager.top_plot.reader.current_state_sig_connected = True

        if not self.plot_manager.top_plot.reader.current_uptake_sig_connected:
            self.plot_manager.top_plot.current_uptake_sig.connect(
                lambda u: self._set_current_state("uptake", u))
            self.plot_manager.top_plot.reader.current_uptake_sig_connected = True

    def _disconnect_continuous_plotting_signals(self):
        if self.plot_manager.top_plot.reader.current_cycle_sig_connected:
            self.plot_manager.top_plot.current_cycle_sig.disconnect()
            self.plot_manager.top_plot.reader.current_cycle_sig_connected = False

        if self.plot_manager.top_plot.reader.current_state_sig_connected:
            self.plot_manager.top_plot.current_state_sig.disconnect()
            self.plot_manager.top_plot.reader.current_state_sig_connected = False

        if self.plot_manager.top_plot.reader.current_uptake_sig_connected:
            self.plot_manager.top_plot.current_uptake_sig.disconnect()
            self.plot_manager.top_plot.reader.current_uptake_sig_connected = False

    def _toggle_log_tracking(self):
        """
        Start or stop log file tracking based on button state.
        """
        try:
            is_on = self.ui.start_stop_log_file_tracker_button.isChecked()
            self._toggle_button(self.ui.start_stop_log_file_tracker_button,
                                "Start Log File Tracking", "Stop Log File Tracking", is_on)
            if is_on:
                self.controller.start_log_tracking()
            else:
                self.controller.stop_log_tracking()
        except Exception as e:
            self.logger.exception("Error toggling log tracking in MainWindow:")

    def _toggle_plotting_mode(self):
        """
        Toggle the UI between static (full‑test) and continuous plotting modes.

        This method is bound to a checkable “Start Static Plot” button. When the button
        is checked, it:

        1. Changes the button’s label and style to “Stop Static Plot.”
        2. Calls `PlotManager.init_static_plots()` to replace the left‑hand plots
           with static/full‑test versions.
        3. Connects the static reader’s `meta_data_sig` to `_meta_data_received()`
           so that any sample‑ID changes emitted by the static reader get fed back
           into the UI.
        4. If a sample ID is already set, invokes `MainController.plot_full_test()`
           to immediately fetch & draw the entire test dataset.

        When the button is unchecked, it:

        1. Stops the current static‑plot reader (if running).
        2. Clears all plots from the UI.
        3. Calls `PlotManager.init_continuous_plots()` to restore the original
           continuous‑update plots (temperature & pressure).

        Raises:
            Any exception during toggling is caught and logged via `self.logger.exception`.
        """

        try:
            is_on = self.ui.start_stop_static_plot_button.isChecked()
            self._toggle_button(self.ui.start_stop_static_plot_button,
                                "Start Static Plot",
                                "Stop Static Plot", is_on)

            if is_on:
                self.logger.info("Switching to static plot mode...")
                self._disconnect_continuous_plotting_signals()
                self.plot_manager.init_static_plots()
                if self.plot_manager.top_plot and hasattr(self.plot_manager.top_plot.reader, 'meta_data_sig'):
                    if not self.plot_manager.top_plot.reader.meta_data_sig_connected:
                        self.plot_manager.top_plot.reader.meta_data_sig.connect(self._meta_data_received)
                        self.plot_manager.top_plot.reader.meta_data_sig_connected = True

                self.logger.info("Static plot mode activated")

            else:
                self.logger.info("Switching to live plot mode..")
                if self.plot_manager.top_plot.reader.meta_data_sig_connected:
                    self.plot_manager.top_plot.reader.meta_data_sig.disconnect()
                    self.plot_manager.top_plot.reader.meta_data_sig_connected = False

                self.plot_manager.init_continuous_plots()
                self._init_continuous_plotting_connections()

                if (self.plot_manager.top_plot and self.plot_manager.bottom_plot
                    and hasattr(self.plot_manager.top_plot, "reader")
                    and hasattr(self.plot_manager.bottom_plot, "reader")):

                    self.plot_manager.top_plot.reader.start()
                    self.logger.info("Live plot activated")

        except Exception as e:
            self.logger.exception("Error toggling plotting mode in MainWindow:")

    def _init_right_plot_xy(self):
        """
        Initialize the right XY plot based on the dropdown selection.
        """
        try:
            self.plot_manager.init_right_plot_xy(self.ui.XyDataSelectDropDown.currentText())
            self.plot_manager.right_plot.cycle_number_sig.connect(lambda number: self._set_current_state(key="number", value=number))
            self.plot_manager.right_plot.de_hyd_state_sig.connect(lambda value: self._set_current_state(key="state", value=value))

        except Exception as e:
            self.logger.exception(f"Error initializing right plot XY in MainWindow: {e}")

    def _init_uptake_plot(self):
        """
        Initialize the hydrogen uptake plot using the current x-axis view range.
        """
        try:
            if self.plot_manager.top_plot:
                view_range = self.plot_manager.top_plot.plotItem.viewRange()[0]
                time_range = [datetime.fromtimestamp(ts, tz=local_tz) for ts in view_range]
                self.plot_manager.init_uptake_plot(time_range)
        except Exception as e:
            self.logger.exception("Error initializing uptake plot in MainWindow:")

    def _init_tp_dependent_plot(self):
        """
        Initialize the TP-dependent plot based on dropdown selection and view range.
        """
        try:
            drop_val = self.ui.T_p_dependent_drop_down.currentText().lower()
            x_col = 'pressure' if 'pressure' in drop_val else 'temperature' if 'temperature' in drop_val else None
            if self.plot_manager.bottom_plot:
                stamps = self.plot_manager.bottom_plot.plotItem.viewRange()[0]
                time_range = [datetime.fromtimestamp(ts, tz=local_tz) for ts in stamps]
                self.plot_manager.init_tp_dependent_plot(x_col, time_range)
        except Exception as e:
            self.logger.exception("Error initializing TP dependent plot in MainWindow:")

    def _on_sample_id_changed(self):
        """
        Handle changes in the sample ID field.
        """
        try:
            sample_id = self.ui.sample_id_edit_field.text()
            self.controller.update_sample_id(sample_id)
            self.meta_manager.update_ui(self.ui)
            if self.ui.start_stop_static_plot_button.isChecked():
                self.plot_manager.init_static_plots()
        except Exception as e:
            self.logger.exception("Error in _on_sample_id_changed:")

    def _on_reservoir_volume_changed(self):
        """
        Handle changes to the reservoir volume field.
        """
        try:
            vol = self._to_float(self.ui.reservoir_volume_edit_field.text())
            if vol is None:
                QMessageBox.warning(self, "Invalid Input", "Please enter a numeric value for reservoir volume.")
            else:
                self.controller.set_reservoir_volume(vol)
        except Exception as e:
            self.logger.exception("Error in _on_reservoir_volume_changed:")

    def _on_meta_data_field_changed(self):
        """
        Handle updates when any metadata field is changed.
        """
        try:
            self.meta_manager.update_from_ui(self.ui, update_meta_button_pushed=False)
            self.meta_manager.update_ui(self.ui)
            self.controller.update_sample_id(self.meta_data.sample_id)
        except Exception as e:
            self.logger.exception("Error in _on_meta_data_field_changed:")

    def _on_constraints_changed(self):
        """
        Handle updates to the measurement constraints.
        """
        try:
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
        except Exception as e:
            self.logger.exception("Error in _on_constraints_changed:")

    def _meta_data_received(self, meta_data):
        """
        Handle receiving updated metadata from a plot widget.
        """
        try:
            self.meta_data = meta_data
            self.meta_manager.meta_data = meta_data
            self.meta_manager.update_ui(self.ui)
            self.controller.update_sample_id(meta_data.sample_id)
        except Exception as e:
            self.logger.exception("Error in _meta_data_received:")

    def _toggle_button(self, button, start_text, stop_text, is_on):
        """
        Update button text and style based on its toggle state.
        """
        try:
            if is_on:
                button.setText(stop_text)
                button.setStyleSheet("background-color: darkgreen")
            else:
                button.setText(start_text)
                button.setStyleSheet("background-color: lightgray")
        except Exception as e:
            self.logger.exception("Error in _toggle_button:")

    @staticmethod
    def _to_float(value):
        """
        Convert a value to float.
        """
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _is_numeric(value):
        """
        Check if a value is numeric.
        """
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _set_current_state(self, key, value):
        """
        Update the UI fields for current cycle, state, or uptake.
        """
        try:
            if key.lower() == "number":
                self.ui.current_cycle_edit_field.setText(str(value))
            elif key.lower() == "state":
                self.ui.current_de_hyd_state_edit_field.setText(str(value))
            elif key.lower() == "uptake":
                self.ui.current_uptake_edit_field.setText(str(value))
        except Exception as e:
            self.logger.exception("Error in _set_current_state:")

    def _set_flags(self):
        self.controller.toggle_cycling_flag(self.ui.cycle_test_check_box.isChecked())
        self.controller.toggle_is_isotherm_flag(self.ui.isotherm_check_box.isChecked())
        self.controller.toggle_h2_uptake_flag(self.ui.h2_uptake_check_box.isChecked())

    def closeEvent(self, event):
        """
        Ensure that recording is stopped when the window is closed.
        """
        try:
            self.logger.info("Shutting down main window...")
            self.controller.stop_t_p_recording()
            self.controller.stop_log_tracking()
            self.plot_manager.clear_plots()
            if hasattr(self, 'memory_timer'):
                if self.memory_timer.isActive():
                    self.memory_timer.stop()
            super().closeEvent(event)
        except Exception as e:
            self.logger.exception("Error in closeEvent of MainWindow:")

    def resizeEvent(self, event):
        """
        Adjust plot sizes when the window is resized.
        """
        try:
            super().resizeEvent(event)
            if self.plot_manager.top_plot and hasattr(self.plot_manager.top_plot, 'adjust_plot_size'):
                self.plot_manager.top_plot.adjust_plot_size()
            if self.plot_manager.bottom_plot and hasattr(self.plot_manager.bottom_plot, 'adjust_plot_size'):
                self.plot_manager.bottom_plot.adjust_plot_size()
            if self.plot_manager.right_plot and hasattr(self.plot_manager.right_plot, 'adjust_plot_size'):
                self.plot_manager.right_plot.adjust_plot_size()
        except Exception as e:
            self.logger.exception("Error in resizeEvent of MainWindow:")


def main():
    """
    Entry point for the application.
    """
    try:
        from src.infrastructure.core.config_reader import GetConfig
        app = QApplication([])
        main_window = MainWindow(config=GetConfig())
        main_window.show()
        app.exec()
    except Exception as e:
        logging.getLogger(__name__).exception("Error in main():")


if __name__ == "__main__":
    main()
