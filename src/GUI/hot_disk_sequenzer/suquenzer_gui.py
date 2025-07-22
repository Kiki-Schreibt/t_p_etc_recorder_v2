# suquenzer_gui.py

"""
 - Uses QThread and a dedicated worker (HotDiskControllerWorker) to run the controller.
 - Separates UI layout from schedule generation logic.
 - Improves error handling and data validation.
 - Ensures proper timezone handling.
"""

import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from PySide6.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QMessageBox, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QSizePolicy, QFormLayout, QGroupBox, QCompleter

)
from PySide6.QtGui import QIntValidator
from PySide6.QtCore import Signal, QDateTime, QObject, QTimer, QThread
import pyqtgraph as pg

# Local project imports (assumed available in your project structure)
from src.tp_program_simulator import TemperatureControllerHotDiskSequenzer
from src.infrastructure.handler.hot_disk_handler import HotDiskSequenzerBackend
from src.infrastructure.core import global_vars

try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging

STYLE_SHEET = global_vars.style
local_tz = global_vars.local_tz
standard_hot_disk_schedule_folder = r"C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\config\tps_schedules"



class SignaledHotDiskSequenzerBackend(QObject):
    """
    Wraps a HotDiskSequenzerBackend and re‑emits its wait_until calls
    as a Qt Signal.
    """
    latest_program_step_sig = Signal(datetime.datetime)

    def __init__(
        self,
        hd_conn_params,
        template_folder_path: str = standard_hot_disk_schedule_folder,
        sensor_insulation: str = "Mica",
        sensor_type: str = "5465",
        standard_number_of_measurements: int = 3,
        parent: QObject = None
    ):
        super().__init__(parent)
        # create the real backend
        self._backend = HotDiskSequenzerBackend(
            hd_conn_params,
            template_folder_path,
            sensor_insulation,
            sensor_type,
            standard_number_of_measurements
        )

    def wait_until(self, target_time: datetime.datetime):
        # first emit your Qt signal…
        self.latest_program_step_sig.emit(target_time)
        # …then delegate to the real backend
        return self._backend.wait_until(target_time)

    def run(self, schedule: list):
        # just forward to the backend.run()
        return self._backend.run(schedule)

    def end(self):
        return self._backend.end()


class SignaledHotDiskSequenzerThreader(QObject):
    """
    Worker that runs a HotDiskController schedule in its own QThread.

    - Listens to controller signals and re-emits them to the UI.
    - Emits `finished` when done.
    """
    latest_program_step = Signal(datetime.datetime)
    finished = Signal()

    def __init__(self, controller, schedule: list, logger: logging.getLogger()):
        """
        :param controller: Instance of SignaledHotDiskController.
        :param schedule:   List of dicts with heating and measurement steps.
        """
        super().__init__()
        self.controller = controller
        self.schedule = schedule
        self.logger = logger

    def run(self):
        """
        Main entry point for QThread. Hooks up the controller's
        latest_program_step_sig to this worker's signal, then
        invokes controller.run(). Always emits `finished`.
        """
        try:
            self.controller.latest_program_step_sig.connect(self.latest_program_step.emit)
            self.controller.run(self.schedule)
        except Exception as e:
            self.logger.error(f"Error in HotDiskControllerWorker: {e}")
        finally:
            self.finished.emit()


class ScheduleGeneratorBase(QWidget):
    """
    Base UI for building a HotDisk measurement schedule.

    - Contains controls for program rows, sensor settings, and repetition.
    - Emits `complete_program_sig` and `meas_times_sig` with DataFrames.
    """
    complete_program_sig = Signal(pd.DataFrame)
    meas_times_sig      = Signal(pd.DataFrame)

    def __init__(self):
        """
        Constructs the entire UI layout (left controls + right plot).
        """
        super().__init__()
        self.setStyleSheet(STYLE_SHEET)
        self.init_ui()

    def init_ui(self):
        """
        Set up top‐level layouts and populate with sub‐widgets.
        """
        self.main_layout = QHBoxLayout()
        self.left_layout = QVBoxLayout()
        self._init_buttons()
        self._init_measurement_settings()
        self._init_sensor_settings()
        self._init_schedule_table()
        self._init_repetition_buttons()
        self.main_layout.addLayout(self.left_layout, stretch=1)

        self.right_layout = QVBoxLayout()
        self._init_plot_widget()
        self.main_layout.addLayout(self.right_layout, stretch=2)

        self.setLayout(self.main_layout)

    def _init_buttons(self):
        """
        Add the “Add Program Row” and “Start Schedule” buttons.
        """
        self.add_measurement_button = QPushButton('Add Program Row')
        self.add_measurement_button.clicked.connect(self.add_program_row)
        self.left_layout.addWidget(self.add_measurement_button)

        self.start_schedule_button = QPushButton('Start Schedule')
        self.left_layout.addWidget(self.start_schedule_button)

        self.continue_schedule_button = QPushButton('Continue Schedule from File')
        self.left_layout.addWidget(self.continue_schedule_button)

    def _init_measurement_settings(self):
        """
        Add inputs for start time, measurement delay, and template folder.
        """
        measurement_group = QGroupBox("Measurement Settings")
        measurement_layout = QFormLayout()

        self.start_measurements_label = QLabel("Start time temperature program:")
        self.start_measurements_input = QLineEdit(
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        self.start_measurements_input.setFixedWidth(200)
        measurement_layout.addRow(
            self.start_measurements_label,
            self.start_measurements_input
        )

        self.measurement_delay_label = QLabel(
            "Start measurement xx min before next temperature is reached:"
        )
        self.measurement_delay_input = QLineEdit("30")
        self.measurement_delay_input.setFixedWidth(50)
        measurement_layout.addRow(
            self.measurement_delay_label,
            self.measurement_delay_input
        )

        self.template_folder_label = QLabel("Template Folder:")
        self.template_folder_path  = QLineEdit(standard_hot_disk_schedule_folder)
        self.template_folder_path.setFixedWidth(200)
        self.template_folder_browse_button = QPushButton("Browse")
        self.template_folder_browse_button.clicked.connect(self.browse_template_folder)
        template_folder_layout = QHBoxLayout()
        template_folder_layout.addWidget(self.template_folder_path)
        template_folder_layout.addWidget(self.template_folder_browse_button)
        measurement_layout.addRow(
            self.template_folder_label,
            template_folder_layout
        )

        measurement_group.setLayout(measurement_layout)
        self.left_layout.addWidget(measurement_group)

    def browse_template_folder(self):
        """
        Open a directory picker to choose the schedule‐template folder.
        """
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        folder_path = QFileDialog.getExistingDirectory(
            self,
            "Select Template Folder",
            standard_hot_disk_schedule_folder,
            options=options
        )
        if folder_path:
            self.template_folder_path.setText(folder_path)

    def _init_schedule_table(self):
        """
        Initialize the QTableWidget for program rows (temp, duration, power, time).
        """
        self.program_table = QTableWidget()
        self.program_table.setColumnCount(4)
        self.program_table.setHorizontalHeaderLabels([
            'Temperature [°C]',
            'Time [hh:mm:ss]',
            'Measurement Power [W]',
            'Measurement Time [s]'
        ])
        header = self.program_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        self.left_layout.addWidget(self.program_table)

        # Add a few default rows
        for _ in range(4):
            self.add_program_row()

    def _init_repetition_buttons(self):
        """
        Add inputs for repeating a subset of rows multiple times.
        """
        self.repeat_start_label = QLabel("Repeat Start Index:")
        self.repeat_start_input = QLineEdit("0")
        self.repeat_start_input.setFixedWidth(50)
        self.repeat_start_input.setValidator(QIntValidator(0, 1000))

        self.repeat_end_label = QLabel("Repeat End Index:")
        self.repeat_end_input = QLineEdit("0")
        self.repeat_end_input.setFixedWidth(50)
        self.repeat_end_input.setValidator(QIntValidator(0, 1000))

        self.repeat_count_label = QLabel("Repeat Count:")
        self.repeat_count_input = QLineEdit("0")
        self.repeat_count_input.setFixedWidth(50)
        self.repeat_count_input.setValidator(QIntValidator(0, 1000))

        repetition_layout = QHBoxLayout()
        repetition_layout.addWidget(self.repeat_start_label)
        repetition_layout.addWidget(self.repeat_start_input)
        repetition_layout.addWidget(self.repeat_end_label)
        repetition_layout.addWidget(self.repeat_end_input)
        repetition_layout.addWidget(self.repeat_count_label)
        repetition_layout.addWidget(self.repeat_count_input)
        repetition_layout.addStretch()
        self.left_layout.addLayout(repetition_layout)

    def _init_sensor_settings(self):
        """
        Add controls to choose sensor insulation and type, with auto-complete.
        """
        sensor_type_suggestions = ["5465", "5465-F1", "5465-F2"]
        sensor_insulation_suggestions = ["Mica", "Kapton", "Teflon"]
        insulation_completer = QCompleter(sensor_insulation_suggestions)
        type_completer       = QCompleter(sensor_type_suggestions)

        sensor_setting_group = QGroupBox("Sensor Settings")
        sensor_setting_layout = QFormLayout()

        self.sensor_insulation_label = QLabel("Sensor insulation:")
        self.sensor_insulation_input = QLineEdit("Mica")
        self.sensor_insulation_input.setCompleter(insulation_completer)
        self.sensor_insulation_input.setFixedWidth(100)
        sensor_setting_layout.addRow(
            self.sensor_insulation_label,
            self.sensor_insulation_input
        )

        self.sensor_type_label = QLabel("Sensor type:")
        self.sensor_type_input = QLineEdit("5465")
        self.sensor_type_input.setCompleter(type_completer)
        self.sensor_type_input.setFixedWidth(100)
        sensor_setting_layout.addRow(
            self.sensor_type_label,
            self.sensor_type_input
        )

        sensor_setting_group.setLayout(sensor_setting_layout)
        self.left_layout.addWidget(sensor_setting_group)

    def _init_plot_widget(self):
        """
        Embed the pyqtgraph-based ProgramPlotWidget on the right side.
        """
        self.plot_widget = ProgramPlotWidget()
        self.plot_widget.setSizePolicy(
            QSizePolicy.Expanding,
            QSizePolicy.Expanding
        )
        self.right_layout.addWidget(self.plot_widget)

    def add_program_row(self):
        """
        Insert a new default row into the schedule table.
        """
        row_position = self.program_table.rowCount()
        self.program_table.insertRow(row_position)
        self.program_table.setItem(row_position, 0, QTableWidgetItem('100'))
        self.program_table.setItem(row_position, 1, QTableWidgetItem('00:10:00'))
        self.program_table.setItem(row_position, 2, QTableWidgetItem('0.01'))
        self.program_table.setItem(row_position, 3, QTableWidgetItem('3'))

    @staticmethod
    def validate_duration(duration_str: str) -> bool:
        """
        Check that a duration is in HH:MM:SS integer format.

        :param duration_str: string to validate
        :returns: True if valid, False otherwise
        """
        try:
            h, m, s = map(int, duration_str.split(':'))
            return True
        except ValueError:
            return False

    def plot_program(self, program: pd.DataFrame):
        """
        Slot: draw the temperature vs. time line.

        :param program: DataFrame with 'end_time' and 'temperature' columns.
        """
        self.plot_widget.update_plot(program)

    def plot_meas_times(self, program: pd.DataFrame):
        """
        Slot: overlay measurement‐time scatter dots.

        :param program: DataFrame with 'measurement_time' and 'temperature' cols.
        """
        self.plot_widget.update_scatter_plot(program)

    @staticmethod
    def safe_schedule(schedule: pd.DataFrame):
        from src.infrastructure.utils.standard_paths import standard_schedule_files_path
        import os
        os.makedirs(standard_schedule_files_path, exist_ok=True)
        current_day = datetime.datetime.now()
        current_day_str = current_day.strftime('%Y-%m-%d')
        file_name = 'Schedule_' + current_day_str + '.csv'

        full_file_path = os.path.join(standard_schedule_files_path, file_name)
        schedule.to_csv(full_file_path, index=False)

    @staticmethod
    def load_schedule_from_csv(file_path):
        schedule = pd.read_csv(file_path)
        schedule['measurement_time'] = pd.to_datetime(schedule['measurement_time'])
        return schedule


class SequenzerMainWindow(ScheduleGeneratorBase):
    """
    Extends the base UI to wire up schedule parsing, controller thread,
    and countdown display.
    """
    def __init__(self, config):
        """
        Connect all signals/slots for schedule creation and start button.
        """
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.hd_conn_params = config.hd_conn_params

        self.start_schedule_button.clicked.connect(self.create_and_start_schedule)
        self.complete_program_sig.connect(self.plot_program)
        self.meas_times_sig.connect(self.plot_meas_times)
        self.repeat_end_input.editingFinished.connect(self.try_parse_program)
        self.repeat_start_input.editingFinished.connect(self.try_parse_program)
        self.repeat_count_input.editingFinished.connect(self.try_parse_program)
        self.program_table.cellChanged.connect(self.try_parse_program)

        self.continue_schedule_button.clicked.connect(self.on_continue_schedule_clicked)

        # countdown support
        self.target_time = None
        self.countdown_timer = QTimer(self)
        self.countdown_timer.timeout.connect(self.update_countdown)

        # controller threading
        self.hot_disk_thread = None
        self.hot_disk_worker = None

    def create_and_start_schedule(self):
        """
        Handler for "Start Schedule" click:
        - validate inputs,
        - build schedule list,
        - instantiate controller and worker thread,
        - begin countdown.
        """

        schedule = self._generate_schedule()
        self.safe_schedule(schedule=schedule)
        self.start_schedule(schedule=schedule)

    def on_continue_schedule_clicked(self):
        """
        Open a file dialog to select a previously saved schedule CSV
        and start continuing the schedule from that file.
        """
        from src.infrastructure.utils.standard_paths import standard_schedule_files_path
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Schedule File",
            standard_schedule_files_path,
            "CSV Files (*.csv);;All Files (*)",
            options=options
        )
        if file_path:
            try:
                self.continue_schedule_from_file(file_path)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to continue schedule from file: {e}")

    def start_schedule(self, schedule):
        if self.hot_disk_thread and self.hot_disk_worker:
            #todo: proper thread termination here... stupid stuff :D
            pass

        if schedule.empty:
            self.logger.error("Error starting schedule. No schedule provided")
            raise

        scheduled_dict_list = schedule.to_dict(orient='records')
        if not scheduled_dict_list:

            QMessageBox.warning(self, "Schedule Error",
                                "Could not generate a valid schedule.")
            return

        if not hasattr(self, 'countdown_label'):
            self._init_countdown_labels()

        sensor_type       = self.sensor_type_input.text().strip()
        sensor_insulation = self.sensor_insulation_input.text().strip()
        folder_path       = self.template_folder_path.text().strip()
        if not sensor_type or not sensor_insulation or not folder_path:
            QMessageBox.warning(self, "Input Error",
                                "Sensor settings or template folder cannot be empty.")
            return
        try:

            self.hot_disk_controller = SignaledHotDiskSequenzerBackend(
                        hd_conn_params=self.hd_conn_params,
                        template_folder_path=folder_path,
                        sensor_insulation=sensor_insulation,
                        sensor_type=sensor_type
                    )

        except Exception as e:
            print(e)
            QMessageBox.critical(self, "Controller Error",
                                 f"Failed to initialize HotDiskController: {e}")
            return

        # thread + worker
        self.hot_disk_thread = QThread()
        self.hot_disk_worker = SignaledHotDiskSequenzerThreader(
            controller=self.hot_disk_controller,
            schedule=scheduled_dict_list,
            logger = self.logger
        )

        self.hot_disk_worker.moveToThread(self.hot_disk_thread)
        self.hot_disk_thread.started.connect(self.hot_disk_worker.run)
        self.hot_disk_worker.finished.connect(self.hot_disk_thread.quit)
        self.hot_disk_worker.finished.connect(self.hot_disk_worker.deleteLater)
        self.hot_disk_thread.finished.connect(self.hot_disk_thread.deleteLater)
        self.hot_disk_worker.latest_program_step.connect(self.set_target_time)
        self.hot_disk_thread.start()

        self.status_label.setText("Status: Running")

    def _generate_schedule(self) -> pd.DataFrame:
        """
        Parse the table into a DataFrame of program steps, adjust units,
        then return as a list of dicts for the controller.

        :returns: list of dicts or empty list on failure
        """
        program_with_meas_times = self.try_parse_program()
        if program_with_meas_times is None or program_with_meas_times.empty:
            return []
        program_with_meas_times = program_with_meas_times.dropna(subset=["measurement_time"])
        scheduled_program = program_with_meas_times.rename(
            columns={'measurement_power_watt': 'heating_power'}
        )
        return scheduled_program

    def get_temperature_program(self) -> list:
        """
        Read each row in the QTableWidget and build a list of
        (temp: float, duration: str, meas_power: float, meas_time: float).

        Skips any incomplete rows and shows warnings for invalid formats.
        """
        temperature_program = []
        for row in range(self.program_table.rowCount()):
            try:
                temp_item       = self.program_table.item(row, 0)
                duration_item   = self.program_table.item(row, 1)
                meas_power_item = self.program_table.item(row, 2)
                meas_time_item  = self.program_table.item(row, 3)

                if None in (temp_item, duration_item, meas_power_item, meas_time_item):
                    continue

                temp       = int(temp_item.text())
                duration   = duration_item.text()

                meas_power = transform_dash_string(meas_power_item.text(), lambda x: x*1e3)
                meas_time = transform_dash_string(meas_time_item.text(), lambda x:x)

                if not self.validate_duration(duration):
                    raise ValueError(
                        f"Invalid duration format in row {row+1} (expected HH:MM:SS)."
                    )
                temperature_program.append((temp, duration, meas_power, meas_time))

            except ValueError as ve:
                QMessageBox.warning(self, "Input Error", str(ve))
                return None
            except Exception as e:
                QMessageBox.warning(self, "Input Error",
                                    f"Error in row {row+1}: {e}")
                return None

        return temperature_program

    def get_repetition_parameters(self):
        """
        Read and validate the repeat‐start, end, and count inputs.
        Returns (start, end, count) or three Nones on failure.
        """
        try:
            repeat_start = int(self.repeat_start_input.text())
            repeat_end   = int(self.repeat_end_input.text())
            repeat_count = int(self.repeat_count_input.text())
            if (repeat_start < 0 or repeat_end < 0 or repeat_count < 0 or
               (repeat_start > repeat_end and repeat_end != 0)):
                raise ValueError("Repetition parameters must be non-negative and start ≤ end.")
            repeat_start = repeat_start or None
            repeat_end   = repeat_end   or None
            return repeat_start, repeat_end, repeat_count
        except ValueError as ve:
            QMessageBox.warning(self, "Input Error", f"Invalid repetition parameters: {ve}")
            return None, None, None

    def _init_countdown_labels(self):
        """
        Create the countdown/time-left display at the bottom of the controls.
        """
        measurement_group  = QGroupBox("Current status")
        measurement_layout = QFormLayout()
        self.countdown_label = QLabel("Time left: N/A", self)
        self.status_label    = QLabel("Status: Idle")
        measurement_layout.addRow(self.countdown_label)
        measurement_layout.addRow(self.status_label)
        measurement_group.setLayout(measurement_layout)
        self.left_layout.addWidget(measurement_group)

    def set_target_time(self, target_time: datetime.datetime):
        """
        Slot: called when the controller emits a new program step time.
        Starts the 1s timer to update the countdown display.
        """
        self.target_time = target_time
        self.countdown_timer.start(1000)

    def update_countdown(self):
        """
        Called every 1s once a target_time is set.
        Updates the label with time remaining until next measurement.
        """
        if not self.target_time:
            return
        now   = datetime.datetime.now(local_tz)
        delta = self.target_time - now
        if delta.total_seconds() <= 0:
            self.countdown_label.setText("Time left: 0s")
            self.countdown_timer.stop()
        else:
            hours, remainder = divmod(int(delta.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            formatted = (
                f"{hours:02d}:{minutes:02d}:{seconds:02d} "
                f"until next measurement at {self.target_time.strftime('%H:%M:%S')}"
            )
            self.countdown_label.setText(f"Time left: {formatted}")

    def try_parse_program(self) -> pd.DataFrame:
        """
        Build the full measurement‐time DataFrame via the simulator,
        handling any parse or logic errors with a dialog.
        Emits `complete_program_sig` and `meas_times_sig` on success.
        """
        try:
            time_delay_minutes = float(self.measurement_delay_input.text())
        except ValueError:
            QMessageBox.warning(self, "Input Error",
                                "Invalid measurement delay. Please enter a number.")
            return None
        time_delay = datetime.timedelta(minutes=time_delay_minutes)

        program = self.get_temperature_program()
        if not program:
            return pd.DataFrame()

        repeat_start, repeat_end, repeat_count = self.get_repetition_parameters()
        try:
            temperature_controller = TemperatureControllerHotDiskSequenzer(
                temperature_program=program,
                repeat_start=repeat_start,
                repeat_end=repeat_end,
                repeat_count=repeat_count
            )
            start_time = datetime.datetime.strptime(
                self.start_measurements_input.text(), "%Y-%m-%d %H:%M:%S"
            ).replace(tzinfo=local_tz)

            program_with_meas_times, _ = (
                temperature_controller.get_program_times(start_time=start_time, time_delay=time_delay)
            )

            self.complete_program_sig.emit(program_with_meas_times.copy())
            self.meas_times_sig.emit(
                program_with_meas_times[['measurement_time', 'temperature']].copy().dropna(subset=['measurement_time'])
            )

            return program_with_meas_times

        except Exception as e:
            QMessageBox.critical(self, "Schedule Generation Error",
                                 f"Failed to parse program: {e}")
            return pd.DataFrame()

    def continue_schedule_from_file(self, file_path):
        """
        Loads old schedule file (.csv) and continues measurements from current time on
        :param file_path: file path of the saved schedule
        :return:
        """
        schedule = self.load_schedule_from_csv(file_path=file_path)
        mask = schedule['measurement_time'] <= datetime.datetime.now(tz=local_tz)
        schedule = schedule.loc[~mask]
        if not schedule.empty:
            self.meas_times_sig.emit(schedule[['measurement_time', 'temperature']].copy().dropna(subset=['measurement_time']))
            self.start_schedule(schedule)
        else:
            self.logger.error("All measurement times are in past already")

    def _add_start_times(self, df):
        # convert your duration strings into real timedeltas
        durations = pd.to_timedelta(df['duration'])

        df['start_time'] = df['end_time'] - durations

        # now build the 2-point series per step
        rows = []
        for _, row in df.iterrows():
            # flat hold: start → end at same temperature
            rows.append({'time': row['start_time'], 'temperature': row['temperature']})
            rows.append({'time': row['end_time'],   'temperature': row['temperature']})
        full_df = pd.DataFrame(rows).sort_values('time')

        # rename the column so update_plot still finds 'end_time'
        full_df = full_df.rename(columns={'time': 'end_time'})
        return full_df

    def closeEvent(self, event):
        """
        Ensure any running worker thread is stopped cleanly on window close.
        """
        if self.hot_disk_worker:
            if hasattr(self, 'hot_disk_controller'):
                self.hot_disk_controller.end()
            if self.hot_disk_thread and self.hot_disk_thread.isRunning():
                self.hot_disk_thread.quit()
                self.hot_disk_thread.wait(2000)
            self.status_label.setText("Status: Stopped")
        super().closeEvent(event)

from typing import Callable
def transform_dash_string(
    s: str,
    func: Callable[[float], float],
    *,
    as_int_when_possible: bool = True
) -> str:
    """
    Split a hyphen‑separated string of numbers, apply `func` to each number,
    and recombine back into a hyphen‑separated string.

    :param s: Input string like "3-5-7.5"
    :param func: A function that takes a float and returns a float
    :param as_int_when_possible: If True, results that are whole numbers
                                 will be formatted without a decimal.
    :returns: e.g. transform_dash_string("3-5", lambda x: x+1) -> "4-6"
    """
    parts = [p.strip() for p in s.split('-') if p.strip()]
    if not parts:
        return None
    out_parts = []
    for p in parts:
        try:
            num = float(p)
        except ValueError:
            raise ValueError(f"Invalid number in range string: {p!r}")
        new_num = func(num)
        if as_int_when_possible and float(new_num).is_integer():
            out_parts.append(str(int(new_num)))
        else:
            out_parts.append(str(new_num))
    return '-'.join(out_parts)


class ProgramPlotWidget(pg.PlotWidget):
    """
    A pyqtgraph PlotWidget specialized for showing:
     - a line of temperature vs. end_time
     - a scatter of measurement times.
    """
    def __init__(self, parent=None):
        """
        Initialize axes, legend, and placeholders for plots.
        """
        super().__init__(parent=parent)
        self.setWindowTitle("Temperature Program")
        self.scatter_plot = None
        self.line_plot    = None
        self._init_left_axis("Temperature")

    def _init_left_axis(self, y_axis: str):
        """
        Replace bottom axis with DateAxisItem and label axes.
        """
        x_axis = DateAxisItem(orientation='bottom')
        self.plotItem.setAxisItems({'bottom': x_axis})
        self.plotItem.getAxis('bottom').setLabel("Time")
        self.plotItem.addLegend(offset=(0, 1))
        self.plotItem.getAxis('left').setLabel(y_axis)

    def update_plot(self, df: pd.DataFrame):
        """
        Draw or update the line plot of temperature vs. end_time.

        :param df: must contain 'end_time' (datetime) and 'temperature' (float).
        """
        if df.empty:
            return
        x = [t.timestamp() for t in df['end_time']]
        y = df['temperature']
        if self.line_plot is None:
            self.line_plot = self.plotItem.plot(
                x=x, y=y, name="Temperature",
                pen=pg.mkPen(color="#FF0000", width=2)
            )
        else:
            self.line_plot.setData(x=x, y=y)

    def update_scatter_plot(self, df: pd.DataFrame):
        """
        Draw or update blue circles at each measurement_time.

        :param df: must contain 'measurement_time' and 'temperature'.
        """
        if df.empty:
            return
        x = [t.timestamp() for t in df['measurement_time']]
        y = df['temperature']
        if self.scatter_plot is None:
            self.scatter_plot = pg.ScatterPlotItem(
                x=x, y=y, symbol='o', size=8,
                pen=pg.mkPen(color='b'),
                brush=pg.mkBrush(color='b'),
                name="Measurements"
            )
            self.plotItem.addItem(self.scatter_plot)
        else:
            self.scatter_plot.setData(x=x, y=y)


class DateAxisItem(pg.AxisItem):
    """
    A custom AxisItem that formats UNIX‐timestamp ticks as human dates.
    """
    def tickStrings(self, values, scale, spacing):
        """
        Choose a date format based on span and convert each tick value.
        """
        if not values:
            return []
        span = max(values) - min(values)
        if span < 3600:
            fmt = "HH:mm:ss"
        elif span < 86400:
            fmt = "HH:mm"
        else:
            fmt = "yyyy MM dd"
        tick_labels = []
        for value in values:
            qdt = QDateTime.fromSecsSinceEpoch(int(value))
            tick_labels.append(qdt.toString(fmt))
        return tick_labels


# =============================================================================
# Main execution
# =============================================================================
if __name__ == '__main__':
    from src.infrastructure.core.config_reader import config
    app = QApplication([])

    window = SequenzerMainWindow(config=config)
    window.setWindowTitle("HotDisk Temperature Schedule Generator")
    window.show()
    app.exec()
