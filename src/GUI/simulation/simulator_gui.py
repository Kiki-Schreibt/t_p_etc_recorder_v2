#simulator_gui.py
import sys

import pandas as pd
from PySide6.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QMessageBox, QComboBox, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QSizePolicy, QFormLayout
)
from PySide6.QtCore import Slot, Signal, QObject, QTimer
from PySide6.QtGui import QIntValidator
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.ticker import FuncFormatter

from src.GUI.simulation.dicon_simulator_v2 import MBServer  # Import your MBServer class
from src.tp_program_simulator import TemperatureControllerDiconSimulator
from src.infrastructure.core import global_vars

try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging

from src.infrastructure.utils.memory_logger import log_memory

logger = logging.getLogger(__name__)
STYLE_SHEET = global_vars.style

def seconds_to_minutes(x, pos):
    # Convert seconds to minutes:seconds
    minutes = int(x // 60)
    seconds = int(x % 60)
    return f'{minutes}:{seconds:02d}'

def seconds_to_hours(x, pos):
    # Convert seconds to hours:minutes
    hours = int(x // 3600)
    minutes = int((x % 3600) // 60)
    return f'{hours}:{minutes:02d}'


#todo: implementation of preview of next files
class ModbusServerControlBusiness(QObject):
    server_started = Signal(str, int)
    server_stopped = Signal()
    plot_updated = Signal(Figure)
    csv_data_received = Signal(pd.DataFrame)
    simulation_error = Signal(str)
    point_to_highlight_received = Signal(pd.Series)

    def __init__(self):
        super().__init__()
        self.server = MBServer(
            host_ip="localhost",
            port=503,  # Use appropriate port
            folder_path="",  # Will be set via GUI
            mode='csv'
        )
        self.server.new_csv_file_received.connect(self.get_data_for_plot_csv)
        self.temperature_program = []
        self.repeat_start = 0
        self.repeat_end = 0
        self.repeat_count = 0
        self.past_time_csv = 0
        self.iterator_tp_program = 1
        self.server.point_to_highlight_received.connect(self.emit_point_to_highlight)
        self.df = pd.DataFrame()

    def set_mode(self, mode):
        self.server.set_mode(mode)

    def set_folder_path(self, path):
        self.server.folder_path = path

    def set_temperature_program(self, temperature_program):
        self.temperature_program = temperature_program
        self.server.set_temperature_program(temperature_program)

    def set_repetition_parameters(self, repeat_start, repeat_end, repeat_count):
        self.repeat_start = repeat_start
        self.repeat_end = repeat_end
        self.repeat_count = repeat_count
        self.server.set_repetition_parameters(repeat_start, repeat_end, repeat_count)

    def set_sleep_interval(self, interval):
        self.server.set_sleep_interval(interval)

    def start_server(self):
        """
        starts server and emits server_started Signal (carrying host_ip and port)
        :return:
        """
        self.server.start_server()
        self.server_started.emit(self.server.host_ip, self.server.port)

    def stop_server(self):
        self.server.stop_server()
        self.server_stopped.emit()

    def simulate_temperature_program(self):
        # Perform the simulation and update the plot
        times, temps, pressures = self._simulate_temperature_program_internal()
        if times and temps:
            figure = Figure(figsize=(5, 4))
            ax = figure.add_subplot(111)
            ax.plot(times, temps, marker='o')
            ax.set_xlabel('Time (seconds)')
            ax.set_ylabel('Temperature (°C)')
            ax.set_title('Temperature Program Simulation')
            ax.grid(True)
            if not pressures:
                self.plot_updated.emit(figure)
        if times and pressures:
            if ax:
                ax2 = ax.twinx()
            # Plot pressures on the secondary y-axis
            color_press = 'tab:red'
            ax2.plot(times, pressures, marker='x', color=color_press, label='Pressure')
            ax2.set_ylabel('Pressure (bar)', color=color_press)
            ax2.tick_params(axis='y', labelcolor=color_press)

            # Combine legends from both axes
            lines_1, labels_1 = ax.get_legend_handles_labels()
            lines_2, labels_2 = ax2.get_legend_handles_labels()
            ax.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper right')
            self.plot_updated.emit(figure)

    def get_data_for_plot_csv(self, df):
        log_memory(logger, "When receiving df in Business")
        self.df = None
        try:
            if not df.empty:
                x_col = 'seconds'
                df['time'] = pd.to_datetime(df['time'])
                time = (df['time'] - df['time'].iloc[0]).dt.total_seconds()
                self.past_time_csv = time.iloc[-1]
                time += self.past_time_csv
                df[x_col] = time
                self.df = df
                self.csv_data_received.emit(df.copy())
                log_memory(logger, "After emiting df for updating canvas on data")

            else:
                print("DataFrame is empty")
        except Exception as e:
            print(f"Error in get_data_for_plot_csv: {e}")

    def _simulate_temperature_program_internal(self):

        times = []
        temps = []
        current_time = 0

        if not self.temperature_program:
            return
        temperature_program = self.temperature_program
        repeat_start = self.repeat_start if self.repeat_start > 0 else None
        repeat_end = self.repeat_end     if self.repeat_end > 0 else None
        repeat_count = self.repeat_count
        tp_simulator = TemperatureControllerDiconSimulator(temperature_program=temperature_program,
                                                                  repeat_start=repeat_start,
                                                                  repeat_end=repeat_end,
                                                                  repeat_count=repeat_count)

        # Adjust repeat indices to be within bounds
        times, temps, pressures = tp_simulator.simulate_whole_program()
        return times, temps, pressures

    @staticmethod
    def parse_duration(duration_str):
        try:
            h, m, s = map(int, duration_str.split(':'))
            return h * 3600 + m * 60 + s
        except ValueError:
            return 0

    def emit_point_to_highlight(self, row=pd.Series()):

        if not self.df.empty:
            if self.server.mode == 'csv':
                row['seconds'] = self.df.loc[self.df['time'] == row['time'], 'seconds'].values[0]
                self.point_to_highlight_received.emit(row.copy())
        if self.server.mode == 'tp_program':

            row['seconds'] = self.iterator_tp_program * self.server.sleep_interval
            self.iterator_tp_program = self.iterator_tp_program + 1 * self.server.sleep_interval
            self.point_to_highlight_received.emit(row.copy())

    def update_manual_values(self, values):
        if self.server:
            self.server.set_manual_values(values)


class ModbusServerControlGUI(QWidget):
    server_started = Signal(str, int)
    server_stopped = Signal()

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Modbus Server Control")
        self.setStyleSheet(STYLE_SHEET)
        self.resize(800, 600)
        self.business = ModbusServerControlBusiness()

        # Create UI elements
        self.start_button = QPushButton("Start Server")
        self.stop_button = QPushButton("Stop Server")
        self.stop_button.setEnabled(False)

        self.mode_label = QLabel("Mode:")
        self.mode_combo = QComboBox()
        self.mode_combo.addItems(["csv", "tp_program", "manual"])

        self.sleep_label = QLabel("Sleep Interval (s):")
        self.sleep_input = QLineEdit("1")
        self.sleep_input.setFixedWidth(50)


        # File selection for CSV mode
        self.csv_path_label = QLabel("CSV File/Folder:")
        self.csv_path_display = QLineEdit()
        self.csv_path_display.setReadOnly(True)
        self.browse_button = QPushButton("Browse")

        # Table for custom temperature program
        self.program_table = QTableWidget(0, 3)
        self.program_table.setHorizontalHeaderLabels(["Temperature (°C)", "Duration (HH:MM:SS)", "Pressure (bar)"])
        self.program_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.program_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.add_row_button = QPushButton("Add Row")
        self.remove_row_button = QPushButton("Remove Row")

        # Repetition parameters
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

        # Plot area
        self.figure = Figure(figsize=(5, 4))
        self.canvas = FigureCanvas(self.figure)
        self.toolbar = NavigationToolbar(self.canvas, self)

        self.pressure_figure = Figure(figsize=(5, 4))
        self.pressure_canvas = FigureCanvas(self.pressure_figure)
        self.pressure_toolbar = NavigationToolbar(self.pressure_canvas, self)

        plot_layout = QVBoxLayout()
        plot_layout.addWidget(self.toolbar)
        plot_layout.addWidget(self.canvas)
        plot_layout.addWidget(self.pressure_toolbar)
        plot_layout.addWidget(self.pressure_canvas)

        # Layouts
        mode_layout = QHBoxLayout()
        mode_layout.addWidget(self.mode_label)
        mode_layout.addWidget(self.mode_combo)
        mode_layout.addStretch()

        sleep_layout = QHBoxLayout()
        sleep_layout.addWidget(self.sleep_label)
        sleep_layout.addWidget(self.sleep_input)
        sleep_layout.addStretch()

        csv_layout = QHBoxLayout()
        csv_layout.addWidget(self.csv_path_label)
        csv_layout.addWidget(self.csv_path_display)
        csv_layout.addWidget(self.browse_button)

        program_buttons_layout = QHBoxLayout()
        program_buttons_layout.addWidget(self.add_row_button)
        program_buttons_layout.addWidget(self.remove_row_button)
        program_buttons_layout.addStretch()

        repetition_layout = QHBoxLayout()
        repetition_layout.addWidget(self.repeat_start_label)
        repetition_layout.addWidget(self.repeat_start_input)
        repetition_layout.addWidget(self.repeat_end_label)
        repetition_layout.addWidget(self.repeat_end_input)
        repetition_layout.addWidget(self.repeat_count_label)
        repetition_layout.addWidget(self.repeat_count_input)
        repetition_layout.addStretch()

        program_layout = QVBoxLayout()
        program_layout.addWidget(QLabel("Temperature Program:"))
        program_layout.addWidget(self.program_table)
        program_layout.addLayout(program_buttons_layout)
        program_layout.addLayout(repetition_layout)

        control_layout = QHBoxLayout()
        control_layout.addWidget(self.start_button)
        control_layout.addWidget(self.stop_button)
        control_layout.addStretch()

        left_layout = QVBoxLayout()
        left_layout.addLayout(mode_layout)
        left_layout.addLayout(sleep_layout)
        left_layout.addLayout(csv_layout)
        left_layout.addLayout(program_layout)
        left_layout.addLayout(control_layout)

        main_layout = QHBoxLayout()
        main_layout.addLayout(left_layout, 3)
        main_layout.addLayout(plot_layout, 2)

        self.setLayout(main_layout)

        path_test_data = r"/test_data/wae-wa-040-some-cycles"
        path_test_data = r"C:\Daten\Kiki\WAE-WA-028-MgFe3wt\WAE-WA-028-TundP-Verläufe"
        self.csv_path_display.setText(path_test_data)
        self.business.set_folder_path(path_test_data)
        self.highlighted_points = []
        self.highlighted_pressure_points = []
        self.ax = None
        self.ax_pressure = None
        self.lines = []
        self.lines_pressure = []

        # Connect signals and slots
        self.start_button.clicked.connect(self.start_server)
        self.stop_button.clicked.connect(self.stop_server)
        self.sleep_input.editingFinished.connect(self.update_sleep_interval)
        self.mode_combo.currentTextChanged.connect(self.update_mode)
        self.browse_button.clicked.connect(self.browse_csv)
        self.add_row_button.clicked.connect(self.add_program_row)
        self.remove_row_button.clicked.connect(self.remove_program_row)
        self.program_table.itemChanged.connect(self.update_plot)
        self.repeat_start_input.editingFinished.connect(self.update_plot)
        self.repeat_end_input.editingFinished.connect(self.update_plot)
        self.repeat_count_input.editingFinished.connect(self.update_plot)


        # Connect business signals to GUI slots
        self.business.plot_updated.connect(self.update_canvas)
        self.business.csv_data_received.connect(self.update_canvas_on_data)
        self.business.server_started.connect(self.on_server_started)
        self.business.server_stopped.connect(self.on_server_stopped)
        self.business.simulation_error.connect(self.on_simulation_error)
        self.business.point_to_highlight_received.connect(self.highlight_point)
        # Initialize UI state
        self.update_mode(self.mode_combo.currentText())
        self._create_input_widgets_manual_mode()
        # Add the manual input widget to the left layout
        left_layout.addWidget(self.manual_input_widget)

        # Set up QTimer with a 1000 ms (1 second) interval
        self.highlight_point_timer = QTimer(self)
        self.highlight_point_timer.setInterval(1000)  # Interval in milliseconds
        self.highlight_point_timer.timeout.connect(self.update_display)
        self.highlight_point_timer.start()

    def _create_input_widgets_manual_mode(self):
        self.manual_values = {}
        self.manual_input_widget = QWidget()
        manual_layout = QFormLayout()

        self.pressure_input = QLineEdit()
        self.temperature_sample_input = QLineEdit()
        self.setpoint_sample_input = QLineEdit()
        self.temperature_heater_input = QLineEdit()
        self.setpoint_heater_input = QLineEdit()

        manual_layout.addRow("Pressure:", self.pressure_input)
        manual_layout.addRow("Sample Temperature:", self.temperature_sample_input)
        manual_layout.addRow("Sample Setpoint:", self.setpoint_sample_input)
        manual_layout.addRow("Heater Temperature:", self.temperature_heater_input)
        manual_layout.addRow("Heater Setpoint:", self.setpoint_heater_input)

        self.manual_input_widget.setLayout(manual_layout)
        self.manual_input_widget.setVisible(False)  # Initially hidden
        # Inside __init__
        self.pressure_input.editingFinished.connect(lambda: self.update_manual_values("pressure"))
        self.temperature_sample_input.editingFinished.connect(lambda: self.update_manual_values("temperature_sample"))
        self.setpoint_sample_input.editingFinished.connect(lambda: self.update_manual_values("setpoint_sample"))
        self.temperature_heater_input.editingFinished.connect(lambda: self.update_manual_values("temperature_heater"))
        self.setpoint_heater_input.editingFinished.connect(lambda: self.update_manual_values("setpoint_heater"))

    @Slot()
    def start_server(self):
        mode = self.mode_combo.currentText()
        self.business.set_mode(mode)

        if mode == 'csv':
            if not self.business.server.folder_path:
                QMessageBox.warning(self, "CSV Path Not Set", "Please select a CSV file or folder.")
                return
        elif mode == 'tp_program':

            temperature_program = self.get_temperature_program()
            if not temperature_program:
                QMessageBox.warning(self, "Invalid Program", "Please enter a valid temperature program.")
                return
            # Get repetition parameters
            repeat_start, repeat_end, repeat_count = self.get_repetition_parameters()
            if repeat_start is None or repeat_end is None or repeat_count is None:
                QMessageBox.warning(self, "Invalid Repetition Parameters", "Please enter valid repetition parameters.")
                return

            self.business.set_temperature_program(temperature_program)
            self.business.set_repetition_parameters(repeat_start, repeat_end, repeat_count)
        elif mode == 'manual':
            pass

        self.business.set_sleep_interval(float(self.sleep_input.text()))

        self.business.start_server()
        self.start_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.mode_combo.setEnabled(False)
        self.sleep_input.setEnabled(True)
        self.browse_button.setEnabled(False)
        self.program_table.setEnabled(False)
        self.add_row_button.setEnabled(False)
        self.remove_row_button.setEnabled(False)
        self.repeat_start_input.setEnabled(False)
        self.repeat_end_input.setEnabled(False)
        self.repeat_count_input.setEnabled(False)

        #QMessageBox.information(self, "Server Started", "Modbus Server has been started.")

    @Slot()
    def stop_server(self):
        self.business.stop_server()
        self.start_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        self.mode_combo.setEnabled(True)
        self.sleep_input.setEnabled(True)
        self.update_mode(self.mode_combo.currentText())
        #QMessageBox.information(self, "Server Stopped", "Modbus Server has been stopped.")

    @Slot()
    def update_sleep_interval(self):
        try:
            interval = float(self.sleep_input.text())
            if interval <= 0:
                raise ValueError
            self.business.set_sleep_interval(interval)
        except ValueError:
            QMessageBox.warning(self, "Invalid Input", "Please enter a valid positive number for sleep interval.")
            self.sleep_input.setText(str(self.business.server.sleep_interval))

    @Slot()
    def update_mode(self, mode):
        if mode == 'csv':
            self.csv_path_label.setEnabled(True)
            self.csv_path_display.setEnabled(True)
            self.browse_button.setEnabled(True)
            self.program_table.setEnabled(False)
            self.add_row_button.setEnabled(False)
            self.remove_row_button.setEnabled(False)
            self.repeat_start_input.setEnabled(False)
            self.repeat_end_input.setEnabled(False)
            self.repeat_count_input.setEnabled(False)
            if hasattr(self, 'manual_input_widget'):
                self.manual_input_widget.setVisible(False)
            self.canvas.show()
            self.update_plot()

        elif mode == 'tp_program':
            self.csv_path_label.setEnabled(False)
            self.csv_path_display.setEnabled(False)
            self.browse_button.setEnabled(False)
            self.program_table.setEnabled(True)
            self.add_row_button.setEnabled(True)
            self.remove_row_button.setEnabled(True)
            self.repeat_start_input.setEnabled(True)
            self.repeat_end_input.setEnabled(True)
            self.repeat_count_input.setEnabled(True)
            if hasattr(self, 'manual_input_widget'):
                self.manual_input_widget.setVisible(False)
            self.canvas.show()
            self.update_plot()

        elif mode == 'manual':
            self.csv_path_label.setEnabled(False)
            self.csv_path_display.setEnabled(False)
            self.browse_button.setEnabled(False)
            self.program_table.setEnabled(False)
            self.add_row_button.setEnabled(False)
            self.remove_row_button.setEnabled(False)
            self.repeat_start_input.setEnabled(False)
            self.repeat_end_input.setEnabled(False)
            self.repeat_count_input.setEnabled(False)
            self.manual_input_widget.setVisible(True)
            self.canvas.hide()

        else:
            pass

    @Slot()
    def browse_csv(self):
        options = QFileDialog.Options()
        options |= QFileDialog.ReadOnly
        path = QFileDialog.getExistingDirectory(self, "Select CSV Directory", options=options)
        if path:
            self.csv_path_display.setText(path)
            self.business.set_folder_path(path)

    @Slot()
    def add_program_row(self):
        row_count = self.program_table.rowCount()
        self.program_table.insertRow(row_count)
        # Set default values
        self.program_table.setItem(row_count, 0, QTableWidgetItem("100"))
        self.program_table.setItem(row_count, 1, QTableWidgetItem("00:05:00"))
        self.program_table.setItem(row_count, 2, QTableWidgetItem("20"))
        if row_count > 1:
            self.update_plot()

    @Slot()
    def remove_program_row(self):
        selected_rows = set(item.row() for item in self.program_table.selectedItems())
        for row in sorted(selected_rows, reverse=True):
            self.program_table.removeRow(row)
        self.update_plot()

    def get_temperature_program(self):
        temperature_program = []
        for row in range(self.program_table.rowCount()):
            try:
                temp_item = self.program_table.item(row, 0)
                duration_item = self.program_table.item(row, 1)
                p_item = self.program_table.item(row, 2)

                if temp_item is None or duration_item is None or p_item is None:
                    return None
                temp = float(temp_item.text())
                duration = duration_item.text()
                p = float(p_item.text())
                if not self.validate_duration(duration):
                    return None
                temperature_program.append((temp, duration, p))
            except ValueError:
                return None
        return temperature_program

    def get_repetition_parameters(self):
        try:
            repeat_start = int(self.repeat_start_input.text())
            repeat_end = int(self.repeat_end_input.text())
            repeat_count = int(self.repeat_count_input.text())
            if repeat_start < 0 or repeat_end < 0 or repeat_count < 0:
                raise ValueError
            if repeat_start > repeat_end:
                raise ValueError
            return repeat_start, repeat_end, repeat_count
        except ValueError:
            return None, None, None

    def validate_duration(self, duration_str):
        try:
            h, m, s = map(int, duration_str.split(':'))
            return True
        except ValueError:
            return False

    @Slot()
    def update_plot(self):
        mode = self.mode_combo.currentText()
        if mode == 'tp_program':
            temperature_program = self.get_temperature_program()
            if not temperature_program:
                return

            # Get repetition parameters
            repeat_start, repeat_end, repeat_count = self.get_repetition_parameters()
            if repeat_start is None or repeat_end is None or repeat_count is None:
                return

            self.business.set_temperature_program(temperature_program)
            self.business.set_repetition_parameters(repeat_start, repeat_end, repeat_count)
            self.business.simulate_temperature_program()
        elif mode == "csv":
            return

    @Slot(Figure)
    def update_canvas(self, figure):
        self.figure = figure
        self.canvas.figure = figure
        self.canvas.draw()

    def update_canvas_on_data(self, df):
        #todo: minutes und hours für plots auch
        log_memory(logger, "Before updating plot")


        self.canvas.figure.clear()
        self.ax = self.canvas.figure.add_subplot(111)
        self.highlighted_points_scatter = None
        self.highlighted_points_scatter_pressure = None
        x_col = 'seconds'
        self.lines = []  # Reset the lines list
        for column in df.columns:
            if "temperature" in column or column == 'setpoint_sample':
                line, = self.ax.plot(df[x_col], df[column], linestyle='-', label=column)
                self.lines.append((line, df[x_col], df[column]))  # Store the line and data
        self.ax.set_xlabel('Time (HH:MM)')
        self.ax.set_ylabel('Temperature (°C)')
        self.ax.set_title('Temperature Program Simulation')
        self.ax.grid(True)
        self.ax.legend(loc='upper right')
        formatter = FuncFormatter(seconds_to_hours)
        self.ax.xaxis.set_major_formatter(formatter)

        self.canvas.draw()
        self.update_pressure_canvas_on_data(df)
        log_memory(logger, "After updating plot")
        del df
        log_memory(logger, "After updating plot and deleting df")

    @Slot(object)
    def update_pressure_canvas_on_data(self, df):
        # Clear the existing pressure figure
        self.pressure_figure.clear()
        self.ax_pressure = self.pressure_figure.add_subplot(111)
        x_col = 'seconds'

        # Loop through columns of the df and plot those related to pressure
        self.lines_pressure = []  # Reset the lines list
        for column in df.columns:
            if "pressure" in column.lower():
                line, = self.ax_pressure.plot(df[x_col], df[column], linestyle='-', label=column)
                self.lines_pressure.append((line, df[x_col], df[column]))  # Store the line and data

        self.ax_pressure.set_xlabel('Time (HH:MM)')
        self.ax_pressure.set_ylabel('Pressure (bar)')
        self.ax_pressure.set_title('Pressure Data')
        self.ax_pressure.grid(True)
        self.ax_pressure.legend(loc='upper right')
        formatter = FuncFormatter(seconds_to_hours)
        self.ax_pressure.xaxis.set_major_formatter(formatter)
        del df
        self.pressure_canvas.draw()

    @Slot(pd.Series)
    def highlight_point(self, rows: pd.Series):
        if self.ax is None or rows.empty:
            return

        x_value = rows['seconds']
        new_offsets = []
        new_offsets_pressure = []
        # Assume you're only highlighting one point per axis;
        # if you want to highlight multiple, build a list of [x,y] pairs.
        for col, value in rows.items():
            if "temperature" in col or col == 'setpoint_sample':
                new_offsets.append([x_value, value])

        for col, value in rows.items():
            if col == 'pressure':
                new_offsets_pressure.append([x_value, value])

        # If the scatter object doesn't exist yet, create it;
        # otherwise, update its data.
        if self.highlighted_points_scatter is None:
            # Create a single scatter plot for all highlight points.
            self.highlighted_points_scatter = self.ax.scatter(
                [pt[0] for pt in new_offsets],
                [pt[1] for pt in new_offsets],
                s=100, c='red', zorder=5
            )
        else:
            self.highlighted_points_scatter.set_offsets(new_offsets)

        if self.highlighted_points_scatter_pressure is None:
            # Create a single scatter plot for all highlight points.
            self.highlighted_points_scatter_pressure = self.ax_pressure.scatter(
                [pt[0] for pt in new_offsets_pressure],
                [pt[1] for pt in new_offsets_pressure],
                s=100, c='red', zorder=5
            )
        else:
            self.highlighted_points_scatter_pressure.set_offsets(new_offsets)
            self.highlighted_points_scatter_pressure.set_offsets(new_offsets_pressure)

    def update_display(self):
        self.canvas.draw()
        self.pressure_canvas.draw()

    @Slot(str)
    def on_simulation_error(self, message):
        QMessageBox.warning(self, "Simulation Error", message)

    @Slot(str, int)
    def on_server_started(self, host_ip, port):
        self.server_started.emit(host_ip, port)

    @Slot()
    def on_server_stopped(self):
        self.server_stopped.emit()

    def closeEvent(self, event):
        super().closeEvent(event)
        self.stop_server()

    def update_manual_values(self, value_name):
        value = None
        try:
            if value_name == 'pressure':
                value = float(self.pressure_input.text())
            if value_name == 'temperature_sample':
                value = float(self.temperature_sample_input.text())
            if value_name == 'setpoint_sample':
                value = float(self.setpoint_sample_input.text())
            if value_name == 'temperature_heater':
                value = float(self.temperature_heater_input.text())
            if value_name == 'setpoint_heater':
                value = float(self.setpoint_heater_input.text())

            self.manual_values[value_name] = value
            self.business.update_manual_values(self.manual_values)
        except ValueError:
            QMessageBox.warning(self, "Invalid Input", "Please enter valid numeric values.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ModbusServerControlGUI()
    window.show()
    sys.exit(app.exec())
