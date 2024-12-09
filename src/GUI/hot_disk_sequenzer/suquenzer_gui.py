import datetime
import threading
from zoneinfo import ZoneInfo

import pandas as pd
from PySide6.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QMessageBox, QComboBox, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QSizePolicy, QFormLayout, QGroupBox, QCompleter
)
from PySide6.QtGui import QIntValidator
from PySide6.QtCore import Signal, QDateTime
import pyqtgraph as pg

from src.simulation.dicon_simulator_v2 import TpProgramSimulator
from src.config_connection_reading_management.hot_disk_controller import HotDiskController

local_tz = ZoneInfo("Europe/Berlin")
standard_hot_disk_schedule_folder = r"C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\config\tps_schedules"


class ScheduleGeneratorBase(QWidget):
    complete_program_sig = Signal(pd.DataFrame)
    meas_times_sig = Signal(pd.DataFrame)
    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        # Create main horizontal layout
        self.main_layout = QHBoxLayout()

        # Left side layout (for controls)
        self.left_layout = QVBoxLayout()

        # Initialize UI components and add them to the left layout
        self._init_buttons()
        self._init_measurement_settings()
        self._init_sensor_settings()
        self._init_schedule_table()
        self._init_repetition_buttons()

        # Add the left layout to the main layout
        self.main_layout.addLayout(self.left_layout, stretch=1)

        # Right side layout (for the plot)
        self.right_layout = QVBoxLayout()
        self._init_plot_widget()  # Initialize the plot widget and add it to the right layout

        # Add the right layout to the main layout
        self.main_layout.addLayout(self.right_layout, stretch=2)

        # Set the main layout
        self.setLayout(self.main_layout)

    def _init_buttons(self):
        # Buttons
        self.add_measurement_button = QPushButton('Add Program Row')
        self.add_measurement_button.clicked.connect(self.add_program_row)
        self.left_layout.addWidget(self.add_measurement_button)

        self.generate_schedule_button = QPushButton('Generate Schedule')
        self.left_layout.addWidget(self.generate_schedule_button)

        self.plot_program_button = QPushButton('Plot Schedule')
        self.plot_program_button.clicked.connect(self.plot_program)
        self.left_layout.addWidget(self.plot_program_button)

    def _init_measurement_settings(self):
        # Measurement settings group
        measurement_group = QGroupBox("Measurement Settings")
        measurement_layout = QFormLayout()

        # start time temperature program program
        self.start_measurements_label = QLabel("Start time temperature program:")
        self.start_measurements_input = QLineEdit(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        self.start_measurements_input.setFixedWidth(200)
        measurement_layout.addRow(self.start_measurements_label, self.start_measurements_input)

        self.measurement_delay_label = QLabel("Start measurement xx min before next temperature is reached:")
        self.measurement_delay_input = QLineEdit(str(30))
        self.measurement_delay_input.setFixedWidth(50)
        measurement_layout.addRow(self.measurement_delay_label, self.measurement_delay_input)

        # Template Folder
        self.template_folder_label = QLabel("Template Folder:")
        self.template_folder_path = QLineEdit()
        self.template_folder_path.setText(standard_hot_disk_schedule_folder)
        self.template_folder_path.setFixedWidth(200)
        self.template_folder_browse_button = QPushButton("Browse")
        self.template_folder_browse_button.clicked.connect(self.browse_template_folder)
        template_folder_layout = QHBoxLayout()
        template_folder_layout.addWidget(self.template_folder_path)
        template_folder_layout.addWidget(self.template_folder_browse_button)
        measurement_layout.addRow(self.template_folder_label, template_folder_layout)

        measurement_group.setLayout(measurement_layout)

        self.left_layout.addWidget(measurement_group)

    def browse_template_folder(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        folder_path = QFileDialog.getExistingDirectory(self, "Select Template Folder", "", options=options)
        if folder_path:
            self.template_folder_path.setText(folder_path)

    def _init_schedule_table(self):
        # Measurement table
        self.program_table = QTableWidget()
        self.program_table.setColumnCount(4)  # Adjust column count
        self.program_table.setHorizontalHeaderLabels(['Temperature [°C]', 'Time [hh:mm:ss]', 'Measurement Power [W]', 'Measurement Time [s]'])
        header = self.program_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        self.left_layout.addWidget(self.program_table)
        self.program_table.insertRow(0)
        self.program_table.insertRow(1)
        self.program_table.insertRow(2)
        self.program_table.insertRow(3)
        self.program_table.setItem(0, 0, QTableWidgetItem('100'))
        self.program_table.setItem(0, 1, QTableWidgetItem('00:01:00'))
        self.program_table.setItem(0, 2, QTableWidgetItem('0.01'))
        self.program_table.setItem(0, 3, QTableWidgetItem('3'))
        self.program_table.setItem(1, 0, QTableWidgetItem('100'))
        self.program_table.setItem(1, 1, QTableWidgetItem('00:01:00'))
        self.program_table.setItem(1, 2, QTableWidgetItem('0.01'))
        self.program_table.setItem(1, 3, QTableWidgetItem('3'))
        self.program_table.setItem(2, 0, QTableWidgetItem('200'))
        self.program_table.setItem(2, 1, QTableWidgetItem('00:01:00'))
        self.program_table.setItem(2, 2, QTableWidgetItem('0.01'))
        self.program_table.setItem(2, 3, QTableWidgetItem('3'))
        self.program_table.setItem(3, 0, QTableWidgetItem('200'))
        self.program_table.setItem(3, 1, QTableWidgetItem('00:01:00'))
        self.program_table.setItem(3, 2, QTableWidgetItem('0.01'))
        self.program_table.setItem(3, 3, QTableWidgetItem('3'))

    def _init_repetition_buttons(self):
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
        sensor_type_suggestions = ["5465", "asdf"]
        sensor_insulation_suggestions = ["Mica", "Kapton", "Teflon"]
        insulation_completer = QCompleter(sensor_insulation_suggestions)
        type_completer = QCompleter(sensor_type_suggestions)
        sensor_setting_group= QGroupBox("Sensor Settings")
        sensor_setting_layout = QFormLayout()

        # start time program
        self.sensor_insulation_label = QLabel("Sensor insulation:")
        self.sensor_insulation_input = QLineEdit("Mica")
        self.sensor_insulation_input.setCompleter(insulation_completer)
        self.sensor_insulation_input.setFixedWidth(100)
        sensor_setting_layout.addRow(self.sensor_insulation_label, self.sensor_insulation_input)

        self.sensor_type_label = QLabel("Sensor type:")
        self.sensor_type_input = QLineEdit("5465")
        self.sensor_type_input.setCompleter(type_completer)
        self.sensor_type_input.setFixedWidth(100)
        sensor_setting_layout.addRow(self.sensor_type_label, self.sensor_type_input)

        sensor_setting_group.setLayout(sensor_setting_layout)
        self.left_layout.addWidget(sensor_setting_group)

    def _init_plot_widget(self):
        # Create a PlotWidget

        self.plot_widget = ProgramPlotWidget()
        self.plot_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.right_layout.addWidget(self.plot_widget)

    def add_program_row(self):
        # Add to measurement table
        row_position = self.program_table.rowCount()
        self.program_table.insertRow(row_position)
        self.program_table.setItem(row_position, 0, QTableWidgetItem('00'))
        self.program_table.setItem(row_position, 1, QTableWidgetItem('00:10:00'))

    @staticmethod
    def validate_duration(duration_str):
        try:
            h, m, s = map(int, duration_str.split(':'))
            return True
        except ValueError:
            return False

    def plot_program(self, program):
        self.plot_widget.update_plot(df=program)

    def plot_meas_times(self, program):
        self.plot_widget.update_scatter_plot(df=program)


class ProgramPlotWidget(pg.PlotWidget):
    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.setWindowTitle("Temperature Program")
        self.scatter_plot = None  # Initialize scatter plot item
        self.line_plot = None     # Initialize line plot item

        # Call axis initialization method if needed
        self._init_left_axis(y_axis="Temperature")

    def _init_left_axis(self, y_axis):
        x_axis = DateAxisItem(orientation='bottom')
        self.plotItem.setAxisItems({'bottom': x_axis})
        x_axis_label = "Time"  # Simplified for this example
        self.plotItem.getAxis('bottom').setLabel(x_axis_label)
        self.plotItem.addLegend(offset=(0, 1))
        y_axis_label = y_axis
        self.plotItem.getAxis('left').setLabel(y_axis_label)
        # Assuming you have methods to set fonts; else, you can omit these
        # self._set_tick_fonts(self.plotItem.getAxis('bottom'))
        # self._set_tick_fonts(self.plotItem.getAxis('left'))

    def update_plot(self, df):
        if df.empty:
            return
        x = [t.timestamp() for t in df['end_time']]
        y = df['temperature']

        if self.line_plot is None:
            # Create a new line plot item
            self.line_plot = self.plotItem.plot(
                x=x,
                y=y,
                name="Temperature",
                pen=pg.mkPen(color="#FF0000", width=2)  # Red
            )
        else:
            # Update existing line plot data
            self.line_plot.setData(x=x, y=y)

    def update_scatter_plot(self, df):
        if df.empty:
            return
        x = [t.timestamp() for t in df['meas_time']]
        y = df['temperature']

        if self.scatter_plot is None:
            # Create a new scatter plot item
            self.scatter_plot = pg.ScatterPlotItem(
                x=x,
                y=y,
                symbol='o',
                size=8,
                pen=pg.mkPen(color='b'),
                brush=pg.mkBrush(color='b'),
                name="Measurements"
            )
            self.plotItem.addItem(self.scatter_plot)
        else:
            # Update existing scatter plot data
            self.scatter_plot.setData(x=x, y=y)


class ScheduleGeneratorMain(ScheduleGeneratorBase):

    def __init__(self):
        super().__init__()
        self.generate_schedule_button.clicked.connect(self.generate_schedule)
        self.complete_program_sig.connect(self.plot_program)
        self.meas_times_sig.connect(self.plot_meas_times)
        self.repeat_end_input.editingFinished.connect(self.parse_program)
        self.repeat_start_input.editingFinished.connect(self.parse_program)
        self.repeat_count_input.editingFinished.connect(self.parse_program)
        self.program_table.cellChanged.connect(self.parse_program)

    def generate_schedule(self):

        sensor_type = self.sensor_type_input.text()
        sensor_insulation = self.sensor_insulation_input.text()
        folder_path = self.template_folder_path.text()

        scheduled_program = self.parse_program()

        scheduled_program = scheduled_program.rename(columns={'measurement_time' : 'heating_time', 'measurement_power_watt' : 'heating_power'})
        scheduled_program['heating_power'] = scheduled_program['heating_power'] * 1e3
        scheduled_dict_list = scheduled_program.to_dict(orient='records')

        self.hot_disk_controller = HotDiskController(sensor_type=sensor_type, sensor_insulation=sensor_insulation, template_folder_path=folder_path)
        self.hot_disk_controller_thread = threading.Thread(target=self.hot_disk_controller.run, args=(scheduled_dict_list,), daemon=True)
        self.hot_disk_controller_thread.start()

        #import pprint
        #pprint.pprint(scheduled_dict_list)

    def get_temperature_program(self):
        temperature_program = []
        for row in range(self.program_table.rowCount()):
            try:
                temp_item = self.program_table.item(row, 0)
                duration_item = self.program_table.item(row, 1)
                meas_power_item = self.program_table.item(row, 2)
                meas_time_item = self.program_table.item(row, 3)

                if temp_item is None or duration_item is None:
                    return None
                temp = float(temp_item.text())
                duration = duration_item.text()
                meas_power = float(meas_power_item.text()) if meas_power_item else None
                meas_time = float(meas_time_item.text()) if meas_time_item else None

                if not self.validate_duration(duration):
                    return None
                temperature_program.append((temp, duration, meas_power, meas_time))
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
            repeat_start = repeat_start if repeat_start > 0 else None
            repeat_end = repeat_end if repeat_end > 0 else None
            return repeat_start, repeat_end, repeat_count
        except ValueError:
            return None, None, None

    def parse_program(self):
        time_delay = float(self.measurement_delay_input.text())
        #
        #time_delay = datetime.timedelta(minutes=float(inputfieldasdfasdf))
        time_delay = datetime.timedelta(minutes=time_delay)
        program = self.get_temperature_program()
        repeat_start, repeat_end, repeat_count = self.get_repetition_parameters()
        temperature_controller = TpProgramSimulator().TemperatureController(
                                                                            temperature_program=program,
                                                                            repeat_start=repeat_start,
                                                                            repeat_end=repeat_end,
                                                                            repeat_count=repeat_count
                                                                            )

        start_time = datetime.datetime.strptime(self.start_measurements_input.text(), "%Y-%m-%d %H:%M:%S")
        start_time.replace(tzinfo=local_tz)

        program_with_meas_times, complete_program = temperature_controller.get_program_times(start_time=start_time)
        program_with_meas_times['meas_time'] = program_with_meas_times['end_time'] - time_delay

        measurement_time_xy = program_with_meas_times[['meas_time', 'temperature']].copy()

        self.complete_program_sig.emit(complete_program)
        self.meas_times_sig.emit(measurement_time_xy)

        return program_with_meas_times

    def closeEvent(self, event):
        if hasattr(self, "hot_disk_controller_thread"):
            self.hot_disk_controller.end()
            self.hot_disk_controller_thread.join(timeout=2)
        super().closeEvent(event)


class DateAxisItem(pg.AxisItem):
    def tickStrings(self, values, scale, spacing):
        if not values:
            return []
        span = max(values) - min(values)
        if span < 3600:  # less than an hour
            fmt = "HH:mm:ss"
        elif span < 86400:  # less than a day
            fmt = "HH:mm"
        else:
            fmt = "yyyy MM dd"
        tick_labels = []
        for value in values:
            qdt = QDateTime.fromSecsSinceEpoch(int(value))
            tick_labels.append(qdt.toString(fmt))
        return tick_labels


class AxisLabel:

    @staticmethod
    def create_axis_label(column_name):
        """
        :param column_name: (str) name of the parameter that is plotted
        :return: axis title str
        """
        if "temperature" in column_name.lower():
            unit_str = "°C"
            variable_name = "Temperature"
        elif "pressure" in column_name.lower():
            unit_str = "bar"
            variable_name = "Pressure"
        elif "conductivity" in column_name.lower():
            unit_str = "Wm⁻¹K⁻¹"
            variable_name = "ETC"
        elif "time" in column_name.lower():
            unit_str = "Y-M-D H:M:s"
            variable_name = "Time"
        elif "uptake" in column_name.lower():
            unit_str = 'wt-%'
            variable_name = 'Capacity'
        elif "cycle" in column_name.lower():
            unit_str = '#'
            variable_name = 'Cycle Number'
        else:
            variable_name = ""
            unit_str = " "  # Default unit if neither temperature nor pressure

        return f"{variable_name} ({unit_str})"




if __name__ == '__main__':
    app = QApplication([])
    window = ScheduleGeneratorMain()
    window.show()
    app.exec()
