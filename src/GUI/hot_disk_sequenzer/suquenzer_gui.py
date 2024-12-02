import datetime
from PySide6.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QMessageBox, QComboBox, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QSizePolicy, QFormLayout, QGroupBox
)
from PySide6.QtGui import QIntValidator

from src.simulation.dicon_simulator_v2 import TpProgramSimulator
from src.GUI.hot_disk_sequenzer.sequenzer import ScheduleCreator


class ScheduleGeneratorBase(QWidget):

    def __init__(self):
        super().__init__()
        self.init_ui()

    def init_ui(self):
        # Create UI elements
        self.layout = QVBoxLayout()

        # Add more input fields as needed
        self._init_buttons()
        self._init_measurement_settings()  # Initialize measurement settings
        self._init_schedule_table()
        self._init_repetition_buttons()
        self.setLayout(self.layout)

    def _init_buttons(self):
        # Buttons
        self.add_measurement_button = QPushButton('Add Program Row')
        self.add_measurement_button.clicked.connect(self.add_program_row)
        self.layout.addWidget(self.add_measurement_button)

        self.generate_schedule_button = QPushButton('Generate Schedule')
        self.layout.addWidget(self.generate_schedule_button)

        self.plot_program_button = QPushButton('Plot Schedule')
        self.plot_program_button.clicked.connect(self.plot_program)
        self.layout.addWidget(self.plot_program_button)

    def _init_measurement_settings(self):
        # Measurement settings group
        measurement_group = QGroupBox("Measurement Settings")
        measurement_layout = QFormLayout()

        # start time program
        self.start_measurements_label = QLabel("Start time temperature program:")
        self.start_measurements_input = QLineEdit(str(datetime.datetime.now()))
        self.start_measurements_input.setFixedWidth(300)
        measurement_layout.addRow(self.start_measurements_label, self.start_measurements_input)


        # Number of Measurements
        self.num_measurements_label = QLabel("Number of Measurements:")
        self.num_measurements_input = QLineEdit("3")
        self.num_measurements_input.setValidator(QIntValidator(1, 1000))
        self.num_measurements_input.setFixedWidth(50)
        measurement_layout.addRow(self.num_measurements_label, self.num_measurements_input)

        # Measurement Interval
        self.measurement_interval_label = QLabel("Measurement Interval (min):")
        self.measurement_interval_input = QLineEdit("3")
        self.measurement_interval_input.setValidator(QIntValidator(1, 1000))
        self.measurement_interval_input.setFixedWidth(50)
        measurement_layout.addRow(self.measurement_interval_label, self.measurement_interval_input)

        # Template File
        self.template_file_label = QLabel("Template File:")
        self.template_file_path = QLineEdit()
        self.template_file_path.setText(r"C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\src\GUI\hot_disk_sequenzer\template.hseq")
        self.template_file_path.setFixedWidth(600)
        self.template_file_browse_button = QPushButton("Browse")
        self.template_file_browse_button.clicked.connect(self.browse_template_file)
        template_file_layout = QHBoxLayout()
        template_file_layout.addWidget(self.template_file_path)
        template_file_layout.addWidget(self.template_file_browse_button)
        measurement_layout.addRow(self.template_file_label, template_file_layout)

        measurement_group.setLayout(measurement_layout)

        self.layout.addWidget(measurement_group)

    def browse_template_file(self):
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_name, _ = QFileDialog.getOpenFileName(self, "Select Template File", "",
                                                   "XML Files (*.xml);;All Files (*)", options=options)
        if file_name:
            self.template_file_path.setText(file_name)

    def _init_schedule_table(self):
        # Measurement table
        self.program_table = QTableWidget()
        self.program_table.setColumnCount(4)  # Adjust column count
        self.program_table.setHorizontalHeaderLabels(['Temperature [°C]', 'Time [hh:mm:ss]', 'Measurement Power [W]', 'Measurement Time [s]'])
        header = self.program_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeToContents)
        self.layout.addWidget(self.program_table)
        self.program_table.insertRow(0)
        self.program_table.insertRow(1)
        self.program_table.insertRow(2)
        self.program_table.insertRow(3)
        self.program_table.setItem(0, 0, QTableWidgetItem('100'))
        self.program_table.setItem(0, 1, QTableWidgetItem('00:10:00'))
        self.program_table.setItem(0, 2, QTableWidgetItem('0.01'))
        self.program_table.setItem(0, 3, QTableWidgetItem('3'))
        self.program_table.setItem(1, 0, QTableWidgetItem('100'))
        self.program_table.setItem(1, 1, QTableWidgetItem('00:10:00'))
        self.program_table.setItem(1, 2, QTableWidgetItem('0.01'))
        self.program_table.setItem(1, 3, QTableWidgetItem('3'))
        self.program_table.setItem(2, 0, QTableWidgetItem('200'))
        self.program_table.setItem(2, 1, QTableWidgetItem('00:10:00'))
        self.program_table.setItem(2, 2, QTableWidgetItem('0.01'))
        self.program_table.setItem(2, 3, QTableWidgetItem('3'))
        self.program_table.setItem(3, 0, QTableWidgetItem('200'))
        self.program_table.setItem(3, 1, QTableWidgetItem('00:10:00'))
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
        self.layout.addLayout(repetition_layout)

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

    def plot_program(self):
        pass


class ScheduleGeneratorMain(ScheduleGeneratorBase):

    def __init__(self):
        super().__init__()
        self.generate_schedule_button.clicked.connect(self.generate_schedule)

    def generate_schedule(self):

        #todo: change to schedule executor (run) from hot_disk_controller
        #
        scheduled_program = self.parse_program()
        scheduled_program_unique_temperatures = scheduled_program.drop_duplicates(subset=['temperature'], keep='last')
        temperatures = scheduled_program_unique_temperatures['temperature'].tolist()
        heating_powers = {}
        meas_times = {}

        for index, row in scheduled_program_unique_temperatures.iterrows():
            heating_powers[row['temperature']] = row['measurement_power_watt']
            meas_times[row['temperature']] = row['measurement_time']

        # Get the number of measurements, measurement interval, and template file
        num_measurements = int(self.num_measurements_input.text())
        measurement_interval = int(self.measurement_interval_input.text())
        template_file = self.template_file_path.text()


        # Initialize ScheduleCreator with the retrieved values
        schedule_creator = ScheduleCreator(
            sample_temperatures=temperatures,
            heating_powers=heating_powers,
            heating_times=meas_times,
            no_of_measurements=num_measurements,
            measurement_interval=measurement_interval,
            template_file=template_file
        )

        schedule_creator.create_schedule(schedule_df=scheduled_program, output_file="test.hseq")

        print(heating_powers)
        print(scheduled_program)

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
            return repeat_start, repeat_end, repeat_count
        except ValueError:
            return None, None, None

    def parse_program(self):
        program = self.get_temperature_program()
        repeat_start, repeat_end, repeat_count = self.get_repetition_parameters()
        temperature_controller = TpProgramSimulator().TemperatureController(
            temperature_program=program,
            repeat_start=repeat_start,
            repeat_end=repeat_end,
            repeat_count=repeat_count
        )
        start_time = datetime.datetime.strptime(self.start_measurements_input.text(), "%Y-%m-%d %H:%M:%S.%f")  # self.program_start_time

        program_with_meas_times = temperature_controller.get_program_times(start_time=start_time)
        return program_with_meas_times


if __name__ == '__main__':
    app = QApplication([])
    window = ScheduleGeneratorMain()
    window.show()
    app.exec()
