import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
import re
from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from PySide6.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QMessageBox, QComboBox, QFileDialog, QTableWidget, QTableWidgetItem,
    QHeaderView, QSizePolicy, QFormLayout, QGroupBox
)
from PySide6.QtGui import QIntValidator


def parse_duration(duration_str):
    if isinstance(duration_str, timedelta):
        return duration_str
    else:
        hours, minutes, seconds = map(int, duration_str.split(':'))
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def format_duration(duration_td):
    total_seconds = int(duration_td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours:02}:{minutes:02}:{seconds:02}'


def combine_consecutive_temperatures(data):
    """combines program steps returns pd.DataFrame
    :returns pd.DataFrame cols  = 'temperature', 'duration', optional : 'measurement_power_watt', optional : 'measurement_time'"""
    if not data:
        return []

    result = []
    df_program = pd.DataFrame()

    if len(data[0]) == 3:
        # Initialize with the first entry
        current_temp, current_duration_str, current_param = data[0]
        current_duration = parse_duration(current_duration_str)
        for values in data[1:]:
            temp = values[0]
            duration_str = values[1]
            param = values[2]
            if temp == current_temp:
                # Sum the durations
                current_duration += parse_duration(duration_str)
            else:
                # Append the accumulated result
                result.append([current_temp, current_duration, current_param])
                # Reset accumulators for the new temperature
                current_temp = temp
                current_duration = parse_duration(duration_str)
                current_param = param
        # Append the last accumulated result
        result.append([current_temp, current_duration, current_param])
        df_program = pd.DataFrame(result, columns=['temperature', 'duration', 'pressure'])

        return df_program
    elif len(data[0]) == 2:
        # Initialize with the first entry
        current_temp = data[0][0]
        current_duration_str = data[0][1]
        current_duration = parse_duration(current_duration_str)

        for values in data[1:]:
            temp = values[0]
            duration_str = values[1]

            if temp == current_temp:
                # Sum the durations
                current_duration += parse_duration(duration_str)
            else:
                # Append the accumulated result
                result.append([current_temp, current_duration])
                # Reset accumulators for the new temperature
                current_temp = temp
                current_duration = parse_duration(duration_str)

        # Append the last accumulated result
        result.append([current_temp, current_duration])
        df_program = pd.DataFrame(result, columns=['temperature', 'duration'])

        return df_program
    elif len(data[0]) > 3:
         # Initialize with the first entry
        current_temp = data[0][0]
        current_duration_str = data[0][1]
        current_duration = parse_duration(current_duration_str)
        current_meas_power = data[0][2]
        current_meas_time = data[0][3]


        for values in data[1:]:
            temp = values[0]
            duration_str = values[1]
            meas_power = values[2]
            meas_time = values[3]

            if temp == current_temp:
                # Sum the durations
                current_duration += parse_duration(duration_str)
            else:
                # Append the accumulated result
                result.append([current_temp, current_duration, current_meas_power, current_meas_time])
                # Reset accumulators for the new temperature
                current_temp = temp
                current_duration = parse_duration(duration_str)
                current_meas_power = meas_power
                current_meas_time = meas_time

        # Append the last accumulated result
        result.append([current_temp, current_duration, current_meas_power, current_meas_time])
        df_program = pd.DataFrame(result, columns=['temperature', 'duration', 'measurement_power_watt', 'measurement_time'])
        return df_program


class TpProgramSimulator:
    class TPProgram:
        def __init__(self):
            pass
            #self.logger = AppLogger().get_logger(__name__)

        def parse_duration(self, duration_str):
            """Converts duration string 'HH:MM:SS' to total seconds."""
            if isinstance(duration_str, timedelta):
                return duration_str.total_seconds()
            else:
                h, m, s = map(int, duration_str.split(':'))
                return h * 3600 + m * 60 + s

        def simulate_program(self, program, repeat_start=None, repeat_end=None,
                                repeat_count=0):
            current_time = 0


            total_program = self.total_program(program=program,
                                               repeat_start=repeat_start,
                                               repeat_count=repeat_count,
                                               repeat_end=repeat_end)

            for i in range(len(total_program) - 1):
                if len(total_program[i]) == 2:
                    start_temp, duration = total_program[i]
                    end_temp, _ = total_program[i + 1]
                    start_p = 0
                    end_p = 0
                elif len(total_program[i]) == 3:
                    start_temp, duration, start_p = total_program[i]
                    end_temp, _, end_p = total_program[i + 1]

                duration_seconds = self.parse_duration(duration)
                for second in range(duration_seconds):
                    interpol_factor = (second / duration_seconds)
                    t_interpolated = (end_temp - start_temp) * interpol_factor
                    temp = start_temp + t_interpolated
                    p_interpolated = (end_p - start_p) * interpol_factor
                    p = start_p + p_interpolated

                    yield current_time, temp, p

                    current_time += 1

            # Handle the last segment


            if len(total_program[-1]) == 2:
                last_temp, duration = total_program[-1]
                duration_seconds = self.parse_duration(duration)
                last_p = 0
            elif len(total_program[-1]) == 3:
                last_temp, duration, last_p = total_program[-1]
                duration_seconds = self.parse_duration(duration)

            for second in range(duration_seconds):
                yield current_time, last_temp, last_p
                current_time += 1

        @staticmethod
        def total_program(program, repeat_start=None, repeat_count=None, repeat_end=None):
            """returns total programm"""
            repeat_segments = []
            if repeat_start is not None and repeat_end is not None:
                repeat_segments = program[repeat_start - 1 : repeat_end]

            t_program = (
                        program[:repeat_start - 1] +
                        repeat_segments * (repeat_count + 1) +
                        program[repeat_end:]
                        if repeat_start is not None and repeat_end is not None
                        else program
                        )
            return t_program

    class TemperatureController:
        def __init__(self, temperature_program=None, repeat_start=None,
                     repeat_end=None, repeat_count=0):
            self.program = TpProgramSimulator.TPProgram()
            #self.logger = AppLogger().get_logger(__name__)

            if temperature_program is None:
                # Default temperature program
                self.temperature_program = [
                    (30, "00:05:00", 20),      # 0
                    (250, "00:05:00", 20),     # 1
                    (250, "00:05:00", 20),     # 2
                    (350, "00:05:00", 10),     # 3
                    (350, "00:02:00", 10),     # 4
                    (400, "00:05:00", 20),     # 5
                    (400, "00:02:00", 20),     # 6
                    (30, "00:01:00", 10)       # 7
                ]
            else:

                self.temperature_program = temperature_program
            self.repeat_start = repeat_start
            self.repeat_end = repeat_end
            self.repeat_count = repeat_count
            self.simulation_generator = self.program.simulate_program(
                self.temperature_program, self.repeat_start,
                self.repeat_end, self.repeat_count)

        def get_next_value(self):
            try:
                current_time, temp, p = next(self.simulation_generator)
                return current_time, temp, p
            except StopIteration:
                #self.logger.info("Temperature program finished")
                return None, None, None

        def simulate_whole_program(self):
            self.simulation_generator = self.program.simulate_program(
                self.temperature_program, self.repeat_start,
                self.repeat_end, self.repeat_count)

            # Collect data for plotting
            times = []
            temps = []
            pressures = []
            while True:
                current_time, temp, p = self.get_next_value()
                if current_time is None:
                   # self.logger.info("Temperature program finished.")
                    break
                times.append(current_time)
                temps.append(temp)
                pressures.append(p)
            return times, temps, pressures

        def plot_program(self):
            # Reset the simulation generator

            times, temps, pressures = self.simulate_whole_program()
            fig, ax1 = plt.subplots()
            ax1.plot(times, temps, label='Temperature Profile', marker='o')
            ax1.set_xlabel('Time (seconds)')
            ax1.set_ylabel('Temperature (°C)')

            ax1.grid(True)
            fig.legend()
            ax2 = ax1.twinx()
            ax2.set_ylabel('Pressure (bar)')
            ax2.plot(times, pressures, label='Pressure')
            plt.show()

        def get_program_times(self, start_time: datetime):
            """returns temperatures pressures and times as a list.
            can be used for scheduling measurement times in hotdisk software"""
            total_program = self.program.total_program(program=self.temperature_program,
                                                       repeat_start=self.repeat_start,
                                                       repeat_end=self.repeat_end,
                                                       repeat_count=self.repeat_count)
            compressed_program = combine_consecutive_temperatures(data=total_program)

            end_times = []

            for index, row in compressed_program.iterrows():
                end_time = start_time + row["duration"]
                end_times.append(end_time)
                start_time = end_time

            compressed_program['end_time'] = end_times
            return compressed_program


class ScheduleCreator:
    def __init__(self,
                 no_of_measurements=3,      # no of measurement when measurement event is triggered
                 measurement_interval=3,    # waiting time in min between two scheduled measurements
                 sample_temperatures=None,  # measurement temperatures from heating program
                 heating_powers=None,       # heating powers chosen in temperature program
                 heating_times=None,        # heating times chosen in temperature program
                 template_file='template_schedule.xml',  # Path to your XML template
                 **kwargs):
        """
        Initializes the ScheduleCreator with dynamic measurement parameters.
        """
        # Default to empty lists/dicts if None
        self.sample_temperatures = sample_temperatures or []

        self.heating_powers = heating_powers or {}
        self.heating_times = heating_times or {}
        self.NPLC = self._calculate_nplc(self.heating_times)
        # Measurement parameters
        self.measurement_params = {
            'NoOfMeasurements': no_of_measurements,
            'MeasurementInterval': measurement_interval,
            'SampleTemperature': self.sample_temperatures,
            'TCR': None,
            'HeatingPower': self.heating_powers,
            'HeatingTime': self.heating_times,
            'NPLC': self.NPLC,
        }
        self.template_file = template_file

    def create_schedule(self, schedule_df: pd.DataFrame, output_file):
        """
        Creates the measurement schedule XML based on the template and schedule DataFrame.

        Parameters:
        - schedule_df (pd.DataFrame): DataFrame containing 'end_time' and 'temperature' columns.
        - output_file (str): Path to the output XML file.
        """
        # Parse the XML template
        tree = ET.parse(self.template_file)
        root = tree.getroot()

        # Find the ActivityList element
        activity_list = root.find('ActivityList')
        if activity_list is None:
            raise ValueError("The template XML does not contain an 'ActivityList' element.")

        sample_temp_data = self._get_measurement_params(root)
        run_config = self._get_run_config(root)
        instr_config = self._get_instr_config(root)
        hardware_data = self._get_hardware_data(root)

        tcr_file_path = sample_temp_data.find('TcrFile').text
        disk_type = instr_config.find("DiskType").text
        sensor_type = instr_config.find("SensorType").text
        sensor_desing = instr_config.find("SensorDesign").text
        sensor_dimension = instr_config.find("SensorDimension").text
        sensor_batch_id = instr_config.find("SensorBatchID").text
        cable_or_holder = instr_config.find("CableOrHolder").text
        scanning_voltage = instr_config.find("ScanningVoltage").text

        hot_disk_model = hardware_data.find("HotDiskAnalyzerModel").text
        power_line_freq = hardware_data.find("PowerLineFrequency").text
        reference_resistance = hardware_data.find("ReferenceResistance").text

        method = run_config.find("MethodType").text
        original_file = run_config.find("OriginalFile").text
        software_version = run_config.find("SoftwareVersion").text

        sample_identity = run_config.find("SampleIdentity")
        identity = sample_identity.find("Identity").text
        probing_depth = sample_identity.find("AvailableProbingDepth").text
        notes = sample_identity.find("Notes").text

        self.TCR = self._calculate_tcr(temperatures=self.measurement_params['SampleTemperature'],
                                       tcr_file=tcr_file_path)

        for index, row in schedule_df.iterrows():
            time_of_measurement = row['end_time']
            temp = row['temperature']

            # Create ScheduledTime Activity
            sched_time = ET.SubElement(activity_list, 'ScheduleActivity', type='ScheduledTime')
            ET.SubElement(sched_time, 'targetTime').text = time_of_measurement.strftime('%Y-%m-%dT%H:%M:%S.%f0')

            # Create MeasurementTriggerEvent Activity
            measurement_event = ET.SubElement(activity_list, 'ScheduleActivity', type='MeasurementTriggerEvent')
            ET.SubElement(measurement_event, 'NoOfMeasurements').text = str(self.measurement_params['NoOfMeasurements'])
            ET.SubElement(measurement_event, 'MeasurementInterval').text = str(self.measurement_params['MeasurementInterval'])
            ET.SubElement(measurement_event, 'DoWaitWhenSwitchingSensor').text = 'false'

            # ExperimentSettingsBySwitchPort
            exp_settings = ET.SubElement(measurement_event, 'ExperimentSettingsBySwitchPort')
            executed_step = ET.SubElement(exp_settings, 'ExecutedStep', type='ExecutedExperiment')
            ET.SubElement(executed_step, 'RowNumber').text = '0'
            ET.SubElement(executed_step, 'LastStatus').text = 'NotRun'
            data = ET.SubElement(executed_step, 'Data')
            run_config = ET.SubElement(data, 'RunConfiguration')
            ET.SubElement(run_config, 'MethodType').text = method

            # SampleIdentity
            sample_identity = ET.SubElement(run_config, 'SampleIdentity')
            ET.SubElement(sample_identity, 'Identity').text = identity
            ET.SubElement(sample_identity, 'AvailableProbingDepth').text = probing_depth
            ET.SubElement(sample_identity, 'Notes').text = notes

            # InstrumentRunConfigurations
            instr_config = ET.SubElement(run_config, 'InstrumentRunConfigurations', type='StandardRunConfigurations')

            # SampleTemperatureData
            temp_data = ET.SubElement(instr_config, 'SampleTemperatureData')
            ET.SubElement(temp_data, 'Sample').text = str(temp)
            ET.SubElement(temp_data, 'Source').text = 'ManualTemp'
            ET.SubElement(temp_data, 'Manual').text = str(temp)

            ET.SubElement(temp_data, 'TCR').text = str(self.TCR.get(temp, 0.0))
            ET.SubElement(temp_data, 'TcrFile').text = tcr_file_path

            # Other instrument settings (fixed)
            ET.SubElement(instr_config, 'DiskType').text = disk_type
            ET.SubElement(instr_config, 'SensorType').text = sensor_type
            ET.SubElement(instr_config, 'SensorDesign').text = sensor_desing
            ET.SubElement(instr_config, 'SensorDimension').text = sensor_dimension
            ET.SubElement(instr_config, 'SensorBatchID').text = sensor_batch_id
            ET.SubElement(instr_config, 'CableOrHolder').text = cable_or_holder

            # Dynamic instrument settings
            heating_time = self.heating_times.get(temp, 0.0)
            ET.SubElement(instr_config, 'HeatingPower').text = str(self.heating_powers.get(temp, 0.0))
            ET.SubElement(instr_config, 'HeatingTime').text = str(heating_time)
            ET.SubElement(instr_config, 'NPLC').text = str(self.NPLC.get(heating_time, 0.0))
            ET.SubElement(instr_config, 'DriftEnable').text = 'true'
            ET.SubElement(instr_config, 'DriftTime').text = '40'
            ET.SubElement(instr_config, 'ScanningVoltage').text = scanning_voltage

            # ExperimentHardware (fixed)
            exp_hardware = ET.SubElement(run_config, 'ExperimentHardware')
            ET.SubElement(exp_hardware, 'HotDiskAnalyzerModel').text = hot_disk_model
            ET.SubElement(exp_hardware, 'PowerLineFrequency').text = power_line_freq
            ET.SubElement(exp_hardware, 'ReferenceResistance').text = reference_resistance

            # OriginalFile and SoftwareVersion (fixed)
            ET.SubElement(run_config, 'OriginalFile').text = original_file
            ET.SubElement(run_config, 'SoftwareVersion').text = software_version
            # FileName (fixed)
            ET.SubElement(executed_step, 'FileName')

            # ScheduledCalculation (fixed parameters)
            calc_activity = ET.SubElement(activity_list, 'ScheduleActivity', type='ScheduledCalculation')
            calc_settings = ET.SubElement(calc_activity, 'CalculationSettingsBySwitchPort')
            calc = ET.SubElement(calc_settings, 'CalcSettings')
            ET.SubElement(calc, 'selectionStartIndex').text = '20'
            ET.SubElement(calc, 'selectionEndIndex').text = '200'
            ET.SubElement(calc, 'specHeatCapOfSensor').text = '0.0111866931'
            ET.SubElement(calc, 'specificHeatOfSample').text = '1E-06'
            ET.SubElement(calc, 'specificHeatOfSampleKnown').text = 'false'
            ET.SubElement(calc, 'specHeatCapSensorCal').text = 'true'
            ET.SubElement(calc, 'UseDefaultSpecHeatCapSensor').text = 'true'
            ET.SubElement(calc, 'timeCorrection').text = 'true'
            ET.SubElement(calc, 'analysisType').text = 'Standard'
            ET.SubElement(calc, 'SwitchPort').text = '-1'

        # Indent the tree (Python 3.9+)
        ET.indent(root, space='    ', level=0)

        # Write the XML to file
        tree.write(output_file, encoding='utf-8', xml_declaration=True)

    @staticmethod
    def _create_root():
        root = ET.Element('ScheduleSettings')
        root.text = ('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                     'xmlns:xsd="http://www.w3.org/2001/XMLSchema" \n')

        # Add basic settings (modify as needed)
        ET.SubElement(root, 'BasicPreMeasurmentDelay').text = '0'
        ET.SubElement(root, 'BasicMeasurementInterval').text = '10'
        ET.SubElement(root, 'BasicNoOfMeasurements').text = '5'
        ET.SubElement(root, 'StandbyTemp').text = '-1'
        ET.SubElement(root, 'StandbyVar').text = '-1'
        ET.SubElement(root, 'ControllerStandbyMode').text = 'Undefined'
        ET.SubElement(root, 'BasicPreMeasurmentDelay').text = '0'
        return root

    @staticmethod
    def _get_run_config(root):
        for activity in root.findall(".//ScheduleActivity[@type='MeasurementTriggerEvent']"):
            run_config = activity.find('.//RunConfiguration')
            if run_config is not None:
                return run_config

    @staticmethod
    def _get_measurement_params(root):
        for activity in root.findall(".//ScheduleActivity[@type='MeasurementTriggerEvent']"):
            run_config = activity.find('.//RunConfiguration')
            instr_config = run_config.find('InstrumentRunConfigurations')
            sample_temp_data = instr_config.find('SampleTemperatureData')
            if sample_temp_data is not None:
                return sample_temp_data

    @staticmethod
    def _get_instr_config(root):
        for activity in root.findall(".//ScheduleActivity[@type='MeasurementTriggerEvent']"):
            run_config = activity.find('.//RunConfiguration')
            instr_config = run_config.find('InstrumentRunConfigurations')
            if instr_config is not None:
                return instr_config

    @staticmethod
    def _get_hardware_data(root):
        for activity in root.findall(".//ScheduleActivity[@type='MeasurementTriggerEvent']"):
            run_config = activity.find('.//RunConfiguration')
            exp_hardware = run_config.find('ExperimentHardware')
            if exp_hardware is not None:
                return exp_hardware

    def _calculate_tcr(self, temperatures=None, tcr_file=''):
        if temperatures is None:
            temperatures = [69, 420]
        standard_tcr_folder = r"C:\HotDiskTPS_7\data\Config\Tcr"
        full_file_path = os.path.join(standard_tcr_folder, tcr_file)
        df_tcr = self._import_tcr_file(full_file_path)
        for temp in temperatures:
            df_tcr.loc[len(df_tcr)] = {'temperature': temp, 'tcr': None}

        df_tcr = df_tcr.sort_values(by='temperature')
        df_tcr = df_tcr.drop_duplicates(subset=['temperature'], keep='first')
        df_tcr = df_tcr.set_index('temperature')

        df_interp = df_tcr.interpolate(method="linear")

        tcr_all = df_interp['tcr'].to_dict()
        tcr_dict = {}
        for temp in temperatures:
            tcr_dict[temp] = tcr_all.get(temp, 0.0)

        return tcr_dict

    @staticmethod
    def _import_tcr_file(file_path):
        """
        Imports a TCR file, handling variable-length headers with or without '!' characters.

        Parameters:
        - file_path (str): The path to the TCR file.

        Returns:
        - pd.DataFrame: A DataFrame containing Temperature and TCR columns.
        """
        data = []  # List to store data rows

        # Regular expression to match lines with two numerical values
        data_line_pattern = re.compile(r'^-?\d+(\.\d+)?\s+-?\d+(\.\d+)?$')

        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()

                # Skip empty lines
                if not line:
                    continue

                # Skip lines starting with '!'
                if line.startswith('!'):
                    continue

                # Check if the line matches the data pattern
                if data_line_pattern.match(line):
                    parts = re.split(r'\s+', line)  # Split by any whitespace
                    temperature = float(parts[0])
                    tcr = float(parts[1])
                    data.append({'temperature': temperature, 'tcr': tcr})
                else:
                    # Optionally, handle lines with column headers like 'Temperature TCR'
                    # Skip them as they are not data lines
                    continue

        # Create DataFrame from the collected data
        df_tcr = pd.DataFrame(data)

        return df_tcr

    @staticmethod
    def _calculate_nplc(heating_times):
        """calculate nplc based on heating times return as dict {heating_times: nplc}"""
        nplc = {}
        for time in heating_times:
            nplc[time] = 1

        return nplc


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
        self.start_measurements_input.setFixedWidth(100)
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
        script_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(script_dir, 'template.hseq')
        self.template_file_path.setText(file_path)
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
        self.program_table.setHorizontalHeaderLabels(['Temperature', 'Time', 'Measurement Power [W]', 'Measurement Time [s]'])
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
        # Collect all measurements from the table
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

        schedule_creator.create_schedule(schedule_df=scheduled_program, output_file="final_schedule.hseq")

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

