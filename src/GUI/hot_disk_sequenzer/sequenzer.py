import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
import re
from io import StringIO
import pandas as pd

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

        tcr_file_path = sample_temp_data.find('TcrFile').text if sample_temp_data is not None else "asdfasdfasdf" #todo standard tcr file path here
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

def test_creator():
    from datetime import datetime, timedelta
    schedule_file_path = r'C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\src\GUI\hot_disk_sequenzer\template.hseq'
    # Define the start time and measurement times
    start_time = datetime(2023, 10, 9, 13, 57)

    # Define dynamic parameters
    sample_temperatures = [350, 420, 500, 400]  # Arbitrary temperatures

    measurement_times = [start_time + timedelta(hours=5 * i) for i in range(len(sample_temperatures))]


    TCR = {
        350: 0.00390518127278611,
        420: 0.00123819761768757,
        500: 0.000980,  # Example value
    }
    heating_powers = {
        350: 0.15,
        420: 0.2,
        500: 0.25,
    }
    NPLC = {
        350: 1,
        420: 0.75,
        500: 0.5,
    }
    heating_times = {
        350: 5,
        420: 2,
        500: 3,
    }

    # Create a DataFrame for the schedule
    schedule_df = pd.DataFrame({
        'end_time': measurement_times,
        'temperature': sample_temperatures
    })

    # Initialize ScheduleCreator with dynamic parameters
    schedule_creator = ScheduleCreator(
        no_of_measurements=3,
        measurement_interval=3,
        sample_identity='WAE-WA-040-042',
        notes='MgH2-5 wt-% Fe- Cyc-350-420-500C',
        sample_temperatures=sample_temperatures,
        TCR=TCR,
        tcr_file=r'TCR-values.txt',  # Ensure the path is correct
        disk_type='Mica',
        sensor_type='DISK',
        sensor_design='5465',
        sensor_dimension='3.189',
        sensor_batch_id='F2',
        cable_or_holder='GreyCable',
        heating_powers=heating_powers,
        heating_times=heating_times,
        NPLC=NPLC,
        original_file=r'C:\Daten\Kiki\WAE-WA-040-042-01.hotb',
        template_file=schedule_file_path  # Path to your template
    )

    output_file = 'measurement_schedule_dynamic.hseq'

    # Generate the schedule
    schedule_creator.create_schedule(schedule_df, output_file)

    print(f"Schedule XML has been created and saved to {output_file}")


if __name__ == '__main__':
    test_creator()
