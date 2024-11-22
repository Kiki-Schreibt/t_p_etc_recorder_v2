import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import pandas as pd


class ScheduleCreator:
    def __init__(self,
                 no_of_measurements=3,
                 measurement_interval=3,
                 sample_identity='Sample',
                 notes='',
                 sample_temperatures=None,
                 TCR=None,
                 tcr_file='',
                 disk_type='Mica',
                 sensor_type='DISK',
                 sensor_design='',
                 sensor_dimension='',
                 sensor_batch_id='',
                 cable_or_holder='',
                 heating_powers=None,
                 heating_times=None,
                 NPLC=None,
                 drift_enable='true',
                 drift_time='40',
                 scanning_voltage='0.05',
                 original_file='',
                 software_version='7.6.9',
                 **kwargs):
        """
        Initializes the ScheduleCreator with dynamic measurement parameters.
        """
        # Default to empty lists/dicts if None
        self.sample_temperatures = sample_temperatures or []
        self.TCR = TCR or {}
        self.heating_powers = heating_powers or {}
        self.NPLC = NPLC or {}
        self.heating_times = heating_times or {}

        # Measurement parameters
        self.measurement_params = {
            'NoOfMeasurements': no_of_measurements,
            'MeasurementInterval': measurement_interval,
            'SampleIdentity': sample_identity,
            'Notes': notes,
            'SampleTemperature': self.sample_temperatures,
            'TCR': self.TCR,
            'TcrFile': tcr_file,
            'DiskType': disk_type,
            'SensorType': sensor_type,
            'SensorDesign': sensor_design,
            'SensorDimension': sensor_dimension,
            'SensorBatchID': sensor_batch_id,
            'CableOrHolder': cable_or_holder,
            'HeatingPower': self.heating_powers,
            'HeatingTime': self.heating_times,
            'NPLC': self.NPLC,
            'DriftEnable': drift_enable,
            'DriftTime': drift_time,
            'ScanningVoltage': scanning_voltage,
            'OriginalFile': original_file,
            'SoftwareVersion': software_version,
        }

    def create_schedule(self, schedule_df: pd.DataFrame, output_file):
        # Create the root element
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

        activity_list = ET.SubElement(root, 'ActivityList')

        for index, row in schedule_df.iterrows():
            time_of_measurement = row['end_time']
            temp = row['temperature']

            # ScheduledTime
            sched_time = ET.SubElement(activity_list, 'ScheduleActivity', type='ScheduledTime')
            ET.SubElement(sched_time, 'targetTime').text = time_of_measurement.strftime('%Y-%m-%dT%H:%M:%S.%f0')

            # MeasurementTriggerEvent
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
            ET.SubElement(run_config, 'MethodType').text = 'Standard'

            # SampleIdentity
            sample_identity = ET.SubElement(run_config, 'SampleIdentity')
            ET.SubElement(sample_identity, 'Identity').text = self.measurement_params['SampleIdentity']
            ET.SubElement(sample_identity, 'AvailableProbingDepth').text = '10'
            ET.SubElement(sample_identity, 'Notes').text = self.measurement_params['Notes']

            # InstrumentRunConfigurations
            instr_config = ET.SubElement(run_config, 'InstrumentRunConfigurations', type='StandardRunConfigurations')

            # SampleTemperatureData
            temp_data = ET.SubElement(instr_config, 'SampleTemperatureData')


            ET.SubElement(temp_data, 'Sample').text = str(temp)
            ET.SubElement(temp_data, 'Source').text = 'ManualTemp'
            ET.SubElement(temp_data, 'Manual').text = str(temp)
            ET.SubElement(temp_data, 'TCR').text = str(self.TCR.get(temp, 0.0))
            ET.SubElement(temp_data, 'TcrFile').text = self.measurement_params['TcrFile']

            # Other instrument settings
            ET.SubElement(instr_config, 'DiskType').text = self.measurement_params['DiskType']
            ET.SubElement(instr_config, 'SensorType').text = self.measurement_params['SensorType']
            ET.SubElement(instr_config, 'SensorDesign').text = self.measurement_params['SensorDesign']
            ET.SubElement(instr_config, 'SensorDimension').text = self.measurement_params['SensorDimension']
            ET.SubElement(instr_config, 'SensorBatchID').text = self.measurement_params['SensorBatchID']
            ET.SubElement(instr_config, 'CableOrHolder').text = self.measurement_params['CableOrHolder']
            ET.SubElement(instr_config, 'HeatingPower').text = str(self.heating_powers.get(temp, 0.0))
            ET.SubElement(instr_config, 'HeatingTime').text = str(self.measurement_params['HeatingTime'])
            ET.SubElement(instr_config, 'NPLC').text = str(self.NPLC.get(temp, 0.0))
            ET.SubElement(instr_config, 'DriftEnable').text = self.measurement_params['DriftEnable']
            ET.SubElement(instr_config, 'DriftTime').text = self.measurement_params['DriftTime']
            ET.SubElement(instr_config, 'ScanningVoltage').text = self.measurement_params['ScanningVoltage']

            # ExperimentHardware
            exp_hardware = ET.SubElement(run_config, 'ExperimentHardware')
            ET.SubElement(exp_hardware, 'HotDiskAnalyzerModel').text = 'TPS2500S'
            ET.SubElement(exp_hardware, 'PowerLineFrequency').text = '50'
            ET.SubElement(exp_hardware, 'ReferenceResistance').text = '6.912091'

            # OriginalFile and SoftwareVersion
            ET.SubElement(run_config, 'OriginalFile').text = self.measurement_params['OriginalFile']
            ET.SubElement(run_config, 'SoftwareVersion').text = self.measurement_params['SoftwareVersion']

            # FileName
            ET.SubElement(executed_step, 'FileName')

            # ScheduledCalculation
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
        tree = ET.ElementTree(root)
        tree.write(output_file, encoding='utf-8', xml_declaration=True)





if __name__ == "__main__":
    from datetime import datetime, timedelta

    # Define the start time and measurement times
    start_time = datetime(2023, 10, 9, 13, 57)
    # For example, 10 measurement times every 5 hours
    measurement_times = [start_time + timedelta(hours=5 * i) for i in range(10)]

    # Define dynamic parameters
    sample_temperatures = [350, 420, 500]  # Arbitrary temperatures
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

    # Initialize ScheduleCreator with dynamic parameters
    schedule_creator = ScheduleCreator(
        no_of_measurements=3,
        measurement_interval=3,
        sample_identity='WAE-WA-040-042',
        notes='MgH2-5 wt-% Fe- Cyc-350-420-500C',
        sample_temperatures=sample_temperatures,
        TCR=TCR,
        tcr_file=r'C:\HotDiskTPS_7\data\Config\Tcr\Estimated-TCR.txt',
        disk_type='Mica',
        sensor_type='DISK',
        sensor_design='5465',
        sensor_dimension='3.189',
        sensor_batch_id='F2',
        cable_or_holder='GreyCable',
        heating_powers=heating_powers,
        heating_times=heating_times,
        NPLC=NPLC,
        original_file=r'C:\Daten\Kiki\WAE-WA-040-042-01.hotb'
    )

    output_file = 'measurement_schedule_dynamic.hseq'

    # Generate the schedule
    schedule_creator.create_schedule(measurement_times, output_file)
