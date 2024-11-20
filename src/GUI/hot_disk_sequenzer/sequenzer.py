import xml.etree.ElementTree as ET
from datetime import datetime, timedelta


def create_schedule(measurement_times, measurement_params, output_file):
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

    for i, measurement_time in enumerate(measurement_times):
        # ScheduledTime
        sched_time = ET.SubElement(activity_list, 'ScheduleActivity', type='ScheduledTime')
        ET.SubElement(sched_time, 'targetTime').text = measurement_time.strftime('%Y-%m-%dT%H:%M:%S.%f0')

        # MeasurementTriggerEvent
        measurement_event = ET.SubElement(activity_list, 'ScheduleActivity', type='MeasurementTriggerEvent')
        ET.SubElement(measurement_event, 'NoOfMeasurements').text = str(measurement_params['NoOfMeasurements'])
        ET.SubElement(measurement_event, 'MeasurementInterval').text = str(measurement_params['MeasurementInterval'])
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
        ET.SubElement(sample_identity, 'Identity').text = measurement_params['SampleIdentity']
        ET.SubElement(sample_identity, 'AvailableProbingDepth').text = '10'
        ET.SubElement(sample_identity, 'Notes').text = measurement_params['Notes']

        # InstrumentRunConfigurations
        instr_config = ET.SubElement(run_config, 'InstrumentRunConfigurations', type='StandardRunConfigurations')

        # SampleTemperatureData
        temp_data = ET.SubElement(instr_config, 'SampleTemperatureData')
        temp = measurement_params['SampleTemperature'][i % len(measurement_params['SampleTemperature'])]
        ET.SubElement(temp_data, 'Sample').text = str(temp)
        ET.SubElement(temp_data, 'Source').text = 'ManualTemp'
        ET.SubElement(temp_data, 'Manual').text = str(temp)
        ET.SubElement(temp_data, 'TCR').text = str(measurement_params['TCR'][temp])
        ET.SubElement(temp_data, 'TcrFile').text = measurement_params['TcrFile']

        # Other instrument settings
        ET.SubElement(instr_config, 'DiskType').text = measurement_params['DiskType']
        ET.SubElement(instr_config, 'SensorType').text = measurement_params['SensorType']
        ET.SubElement(instr_config, 'SensorDesign').text = measurement_params['SensorDesign']
        ET.SubElement(instr_config, 'SensorDimension').text = measurement_params['SensorDimension']
        ET.SubElement(instr_config, 'SensorBatchID').text = measurement_params['SensorBatchID']
        ET.SubElement(instr_config, 'CableOrHolder').text = measurement_params['CableOrHolder']
        ET.SubElement(instr_config, 'HeatingPower').text = str(measurement_params['HeatingPower'][temp])
        ET.SubElement(instr_config, 'HeatingTime').text = str(measurement_params['HeatingTime'])
        ET.SubElement(instr_config, 'NPLC').text = str(measurement_params['NPLC'][temp])
        ET.SubElement(instr_config, 'DriftEnable').text = 'true'
        ET.SubElement(instr_config, 'DriftTime').text = '40'
        ET.SubElement(instr_config, 'ScanningVoltage').text = '0.05'

        # ExperimentHardware
        exp_hardware = ET.SubElement(run_config, 'ExperimentHardware')
        ET.SubElement(exp_hardware, 'HotDiskAnalyzerModel').text = 'TPS2500S'
        ET.SubElement(exp_hardware, 'PowerLineFrequency').text = '50'
        ET.SubElement(exp_hardware, 'ReferenceResistance').text = '6.912091'

        # OriginalFile and SoftwareVersion
        ET.SubElement(run_config, 'OriginalFile').text = measurement_params['OriginalFile']
        ET.SubElement(run_config, 'SoftwareVersion').text = '7.6.9'

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

    ET.indent(root, space='    ', level=0)
    # Write the XML to file
    tree = ET.ElementTree(root)
    tree.write(output_file, encoding='utf-8', xml_declaration=True)


# Example usage
if __name__ == "__main__":
    # Define the start time and measurement times
    start_time = datetime(2023, 10, 9, 13, 57)
    measurement_times = [start_time + timedelta(hours=5 * i) for i in range(1)]  # Adjust as needed

    # Define measurement parameters
    measurement_params = {
        'NoOfMeasurements': 3,
        'MeasurementInterval': 3,
        'SampleIdentity': 'WAE-WA-040-042',
        'Notes': 'MgH2-5 wt-% Fe- Cyc-350-420C',
        'SampleTemperature': [350, 420],
        'TCR': {
            350: 0.00390518127278611,
            420: 0.00123819761768757
        },
        'TcrFile': r'C:\HotDiskTPS_7\data\Config\Tcr\Estimated-TCR-Man-Deleted2-Old_F2.txt',
        'DiskType': 'Mica',
        'SensorType': 'DISK',
        'SensorDesign': '5465',
        'SensorDimension': '3.189',
        'SensorBatchID': 'F2',
        'CableOrHolder': 'GreyCable',
        'HeatingPower': {
            350: 0.15,
            420: 0.2
        },
        'HeatingTime': 5,
        'NPLC': {
            350: 1,
            420: 0.75
        },
        'OriginalFile': r'C:\Daten\Kiki\WAE-WA-040-MgFe5wt\WAE-WA-040-042-01.hotb'
    }

    output_file = 'measurement_schedule.hseq'
    create_schedule(measurement_times, measurement_params, output_file)
