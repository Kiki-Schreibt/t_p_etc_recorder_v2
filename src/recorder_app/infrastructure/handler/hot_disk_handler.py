#hot_disk_handler.py

import os
import threading
from datetime import datetime
import time
import re

from recorder_app.infrastructure.connections.connections import HotDiskConnection
from recorder_app.infrastructure.core import global_vars

try:
    import recorder_app.infrastructure.core.logger as logging
except ImportError:
    import logging


standard_hot_disk_schedule_folder = r"C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\config\tps_schedules"
local_tz = global_vars.local_tz


class HotDiskScheduleGrabber:
    def __init__(self, template_folder_path=standard_hot_disk_schedule_folder, sensor_insulation="Mica", sensor_type="5465"):
        self.logger = logging.getLogger(__name__)
        self.template_folder_path = template_folder_path
        self.sensor_type = sensor_type
        self.sensor_insulation = sensor_insulation

    def add_file_names_to_dict(self, temp_schedule_dict_list: list):
        """
        Picks the correct schedule files based on a dictionary filled with measurement times and parameters temperatures.
        Schedule files name must include measurement temperature, heating time and heating power.
        :param temp_schedule_dict_list: = list of dictionaries = {datetime_of_measurement, temperature, heating_time, heating_power}
        :return:
        """
        if not temp_schedule_dict_list:
            self.logger.error("No schedule arrived")
        for measurement in temp_schedule_dict_list:
            if not isinstance(measurement['measurement_time'], datetime):
                measurement['measurement_time'] = datetime.strptime(measurement['measurement_time'], '%Y-%m-%d %H:%M:%S')
                measurement['measurement_time'].replace(tzinfo=local_tz)
        temp_schedule_dict_list.sort(key=lambda x: x['measurement_time'])

        for measurement in temp_schedule_dict_list:
            datetime_of_measurement = measurement['measurement_time']
            temperature = measurement['temperature']
            heat_pulse_duration = measurement['heat_pulse_duration']
            heating_power = measurement['heating_power']
            measurement['file_path'] = self.search_file(temperature=temperature,
                                                        heat_pulse_duration=heat_pulse_duration,
                                                        heating_power=heating_power)
        return temp_schedule_dict_list

    def search_file(self, temperature, heat_pulse_duration, heating_power):
        """
        Browse through folder and get the schedule file name from files that match parameters,
        regardless of the order of the parameters in the file name.
        :param temperature: Measurement temperature
        :param heat_pulse_duration: Heating time
        :param heating_power: Heating power
        :return: List of matching file paths
        """
        # Build regex pattern to match filenames containing the parameters in any order
        pattern = rf"^(?=.*{temperature}_C)(?=.*{heat_pulse_duration}_s)(?=.*{heating_power}_mW)(?=.*{self.sensor_insulation}_ins)(?=.*{self.sensor_type}_type).*\.hseq$"
        regex = re.compile(pattern)

        # List all files in the directory
        all_files = os.listdir(self.template_folder_path)
        matching_files = []

        for filename in all_files:
            if regex.match(filename):
                full_path = os.path.join(self.template_folder_path, filename)
                full_path = os.path.normpath(full_path)
                return full_path


        if not matching_files:
            self.logger.error(f"No files found for measurement at {temperature} °C, {heat_pulse_duration} s, {heating_power} mW")
        else:
            self.logger.info(f"Found files for measurement at {temperature} °C, {heat_pulse_duration} s, {heating_power} mW: {matching_files}")

        return matching_files


class HotDiskSequenzerBackend:

    def __init__(self, hd_conn_params,
                     template_folder_path=standard_hot_disk_schedule_folder,
                     sensor_insulation="Mica",
                     sensor_type="5465", standard_number_of_measurements=3):
        self.logger = logging.getLogger(__name__)
        self.hd_conn_params = hd_conn_params
        self.schedule_grabber = HotDiskScheduleGrabber(template_folder_path, sensor_insulation=sensor_insulation, sensor_type=sensor_type)
        self.running_event = threading.Event()
        self.stop_event = threading.Event()
       # self.current_experiment_row = self.get_row_count()
        self.current_experiment_row = None #implement later
        self.number_of_measurements = standard_number_of_measurements

    def run(self, temp_schedule_dict_list):
        """
        selects correct schedule files and starts them in order. Always waits for next calculated starting time from temperature program.
        :param temp_schedule_dict_list: dictionatry with measurement parameters and times for measurement execution
        :return:
        """

        self.running_event.set()  # Indicate that the thread is running
        try:
            temp_schedule_dict_list = self.schedule_grabber.add_file_names_to_dict(temp_schedule_dict_list)
        except Exception as e:
            self.logger.exception(e)

        for measurement in temp_schedule_dict_list:
            if not measurement['file_path']:
                self.logger.error(f"No scheduler file for measurement at {measurement['measurement_time']}")
                self.running_event.clear()
        while self.running_event.is_set() and not self.stop_event.is_set():
            for measurement in temp_schedule_dict_list:
                datetime_of_measurement = measurement['measurement_time']
                full_file_path = measurement['file_path']
                self.wait_until(datetime_of_measurement)
                if self.stop_event.is_set():
                    break  # Exit if the stop event has been set
                self.start_schedule_file(full_file_path=full_file_path)
                #self.print_schedule_file(full_file_path=full_file_path, measurement_time=str(datetime_of_measurement))
            self.running_event.clear()  # Stop after one iteration

    def start_schedule_file(self, full_file_path):
        if full_file_path:
            try:
                with HotDiskConnection(**self.hd_conn_params) as client:
                    client.send_command(f"SCHED:INIT {full_file_path}")
                    self.logger.info(f"Measurement started with schedule {full_file_path}")
            except Exception as e:
                self.logger.error(f"Could not start schedule file: {e}")

    def print_schedule_file(self, full_file_path: str, measurement_time=""):
        print(full_file_path)
        print(measurement_time)

    def wait_until(self, target_time):
        """Pause execution until a specified target_time or until the stop_event is set.

        Args:
            target_time (datetime.datetime): The time to wait until.
        """
        now = datetime.now(tz=local_tz)
        delay = (target_time - now).total_seconds()
        passed_time = 0
        if delay > 0:
            end_time = time.time() + delay
            while time.time() < end_time and not self.stop_event.is_set():
                time_left = end_time - time.time()
                wait_time = min(1, time_left)
                passed_time += wait_time
                if passed_time >= 10:
                    passed_time = 0
                    self.print_remaining_time(target_time=target_time, time_left=time_left)
                self.stop_event.wait(timeout=wait_time)  # Wait for wait_time seconds or until stop_event is set
                if self.stop_event.is_set():
                    #todo: save current state maybe to reload in case of interruptions
                    break

    def end(self):
        self.running_event.clear()
        self.stop_event.set()

    def get_row_count(self):
        with HotDiskConnection(**self.hd_conn_params) as client:
            client.send_command("ROW:COUNT?")
            row_count = client.receive_response()
            return int(row_count)

    def select_rows(self, current_experiment_row=None, number_of_measurements=None, include_avg_div=False):
        """
        selects experiment rows in constants analyser. Auto selects rows based on the number of measurements to average either for calculation (include_avg_div = False)
        or for exporting (include_avg_div = True)
        :param current_experiment_row:
        :param number_of_measurements:
        :param include_avg_div:
        :return:
        """
        start_idx = current_experiment_row if current_experiment_row else self.current_experiment_row
        number_of_measurements = number_of_measurements if number_of_measurements else self.number_of_measurements

        if include_avg_div:
            end_idx = start_idx + number_of_measurements + 1
        else:
            end_idx = start_idx + number_of_measurements - 1

        with HotDiskConnection(**self.hd_conn_params) as client:
            client.send_command(f"ROW:SEL {start_idx}-{end_idx}")

    def calculate_results(self):
        with HotDiskConnection(**self.hd_conn_params) as client:
            client.send_command("CAlC:EXE")

    def update_row_count(self, incr_increase=None):
        """
        updates self.current_experiment_row to last experiment row. or increases self.current_experiment_row by values (should be picked number of measurements + 2 for iterating through a batch file)
        """
        if not incr_increase:
            self.current_experiment_row = self.get_row_count()
        else:
            self.current_experiment_row += incr_increase

    def get_result_val(self):
        with HotDiskConnection(**self.hd_conn_params) as client:
           result_val =  client.send_command_receive_response(command="CALC:TCOND?")
        return result_val

    def print_remaining_time(self, target_time, time_left):

        hours, remainder = divmod(int(time_left), 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d} till next measurement at {target_time}"
        #print(formatted)


class HotDiskController:

    def __init__(self, hd_conn_params, response_delay=10):
        self.logger = logging.getLogger(__name__)
        self.hd_conn_params = hd_conn_params
        self.response_delay = response_delay
        self.running = False

    def run(self):
        self.running = True
        try:
            with HotDiskConnection(**self.hd_conn_params, response_delay=self.response_delay) as client:
                status = self.ask_status(client=client)
            return status
        except Exception as e:
            self.logger.error(f"{e}")
            return None

    def select_rows(self, start_row=None, end_row=None):
        if start_row and end_row:
            row_str = f"{start_row}-{end_row}"
        else:
            row_str = "ALL"

        with HotDiskConnection(**self.hd_conn_params) as client:
            client.send_command(f"ROW:SEL {row_str}")

    def ask_status(self,client):

        response = client.send_command_receive_response("*OPC?")
        return response

    def export_results(self, folder_path, result_file_name, start_row=None, end_row=None):
        full_path = os.path.join(folder_path, result_file_name+'.xlsx')

        delete_file_if_exists(full_file_path=full_path, logger=self.logger)
        self.select_rows(start_row=start_row,
                             end_row=end_row)
        with HotDiskConnection(**self.hd_conn_params, response_delay=self.response_delay) as client:
            client.send_command(f"EXPORT {full_path}")

    def start_active_schedule(self):
        try:
            with HotDiskConnection(**self.hd_conn_params, response_delay=self.response_delay) as client:
                client.send_command(f"SCHED:INIT")
                self.logger.info(f"Current loaded schedule started in constants analyzer")
        except Exception as e:
            self.logger.error(f"Could not start schedule file: {e}")


def delete_file_if_exists(full_file_path, logger):
    if os.path.exists(full_file_path):
        try:
            os.remove(full_file_path)  # Delete the file
            logger.info(f"File '{full_file_path}' already exists and has been deleted.")
        except Exception as e:
            print(f"Error while deleting file: {e}")


def test_hd_controller():
    from datetime import timedelta
    measurements = [
        {
            'measurement_time': datetime.now(tz=local_tz),
            'temperature': 100,
            'heat_pulse_duration': 3,
            'heating_power': 100
        },
        {
            'measurement_time': datetime.now(tz=local_tz) + timedelta(seconds=5),
            'temperature': 100,
            'heat_pulse_duration': 3,
            'heating_power': 100
        },
        {
            'measurement_time': datetime.now(tz=local_tz) + timedelta(seconds=10),
            'temperature': 100,
            'heat_pulse_duration': 3,
            'heating_power': 100
        },
        # More entries...
    ]
    #hd_controller = HotDiskSequenzerBackend()
    #hd_controller.run(temp_schedule_dict_list=measurements)    # Print the updated measurements list
    #import pprint
    #pprint.pprint(measurements)


# tests/test_hot_disk_schedule_grabber.py



if __name__ == '__main__':

    from recorder_app.infrastructure.core.config_reader import config
    hd_conn_params = config.hd_conn_params
    hd_controller = HotDiskController(hd_conn_params=hd_conn_params, response_delay=None)
    file_name = 'WAE-WA-060-000-08'
    folder_path = r'C:\Daten\Kiki\WAE-WA-060-Mg5wtFe\WAE-WA-060-All\WAE-WA-060-000-AngleTest\WAE-WA-060-000-08-00dlong_0dtrans_50C'



    #with concurrent.futures.ThreadPoolExecutor() as executor:
    #  future = executor.submit(hd_controller.run)
    #  result = future.result()
    #  print(result)


    # hd_controller.export_results(folder_path=folder_path,
              #                   result_file_name=file_name)
    # print(result)
