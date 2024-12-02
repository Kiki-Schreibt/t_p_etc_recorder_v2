import os
import threading
from datetime import datetime
import time
import re

from src.config_connection_reading_management.connections import HotDiskConnection
from src.config_connection_reading_management.logger import AppLogger

temp_folder_path = r'C:\Daten\Kiki\ProgrammingStuff\t_p_etc_recorder_v2\Scripts\temp_for_hotdisk'


class HotDiskScheduleGrabber:
    def __init__(self, template_folder_path=temp_folder_path):
        self.template_folder_path = template_folder_path

    def add_file_names_to_dict(self, temp_schedule_dict_list: list):
        """
        Picks the correct schedule files based on a dictionary filled with measurement times and parameters temperatures.
        Schedule files name must include measurement temperature, heating time and heating power.
        :param temp_schedule_dict_list: = list of dictionaries = {datetime_of_measurement, temperature, heating_time, heating_power}
        :return:
        """
        for measurement in temp_schedule_dict_list:
            if not isinstance(measurement['time'], datetime):
                measurement['time'] = datetime.strptime(measurement['time'], '%Y-%m-%d %H:%M:%S')
        temp_schedule_dict_list.sort(key=lambda x: x['time'])

        for measurement in temp_schedule_dict_list:
            datetime_of_measurement = measurement['time']
            temperature = measurement['temperature']
            heating_time = measurement['heating_time']
            heating_power = measurement['heating_power']
            measurement['file_path'] = self.search_file(temperature=temperature,
                                                        heating_time=heating_time,
                                                        heating_power=heating_power)
        return temp_schedule_dict_list

    def search_file(self, temperature, heating_time, heating_power):
        """
        Browse through folder and get the schedule file name from files that match parameters,
        regardless of the order of the parameters in the file name.
        :param temperature: Measurement temperature
        :param heating_time: Heating time
        :param heating_power: Heating power
        :return: List of matching file paths
        """
        # Build regex pattern to match filenames containing the parameters in any order
        pattern = rf"^(?=.*{temperature}_C)(?=.*{heating_time}_s)(?=.*{heating_power}_mW).*\.hseq$"
        regex = re.compile(pattern)

        # List all files in the directory
        all_files = os.listdir(self.template_folder_path)
        matching_files = []

        for filename in all_files:
            if regex.match(filename):
                full_path = os.path.join(self.template_folder_path, filename)
                matching_files.append(full_path)

        if not matching_files:
            print(f"No files found for measurement at {temperature} °C, {heating_time} s, {heating_power} mW")
        else:
            print(f"Found files for measurement at {temperature} °C, {heating_time} s, {heating_power} mW: {matching_files}")

        return matching_files


class HotDiskController:
    def __init__(self, template_folder_path=temp_folder_path):
        self.logger = AppLogger().get_logger(__name__)
        self.schedule_grabber = HotDiskScheduleGrabber(template_folder_path)
        self.running_event = threading.Event()

    def run(self, temp_schedule_dict_list):
        self.running_event.set()  # Indicate that the thread is running
        temp_schedule_dict_list = self.schedule_grabber.add_file_names_to_dict(temp_schedule_dict_list)
        for measurement in temp_schedule_dict_list:
            if not measurement['file_path']:
                print(f"No scheduler file for measurement at {measurement['time']}")
                self.running_event.clear()
        while self.running_event.is_set():
            for measurement in temp_schedule_dict_list:
                datetime_of_measurement = measurement['time']
                full_file_path = measurement['file_path']
                self.wait_until(datetime_of_measurement)
                if not self.running_event.is_set():
                    break  # Exit if the event has been cleared
                self.start_schedule_file(full_file_path=full_file_path)
            self.running_event.clear()  # Stop after one iteration

    def start_schedule_file(self, full_file_path):
        if full_file_path:
            print("I'm starting a file")
            # Uncomment and adjust the following code when ready
            # try:
            #     with HotDiskConnection() as client:
            #         client.send_command(f"SCHED:INIT {full_file_path}")
            # except Exception as e:
            #     self.logger.error(f"Could not start schedule file: {e}")

    def wait_until(self, target_time):
        """Pause execution until a specified target_time or until the running event is cleared.

        Args:
            target_time (datetime.datetime): The time to wait until.
        """
        now = datetime.now()
        delay = (target_time - now).total_seconds()
        if delay > 0:
            end_time = time.time() + delay
            while time.time() < end_time and self.running_event.is_set():
                time_left = end_time - time.time()
                wait_time = min(1, time_left)  # Wait in 1-second increments
                self.running_event.wait(timeout=wait_time)

    def end(self):
        self.running_event.clear()


def test_hd_controller():
    from datetime import timedelta
    measurements = [
        {
            'time': datetime.now(),
            'temperature': 25,
            'heating_time': 3,
            'heating_power': 100
        },
        {
            'time': datetime.now() + timedelta(seconds=5),
            'temperature': 25,
            'heating_time': 3,
            'heating_power': 100
        },
        {
            'time': datetime.now() + timedelta(seconds=10),
            'temperature': 25,
            'heating_time': 3,
            'heating_power': 100
        },
        # More entries...
    ]
    hd_controller = HotDiskController()
    hd_controller.run(temp_schedule_dict_list=measurements)    # Print the updated measurements list
    import pprint
    pprint.pprint(measurements)



if __name__ == '__main__':
    test_hd_controller()
    with HotDiskConnection() as client:
        client.send_command("*IDN?")
        response = client.receive_response()
