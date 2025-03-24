#hot_disk_log_file_tracker.py
import logging
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import glob
import os

from PySide6.QtCore import Signal, QObject

from src.config_connection_reading_management.database_reading_writing import ExcelDataProcessor
from src.standard_paths import standard_hot_disk_file_path
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging


class LogFileTracker(QObject):
    time_range_etc_import = Signal(tuple)

    def __init__(self, meta_data, hd_log_file_tracker_params):
        super().__init__()
        self.meta_data = meta_data
        self.hot_disk_log_dir_path = hd_log_file_tracker_params['HOT_DISK_LOG_FILE_PATH']
        self.observer = None
        self.running = False
        self.logger = logging.getLogger(__name__)
        self.update_lock = threading.Lock()
        self.event_handler = LogFileHandler(self.meta_data)
        self.event_handler.time_range_from_export.connect(self._emit_times)

    def _emit_times(self, time_range):
        self.time_range_etc_import.emit(time_range)

    def start(self):
        try:
            if not self.running:
                self.logger.info("Starting log file tracker")
                self.running = True
                self.observer = Observer()  # Create a new Observer instance

                self.observer.schedule(self.event_handler, self.hot_disk_log_dir_path, recursive=False)
                self.observer.start()
                self.logger.info("Started logging of exported thermal conductivity data")
        except Exception as e:
            self.logger.error("Error starting log file tracker: %s", e)

    def stop(self):
        try:
            if self.running:
                #print("Stopping LogFileTracker")
                self.logger.info("Stopping log file tracker")
                self.running = False
                self.observer.stop()
                self.observer.join()
                #print("LogFileTracker stopped")
                self.logger.info("Stopped logging of exported thermal conductivity data")
        except Exception as e:
            #print(f"Error stopping LogFileTracker: {e}")
            self.logger.error("Error stopping logging of exported thermal conductivity data: %s", e)

    def update_sample_id(self, new_val, mode="sample_id"):
        with self.update_lock:
            if mode == "sample_id":
                self.meta_data.sample_id = new_val
                self.meta_data.read()
            else:
                self.meta_data = new_val
            if self.event_handler:
                self.event_handler.meta_data = self.meta_data


class LogFileHandler(FileSystemEventHandler, QObject):

    time_range_from_export = Signal(tuple)

    def __init__(self, meta_data):
        super().__init__()
        self.meta_data = meta_data
        latest_file = self.get_latest_file(folder_path=standard_hot_disk_file_path)
        self.latest_export_path = latest_file
        self.logger = logging.getLogger(__name__)
        self._test_mode = False

    @staticmethod
    def get_export_file_path(log_file_path):
        last_result_path = None
        with open(log_file_path, 'r') as file:
            for line in file:
                if "Progress\tFinish" in line and "<a>file:" in line:
                    start_index = line.find("<a>file:") + len("<a>file:")
                    end_index = line.find("</a>", start_index)
                    if start_index != -1 and end_index != -1:
                        # Update the last_result_path for each matching line
                        last_result_path = line[start_index:end_index]
        return last_result_path

    @staticmethod
    def get_latest_file(folder_path):
        # Get a list of all files in the folder
        files = glob.glob(os.path.join(folder_path, '*.log'))
        # Filter out directories, only keep files
        files = [f for f in files if os.path.isfile(f)]
        if not files:
            return None  # No files found
        # Find the file with the latest modification time
        latest_file = max(files, key=os.path.getmtime)
        return latest_file

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.log'):
            self.process_log_file(event.src_path)

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.log'):
            self.process_log_file(event.src_path)

    def process_log_file(self, file_path):
        #print(f"Processing log file: {file_path}")
        self.logger.info(f"Processing log file: {file_path}")

        # Variable to keep track of the last result path found
        time.sleep(15)
        last_result_path = self.get_export_file_path(log_file_path=file_path)

        # Print the last result path found after reading the entire file
        if last_result_path != self.latest_export_path:
            #print(f"Last exported to file: {last_result_path}")
            self.logger.info(f"Last exported to file: {last_result_path}")
            self.latest_export_path = last_result_path
            self.handle_new_data(last_result_path)

    def handle_new_data(self, file_path):
        processor = ExcelDataProcessor(file_path=file_path, meta_data=self.meta_data)
        if self._test_mode:
            processor._test_mode = True
            self.logger.info("Test mode activated. Times of ETC data will be corrected with actual time")

        time_range = processor.execute()
        self.time_range_from_export.emit(time_range)


def create_temp_log_file(temp_log_file_path):
    # Create a temporary log file for testing
    with open(temp_log_file_path, 'w') as file:
        file.write("Initial log content\n")


def modify_temp_log_file(temp_log_file_path, content):
    # Append new content to the temporary log file
    with open(temp_log_file_path, 'a') as file:
        file.write(content + "\n")


def test_log_file_tracker():
    temp_log_file_path = r'C:\Daten\Kiki\ProgrammingStuff\kikis_hot_v3\For_testing\HotDiskLogFiles\HotDisk 2023 08 13 10.27.22.log'
    log_file_tracker = LogFileTracker()

    try:
        # Create a temporary log file
        create_temp_log_file(temp_log_file_path)
        # Start the LogFileTracker in a separate thread
        tracker_thread = threading.Thread(target=log_file_tracker.start, daemon=True)
        tracker_thread.start()
        # Simulate a wait for the observer to start
        time.sleep(5)
        # Simulate modifying the log file
        modify_temp_log_file(temp_log_file_path, "New log entry")
        # Wait to allow the handler to process the file modification
        time.sleep(10)
        # Stop the LogFileTracker
        log_file_tracker.stop()
        tracker_thread.join()
        print("LogFileTracker testing completed.")
    finally:
        # Clean up: remove the temporary log file
        if os.path.exists(temp_log_file_path):
            os.remove(temp_log_file_path)


# Run the test function
if __name__ == "__main__":
    test_log_file_tracker()




