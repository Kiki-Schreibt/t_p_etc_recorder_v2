"""
dicon_simulator_v2_improved.py

This module implements the following concerns:
1. Helper functions for duration formatting and combining temperature program steps.
2. CSV simulation support (CSVReaderForProgramSimulation and Simulator), with enhanced cleanup.
3. Temperature Program Simulation (TpProgramSimulator with inner classes TPProgram and TemperatureController).
4. The MBServer class – a Modbus server that can run in CSV mode, temperature program simulation mode, or manual mode.
"""

from datetime import timedelta
import threading
import os
import time
import struct
import gc  # Explicit garbage collection

from zoneinfo import ZoneInfo

import pandas as pd
from pyModbusTCP.server import ModbusServer
from PySide6.QtCore import Signal, QObject


from recorder_app.infrastructure.core.table_config import TableConfig
from recorder_app.tp_program_simulator import TemperatureControllerDiconSimulator

try:
    import recorder_app.infrastructure.core.logger as logging
except ImportError:
    import logging

from recorder_app.infrastructure.utils.memory_logger import log_memory

local_tz = ZoneInfo("Europe/Berlin")


# =============================================================================
# Helper Functions
# =============================================================================
def parse_duration(duration_str):
    """Convert a duration string 'HH:MM:SS' into a timedelta object."""
    if isinstance(duration_str, timedelta):
        return duration_str
    else:
        hours, minutes, seconds = map(int, duration_str.split(':'))
        return timedelta(hours=hours, minutes=minutes, seconds=seconds)


def format_duration(duration_td):
    """Format a timedelta into a 'HH:MM:SS' string."""
    total_seconds = int(duration_td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours:02}:{minutes:02}:{seconds:02}'


def combine_consecutive_temperatures(data):
    """
    Combine consecutive program steps with the same temperature.
    Returns a pandas DataFrame with appropriate columns.
    """
    if not data:
        return []

    result = []
    if len(data[0]) == 3:
        current_temp, current_duration_str, current_param = data[0]
        current_duration = parse_duration(current_duration_str)
        for values in data[1:]:
            temp = values[0]
            duration_str = values[1]
            param = values[2]
            if temp == current_temp:
                current_duration += parse_duration(duration_str)
            else:
                result.append([current_temp, current_duration, current_param])
                current_temp = temp
                current_duration = parse_duration(duration_str)
                current_param = param
        result.append([current_temp, current_duration, current_param])
        df_program = pd.DataFrame(result, columns=['temperature', 'duration', 'pressure'])
        return df_program

    elif len(data[0]) == 2:
        current_temp = data[0][0]
        current_duration_str = data[0][1]
        current_duration = parse_duration(current_duration_str)
        for values in data[1:]:
            temp = values[0]
            duration_str = values[1]
            if temp == current_temp:
                current_duration += parse_duration(duration_str)
            else:
                result.append([current_temp, current_duration])
                current_temp = temp
                current_duration = parse_duration(duration_str)
        result.append([current_temp, current_duration])
        df_program = pd.DataFrame(result, columns=['temperature', 'duration'])
        return df_program

    elif len(data[0]) > 3:
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
                current_duration += parse_duration(duration_str)
            else:
                result.append([current_temp, current_duration, current_meas_power, current_meas_time])
                current_temp = temp
                current_duration = parse_duration(duration_str)
                current_meas_power = meas_power
                current_meas_time = meas_time
        result.append([current_temp, current_duration, current_meas_power, current_meas_time])
        df_program = pd.DataFrame(result, columns=['temperature', 'duration', 'measurement_power_watt', 'measurement_time'])
        return df_program


# =============================================================================
# CSV Simulation Support
# =============================================================================
class CSVReaderForProgramSimulation:
    """
    Reads and processes CSV files for simulation.
    """
    def __init__(self, csv_file_path, file_name):
        self.csv_file_path = csv_file_path
        self.file_name = file_name

    def read_and_process_csv(self):
        df = pd.DataFrame()
        file_path = os.path.join(self.csv_file_path, self.file_name)
        # Using low_memory can reduce memory overhead in some cases.
        df = pd.read_csv(file_path, low_memory=True)
        df = self._process_csv_sheets(df=df)
        return df

    def _process_csv_sheets(self, df):
        df = df.copy()
        df['Time'] = df['Time'].apply(self._correct_time_format)
        df['Time'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        df = df.rename(columns={'Time': TableConfig.TPDataTable.time})
        df[TableConfig.TPDataTable.time] = df[TableConfig.TPDataTable.time].dt.tz_localize(local_tz, ambiguous='NaT')
        df = df.dropna(subset=[TableConfig.TPDataTable.time])
        df = df.drop('Date', axis=1)
        df = df.rename(columns={'SampleTemp': 'temperature_sample',
                                'HeaterTemp': 'temperature_heater',
                                'Pressure': 'pressure',
                                'TShouldSample': 'setpoint_sample'})
        df['setpoint_heater'] = None
        new_df = df[['time', 'pressure', 'temperature_sample', 'setpoint_sample', 'temperature_heater', 'setpoint_heater']]
        return new_df

    @staticmethod
    def _correct_time_format(time_str):
        if '.' in time_str and len(time_str) < 8:
            return '00:00:00' + time_str
        else:
            return time_str


class Simulator:
    """
    Yields rows from a DataFrame for simulation.
    """
    def __init__(self, df):
        self.df = df

    def data_simulator(self):
        total_rows = len(self.df)
        for index, row in self.df.iterrows():
            is_last_row = (index != total_rows - 1)
            yield row, is_last_row
        # Clear the DataFrame reference after iteration
        self.df = None


# =============================================================================
# Modbus Server (MBServer) Implementation with periodic recycling for CSV mode
# =============================================================================
class MBServer(QObject):
    """
    A Modbus server that simulates data either by reading CSV files,
    using a temperature program simulation, or from manually supplied values.
    """
    new_csv_file_received = Signal(pd.DataFrame)
    point_to_highlight_received = Signal(object)  # Accepts dict or pd.Series

    def __init__(self, host_ip, port, folder_path=None, mode='csv', sleep_interval=1):
        super().__init__()
        self.host_ip = host_ip
        self.port = port
        self._server = ModbusServer(host=host_ip, port=port, no_block=True)
        self.folder_path = folder_path
        self.logger = logging.getLogger(__name__)
        self.mode = mode
        self.sleep_interval = sleep_interval
        self.sleep_interval_lock = threading.Lock()
        self.running_event = threading.Event()
        self.server_thread = None
        self.temperature_program = None
        self.repeat_start = None
        self.repeat_end = None
        self.repeat_count = 0
        self.update_modbus_intervall = 1  # [s]
        self.manual_values = {
            'pressure': 0.0,
            'temperature_sample': 0.0,
            'setpoint_sample': 0.0,
            'temperature_heater': 0.0,
            'setpoint_heater': 0.0
        }
        self.manual_values_lock = threading.Lock()

    def start_server(self):
        if not self.running_event.is_set():
            self.running_event.set()
            self.server_thread = threading.Thread(target=self.run_server, daemon=True)
            self.server_thread.start()
            self.logger.info(f"Server thread started on host {self.host_ip} and port {self.port}")
        else:
            self.logger.info("Server is already running.")

    def run_server(self):
        try:
            self._server.start()
            self.logger.info("Modbus Server started.")
            if self.mode == 'csv':
                self.run_csv_mode()
            elif self.mode == 'tp_program':
                self.run_tp_program_mode()
            elif self.mode == 'manual':
                self.run_manual_mode()
            else:
                self.logger.error(f"Unknown mode: {self.mode}")
        except Exception as e:
            self.logger.error(f"Error: {e}")
        finally:
            self._server.stop()
            self.logger.info("Modbus Server stopped.")

    def run_csv_mode(self):
        # Optionally, keep track of the last file processed if desired:
        last_file = ""  # This might be read from a configuration file, for example.
        skip_to_file = bool(last_file)
        # Get the sorted list of CSV files from the specified folder.
        file_list = sorted([f for f in os.listdir(self.folder_path) if f.endswith(".csv")])
        for file_name in file_list:
            if not self.running_event.is_set():
                break
            if skip_to_file:
                if file_name == last_file:
                    skip_to_file = False
                else:
                    continue

            log_memory(self.logger, "Before CSV file loaded into simulator")
            try:
                # Process each CSV file in an isolated scope.
                df = self._simulate_csv_file(file_name)
            except Exception as e:
                self.logger.error(f"Error processing {file_name}: {e}")
            finally:
                # Make sure to delete local references and run garbage collection.
                gc.collect()
            # Check memory after each file processing cycle.
            log_memory(self.logger, "After processing CSV file and GC")

    def _simulate_csv_file(self, file_name):
        """Isolated function to process a CSV file and run its simulation."""
        csv_reader = CSVReaderForProgramSimulation(csv_file_path=self.folder_path, file_name=file_name)
        df = csv_reader.read_and_process_csv()
        if df.empty:
            return
        simulator = Simulator(df=df)
        # Emit the newly processed DataFrame to the connected UI.
        self.new_csv_file_received.emit(df.copy())
        self.logger.info(f"Processing file: {file_name}")
        counter = 0
        # Process the CSV file row by row.
        for row, _ in simulator.data_simulator():
            if not self.running_event.is_set():
                break
            if counter >= self.update_modbus_intervall:
                self._set_register_values(row=row)
                # Emit the row (or highlight info) to GUI. It can be a dict or series.
                self.point_to_highlight_received.emit(row.copy())
                counter = 0
            counter += 1 * self.sleep_interval
            with self.sleep_interval_lock:
                time.sleep(self.sleep_interval)


    def run_manual_mode(self):
        self.logger.info("Running in manual mode.")
        while self.running_event.is_set():
            with self.manual_values_lock:
                values = self.manual_values.copy()
            self._set_register_values(row=values)
            with self.sleep_interval_lock:
                time.sleep(self.sleep_interval)

    def set_temperature_program(self, temperature_program):
        self.temperature_program = temperature_program

    def set_repetition_parameters(self, repeat_start, repeat_end, repeat_count):
        self.repeat_start = repeat_start
        self.repeat_end = repeat_end
        self.repeat_count = repeat_count

    def run_tp_program_mode(self):
        tp_simulator = TemperatureControllerDiconSimulator(
            temperature_program=self.temperature_program,
            repeat_start=self.repeat_start,
            repeat_end=self.repeat_end,
            repeat_count=self.repeat_count,
        )
        self.logger.info("Starting temperature program simulation.")
        counter = 0
        while self.running_event.is_set():
            current_time, temp, p = tp_simulator.get_next_value()
            if current_time is None:
                self.logger.info("Temperature program finished.")
                break
            row = {
                'pressure': p,
                'temperature_sample': temp,
                'setpoint_sample': temp,
                'temperature_heater': temp,
                'setpoint_heater': temp,
            }
            if counter >= self.update_modbus_intervall:
                self._set_register_values(row=row)
                self.point_to_highlight_received.emit(row.copy())
                counter = 0
            counter += 1 * self.sleep_interval
            with self.sleep_interval_lock:
                time.sleep(self.sleep_interval)

    def stop_server(self):
        self.running_event.clear()
        if self.server_thread is not None:
            self.server_thread.join()
            self.server_thread = None
        self._server.stop()
        self.logger.info("Modbus Server stopped.")

    def set_sleep_interval(self, interval):
        with self.sleep_interval_lock:
            self.sleep_interval = interval
        self.logger.info(f"Sleep interval set to {self.sleep_interval} seconds.")

    def set_mode(self, mode):
        if mode in ['csv', 'tp_program', 'manual']:
            self.mode = mode
            self.logger.info(f"Mode set to {self.mode}.")
        else:
            self.logger.error(f"Invalid mode: {mode}")

    def _set_register_values(self, row):
        def _convert_float_to_registers(float_number):
            try:
                if float_number is None:
                    float_number = float('nan')
                else:
                    float_number = float(float_number)
                raw = struct.pack('>f', float_number)
                part1, part2 = struct.unpack('>HH', raw)
                return [part2, part1]
            except (struct.error, TypeError, ValueError):
                raw = struct.pack('>I', 0xFFFFFFFF)
                part1, part2 = struct.unpack('>HH', raw)
                return [part2, part1]

        table = TableConfig.TPDataTable
        regs_of_interest = [4605, 4653, 4655, 4669, 4671]
        columns = [
            table.pressure,
            table.temperature_sample,
            table.setpoint_sample,
            table.temperature_heater,
            table.setpoint_heater
        ]
        for col, reg in zip(columns, regs_of_interest):
            try:
                # Support both dictionary-based and pandas row input.
                value = row.get(col, 0) if isinstance(row, dict) else row[col]
                reg_values = _convert_float_to_registers(value)
                self._server.data_bank.set_holding_registers(address=reg, word_list=reg_values)
            except Exception as e:
                self.logger.error(f"Error writing register for column {col}: {e}")

    def set_manual_values(self, values):
        with self.manual_values_lock:
            self.manual_values.update(values)


# =============================================================================
# Main block (for testing)
# =============================================================================
if __name__ == "__main__":
    # Example usage:

    path_test_data = r"/test_data/wae-wa-040-some-cycles"
    path_test_data = r"C:\Daten\Kiki\WAE-WA-028-MgFe3wt\WAE-WA-028-TundP-Verläufe"

    server = MBServer(
            host_ip="localhost",
            port=503,  # Use appropriate port
            folder_path=path_test_data,  # Will be set via GUI
            mode='csv'
        )
    # Optionally set a more moderate sleep interval in production
    server.sleep_interval = 0.00001
    try:
        server.start_server()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        server.stop_server()
        if server.server_thread is not None:
            server.server_thread.join()
