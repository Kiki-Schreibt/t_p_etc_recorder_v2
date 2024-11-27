
from datetime import datetime, timedelta
import threading
import os
import time
import struct
from zoneinfo import ZoneInfo

import pandas as pd
import matplotlib.pyplot as plt
from pyModbusTCP.server import ModbusServer
from PySide6.QtCore import Signal, QObject

from src.config_connection_reading_management.logger import AppLogger
from src.table_data import TableConfig

local_tz = ZoneInfo("Europe/Berlin")


class MBServer(QObject):
    new_csv_file_received = Signal(pd.DataFrame)
    point_to_highlight_received = Signal(pd.Series)

    def __init__(self, host_ip, port, folder_path=None, mode='csv', sleep_interval=1):
        """
        Modbus Server that can simulate data from CSV files or a temperature program.
        mode: 'csv' for CSV data simulation, 'tp_program' for temperature program simulation.
        """
        super().__init__()
        self.host_ip = host_ip
        self.port = port
        self._server = ModbusServer(host=host_ip, port=port, no_block=True)
        self.folder_path = folder_path
        self.logger = AppLogger().get_logger(__name__)
        self.mode = mode
        self.sleep_interval = sleep_interval
        self.sleep_interval_lock = threading.Lock()  # To safely update sleep_interval
        self.running_event = threading.Event()
        self.server_thread = None
        self.temperature_program = None
        self.repeat_start = None
        self.repeat_end = None
        self.repeat_count = 0
        self.update_modbus_intervall = 1 #[s]
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
            self.server_thread = threading.Thread(target=self.run_server)
            self.server_thread.start()
            self.logger.info("Server thread started.")
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
        # Load configuration file
        try:
            # Assuming configuration is managed elsewhere
            last_file = ""
        except FileNotFoundError:
            self.logger.warning("Configuration file not found. Using defaults.")
            last_file = ""

        skip_to_file = bool(last_file)

        # Iterate through files in the directory
        for file_name in sorted(os.listdir(self.folder_path)):
            if not self.running_event.is_set():
                break

            if skip_to_file:
                if file_name == last_file:
                    skip_to_file = False
                else:
                    continue

            if not file_name.endswith(".csv"):
                continue

            csv_reader = CSVReaderForProgramSimulation(csv_file_path=self.folder_path, file_name=file_name)
            df = csv_reader.read_and_process_csv()
            simulator = Simulator(df=df)
            self.new_csv_file_received.emit(df)

            self.logger.info(f"Processing file: {file_name}")
            counter = 0
            # Simulation loop
            for row, _ in simulator.data_simulator():
                if not self.running_event.is_set():
                    break
                if counter >= self.update_modbus_intervall:
                    self._set_register_values(row=row)
                    self.point_to_highlight_received.emit(row)
                    counter = 0

                counter += 1 * self.sleep_interval



                # Sleep for the desired interval
                with self.sleep_interval_lock:
                    time.sleep(self.sleep_interval)

    def run_manual_mode(self):
        self.logger.info("Running in manual mode.")
        while self.running_event.is_set():
            # Get the current manual values
            with self.manual_values_lock:
                values = self.manual_values.copy()
            # Update the Modbus registers
            self._set_register_values(row=values)
            #print(values)
            # Sleep for the desired interval
            with self.sleep_interval_lock:
                time.sleep(self.sleep_interval)

    def set_temperature_program(self, temperature_program):
        self.temperature_program = temperature_program

    def set_repetition_parameters(self, repeat_start, repeat_end, repeat_count):
        self.repeat_start = repeat_start
        self.repeat_end = repeat_end
        self.repeat_count = repeat_count

    def run_tp_program_mode(self):
        # Create a temperature program simulator with the custom program and repetition parameters
        tp_simulator = TpProgramSimulator.TemperatureController(
            temperature_program=self.temperature_program,
            repeat_start=self.repeat_start,
            repeat_end=self.repeat_end,
            repeat_count=self.repeat_count,

        )
        self.logger.info("Starting temperature program simulation.")
        counter = 0
        # Simulation loop
        while self.running_event.is_set():
            current_time, temp, p = tp_simulator.get_next_value()
            if current_time is None:
                self.logger.info("Temperature program finished.")
                break

            # Create a row with the simulated data
            row = {
                'pressure': p,  # Simulate pressure or set as needed
                'temperature_sample': temp,
                'setpoint_sample': temp,
                'temperature_heater': temp,
                'setpoint_heater': temp,
            }

            if counter >= self.update_modbus_intervall:
                self._set_register_values(row=row)
                self.point_to_highlight_received.emit(row)
                counter = 0

            counter += 1 * self.sleep_interval

            # Sleep for the desired interval
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
                    # Represent None as NaN
                    float_number = float('nan')
                else:
                    float_number = float(float_number)

                raw = struct.pack('>f', float_number)
                part1, part2 = struct.unpack('>HH', raw)
                return [part2, part1]
            except (struct.error, TypeError, ValueError):
                # Handle cases where float_number cannot be converted
                # Set to a specific error code or default value
                raw = struct.pack('>I', 0xFFFFFFFF)  # All bits set
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
                value = row.get(col, 0)

                reg_values = _convert_float_to_registers(value)

                # Set the two consecutive registers
                self._server.data_bank.set_holding_registers(address=reg, word_list=reg_values)
            except Exception as e:
                self.logger.error(f"Error in writing regs: {e}")

    def set_manual_values(self, values):
        with self.manual_values_lock:
            self.manual_values.update(values)


class CSVReaderForProgramSimulation:
    def __init__(self, csv_file_path, file_name):
        self.csv_file_path = csv_file_path
        self.file_name = file_name

    def read_and_process_csv(self):
        file_path = os.path.join(self.csv_file_path, self.file_name)
        # Read the CSV file using Pandas
        df = pd.read_csv(file_path)
        df = self._process_csv_sheets(df=df)
        return df

    def _process_csv_sheets(self, df):
        # Combine the Date and Time columns into a single datetime column
        df = df.copy()
        df['Time'] = df['Time'].apply(self._correct_time_format)
        df['Time'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

        # Rename the column to 'time'
        df = df.rename(columns={'Time': TableConfig.TPDataTable.time})
        df[TableConfig.TPDataTable.time] = df[TableConfig.TPDataTable.time].dt.tz_localize(local_tz, ambiguous='NaT')
        df = df.dropna(subset=[TableConfig.TPDataTable.time])
        # Drop the original Date column
        df = df.drop('Date', axis=1)
        df = df.rename(columns={'SampleTemp': 'temperature_sample'})
        df = df.rename(columns={'HeaterTemp': 'temperature_heater'})
        df = df.rename(columns={'Pressure': 'pressure'})
        df = df.rename(columns={'TShouldSample': 'setpoint_sample'})
        df['setpoint_heater'] = None

        new_df = df[['time', 'pressure', 'temperature_sample', 'setpoint_sample', 'temperature_heater', 'setpoint_heater']]
        return new_df

    @staticmethod
    def _correct_time_format(time_str):
        if '.' in time_str and len(time_str) < 8:
            # If the time is unusual (like ".890"), replace it with midnight ("00:00:00")
            return '00:00:00' + time_str
        else:
            return time_str


class Simulator:
    """yields df for simulation"""
    def __init__(self, df):
        self.df = df

    def data_simulator(self):
        total_rows = len(self.df)
        for index, row in self.df.iterrows():
            is_last_row = index != total_rows - 1
            yield row, is_last_row


class TpProgramSimulator:
    class TPProgram:
        def __init__(self):
            self.logger = AppLogger().get_logger(__name__)

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
            self.logger = AppLogger().get_logger(__name__)

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
                self.logger.info("Temperature program finished")
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
                    self.logger.info("Temperature program finished.")
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


if __name__ == "__main__":
    # Example usage
    # You can specify mode='csv' or mode='tp_program'
    #mbs = MBServer(
    #    host_ip="localhost",
    #    port=502,  # Use port 502 for standard Modbus TCP
    #    folder_path=r"C:\Daten\Kiki\WAE-WA-030-Mg2NiH4\WAE-WA-030-TundP-All",  # Replace with your CSV files folder path
    #    mode='csv'  # Change to 'csv' to use CSV data simulation
    #)
    #try:
    #    mbs.start_server()
    #    # Keep the main thread alive to let the server run
    #    while True:
    #        time.sleep(1)
    #except KeyboardInterrupt:
    #    mbs.stop_server()
    start = datetime.now()
    program = TpProgramSimulator().TemperatureController(repeat_start=3,
                                                        repeat_end=6,
                                                        repeat_count=2)
    p =program.get_program_times(start)
    print(p)

