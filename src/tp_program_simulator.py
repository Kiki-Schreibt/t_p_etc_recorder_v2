# tp_program_simulator.py

import logging
import struct
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pandas as pd


# ---------------------------------------------------------------------------
# Helper Functions (unchanged)
# ---------------------------------------------------------------------------
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


def combine_consecutive_temperatures(data, expected_columns=None):
    """
    Combine consecutive program steps.
    If expected_columns is provided, use that to determine the combining logic.
    """
    if not data:
        return pd.DataFrame()

    if expected_columns is None:
        expected_columns = len(data[0])

    result = []
    if expected_columns == 2:
        # Process two-column data
        current_temp = data[0][0]
        current_duration = parse_duration(data[0][1])
        for values in data[1:]:
            temp, duration_str = values[0], values[1]
            if temp == current_temp:
                current_duration += parse_duration(duration_str)
            else:
                result.append([current_temp, current_duration])
                current_temp = temp
                current_duration = parse_duration(duration_str)
        result.append([current_temp, current_duration])
        return pd.DataFrame(result, columns=['temperature', 'duration'])

    elif expected_columns == 3:
        # Process three-column data
        current_temp, current_param = data[0][0], data[0][2]
        current_duration = parse_duration(data[0][1])
        for values in data[1:]:
            temp, duration_str, param = values[0], values[1], values[2]
            if temp == current_temp:
                current_duration += parse_duration(duration_str)
            else:
                result.append([current_temp, current_duration, current_param])
                current_temp = temp
                current_duration = parse_duration(duration_str)
                current_param = param
        result.append([current_temp, current_duration, current_param])
        return pd.DataFrame(result, columns=['temperature', 'duration', 'pressure'])

    elif expected_columns >= 4:
        # Process four-column data (for sequencer)
        current_temp, current_meas_power, current_meas_time = data[0][0], data[0][2], data[0][3]
        current_duration = parse_duration(data[0][1])
        for values in data[1:]:
            temp, duration_str, meas_power, meas_time = values[0], values[1], values[2], values[3]
            if temp == current_temp:
                current_duration += parse_duration(duration_str)
            else:
                result.append([current_temp, current_duration, current_meas_power, current_meas_time])
                current_temp = temp
                current_duration = parse_duration(duration_str)
                current_meas_power = meas_power
                current_meas_time = meas_time
        result.append([current_temp, current_duration, current_meas_power, current_meas_time])
        return pd.DataFrame(result, columns=['temperature', 'duration', 'measurement_power_watt', 'measurement_time'])


# ---------------------------------------------------------------------------
# Program Expansion & Interpolation (Unchanged)
# ---------------------------------------------------------------------------
class TemperatureProgram:
    """
    Expands a raw temperature program using optional repetition parameters.
    """
    def __init__(self, program_steps, repeat_start=None, repeat_end=None, repeat_count=0):
        self.program_steps = program_steps
        self.repeat_start = repeat_start
        self.repeat_end = repeat_end
        self.repeat_count = repeat_count

    def expand(self):
        if self.repeat_start is not None and self.repeat_end is not None:
            repeat_segment = self.program_steps[self.repeat_start - 1 : self.repeat_end]
            expanded = (
                self.program_steps[:self.repeat_start - 1] +
                repeat_segment * (self.repeat_count + 1) +
                self.program_steps[self.repeat_end:]
            )
        else:
            expanded = self.program_steps
        return expanded


class TemperatureInterpolator:
    """
    Performs linear interpolation between the steps in an expanded temperature program.
    """
    def __init__(self, expanded_program):
        self.expanded_program = expanded_program

    def _parse_duration(self, duration):
        if isinstance(duration, timedelta):
            return duration.total_seconds()
        else:
            h, m, s = map(int, duration.split(':'))
            return h * 3600 + m * 60 + s

    def simulate(self):
        current_time = 0
        prog = self.expanded_program
        for i in range(len(prog) - 1):
            current_step = prog[i]
            next_step = prog[i + 1]
            if len(current_step) == 2:
                start_temp, duration = current_step
                end_temp, _ = next_step
                start_pressure = 0
                end_pressure = 0
            elif len(current_step) == 3:
                start_temp, duration, start_pressure = current_step
                end_temp, _, end_pressure = next_step
            else:
                continue

            duration_sec = self._parse_duration(duration)
            for second in range(int(duration_sec)):
                factor = second / duration_sec
                temp = start_temp + factor * (end_temp - start_temp)
                pressure = start_pressure + factor * (end_pressure - start_pressure)
                yield current_time, temp, pressure
                current_time += 1

        # Process last step
        last_step = prog[-1]
        if len(last_step) == 2:
            last_temp, duration = last_step
            last_pressure = 0
        elif len(last_step) == 3:
            last_temp, duration, last_pressure = last_step
        else:
            return
        duration_sec = self._parse_duration(duration)
        for second in range(int(duration_sec)):
            yield current_time, last_temp, last_pressure
            current_time += 1


# ---------------------------------------------------------------------------
# Base Class for Temperature Simulation
# ---------------------------------------------------------------------------
class BaseTemperatureController:
    """
    Shared base class for temperature program simulation.
    Implements program expansion, interpolation, and per‑second simulation.
    """
    def __init__(self, temperature_program=None, repeat_start=None, repeat_end=None, repeat_count=0):
        self.logger = logging.getLogger(__name__)
        if temperature_program is None:
            # Provide a default program if none is supplied.
            self.temperature_program = [
                (30, "00:05:00", 20),
                (250, "00:05:00", 20),
                (250, "00:05:00", 20),
                (350, "00:05:00", 10),
                (350, "00:02:00", 10),
                (400, "00:05:00", 20),
                (400, "00:02:00", 20),
                (30, "00:01:00", 10)
            ]
        else:
            self.temperature_program = temperature_program

        self.repeat_start = repeat_start
        self.repeat_end = repeat_end
        self.repeat_count = repeat_count

        # Expand the program and create an interpolator.
        program_obj = TemperatureProgram(self.temperature_program, self.repeat_start, self.repeat_end, self.repeat_count)
        self.expanded_program = program_obj.expand()
        self.interpolator = TemperatureInterpolator(self.expanded_program)
        self.simulation_generator = self.interpolator.simulate()

    def get_next_value(self):
        """
        Returns the next (time, temperature, pressure) tuple from the simulation,
        or (None, None, None) if finished.
        """
        try:
            return next(self.simulation_generator)
        except StopIteration:
            self.logger.info("Temperature program finished")
            return None, None, None

    def simulate_whole_program(self):
        """
        Resets and runs the simulation, returning lists of times, temperatures, and pressures.
        """
        self.simulation_generator = self.interpolator.simulate()
        times, temps, pressures = [], [], []
        for t, temp, p in self.simulation_generator:
            times.append(t)
            temps.append(temp)
            pressures.append(p)
        return times, temps, pressures

    def plot_program(self):
        """
        Plots the entire simulated temperature and pressure profiles.
        """
        times, temps, pressures = self.simulate_whole_program()
        fig, ax1 = plt.subplots()
        ax1.plot(times, temps, label='Temperature Profile', marker='o')
        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('Temperature (°C)')
        ax1.grid(True)
        ax1.legend()
        ax2 = ax1.twinx()
        ax2.set_ylabel('Pressure (bar)')
        ax2.plot(times, pressures, label='Pressure', color='tab:red')
        plt.show()


# ---------------------------------------------------------------------------
# Simulator Temperature Controller
# ---------------------------------------------------------------------------
class TemperatureControllerDiconSimulator(BaseTemperatureController):
    """
    For real-time simulation (e.g. used in the simulator GUI or Modbus Server).
    Inherits common behavior from BaseTemperatureController.
    """

    def combine_consecutive(self, data):
        # Custom combining logic for sequencer (4 columns)
        return combine_consecutive_temperatures(data, expected_columns=len(data[0]))


# ---------------------------------------------------------------------------
# Sequencer Temperature Controller
# ---------------------------------------------------------------------------
class TemperatureControllerHotDiskSequenzer(BaseTemperatureController):
    """
    For scheduling/sequencing the temperature program (e.g. to create measurement schedules).
    Adds a method to compute end times and compressed program steps.
    """
    def get_program_times(self, start_time: datetime):
        """
        Returns two DataFrames:
         - A "compressed" program (with combined steps).
         - A total program with computed end times.
        """
        total_program = self.expanded_program
        compressed_program = self.combine_consecutive(total_program)
        start_time_copy = start_time
        end_times = []
        for index, row in compressed_program.iterrows():
            end_time = start_time + row["duration"]
            end_times.append(end_time)
            start_time = end_time
        compressed_program['end_time'] = end_times

        expected_cols = ['temperature', 'duration', 'measurement_power_watt', 'measurement_time']

        df_total_program = pd.DataFrame(total_program, columns=expected_cols)
        end_times = []
        start_time = start_time_copy
        for index, row in df_total_program.iterrows():
            end_time = start_time + parse_duration(row["duration"])
            end_times.append(end_time)
            start_time = end_time
        df_total_program['end_time'] = end_times

        return compressed_program, df_total_program

    def combine_consecutive(self, data):
        # Custom combining logic for sequencer (4 columns)
        return combine_consecutive_temperatures(data, expected_columns=4)
