#modbus_handler.py
from datetime import datetime, timedelta
import numpy as np
import struct
import time
from zoneinfo import ZoneInfo
import threading

import pandas as pd
from pymodbus.exceptions import ModbusException, ConnectionException
from pymodbus.pdu import ExceptionResponse
from psycopg2 import IntegrityError

from src.config_connection_reading_management.connections import DatabaseConnection, ModbusConnection
from src.calculations.eq_p_calculation import VantHoffCalcEq as EqCalculator
from src.config_connection_reading_management.query_builder import QueryBuilder
from src.meta_data.meta_data_handler import MetaData
from src.table_data import TableConfig
from src.config_connection_reading_management.database_reading_writing import DataRetriever, DataBaseManipulator
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging

local_tz = ZoneInfo("Europe/Berlin")

STANDARD_MODE = 'recording'
STATE_HYD = 'Hydrogenated'
STATE_DEHYD = 'Dehydrogenated'


class ModbusProcessor:
    """
    Handles Modbus processing tasks including reading from the Modbus device,
    processing the data, and writing the data to a database.

    Attributes
    ----------
    meta_data : MetaData
        MetaData instance containing sample metadata.
    mb_host : str
        Host address for the Modbus connection.
    mb_port : int
        Port number for the Modbus connection.
    logger : logging.Logger
        Logger instance for logging information.
    running : bool
        A flag to indicate if the processing is running.
    mb_reader : ModbusReader
        Instance of the ModbusReader class for reading data.
    mb_data_handler : ModbusDataHandler
        Instance of the ModbusDataHandler class for counting hydrogen uptake of last cycle when as soon as a cycle finishes
    mb_db_writer : ModbusDBWriter
        Instance of the ModbusDBWriter class for writing data to the database.

    Methods
    -------
    run()
        Starts the Modbus data processing.
    stop()
        Stops the Modbus data processing.
    on_sample_id_change(new_val, mode="sample_id")
        Handles changes in sample ID.
    """
    def __init__(self, meta_data, config):
        """
        Initializes the ModbusProcessor with the provided meta_data, host, and port.

        Parameters
        ----------
        meta_data : MetaData, optional
            MetaData instance (default is MetaData()).
        mb_host : str, optional
            Host address for the Modbus connection (default is config.MODBUS_HOST).
        mb_port : int, optional
            Port number for the Modbus connection (default is config.MODBUS_PORT).
        """
        self.meta_data = meta_data
        self.logger = logging.getLogger(__name__)
        try:
            self.db_conn_params = config.db_conn_params
            self.mb_conn_params = config.mb_conn_params
            self.mb_reading_params = config.mb_reading_params
            self.hd_log_file_tracker_params = config.hd_log_file_tracker_params
        except Exception as e:
            self.logger.error(f"No config provided: {e}")
            raise
        self.running = False
        self.mb_reader = ModbusReader(mb_reading_params=self.mb_reading_params, mb_conn_params=self.mb_conn_params)
        self.mb_data_handler = ModbusDataHandler(meta_data=self.meta_data, db_conn_params=self.db_conn_params)
        self.mb_db_writer = ModbusDBWriter(meta_data=self.meta_data, db_conn_params=self.db_conn_params)

    def run(self):
        """
        Starts data reading, processing and writing in a continuous loop.
        """
        self.running = True
        retry_count = 0  # Initialize retry count

        with (ModbusConnection(**self.mb_conn_params) as modbus_connection,
              DatabaseConnection(**self.db_conn_params) as db_conn):
            self.logger.info("Starting continuous data recording...")
            while self.running:
                try:
                    df = self.mb_reader.read_from_dicon(client=modbus_connection.client)
                    if df.empty:
                        self.logger.error("No data to write")
                        continue
                    for index, row in df.iterrows():
                        tp_df = self.mb_data_handler.process_data(index, row)
                        self.mb_db_writer.insert_data_into_table(data=tp_df, cursor=db_conn.cursor)
                    time.sleep(self.mb_reading_params["SLEEP_INTERVAL"])
                except Exception as e:
                    self.running = False
                    self.logger.error("An error occurred in the main loop: %s", e)

    def stop(self):
        """
        Stops the data recording, processing and writing.
        """
        self.logger.info("Ending temperature and pressure recording")
        self.running = False
        self.mb_reader.running = False
        self.logger.info("Temperature and pressure recording stopped")

    def on_sample_id_change(self, new_val, mode="sample_id"):
        """
        Handles changes in sample ID or MetaData.

        Parameters
        ----------
        new_val : str
            The new sample ID.
        mode : str, optional
            The mode indicating the type of change (default is "sample_id").
        """
        if mode == "sample_id":
            self.meta_data.sample_id = new_val
            self.meta_data.read()
        else:
            self.meta_data = new_val
        self.mb_data_handler.meta_data = self.meta_data
        self.mb_data_handler.de_hyd_state, self.cycle = self.mb_data_handler.data_retriever.fetch_last_state_and_cycle(meta_data=self.meta_data)
        self.mb_data_handler.cycle = self.cycle
        self.logger.info(f"Sample ID changed to: {self.meta_data.sample_id}")
        if self.cycle == 0:
            self.mb_data_handler._new_test_handling()


class ModbusReader:
    """
    A class to read data from a Modbus device and process it.

    Attributes
    ----------
    running : bool
        A flag to indicate if the reading process is running.
    table : TableConfig.TPDataTable
        Configuration for table columns related to temperature and pressure data.
    logger : logging.Logger
        Logger instance for logging information.
    max_retries : int
        Maximum number of retries for Modbus connection.
    retry_delay : int
        Delay in seconds between retries.
    retry_count : int
        Current count of connection retries.
    none_T_p : float
        Threshold value to identify and replace invalid readings.

    Methods
    -------
    run(client)
        Starts the continuous data recording process.
    stop(client)
        Stops the continuous data recording process.
    read_from_dicon(client, start_reg, end_reg, regs_of_interest)
        Reads and processes data from the Modbus device.
    _sort_out_false_values(df)
        Filters out invalid readings based on a threshold value.
    """

    def __init__(self, mb_conn_params, mb_reading_params):
        """
        Initializes the ModbusReader with default values and configurations.
        """
        self.running = False
        self.mb_conn_params = mb_conn_params or {}
        self.mb_reading_params = mb_reading_params or {}
        self.table = TableConfig().TPDataTable
        self.logger = logging.getLogger(__name__)
        self.max_retries = 10  # Maximum number of retries for Modbus connection
        self.retry_delay = 5  # Delay in seconds between retries
        self.retry_count = 0
        self.none_T_p = 1e20

    def run(self, client):
        """
        Starts the continuous data recording process.

        Parameters
        ----------
        client : Modbus client instance
            The client instance to communicate with the Modbus device.
        """
        SLEEP_INTERVAL = self.mb_reading_params['SLEEP_INTERVAL']

        self.running = True
        retry_count = 0  # Initialize retry count
        self.logger.info("Starting continuous data recording...")
        while self.running:
            try:
                converted_dicon_data = self.read_from_dicon(client)
                time.sleep(SLEEP_INTERVAL)

            except Exception as e:
                self.logger.error("An error occurred in the main loop: %s", e)
                self.stop(client)
                #if self.retry_count <= self.max_retries:
                #    self.logger.info("Connection failed")
                #    self.retry_count += 1
                #    time.sleep(self.retry_delay)
        self.stop(client)

    def stop(self, client):
        """
        Stops the continuous data recording process.

        Parameters
        ----------
        client : Modbus client instance
            The client instance to communicate with the Modbus device.
        """
        self.logger.info("Ending temperature and pressure recording")
        self.running = False
        if client:
            client.close()
        self.logger.info("Temperature and pressure recording stopped")

    def read_from_dicon(self, client):
        """
        Reads and processes data from the Modbus device.

        Parameters
        ----------
        client : Modbus client instance
            The client instance to communicate with the Modbus device.
        START_REG : int
            The starting register to read from.
        END_REG : int
            The ending register to read from.
        REGS_OF_INTEREST : list of int
            The list of registers of interest to be processed.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the processed data, with invalid readings replaced.
        """

        def _convert_dicon_to_df(numbers_to_convert):
            values = []
            df_Tp = pd.DataFrame()

            for i in range(0, len(numbers_to_convert), 2):
                raw = struct.pack('>HH', numbers_to_convert[i + 1], numbers_to_convert[i])
                values.append(struct.unpack('>f', raw)[0])

            if values:
                df_Tp = pd.DataFrame([values], columns=[self.table.pressure, self.table.temperature_sample, self.table.setpoint_sample, self.table.temperature_heater, self.table.setpoint_heater])
            return df_Tp  # Returning df

        """
            Converts raw register data to a pandas DataFrame.

            Parameters
            ----------
            numbers_to_convert : list of int
                The list of raw register values to be converted.

            Returns
            -------
            pandas.DataFrame
                A DataFrame containing the converted data.
        """
        START_REG = self.mb_reading_params['START_REG']
        END_REG = self.mb_reading_params['END_REG']
        REGS_OF_INTEREST = self.mb_reading_params['REGS_OF_INTEREST']

        filtered_regs = []
        if (END_REG-START_REG) % 2 == 0:
            final_reg = END_REG - START_REG
        else:
            final_reg = END_REG - START_REG + 1

        index_of_interest = [num - START_REG for num in REGS_OF_INTEREST]
        try:
            rr = client.read_holding_registers(START_REG, final_reg, 255)
        except ModbusException as exc:
            self.logger.error(f"Received ModbusException({exc}) from library")
            client.close()
            return
        if isinstance(rr, ExceptionResponse):
            self.logger.error(f"Received Modbus library exception ({rr})")
            client.close()
            return
        elif rr.isError():
            self.logger.error(f"Received Modbus library error({rr})")
            client.close()
            return
        if rr.registers:
            for i in index_of_interest:
                filtered_regs.append(rr.registers[i])
                filtered_regs.append(rr.registers[i+1])
            if filtered_regs:
                df_tp = _convert_dicon_to_df(filtered_regs)
                df_tp[self.table.time] = datetime.now(tz=local_tz)
                df_tp = self._sort_out_false_values(df_tp)
                return df_tp
            else:
                return pd.DataFrame()
        else:
            self.logger.error("No registers received")
            client.close()

    def _sort_out_false_values(self, df):
        """
        Filters out invalid readings based on a threshold value.

        Parameters
        ----------
        df : pandas.DataFrame
            The DataFrame to be filtered.

        Returns
        -------
        pandas.DataFrame
            The filtered DataFrame with invalid readings replaced by None.
        """
        if not df.empty:
            df.loc[df[self.table.pressure] >= self.none_T_p, self.table.pressure] = None
            df.loc[df[self.table.temperature_sample] >= self.none_T_p, self.table.temperature_sample] = None
            df.loc[df[self.table.temperature_heater] >= self.none_T_p, self.table.temperature_heater] = None
            df.loc[df[self.table.setpoint_sample] >= self.none_T_p, self.table.setpoint_sample] = None
            df.loc[df[self.table.setpoint_heater] >= self.none_T_p, self.table.setpoint_heater] = None
            return df
        else:
            return pd.DataFrame()


class ModbusDataHandler:
    """
    A class to handle and process Modbus data, including state checking and cycle counting.

    Attributes
    ----------
    logger : logging.Logger
        Logger instance for logging information.
    data_retriever : DataRetriever
        Instance of DataRetriever to fetch data from the database.
    tp_table : TableConfig.TPDataTable
        Configuration for table columns related to temperature and pressure data.
    meta_data : MetaData
        MetaData instance containing sample metadata.
    reservoir_volume : float
        Volume of the reservoir.
    temperature_no_thermoelement : float
        Threshold value for temperatures indicating no thermoelement.
    cycling_flag : bool
        Flag indicating if cycling is enabled.
    h2_uptake_flag : bool
        Flag indicating if H2 uptake calculation is enabled.
    eq_calculator : EqCalculator
        Instance of EqCalculator for equilibrium pressure calculations.
    de_hyd_state : str
        Current dehydrogenation/hydrogenation state.
    cycle : int
        Current cycle number.

    Methods
    -------
    process_data(df_Tp)
        Processes the DataFrame containing Modbus data.
    _de_hyd_state_checker(is_pressure, eq_pressure, temperature)
        Checks and updates the dehydrogenation/hydrogenation state.
    _on_cycle_number_changed()
        Handles changes in cycle number.
    _new_test_handling()
        Initializes values for a new test.
    on_meta_data_changed(new_meta_data)
        Handles changes in metadata.
    """
    def __init__(self, meta_data, db_conn_params):
        """
        Initializes the ModbusDataHandler with the provided metadata.

        Parameters
        ----------
        meta_data : MetaData, optional
            MetaData instance (default is MetaData()).
        """
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params
        self.data_retriever = DataRetriever(db_conn_params=self.db_conn_params)
        self.tp_table = TableConfig().TPDataTable
        self.meta_data = meta_data
        if self.meta_data.sample_id:
            self.reservoir_volume = self.meta_data.reservoir_volume

        self.temperature_no_thermoelement = 1e10
        # Fetch last state and cycle number
        self.cycling_flag = False
        self.h2_uptake_flag = False

        self.eq_calculator = EqCalculator(meta_data=self.meta_data, db_conn_params=self.db_conn_params)
        self.de_hyd_state, self.cycle = self.data_retriever.fetch_last_state_and_cycle(meta_data=self.meta_data)

        if not self.cycle:
            self._new_test_handling()


    def process_data(self, index, row, reservoir_volume=None):
        """
        Processes the DataFrame containing Modbus data.

        Parameters
        ----------
        df_Tp : pandas.DataFrame
            DataFrame containing Modbus data.

        Returns
        -------
        pandas.DataFrame
            Processed DataFrame with additional columns for equilibrium pressure,
            dehydrogenation/hydrogenation state, cycle number, reservoir volume, and flags.
        """
        row[self.tp_table.eq_pressure] = self.eq_calculator.calc_eq(row[self.tp_table.setpoint_sample])
        row[self.tp_table.eq_pressure_real] = self.eq_calculator.calc_eq(row[self.tp_table.temperature_sample])

        # Iterate over DataFrame rows

        self._de_hyd_state_checker(row[self.tp_table.pressure], row[self.tp_table.eq_pressure], row[self.tp_table.temperature_sample])
        row[self.tp_table.de_hyd_state] = self.de_hyd_state

        row[self.tp_table.cycle_number] = self.cycle

        if reservoir_volume:
            row[self.tp_table.reservoir_volume] = reservoir_volume
        elif self.meta_data.reservoir_volume:
            row[self.tp_table.reservoir_volume] = self.meta_data.reservoir_volume
        else:
            self.logger.error("No reservoir found. 1 l will be default. Please enter a volume for reservoir and press update meta data")
            self.meta_data.reservoir_volume = 1
            row[self.tp_table.reservoir_volume] = 1
        row[self.tp_table.sample_id] = self.meta_data.sample_id
        row[self.tp_table.cycle_number_flag] = self.cycling_flag
        row[self.tp_table.h2_uptake_flag] = self.h2_uptake_flag
        df_Tp = pd.DataFrame([row])
        return df_Tp

    def _de_hyd_state_checker(self, is_pressure, eq_pressure, temperature):
        """
        Checks and updates the dehydrogenation/hydrogenation state based on the current pressure, equilibrium pressure, and temperature.

        Parameters
        ----------
        is_pressure : float
            Current pressure value.
        eq_pressure : float
            Equilibrium pressure value.
        temperature : float
            Current temperature value.
        """


        state_empty_bool = (self.de_hyd_state == 'NaN' or self.de_hyd_state is None)
        thermo_element_connected_bool = temperature < self.temperature_no_thermoelement
        if self.meta_data.min_temperature_cycling:
            temperature_kinetics_bool = temperature > self.meta_data.min_temperature_cycling
        else:
            temperature_kinetics_bool = True

        if state_empty_bool:
            previous_state = "Dehydrogenated"
        else:
            previous_state = self.de_hyd_state

        if (self.cycling_flag and state_empty_bool
                and temperature_kinetics_bool
                and thermo_element_connected_bool):
            if self.cycle == 0:
                previous_state = "Dehydrogenated"
            elif is_pressure > eq_pressure:
                previous_state = 'Hydrogenated'
            elif is_pressure < eq_pressure:
                previous_state = 'Dehydrogenated'

        if (self.cycling_flag and temperature_kinetics_bool
                and thermo_element_connected_bool):

            tolerance = eq_pressure * 1e-2
            if abs(is_pressure - eq_pressure) < tolerance:
                self.de_hyd_state = previous_state
            elif is_pressure > eq_pressure + tolerance:
                self.de_hyd_state = 'Hydrogenated'
            elif is_pressure < eq_pressure - tolerance:
                self.de_hyd_state = 'Dehydrogenated'
             # Check for transition from Hydrogenated to Dehydrogenated

            if previous_state != self.de_hyd_state:
                self._on_cycle_number_changed()
                self.meta_data.total_number_cycles = self.cycle
                self.meta_data.last_de_hyd_state = self.de_hyd_state
                self.meta_data.write()
                self.logger.info(f"State changed from {previous_state} to {self.de_hyd_state}. Current cycle-#{self.cycle}")
        else:
            self.de_hyd_state = previous_state

    def _on_cycle_number_changed(self):
        """
        Handles changes in cycle number by starting a thread to estimate H2-Uptake and updates cycle_data table in database.
        """
        cycle_counter = CycleCounter(meta_data=self.meta_data,
                                     current_cycle=self.cycle,
                                     current_state=self.de_hyd_state,
                                     db_conn_params=self.db_conn_params)
        cycle_counter_thread = threading.Thread(target=cycle_counter.count, daemon=True)
        cycle_counter_thread.start()
        cycle_counter_thread.join()
        self.cycle = cycle_counter.cycle

    def _new_test_handling(self):
        """sets dehydrogenation state and cycle number to defaul values on test start"""
        self.cycle = 0
        self.de_hyd_state = "Dehydrogenated"

    def on_meta_data_changed(self, new_meta_data):
        """
        Handles changes in metadata by updating the instance and equilibrium calculator.

        Parameters
        ----------
        new_meta_data : MetaData
            New MetaData instance.
        """
        self.meta_data = new_meta_data
        self.eq_calculator.meta_data = self.meta_data
        self.logger.info(f"Updated meta_data in ModbusHandler {self.meta_data.sample_id}")


class CycleCounter:
    """
    Counts and processes hydrogenation and dehydrogenation cycles.
    """

    def __init__(self, meta_data: MetaData, current_cycle: float, current_state: str, db_conn_params):
        """
        Initializes the CycleCounter.

        Parameters:
            meta_data (MetaData): Metadata for the sample.
            current_cycle (int): The current cycle number.
            current_state (str): The current de/hydrogenation state.
        """
        self.db_conn_params = db_conn_params
        self.meta_data = meta_data
        self.cycle = current_cycle or 0
        self.current_state = current_state
        self.tp_table = TableConfig().TPDataTable
        self.cycle_table = TableConfig().CycleDataTable
        self.logger = logging.getLogger(__name__)
        self.data_retriever = DataRetriever(db_conn_params=db_conn_params)
        self.temp_tolerance = 10
        self.time_start_whole_cycle = None
        self.time_end_whole_cycle = None
        self.time_start_current_cycle = None
        self.cycle_line = pd.DataFrame()

    def count(self, mode=STANDARD_MODE):
        """
        Counts and processes the cycles, updating the cycle number and writing to the database.

        Returns:
            int: Updated cycle number.
        """
        # Handle the first hydrogenation cycle
        if self.cycle == 0:
            self._handle_first_hydrogenation()
            return self.cycle

        # Retrieve and separate cycle data
        cycle_data = self._retrieve_cycle_data()
        if not cycle_data:
            return self.cycle

        df_one_cycle, df_current_cycle, df_previous_cycle, is_uptake = cycle_data

        if df_current_cycle.empty or df_previous_cycle.empty:
            self.logger.warning("Insufficient data for uptake calculation.")
            return self.cycle

        cycle_long_enough_bool = self._is_valid_cycle_duration(df_current_cycle=df_current_cycle, df_previous_cycle=df_previous_cycle)
        if not cycle_long_enough_bool:
            self.logger.info("cycle not long enough")
            #treat_cycle_bool is true in case cycle should be handled. Will be true if matches manual wrote exceptions
            treat_cycle_bool = self._on_cycle_too_short(df=df_current_cycle, mode=mode)
            if not treat_cycle_bool:
                return self.cycle

        # Calculate H2 uptake

        line_hyd, line_dehyd = self._get_extreme_values(df_current_cycle, df_previous_cycle)


        wt_percent = self._calculate_uptake(line_hyd, line_dehyd, is_uptake)
            # Create new cycle entry
        self.cycle_line = self._create_cycle_entry(
            wt_percent=wt_percent,
            line_hyd=line_hyd,
            line_dehyd=line_dehyd,
            current_state=self._find_majority_state(df_current_cycle)
            )


        self._write_cycle_to_database()
        self.cycle += 0.5
        return self.cycle

    def _handle_first_hydrogenation(self):
        """
        Handles the first hydrogenation process.
        """
        cycle_data = self._retrieve_cycle_data(special_case=True)
        df_one_cycle, df_current_cycle, df_previous_cycle, is_uptake = cycle_data
        time_first_hyd = df_one_cycle[self.tp_table.time].iloc[-1]
        self.cycle += 0.5
        self.meta_data.first_hydrogenation = time_first_hyd
        self.meta_data.write()
        self.logger.info(
            f"First hydrogenation for {self.meta_data.sample_id} set to {self.meta_data.first_hydrogenation}"
        )

    def _retrieve_cycle_data(self, special_case=False):
        """
        Retrieves and separates cycle data into current and previous cycles.

        Returns:
            tuple: (pd.DataFrame, pd.DataFrame, pd.DataFrame, bool) """

        prev_cycle = self.cycle-0.5 if not special_case else self.cycle

        df_one_cycle = self._retrieve_cycle_data_by_cycle_number(prev_cycle=prev_cycle)

        if df_one_cycle.empty:
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), False

        # Separate current and previous cycles
        df_current_cycle = df_one_cycle[df_one_cycle[self.tp_table.cycle_number] == self.cycle]
        df_previous_cycle = df_one_cycle[df_one_cycle[self.tp_table.cycle_number] == prev_cycle]

        # Determine if uptake calculation is needed
        is_uptake = df_one_cycle[self.tp_table.h2_uptake_flag].any()

        return df_one_cycle, df_current_cycle, df_previous_cycle, is_uptake

    def _retrieve_cycle_data_by_cycle_number(self, prev_cycle):
        """
        Retrieves and separates cycle data into current and previous cycles.

        Returns:
            pd.DataFrame """

        try:
            df_one_cycle = self.data_retriever.fetch_data_by_cycle(
                sample_id=self.meta_data.sample_id,
                cycle_numbers=[prev_cycle, self.cycle]
            )
            if df_one_cycle.empty:
                self.logger.warning("No data found for the current and previous cycles.")

                return pd.DataFrame()

            return df_one_cycle
        except Exception as e:
            self.logger.error(f"Error retrieving cycle data: {e}")
            return pd.DataFrame()

    def _get_extreme_values(self, df_current, df_previous):
        """
        Gets the extreme temperature and pressure values for hydrogenation and dehydrogenation.

        Parameters:
            df_current (DataFrame): Current cycle data.
            df_previous (DataFrame): Previous cycle data.

        Returns:
            tuple: (line_hyd, line_dehyd)
        """
        line_hyd = pd.DataFrame()
        line_dehyd = pd.DataFrame()
        df_hyd, df_dehyd = self._separate_hyd_dehyd(df_current, df_previous)
        if df_hyd.empty or df_dehyd.empty:
            self.logger.error("Unable to separate hydrogenation and dehydrogenation data.")
            return None, None

        # Get minimum temperature for hydrogenation
        if self.meta_data.min_temperature_cycling:
            min_temp_hyd = df_hyd.loc[df_hyd[self.tp_table.setpoint_sample] > self.meta_data.min_temperature_cycling, self.tp_table.setpoint_sample].min()
        else:
            min_temp_hyd = df_hyd.loc[df_hyd[self.tp_table.setpoint_sample] != 0, self.tp_table.setpoint_sample].min()

        mask_min_temp = self._temperature_mask(df_hyd, min_temp_hyd)

        # Get maximum temperature for dehydrogenation
        max_temp_dehyd = df_dehyd[self.tp_table.setpoint_sample].max()
        mask_max_temp = self._temperature_mask(df_dehyd, max_temp_dehyd)

        df_min_temp = df_hyd[mask_min_temp]
        df_max_temp = df_dehyd[mask_max_temp]

        # Get pressure values
        if not df_min_temp.empty:
            line_hyd = df_min_temp.loc[df_min_temp[self.tp_table.pressure].idxmin()]
        if not df_max_temp.empty:
            line_dehyd = df_max_temp.loc[df_max_temp[self.tp_table.pressure].idxmax()]

        return line_hyd, line_dehyd

    def _separate_hyd_dehyd(self, df_current, df_previous):
        """
        Separates the data into hydrogenation and dehydrogenation DataFrames.

        Parameters:
            df_current (DataFrame): Current cycle data.
            df_previous (DataFrame): Previous cycle data.

        Returns:
            tuple: (df_hyd, df_dehyd)
        """
        majority_state_current = self._find_majority_state(df_current)
        majority_state_previous = self._find_majority_state(df_previous)

        if majority_state_current == "Hydrogenated":
            return df_current, df_previous
        else:
            return df_previous, df_current

    def _find_majority_state(self, df):
        """
        Finds the majority state in the DataFrame.

        Parameters:
            df (DataFrame): The data.

        Returns:
            str: "Hydrogenated" or "Dehydrogenated"
        """
        state_counts = df[self.tp_table.de_hyd_state].value_counts()
        if state_counts.empty:
            return None
        return state_counts.idxmax()

    def _temperature_mask(self, df, target_temp):
        """
        Creates a boolean mask for temperatures within tolerance.

        Parameters:
            df (DataFrame): The data.
            target_temp (float): The target temperature.

        Returns:
            Series: Boolean mask.
        """
        return (df[self.tp_table.temperature_sample] >= target_temp - self.temp_tolerance) & \
               (df[self.tp_table.temperature_sample] <= target_temp + self.temp_tolerance)

    def _calculate_uptake(self, hyd_vals, dehyd_vals, is_uptake):
        """
        Calculates the hydrogen uptake.

        Parameters:
            hyd_vals (Series): Hydrogenation values.
            dehyd_vals (Series): Dehydrogenation values.
            is_uptake (bool): Whether to calculate uptake.

        Returns:
            float: Hydrogen uptake in weight percent.
        """
        if not is_uptake or hyd_vals.empty or dehyd_vals.empty:
            return None


        eq_calculator = EqCalculator(meta_data=self.meta_data, db_conn_params=self.db_conn_params)

        try:
            wt_percent = eq_calculator.calc_h2_uptake(
                p_hyd=hyd_vals[self.tp_table.pressure],
                T_hyd=hyd_vals[self.tp_table.temperature_sample],
                p_dehyd=dehyd_vals[self.tp_table.pressure],
                T_dehyd=dehyd_vals[self.tp_table.temperature_sample],
                m_sample=self.meta_data.sample_mass,
                V_cell=self.meta_data.volume_measurement_cell,
                V_res=hyd_vals[self.tp_table.reservoir_volume]
            )
            return wt_percent
        except Exception as e:
            self.logger.error(f"Error calculating hydrogen uptake: {e}")
            return None

    def _create_cycle_entry(self, wt_percent, line_hyd, line_dehyd,
                            current_state) -> pd.Series:
        """
        Creates a new cycle entry.

        Parameters:
            wt_percent (float): Hydrogen uptake.
            line_hyd (Series): Hydrogenation values.
            line_dehyd (Series): Dehydrogenation values.
            current_state (str): Current state.

        Returns:
            Series: New cycle entry.
        """
        if not line_hyd.empty and not line_dehyd.empty:
            cycle_entry = pd.Series({
                self.cycle_table.sample_id: self.meta_data.sample_id,
                self.cycle_table.time_start: self.time_start_current_cycle,
                self.cycle_table.time_end: self.time_end_whole_cycle,
                self.cycle_table.cycle_duration: str(self.time_end_whole_cycle - self.time_start_current_cycle),
                self.cycle_table.pressure_min: line_hyd[self.tp_table.pressure],
                self.cycle_table.pressure_max: line_dehyd[self.tp_table.pressure],
                self.cycle_table.temperature_min: line_hyd[self.tp_table.temperature_sample],
                self.cycle_table.temperature_max: line_dehyd[self.tp_table.temperature_sample],
                self.cycle_table.volume_reservoir: line_hyd[self.tp_table.reservoir_volume],
                self.cycle_table.volume_cell: self.meta_data.volume_measurement_cell,
                self.cycle_table.h2_uptake: wt_percent,
                self.cycle_table.de_hyd_state: current_state,
                self.cycle_table.cycle_number: self.cycle,
                self.cycle_table.time_min: line_hyd[self.tp_table.time],
                self.cycle_table.time_max: line_dehyd[self.tp_table.time]
            })
            return cycle_entry

        else:
            cycle_entry = pd.Series({
                self.cycle_table.sample_id: self.meta_data.sample_id,
                self.cycle_table.time_start: self.time_start_whole_cycle,
                self.cycle_table.time_end: self.time_end_whole_cycle,
                self.cycle_table.cycle_duration: str(self.time_end_whole_cycle - self.time_start_whole_cycle),
                self.cycle_table.pressure_min: None,
                self.cycle_table.pressure_max: None,
                self.cycle_table.temperature_min: None,
                self.cycle_table.temperature_max: None,
                self.cycle_table.volume_reservoir: None,
                self.cycle_table.volume_cell: self.meta_data.volume_measurement_cell,
                self.cycle_table.h2_uptake: wt_percent,
                self.cycle_table.de_hyd_state: current_state,
                self.cycle_table.cycle_number: self.cycle,
                self.cycle_table.time_min: None,
                self.cycle_table.time_max: None
            })
            self.logger.info(f"Couldn't determine uptake data for cycle #{self.cycle}. "
                             f"Will be set to none in table.")
            return cycle_entry

    def _write_cycle_to_database(self):
        """
        Writes the cycle data to the database.
        """
        try:
            mb_writer = ModbusDBWriter(meta_data=self.meta_data, db_conn_params=self.db_conn_params)
            mb_writer.write_cycle_to_table(
                new_line_cycle=self.cycle_line,
                time_start=self.time_start_whole_cycle,
                time_end=self.time_end_whole_cycle,
                time_start_half_cycle=self.time_start_whole_cycle,
                time_end_half_cycle=self.time_end_whole_cycle
            )
            self.logger.info(f"Cycle {self.cycle} data written to database.")
        except Exception as e:
            self.logger.error(f"Error writing cycle data to database: {e}")

    def _is_valid_cycle_duration(self, df_current_cycle, df_previous_cycle):
        if not self.meta_data.average_cycle_duration:
            self.logger.info("No cycle duration set. Please give an estimation for meta data")
            return True
        if df_current_cycle.empty or df_previous_cycle.empty:
            return False

        # Get the start and end times of the cycle
        start_time_prev = df_previous_cycle[self.tp_table.time].min()
        end_time_prev = df_previous_cycle[self.tp_table.time].max()
        start_time_current = df_current_cycle[self.tp_table.time].min()
        end_time_current = df_current_cycle[self.tp_table.time].max()


        #prev_cycle_long_enough_bool = end_time_prev-start_time_prev >= self.meta_data.average_cycle_duration
        current_cycle_long_enough_bool = end_time_current-start_time_current >= self.meta_data.average_cycle_duration
        full_cycle_long_enough = end_time_current - start_time_prev >= self.meta_data.average_cycle_duration

        # Store the times for potential use
        self.time_start_whole_cycle = start_time_prev
        self.time_end_whole_cycle = end_time_current
        self.time_start_current_cycle = start_time_current

        # Check if duration meets the minimum requirement
        return (current_cycle_long_enough_bool and full_cycle_long_enough)

    def _on_cycle_too_short(self, df, mode=STANDARD_MODE):
        """
        Handles cases where the cycle duration is too short.

        Parameters
        ----------
        df : pandas.DataFrame
             containing the cycle data of the current just finished cycle.
        """
        treat_cycle_bool = False
        time_start = df[self.tp_table.time].min()
        time_end = df[self.tp_table.time].max()
        time_range_to_update = (time_start, time_end)
        if mode == STANDARD_MODE:
            treat_cycle_bool = self._handle_too_short_cycles_recording(df=df,  time_range_to_update=time_range_to_update)
        elif mode == 'CSV_Recorder':
            treat_cycle_bool = self._handle_too_short_cycles_csv_recorder(df=df, time_range_to_update=time_range_to_update)

        return treat_cycle_bool

    def _handle_too_short_cycles_csv_recorder(self, df: pd.DataFrame, time_range_to_update: tuple):
        treat_cycle_bool = False
        time_exceptions = self._get_exceptions("time")
        condition_treat_cycle = (
                            (min(time_range_to_update) >= time_exceptions['start_time'] - timedelta(minutes=5)) &
                            (min(time_range_to_update) <= time_exceptions['end_time'] + timedelta(minutes=5))
                        )
        if condition_treat_cycle.any():
            self.logger.info(f"Exception at {time_exceptions['start_time']} "
                             f"till {time_exceptions['end_time']} "
                             f"detected. Will be treated as real cycle")
            treat_cycle_bool = True

            return treat_cycle_bool

        end_time_cycle = max(time_range_to_update)
        query_update_rest_of_table = (f"UPDATE {self.tp_table.table_name} SET "
                                      f"{self.tp_table.cycle_number} = {self.tp_table.cycle_number} - 1 "
                                      f"WHERE {self.tp_table.time} > %s")
        values = (end_time_cycle,)

        DataBaseManipulator(db_conn_params=self.db_conn_params).execute_updating(query=query_update_rest_of_table,
                                               values=values,
                                               many_bool=False)


        treat_cycle_bool = self._handle_too_short_cycles_recording(df=df,
                                                time_range_to_update=time_range_to_update)

        return treat_cycle_bool

    def _handle_too_short_cycles_recording(self, df: pd.DataFrame, time_range_to_update: tuple):
        treat_cycle_bool = False
        de_hyd_state_to_update = self._get_state_to_overwrite(df=df)
        series_to_update = self._create_series_to_overwrite(de_hyd_state=de_hyd_state_to_update)
        db_manipulator = DataBaseManipulator(db_conn_params=self.db_conn_params)

        update_success_flag = db_manipulator.update_data(table=self.tp_table,
                                                         sample_id=self.meta_data.sample_id,
                                                         update_df=series_to_update,
                                                         update_between_vals=time_range_to_update,
                                                         col_to_match=self.tp_table.time)
        if update_success_flag:
            # reduce cycle by half a cycle
            self.cycle -= 0.5
            self.logger.info(f"Too short cycle detetected. Cycle number will stay {self.cycle}")
            return treat_cycle_bool
        else:
            self.logger.error(f"Error in handling too short cycle. Will be calculated as regular cycle")
            treat_cycle_bool = True
            return treat_cycle_bool

    def _get_exceptions(self, type="time"):
        if type == 'time':
            #WAE-WA-030
            exceptions_data = {
                'start_time': [
                    datetime(2022, 5, 12, 3, 46, tzinfo=local_tz),

                ],
                'end_time': [
                    datetime(2022, 5, 12, 14, 53, tzinfo=local_tz),

                ]
            }
            exceptions_data = pd.DataFrame(exceptions_data)
            return exceptions_data

    def _get_state_to_overwrite(self, df):
        de_hyd_state_to_update = ""
        # check if all entries in de_hyd_state_column are equal. inform if not
        if not df[self.tp_table.de_hyd_state].eq(df[self.tp_table.de_hyd_state].iloc[0]).all():
            self.logger.error("Cycle seems to contain mixed de_hyd_state_values. Majority occurence will be picked")
            de_hyd_state_just_finished_cycle = df[self.tp_table.de_hyd_state].value_counts().idxmax()
        else:
            de_hyd_state_just_finished_cycle = df[self.tp_table.de_hyd_state].iloc[0]

        if de_hyd_state_just_finished_cycle == STATE_HYD:
            de_hyd_state_to_update = STATE_DEHYD
        elif de_hyd_state_just_finished_cycle == STATE_DEHYD:
            de_hyd_state_to_update = STATE_HYD

        return de_hyd_state_to_update

    def _create_series_to_overwrite(self, de_hyd_state: str):
        series_to_update = pd.Series()
        series_to_update[self.tp_table.cycle_number] = self.cycle - 0.5
        series_to_update[self.tp_table.de_hyd_state] = de_hyd_state
        series_to_update[self.tp_table.cycle_number_flag] = False

        return series_to_update


class ModbusDBWriter:
    def __init__(self, meta_data, db_conn_params):
        self.db_conn_params = db_conn_params
        self.logger = logging.getLogger(__name__)
        self.tp_table = TableConfig().TPDataTable
        self.qb = QueryBuilder(db_conn_params=self.db_conn_params)
        self.meta_data = meta_data

    def insert_data_into_table(self, cursor, data, mode=STANDARD_MODE):
        if data.empty:
            self.logger.error("No data to write")
            return

        # Set the start time if it doesn't exist
        if not self.meta_data.start_time:
            if mode == STANDARD_MODE:
                self.meta_data.start_time = datetime.now(tz=local_tz)
            else:
                self.meta_data.start_time = data[self.tp_table.time].min()
            self.meta_data.write()

        self.meta_data.end_time = datetime.now(tz=local_tz)
        self.meta_data.write(quiet=True)

        # Generate the insert query
        insert_query = self.qb.create_writing_query(
            table_name=self.tp_table.table_name,
            column_names=data.columns.tolist()
        )

        # Prepare data as a list of tuples (handles both single and multiple rows)
        data_tuples = list(data.itertuples(index=False, name=None))

        try:
            # Use executemany even if it's a single row (works for both cases)
            cursor.executemany(insert_query, data_tuples)
            cursor.connection.commit()
            # self.logger.info("Data inserted")
        except Exception as e:
            self.logger.error("Error occurred while inserting data: %s", e)
            cursor.connection.rollback()
            raise

    def _delete_data_from_tp_table(self, data_to_delete_time=None, time_min=None, time_max=None):

        with DatabaseConnection(**self.db_conn_params) as db_conn:
            self.logger.info("Starting deletion of data from %s data base...", self.tp_table.table_name)
            # Assuming data_to_delete is a DataFrame or list of identifiers (like primary keys) of the rows to be deleted
            try:
                if data_to_delete_time is not None:
                    data_to_delete_time = [(dt,) for dt in data_to_delete_time]
                    delete_query = (f"DELETE FROM {self.tp_table.table_name} "
                                    f"WHERE {self.tp_table.time} = %s")  # Replace with your table and column names
                    db_conn.cursor.executemany(delete_query, (data_to_delete_time,))
                    db_conn.cursor.connection.commit()
                elif time_min is not None and time_max is not None:
                    delete_query = (f"DELETE FROM {self.tp_table.table_name} WHERE {self.tp_table.time} "
                                    f"BETWEEN %s AND %s")  # Replace with your table and column names
                    db_conn.cursor.execute(delete_query, (time_min, time_max))
                    db_conn.cursor.connection.commit()
                self.logger.info("Data deleted successfully")
            except Exception as e:
                self.logger.error("Error occurred while deleting data: %s", e)
                db_conn.cursor.connection.rollback()

    def write_cycle_to_table(self, new_line_cycle, time_start, time_end, time_start_half_cycle, time_end_half_cycle):
        cycle_table = TableConfig().CycleDataTable
        t_p_table = TableConfig().TPDataTable

        with DatabaseConnection(**self.db_conn_params) as db_conn:
            data_written = self._write_new_line_cycle(new_line_cycle=new_line_cycle,
                                                       cycle_table=cycle_table,
                                                       time_start=time_start,
                                                       time_end=time_end,
                                                       cursor=db_conn.cursor)

        if data_written:
            with DatabaseConnection(**self.db_conn_params) as db_conn:
                self._update_cycle_t_p_table(new_line_cycle=new_line_cycle,
                                             t_p_table=t_p_table,
                                             time_start=time_start_half_cycle,
                                             time_end=time_end_half_cycle,
                                             cycle_table=cycle_table,
                                             cursor=db_conn.cursor)
        self._update_end_time_meta_data(time_end=time_end_half_cycle)

    def _write_new_line_cycle(self, new_line_cycle: pd.Series, cycle_table: TableConfig(),
                              time_start, time_end, cursor: DatabaseConnection().cursor):


        cycle_table_name = cycle_table.table_name
        insert_query = self.qb.create_writing_query(table_name=cycle_table_name,
                                                    column_names=(new_line_cycle.index.tolist()))
        values = [convert_value(value) for _, value in new_line_cycle.items()]

        try:
            cursor.execute(insert_query, values)
            cursor.connection.commit()
            self.logger.info(f"Data inserted into {cycle_table_name}: \n{new_line_cycle}")
            return True
        except IntegrityError as e:

            self.logger.error("Error occurred while inserting data method _write_new_line_cycle: %s", e)
            with DatabaseConnection(**self.db_conn_params) as db_conn:
                self._delete_line_cycle(cursor=db_conn.cursor,
                                        time_data=new_line_cycle[cycle_table.time_start],
                                        time_col=cycle_table.time_start,
                                        table_name=cycle_table_name)
                self._write_new_line_cycle(new_line_cycle=new_line_cycle,
                                           cycle_table=cycle_table,
                                           time_start=time_start, time_end=time_end,
                                           cursor=db_conn.cursor)

        except Exception as e:
            self.logger.error("Error occurred while inserting data method _write_new_line_cycle: %s", e)
            return False

    def _update_cycle_t_p_table(self, new_line_cycle: pd.Series, t_p_table: TableConfig(),
                                time_start, time_end, cycle_table: TableConfig(), cursor: DatabaseConnection().cursor):

        t_p_table_name = t_p_table.table_name

        update_tp_query = f"UPDATE {t_p_table_name} SET {t_p_table.h2_uptake} = %s" \
                          f" WHERE {t_p_table.time} BETWEEN %s AND %s"
        value = convert_value(new_line_cycle[cycle_table.h2_uptake])

        try:
            cursor.execute(update_tp_query, (value, time_start, time_end))
            cursor.connection.commit()
            self.logger.info(f"{t_p_table_name} updated for cycle #{new_line_cycle[cycle_table.cycle_number]-0.5}: "
                             f"{t_p_table.h2_uptake} = {new_line_cycle[cycle_table.h2_uptake]}")
        except Exception as e:
            self.logger.error(f"Error occurred while updating {t_p_table_name} in method _update_cycle_t_p_table: %s", e)
            cursor.connection.rollback()

    def _delete_line_cycle(self, time_data, table_name: TableConfig(),
                           time_col: str, cursor):
        self.logger.info(f"Start deletion from {table_name}")
        delete_query = f"DELETE from {table_name} WHERE {time_col} = %s"
        try:
            with DatabaseConnection(**self.db_conn_params) as db_conn:
                db_conn.cursor.execute(delete_query, (convert_value(time_data),))
                db_conn.cursor.connection.commit()
                self.logger.info("Data deleted successfully")
        except Exception as e:
            self.logger.error("Error occurred while deleting data: %s", e)

    def _update_end_time_meta_data(self, time_end):
        if self.meta_data.end_time:
            if self.meta_data.end_time < time_end:
                self.meta_data.end_time = time_end
                self.meta_data.write()
        else:
            self.meta_data.end_time = time_end
            self.meta_data.write()


#global methods
def convert_value(value):
    # Convert NumPy types
    if isinstance(value, (np.integer, np.int64)):
        return int(value)
    elif isinstance(value, (np.floating, np.float64)):
        return float(value)
    # Convert Pandas Timestamp
    elif isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    # Convert Pandas Timedelta
    elif isinstance(value, pd.Timedelta):
        return str(value)  # Or value.total_seconds()
    else:
        return value


def test_mb_reading_writing(sample_id):
    from src.config_connection_reading_management.config_reader import GetConfig
    config = GetConfig()
    meta = MetaData(sample_id=sample_id, db_conn_params=config.db_conn_params)
    mb_reader = ModbusReader(mb_reading_params=config.mb_reading_params, mb_conn_params=config.mb_conn_params)

    mb_processor = ModbusDataHandler(meta_data=meta, db_conn_params=config.db_conn_params)
    mb_writer = ModbusDBWriter(meta_data=meta, db_conn_params=config.db_conn_params)
    i = 10

    with ModbusConnection(**config.mb_conn_params) as modbus_connection, DatabaseConnection(**config.db_conn_params) as db_conn:
        while i > 0:
            df = mb_reader.read_from_dicon(modbus_connection.client,
                                           **config.mb_reading_params)
            for index, row in df.iterrows():
                df_tp = mb_processor.process_data(index, row)

            mb_writer.insert_data_into_table(cursor=db_conn.cursor, data=df_tp)
            time.sleep(1)
            i = i-1
            print(df)

if __name__ == "__main__":


    sample_id = "test-simulator-01"
    test_mb_reading_writing(sample_id=sample_id)


