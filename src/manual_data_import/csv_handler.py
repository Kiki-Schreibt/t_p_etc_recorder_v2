import logging

import numpy as np
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, List

import pandas as pd
from psycopg2 import IntegrityError

from src.calculations.eq_p_calculation import VantHoffCalcEq as EqCalculator
from src.meta_data.meta_data_handler import MetaData
from src.config_connection_reading_management.modbus_handler import ModbusDBWriter, CycleCounter
from src.config_connection_reading_management.database_reading_writing import DataBaseManipulator
from src.config_connection_reading_management.database_reading_writing import DataRetriever, write_ETC_in_parallel
from src.config_connection_reading_management.connections import DatabaseConnection
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging
from src.table_data import TableConfig

local_tz = ZoneInfo("Europe/Berlin")
REALISTIC_MAX_TEMP = 1000
REALISTIC_MIN_PRESSURE = 1.0
SUPPORTED_FILE_EXTENSIONS = ['.csv', '.txt']
STATE_HYD = 'Hydrogenated'
STATE_DEHYD = 'Dehydrogenated'
CYCLE_COUNTER_MODE = 'CSV_Recorder'
COMPRESSION_FACTOR = 30


class CSVProcessor:
    """
    Processes CSV files containing temperature and pressure data for a given sample.

    This class handles importing CSV files, processing the data, writing it to the database,
    and counting cycles based on the imported data.
    """

    def __init__(self, sample_id: str, folder_path: Optional[str] = None,
                full_file_path: Optional[str] = None, mode: str = 'auto',
                 compress_data: Optional[bool] = False, config=None):
        """
        Initializes the CSVProcessor with metadata and file paths.

        Args:
            sample_id (str): The sample identifier.
            folder_path (Optional[str]): The path to the folder containing CSV files.
            full_file_path (Optional[str]): The full path to a specific CSV file.
            mode (str): The mode of operation ('auto' or manual).
        """
        self.logger = logging.getLogger(__name__)
        try:
            self.config = config
            self.db_conn_params = config.db_conn_params
            self.mb_conn_params = config.mb_conn_params
            self.mb_reading_params = config.mb_reading_params
            self.hd_log_file_tracker_params = config.hd_log_file_tracker_params
        except Exception as e:
            self.logger.error(f"No config provided: {e}")
            raise
        self.meta_data = MetaData(sample_id=sample_id, db_conn_params=self.db_conn_params)
        self.folder_path = folder_path
        self.full_file_path = full_file_path
        if mode == 'auto':
            dir_t_p, dir_etc, reservoir_volume = get_folders_for_id(sample_id=sample_id)
            if not self.meta_data.reservoir_volume:
                self.meta_data.reservoir_volume = reservoir_volume
                self.reset_meta_data(meta_data=self.meta_data)
            self.folder_path = dir_t_p

        self.csv_importer = CSVImporter()
        self.csv_handler = CSVDataHandler(meta_data=self.meta_data, config=self.config)
        self.csv_writer = CSVWriter(meta_data=self.meta_data, compress_data=compress_data, config=self.config)

    def process(self, init_state: str = STATE_DEHYD) -> None:
        """
        Processes CSV files and counts cycles.

        Args:
            init_state (str): The initial state for cycle counting ('Hydrogenated' or 'Dehydrogenated').
        """

        self.logger.info(f"Start importing temperature and pressure data for {self.meta_data.sample_id}")
        # Initialize the file count and a thread-safe lock

        if self.full_file_path:
            self._read_process_write(full_file_path=self.full_file_path)
            files_processed = 1
        elif self.folder_path:
            file_names = [
                file_name for file_name in os.listdir(self.folder_path)
                if any(file_name.endswith(ext) for ext in SUPPORTED_FILE_EXTENSIONS)
            ]

            files_processed = self._read_process_write_threading(file_names=file_names)

        self.logger.info(f"Temperature and pressure data for {self.meta_data.sample_id} imported. {files_processed} files processed in total")

        self.count_cycles(init_state=init_state, sample_id=sample_id)

    def _read_process_write_threading(self, file_names: List[str]) -> int:
        """
        Processes multiple CSV files using multithreading.

        Args:
            file_names (List[str]): A list of CSV file names to process.

        Returns:
            int: The number of files successfully processed.
        """
        # Use ThreadPoolExecutor to process files in parallel
        files_processed = 0
        files_processed_lock = Lock()
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(self._read_process_write,
                                file_name,
                                None,
                                self.folder_path): file_name
                for file_name in file_names
            }

            # Process results as they complete
            for future in as_completed(futures):
                file_name = futures[future]
                try:
                    future.result()  # To catch any exceptions raised
                    # Increment the counter in a thread-safe way
                    with files_processed_lock:
                        files_processed += 1
                        self.logger.info(f"Temperature and pressure data for {self.meta_data.sample_id} imported. {files_processed} files processed")
                except Exception as e:
                    self.logger.error(f"Error occurred while processing {file_name}: {e}")

        return files_processed

    def _read_process_write(self, file_name: Optional[str] = None,
                            full_file_path: Optional[str] = None,
                            file_path: Optional[str] = None) -> None:
        """
        Reads, processes, and writes a single CSV file to the database.

        Args:
            file_name (Optional[str]): The name of the CSV file.
            full_file_path (Optional[str]): The full path to the CSV file.
            file_path (Optional[str]): The path to the directory containing the CSV file.
        """
        try:
            df = self.csv_importer.import_csv(file_name=file_name, file_path=file_path, full_file_path=full_file_path)
            df_processed = self.csv_handler.process(df=df)
            self.csv_writer.write_to_database(df=df_processed, file_name=file_name)
            file_reference = file_name or full_file_path
            self.logger.info(f"{file_reference} written to database.")

        except Exception as e:
            file_reference = file_name or full_file_path
            self.logger.error(f"Error occurred while processing {file_reference}: {e}")

    def reset_meta_data(self, meta_data: MetaData) -> None:
        """
        Resets the metadata and updates dependent components.

        Args:
            meta_data (MetaData): The metadata object to use.
        """
        self.meta_data = meta_data
        self.meta_data.write()
        # Update only the necessary parts, like CSV handler, etc.
        self.csv_handler.meta_data = meta_data
        self.csv_writer.meta_data = meta_data
        self.logger.info("Meta data for CSVProcessor and subclasses updated")

    def count_cycles(self, sample_id: str, init_state: str = STATE_DEHYD) -> None:
        """
        Counts cycles based on the processed data.

        Args:
            sample_id (str): The sample identifier.
            init_state (str): The initial state for cycle counting ('Hydrogenated' or 'Dehydrogenated').
        """
        csv_counter = CSVCounter(config=self.config)
        csv_counter.count(sample_id=sample_id, init_state=init_state)


class CSVImporter:
    """
    Imports CSV files and translates them into the required DataFrame format.
    """

    def __init__(self):
        """
        Initializes the CSVImporter with a logger and table configuration.
        """
        self.logger = logging.getLogger(__name__)
        self.tp_table = TableConfig().TPDataTable

    def import_csv(self, file_name: Optional[str] = None, file_path: Optional[str] = None,
        full_file_path: Optional[str] = None, jumo_version: str = 'standard') -> pd.DataFrame():
        """
        Imports a CSV file and processes it according to the specified Jumo version.

        Args:
            file_name (Optional[str]): The name of the CSV file.
            file_path (Optional[str]): The path to the directory containing the CSV file.
            full_file_path (Optional[str]): The full path to the CSV file.
            jumo_version (str): The version of the Jumo format ('standard' by default).

        Returns:
            DataFrame: The processed DataFrame.
        """
        if full_file_path is None:
            full_file_path = os.path.join(file_path, file_name)

        df = self._read_csv(full_file_path=full_file_path)
        df_processed = self._translate_csv(df=df, jumo_version=jumo_version)

        return df_processed

    def _read_csv(self, full_file_path: str) -> pd.DataFrame:
        """
        Reads a CSV file into a DataFrame.

        Args:
            full_file_path (str): The full path to the CSV file.

        Returns:
            DataFrame: The read DataFrame.
        """
        try:
            df = pd.read_csv(full_file_path)
            return df
        except Exception as e:
            self.logger.error(f"Error occurred while importing CSV. {e}")
            return pd.DataFrame()

    def _translate_csv(self, df: pd.DataFrame, jumo_version: str = 'standard') -> pd.DataFrame:
        """
        Translates the CSV DataFrame into the required format.

        Args:
            df (DataFrame): The original DataFrame.
            jumo_version (str): The version of the Jumo format.

        Returns:
            DataFrame: The translated DataFrame.
        """
        if df.empty:
            return pd.DataFrame()

        try:
            if jumo_version == 'standard':
                df_processed = self._translate_csv_standard(df=df)
                return df_processed
            else:
                self.logger.error(f"Jumo version '{jumo_version}' not supported.")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(
                f"Error occurred while processing CSV during import for Jumo version '{jumo_version}'. {e}"
            )
            return pd.DataFrame()

    def _translate_csv_standard(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Translates a standard Jumo CSV file into the required format.

        Args:
            df (DataFrame): The original DataFrame.

        Returns:
            DataFrame: The translated DataFrame.
        """
        df = df.copy()
        df['Time'] = df['Time'].apply(self._correct_time_format)
        df['Time'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

        # Rename the columns
        df = df.rename(columns={'Time': self.tp_table.time})
        df[self.tp_table.time] = df[self.tp_table.time].dt.tz_localize(local_tz, ambiguous='NaT')
        df = df.dropna(subset=[self.tp_table.time])
        df = df.drop('Date', axis=1)
        df = df.rename(columns={
            'SampleTemp': 'temperature_sample',
            'HeaterTemp': 'temperature_heater',
            'Pressure': 'pressure',
            'TShouldSample': 'setpoint_sample'
        })
        df['setpoint_heater'] = None
        df['time'] = df['time'].where(pd.notna(df['time']), None)
        df = df.drop_duplicates(subset=['time'])
        df = df.dropna(subset=[self.tp_table.time])
        new_df = df[[
            self.tp_table.time,
            self.tp_table.pressure,
            self.tp_table.temperature_sample,
            self.tp_table.setpoint_sample,
            self.tp_table.temperature_heater,
            self.tp_table.setpoint_heater
        ]]

        return new_df

    @staticmethod
    def _correct_time_format(time_str: str) -> str:
        """
        Corrects time format issues in the CSV data.

        Args:
            time_str (str): The time string to correct.

        Returns:
            str: The corrected time string.
        """
        if '.' in time_str and len(time_str) < 8:
            # If the time is unusual (like ".890"), replace it with midnight ("00:00:00")
            return '00:00:00' + time_str
        else:
            return time_str


class CSVDataHandler:
    """
    Handles processing of the imported CSV data for cycle counting.
    """

    def __init__(self, meta_data: MetaData, config):
        """
        Initializes the CSVDataHandler with metadata.

        Args:
            meta_data (MetaData): The metadata object containing sample information.
        """
        self.current_de_hyd_state = STATE_DEHYD
        self.tp_table = TableConfig().TPDataTable
        self.meta_data = meta_data
        self.eq_calculator = EqCalculator(meta_data=meta_data, db_conn_params=config.db_conn_params)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes the DataFrame by adding necessary flags and conditioning for cycle counting.

        Args:
            df (DataFrame): The DataFrame to process.

        Returns:
            DataFrame: The processed DataFrame.
        """
        df['sample_id'] = self.meta_data.sample_id

        df_conditioned = self._conditioning_for_cycle_count(df=df)
        df_conditioned = self._treat_special_cases(df=df_conditioned, mode='res_volume')
        return df_conditioned

    def _conditioning_for_cycle_count(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds necessary columns and flags to the DataFrame for cycle counting.

        Args:
            df (DataFrame): The original DataFrame.

        Returns:
            DataFrame: The conditioned DataFrame.
        """
        df = df.copy()
        if df.empty:
            return pd.DataFrame()
        # Pre-process the DataFrame
        self._add_equilibrium_pressure(df)
        self._add_cycle_flags(df)

        # Apply conditions for hydrogenation and dehydrogenation states
        condition_is_cycle = self._condition_is_cycle(df)
        condition_uptake = self._condition_uptake(df, condition_is_cycle)


        df[self.tp_table.h2_uptake_flag] = False
        df[self.tp_table.cycle_number_flag] = False
        df.loc[condition_uptake, self.tp_table.h2_uptake_flag] = True
        df.loc[condition_is_cycle, self.tp_table.cycle_number_flag] = True
        self._set_hydrogenation_states(df)

        if pd.isna(df.loc[0, self.tp_table.de_hyd_state]) or df.loc[0, self.tp_table.de_hyd_state] in ["nan", "NaN"]:
            df.loc[0, self.tp_table.de_hyd_state] = self.current_de_hyd_state

        # Clean up 'nan' strings and handle missing values
        df[self.tp_table.de_hyd_state] = df[self.tp_table.de_hyd_state].replace('nan', pd.NA).ffill()
        check_nan_values(df, "conditioning for cycle count after ffill")
        # Update current state
        self.current_de_hyd_state = df[self.tp_table.de_hyd_state].iloc[-1]

        return df

    def _add_equilibrium_pressure(self, df: pd.DataFrame) -> None:
        """
        Adds the equilibrium pressure to the DataFrame.

        Args:
            df (DataFrame): The DataFrame to update.
        """
        #print(df[self.tp_table.setpoint_sample])
        df[self.tp_table.eq_pressure] = self.eq_calculator.calc_eq(df[self.tp_table.setpoint_sample])
        df[self.tp_table.eq_pressure] = self.eq_calculator.calc_eq(df[self.tp_table.temperature_sample])

    def _add_cycle_flags(self, df: pd.DataFrame) -> None:
        """
        Adds cycle length flags to the DataFrame.

        Args:
            df (DataFrame): The DataFrame to update.
        """
        df[self.tp_table.cycle_length_flag] = True

    def _condition_is_cycle(self, df: pd.DataFrame) -> pd.Series:
        """
        Determines whether each row meets the conditions to be part of a cycle.

        Args:
            df (DataFrame): The DataFrame to evaluate.

        Returns:
            Series: A boolean Series indicating cycle eligibility.
        """
        return (
            (df[self.tp_table.temperature_sample] > self.meta_data.min_temperature_cycling) &
            (df[self.tp_table.temperature_sample] < REALISTIC_MAX_TEMP) &
            (df[self.tp_table.pressure] > REALISTIC_MIN_PRESSURE) &
            (df[self.tp_table.setpoint_sample] > self.meta_data.min_temperature_cycling) &
            (df[self.tp_table.setpoint_sample] < REALISTIC_MAX_TEMP) &
            df[self.tp_table.cycle_length_flag]
        )

    def _condition_uptake(self, df: pd.DataFrame, condition_is_cycle: pd.Series) -> pd.Series:
        """
        Determines whether each row meets the conditions for hydrogen uptake.

        Args:
            df (DataFrame): The DataFrame to evaluate.
            condition_is_cycle (Series): The cycle eligibility conditions.

        Returns:
            Series: A boolean Series indicating uptake eligibility.
        """
        return (
            (df[self.tp_table.pressure] <= self.meta_data.max_pressure_cycling) &
            (df[self.tp_table.pressure] >= REALISTIC_MIN_PRESSURE) &
            condition_is_cycle
        )

    def _set_hydrogenation_states(self, df: pd.DataFrame) -> None:
        """
        Sets the hydrogenation or dehydrogenation state for each row.

        Args:
            df (DataFrame): The DataFrame to update.
        """
        eq_pressure_real = df[self.tp_table.eq_pressure]

        condition_hydrogenated = (
            (df[self.tp_table.pressure] > df[self.tp_table.eq_pressure]) &
            df[self.tp_table.cycle_number_flag] &
            (df[self.tp_table.pressure] > eq_pressure_real)
        )

        condition_dehydrogenated = (
            (df[self.tp_table.pressure] < df[self.tp_table.eq_pressure]) &
            df[self.tp_table.cycle_number_flag] &
            (df[self.tp_table.pressure] < df[self.tp_table.eq_pressure])
        )

        condition_first_hydrogenation = (
            df[self.tp_table.time] <= self.meta_data.first_hydrogenation
        )

        df.loc[condition_hydrogenated, self.tp_table.de_hyd_state] = STATE_HYD
        df.loc[condition_dehydrogenated, self.tp_table.de_hyd_state] = STATE_DEHYD
        df.loc[condition_first_hydrogenation, self.tp_table.de_hyd_state] = STATE_DEHYD

    def _treat_special_cases(self, df: pd.DataFrame, mode: str = 'res_volume') -> pd.DataFrame:
        """
        Applies special case treatments to the DataFrame.

        Args:
            df (DataFrame): The DataFrame to process.
            mode (str): The mode of special case treatment ('res_volume' by default).

        Returns:
            DataFrame: The updated DataFrame.
        """
        if df.empty:
            return pd.DataFrame()

        if mode == 'res_volume':
            if self.meta_data.sample_id == 'WAE-WA-030':
                df.loc[
                    df[self.tp_table.time] > datetime(2022, 8, 1, 15, 0, 0, tzinfo=local_tz),
                    'reservoir_volume'
                ] = 1
                df.loc[
                    df[self.tp_table.time] < datetime(2022, 8, 1, 15, 0, 0, tzinfo=local_tz),
                    'reservoir_volume'
                ] = 3.75
            else:
                df[self.tp_table.reservoir_volume] = self.meta_data.reservoir_volume

        if mode == 'cycle_length_exceptions':
            pass  # Implement as needed

        return df


class CSVWriter:
    """
    Writes processed CSV data to the database.
    """

    def __init__(self, meta_data: MetaData, compress_data=False, config=None):
        """
        Initializes the CSVWriter with metadata.

        Args:
            meta_data (MetaData): The metadata object containing sample information.
        """
        self.logger = logging.getLogger(__name__)
        try:
            self.db_conn_params = config.db_conn_params
            self.mb_conn_params = config.mb_conn_params
            self.mb_reading_params = config.mb_reading_params
            self.hd_log_file_tracker_params = config.hd_log_file_tracker_params
        except Exception as e:
            self.logger.error(f"No config provided: {e}")
            raise
        self.writer = ModbusDBWriter(meta_data=meta_data, db_conn_params=self.db_conn_params)
        self.tp_table = TableConfig().TPDataTable
        self.compress_data = compress_data

    def write_to_database(self, df: pd.DataFrame, file_name: str = '', after_deletion: str = '') -> None:
        """
        Writes the DataFrame to the database.

        Args:
            df (DataFrame): The DataFrame to write.
            file_name (str): The name of the file being processed.
            after_deletion (str): Additional info to log after deletion, if any.
        """
        if df.empty:
            return
        if self.compress_data:
            df_copy = df.iloc[::COMPRESSION_FACTOR]
            del df
            df = df_copy

        with DatabaseConnection(**self.db_conn_params) as db_conn:
            try:
                self.logger.info(f"Start writing {file_name} to database")
                self.writer.insert_data_into_table(data=df,
                                                   cursor=db_conn.cursor,
                                                   mode=CYCLE_COUNTER_MODE)
                self.logger.info(f'Data inserted into: {self.tp_table.table_name}  {after_deletion}')
            except IntegrityError:
                try:
                    self.writer._delete_data_from_tp_table(
                        time_min=df[self.tp_table.time].min(),
                        time_max=df[self.tp_table.time].max()
                    )
                    self.write_to_database(df=df, after_deletion=' after deletion')
                except Exception as e:
                    self.logger.error(f"Error occurred while deleting {file_name} from database {e}")
            except Exception as e:
                self.logger.error(f"Error occurred while writing {file_name} data to database: {e}")

    def write_cycles_to_database(self, df_cycles: pd.DataFrame, after_deletion: str = '') -> None:
        """
        Writes cycle data to the database.

        Args:
            df_cycles (DataFrame): The DataFrame containing cycle data.
            after_deletion (str): Additional info to log after deletion, if any.
        """
        pass  # Implement as needed


class CSVCounter:
    """
    Counts cycles based on the processed CSV data and updates the database.
    """

    def __init__(self, config):
        """
        Initializes the CSVCounter with necessary configurations.
        """
        self.logger = logging.getLogger(__name__)
        self.tp_table = TableConfig().TPDataTable
        self.cycle_table = TableConfig().CycleDataTable
        try:
            self.db_conn_params = config.db_conn_params
            self.mb_conn_params = config.mb_conn_params
            self.mb_reading_params = config.mb_reading_params
            self.hd_log_file_tracker_params = config.hd_log_file_tracker_params
        except Exception as e:
            self.logger.error(f"No config provided: {e}")
            raise

    def count(self, sample_id: str, init_state: str = STATE_DEHYD) -> None:
        """
        Counts the cycles for the given sample and updates the database.

        Args:
            sample_id (str): The sample identifier.
            init_state (str): The initial state for cycle counting.
        """
        df_all_cycles = self._preallocate_cycles_by_sample_id(sample_id=sample_id)
        # Initial counting
        self._initial_counting(df=df_all_cycles, init_state=init_state)
        self._update_tp_table_with_cycle_data(df_all_cycles, sample_id=sample_id)
        #df_all_cycles.to_csv('df_preallocated_cycles.csv')
        self._count_cycles_calc_uptake(df=df_all_cycles, sample_id=sample_id)

    def _preallocate_cycles_by_sample_id(self, sample_id: str) -> pd.DataFrame:
        """
        Pre-determines cycles based on state changes and returns a DataFrame of cycles.

        Args:
            sample_id (str): The sample identifier.

        Returns:
            DataFrame: A DataFrame containing cycle information.
        """
        self.logger.info('Start pre-determining cycles')

        query_vals = (sample_id,)
        query = f"""
        WITH state_changes AS (
            SELECT {self.tp_table.time}, {self.tp_table.de_hyd_state},
            LAG({self.tp_table.de_hyd_state}) OVER (ORDER BY {self.tp_table.time}) AS prev_state
            FROM {self.tp_table.table_name}
            WHERE {self.tp_table.sample_id} = %s
        ),
        state_groups AS (
            SELECT {self.tp_table.time}, {self.tp_table.de_hyd_state},
            SUM(
                CASE
                    WHEN prev_state IS NULL OR prev_state != {self.tp_table.de_hyd_state} THEN 1
                    ELSE 0
                END
            ) OVER (ORDER BY {self.tp_table.time}) AS group_id
            FROM state_changes
        )
        SELECT
            {self.tp_table.de_hyd_state} AS de_hyd_state, group_id,
            MIN({self.tp_table.time}) AS start_time, MAX({self.tp_table.time}) AS end_time
        FROM state_groups
        GROUP BY {self.tp_table.de_hyd_state}, group_id
        ORDER BY start_time;
        """
        try:
            with DatabaseConnection(**self.db_conn_params) as db_conn:
                db_conn.cursor.execute(query, query_vals)
                records = db_conn.cursor.fetchall()

                column_names_for_df_cycles = [
                    self.cycle_table.de_hyd_state,
                    self.cycle_table.cycle_number,
                    self.cycle_table.time_start,
                    self.cycle_table.time_end
                ]
                df_cycles = pd.DataFrame.from_records(records, columns=column_names_for_df_cycles)
                df_cycles[self.cycle_table.cycle_duration] = (
                    df_cycles[self.cycle_table.time_end] - df_cycles[self.cycle_table.time_start]
                )

                check_nan_values(df_cycles, "_count_cycles after import")

            if len(df_cycles) <= 1:
                self.logger.info("No Cycles found")
                return pd.DataFrame()
            else:
                return df_cycles
        except Exception as e:
            self.logger.error(f'Error occurred while pre-determining cycles: {type(e).__name__}, Message: {str(e)}')
            return pd.DataFrame()

    def _initial_counting(self, df: pd.DataFrame, init_state: str) -> None:
        """
        Performs the initial counting and adjustment of cycle numbers.

        Args:
            df (DataFrame): The DataFrame containing cycle information.
            init_state (str): The initial state for cycle counting.
        """
        is_increasing_by_one = (df[self.tp_table.cycle_number].diff().fillna(1) == 1).all()
        if not is_increasing_by_one:
            raise ValueError('Cycles are not increasing properly')
        if init_state == STATE_DEHYD:
            df[self.tp_table.cycle_number] = df[self.tp_table.cycle_number] / 2 - 0.5
        else:
            df[self.tp_table.cycle_number] = df[self.tp_table.cycle_number] / 2

    def _update_tp_table_with_cycle_data(self, df: pd.DataFrame, sample_id: str) -> None:
        """
        Updates the cycle_number column in the temperature-pressure data table.

        Args:
            df (DataFrame): The DataFrame containing cycle information.
            sample_id (str): The sample identifier.
        """
        db_manipulator = DataBaseManipulator(db_conn_params=self.db_conn_params)
        self.logger.info(f"Updating cycle numbers for {sample_id} in {self.tp_table.table_name}")

        db_manipulator.batch_update_data(
            df_vals_to_update=df[[self.tp_table.cycle_number]],
            sample_id=sample_id,
            table=self.tp_table,
            col_to_match=self.tp_table.time,
            other_col_to_match=self.tp_table.de_hyd_state,
            other_col_to_match_values=df[self.tp_table.de_hyd_state],
            update_between_min_list=df[self.cycle_table.time_start],
            update_between_max_list=df[self.cycle_table.time_end]
        )

    def _count_cycles_calc_uptake(self, df: pd.DataFrame, sample_id: str, already_counted: float = 0) -> None:
        """
        Counts the cycles and calculates hydrogen uptake.

        This method should be implemented with the logic to count cycles and
        calculate uptake based on your specific requirements.
        """
        self.logger.info(f"Start precise cycle count and uptake calculation for {sample_id}")

        total_number_cycles = df[self.tp_table.cycle_number].max()

        cycles_counted = already_counted
        cycle_number_checker = already_counted
        cycle_counter = CycleCounter(meta_data=MetaData(sample_id=sample_id),
                                     current_cycle=cycles_counted,
                                     current_state='Dehydrogenated' if cycles_counted//1 == 0 else 'Hydrogenated',
                                     db_conn_params=self.db_conn_params
                                     )

        while cycles_counted <= total_number_cycles:
            cycles_counted = cycle_counter.count(mode=CYCLE_COUNTER_MODE)

            #decrease cycles to count if too short cycle was detected
            if cycles_counted-cycle_number_checker == -0.5:
                total_number_cycles -= 1
                self.logger.info(f"Total number cycles decreased to {total_number_cycles} because too short cycle were detected.")

            cycle_number_checker = cycles_counted
            self.logger.info(f"Cycle# {cycles_counted - 0.5} calculated")

        self.logger.info(f"Precise cycle count and uptake calculation for {sample_id} finished")



#Global methods
def get_folders_for_id(sample_id):

    if sample_id == 'WAE-WA-028':
        #WAE-WA-030 MG2NiH4
        dir_t_p = r"C:\Daten\Kiki\WAE-WA-028-MgFe3wt\WAE-WA-028-TundP-Verläufe"
        dir_etc = r"C:\Daten\Kiki\WAE-WA-028-MgFe3wt\WAE-WA-028-All"
        reservoir_volume = 1

    elif sample_id == 'WAE-WA-030':
        #WAE-WA-030 MG2NiH4
        dir_t_p = r"C:\Daten\Kiki\WAE-WA-030-Mg2NiH4\WAE-WA-030-TundP-All"
        #dir_t_p = r"C:\Daten\Kiki\WAE-WA-030-Mg2NiH4\WAE-WA-030-some-cycles"
        dir_etc = r"C:\Daten\Kiki\WAE-WA-030-Mg2NiH4\WAE-WA-030-All"
        reservoir_volume = 3.75

    elif sample_id == 'WAE-WA-040':
        #WAE-WA-030 MG2NiH4
        dir_t_p = r"C:\Daten\Kiki\WAE-WA-040-MgFe5wt\WAE-WA-040-TundPVerläufe"
        dir_etc = r"C:\Daten\Kiki\WAE-WA-040-MgFe5wt\WAE-WA-040-All"
        reservoir_volume = 3.75

    else:
        raise ValueError(f"Unknown sample_id: {sample_id}")

    return dir_t_p, dir_etc, reservoir_volume


def get_time_exceptions_for_test(sample_id):
    exceptions_data = pd.DataFrame()
    if sample_id == "WAE-WA-030":
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


def check_nan_values(df, fun_str=""):
        for col in df.columns:
            if col == 'de_hyd_state':
                for idx, val in df[col].items():
                    if pd.isna(val) or val in ["nan", "NaN"]:
                        print(f"NaN value found during {fun_str}in df at row {idx}, column '{col}': {val}")


def read_and_plot_tp(sample_id=None, inserter_wizard=None, data_points_max=100000):

    def _plot_temperatures_and_pressures(df):
        import matplotlib.pyplot as plt
        # Create a figure and a set of subplots

        df['Time'] = df['time']
        fig, axs = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
        condition = (df['temperature_sample'] <= 1000) & (df['pressure'] <= 1000)
        df = df[condition].copy()

        # Plot temperatures
        axs[0].plot(df['Time'], df['temperature_sample'], label='Temperature Sample', color='blue')
        axs[0].plot(df['Time'], df['setpoint_sample'], label='Setpoint Temperature', color='cyan', linestyle='--')
        axs[0].set_ylabel('Temperature (°C)')
        axs[0].legend(loc='upper left')
        axs[0].set_title('Temperature over Time')

        ax_right = axs[0].twinx()
         # Convert boolean conditions to integers for plotting
        df['numeric_cycle'] = df['cycle_number_flag'].astype(int) * 2  # Example conversion
        df['numeric_hydrogenated'] = np.where(df["de_hyd_state"] == "Hydrogenated", 3, 0)

        # Create 'numeric_dehydrogenated' column
        df['numeric_dehydrogenated'] = np.where(df["de_hyd_state"] == "Dehydrogenated", 4, 0)
        df['numeric_uptake'] = df['h2_uptake_flag'].astype(int) * 5

        # Plot conditions
        ax_right.scatter(df['Time'], df['numeric_cycle'], label='Cycle', color='green', marker='o', alpha=0.5)
        ax_right.scatter(df['Time'], df['numeric_hydrogenated'], label='Hydrogenated', color='gold', marker='^', alpha=0.5)
        ax_right.scatter(df['Time'], df['numeric_dehydrogenated'], label='Dehydrogenated', color='red', marker='x', alpha=0.5)
        ax_right.scatter(df['Time'], df['numeric_uptake'], label='Uptake Flag', color='blue', marker='x', alpha=0.5)

        ax_right.set_ylabel('Conditions')
        ax_right.set_ylim(0, 6)  # Adjust as needed
        ax_right.set_yticks([2, 3, 4, 5])
        ax_right.set_yticklabels(['Is_Cycle', 'Hydrogenated', 'Dehydrogenated', 'Uptake_Flag'])


        # Plot pressures
        axs[1].plot(df['Time'], df['pressure'], label='Pressure', color='red')
        axs[1].plot(df['Time'], df['eq_pressure'], label='Eq Pressure', color='green', linestyle='--')
        # Assuming eq_pressure_real is calculated and added to df
        axs[1].plot(df['Time'], df['eq_pressure_real'], label='Eq Pressure Real', color='purple', linestyle='-.')
        axs[1].set_ylabel('Pressure')
        axs[1].set_xlabel('Time')
        axs[1].legend(loc='upper left')
        axs[1].set_title('Pressure over Time')

        # Shade regions for Hydrogenated/Dehydrogenated states
        # Assuming 'de_hyd_state' is in df
        for state, sub_df in df.groupby((df['de_hyd_state'] != df['de_hyd_state'].shift()).cumsum()):
            if sub_df['de_hyd_state'].iloc[0] == 'Hydrogenated':
                axs[0].axvspan(sub_df['Time'].iloc[0], sub_df['Time'].iloc[-1], color='lightgreen', alpha=0.3)
                axs[1].axvspan(sub_df['Time'].iloc[0], sub_df['Time'].iloc[-1], color='lightgreen', alpha=0.3, label='Hydrogenated')
            elif sub_df['de_hyd_state'].iloc[0] == 'Dehydrogenated':
                axs[0].axvspan(sub_df['Time'].iloc[0], sub_df['Time'].iloc[-1], color='lightcoral', alpha=0.3)
                axs[1].axvspan(sub_df['Time'].iloc[0], sub_df['Time'].iloc[-1], color='lightcoral', alpha=0.3, label='Dehydrogenated')



        cycle_df = df.drop_duplicates(subset='cycle_number')
        # Plot h2_uptake over time
        axs[2].scatter(cycle_df['time'], cycle_df['h2_uptake'], label='H2 Uptake', color='blue')
        axs[2].set_ylabel('H2 Uptake (wt-%)')
        axs[2].set_xlabel('Time')
        axs[2].legend(loc='upper left')
        axs[2].set_title('H2 Uptake over Time')


        # Annotate cycle numbers
        cycle_numbers = df['cycle_number'].unique()
        for cycle_number in cycle_numbers:
            cycle_df = df[df['cycle_number'] == cycle_number]
            cycle_start_time = cycle_df['Time'].iloc[0]
            axs[2].annotate(f'Cycle {str(cycle_number)}', xy=(cycle_start_time, 5),
                            xytext=(cycle_start_time, 5.0),
                            arrowprops=dict(facecolor='black', shrink=0.05))

        # Plot h2_uptake over cycle_number
       # axs[2].plot(df['cycle_number'], df['h2_uptake'], label='H2 Uptake', color='red')
       # axs[2].set_ylabel('H2 Uptake (wt-%)')
       # axs[2].set_xlabel('Cycle Number')
       # axs[2].legend(loc='upper left')
       # axs[2].set_title('H2 Uptake over Cycle Number')
        # Improve layout and display the plot
        plt.tight_layout()
        plt.gcf().autofmt_xdate()
        plt.gca().autoscale()
        plt.show()

    config = GetConfig()
    data_retriever = DataRetriever(db_conn_params=config.db_conn_params)
    data_retriever.limit_datapoints = data_points_max
    table_name = TableConfig().TPDataTable.table_name
    if not sample_id:
        time_one = "2023-10-29 00:21:30"
        time_two = "2023-10-29 04:00:20"
        time_range = (time_one, time_two)
        column_names = TableConfig().get_table_column_names(table_name=table_name)
        df = data_retriever.fetch_data_by_time_2(time_range=time_range,
                                                 column_names=column_names,
                                                 table_name=table_name)
    else:
        column_names = TableConfig().get_table_column_names(table_name=table_name)
        df = data_retriever.fetch_data_by_sample_id_2(sample_id=sample_id, table_name=table_name,
                                                      column_names=column_names)

    df['eq_pressure_real'] = inserter_wizard.eq_calculator.calc_eq(df['temperature_sample'])
    _plot_temperatures_and_pressures(df=df)
    return df


#Methods for usage
if __name__ == '__main__':
    sample_ids = ('WAE-WA-028', 'WAE-WA-030', 'WAE-WA-040')
    #sample_ids = ('WAE-WA-030',)
    logger = logging.getLogger(__name__)
    from src.config_connection_reading_management.config_reader import GetConfig
    config = GetConfig()
    for sample_id in sample_ids:
        dir_tp, dir_etc, vol_res = get_folders_for_id(sample_id=sample_id)
        csv_processor = CSVProcessor(sample_id=sample_id, config=config)
        csv_processor.process()
        write_ETC_in_parallel(dir_etc_folder=dir_etc, sample_id=sample_id)

        print(f"{sample_id} processed")
    #csv_counter = CSVCounter()
    #writer = ModbusDBWriter(meta_data=MetaData(sample_id=sample_id))
    #sample_id = 'WAE-WA-030'
    #dir_tp, dir_etc, vol_res = get_folders_for_id(sample_id=sample_id)
    #write_ETC_folder(dir_etc_folder=dir_etc, sample_id=sample_id)


    #csv_counter.count(sample_id=sample_id)
