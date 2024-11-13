
import numpy as np
import win32com.client as win32
import re
import os
import struct
import time
from zoneinfo import ZoneInfo
import threading
from multiprocessing import Pool

import pandas as pd
from pymodbus.exceptions import ModbusException, ConnectionException
from pymodbus.pdu import ExceptionResponse
from psycopg2 import IntegrityError

from src.config_connection_reading_management.connections_and_logger import AppLogger, DatabaseConnection, GetConfig, ModbusConnection
from datetime import datetime, timedelta
from src.calculations.eq_p_calculation import VantHoffCalcEq as EqCalculator
from src.config_connection_reading_management.query_builder import QueryBuilder
from src.meta_data.meta_data_handler import MetaData
from src.table_data import TableConfig
from src.standard_paths import standard_log_dir

qb = QueryBuilder()
# Load the configuration
config = GetConfig()
# Accessing the variables
LOG_DIRECTORY   = config.LOG_DIRECTORY
LOG_FILE        = config.LOG_FILE
DB_SERVER       = config.DB_SERVER
DB_DATABASE     = config.DB_DATABASE
DB_USERNAME     = config.DB_USERNAME
DB_PASSWORD     = config.DB_PASSWORD
DB_PORT         = config.DB_PORT
MODBUS_HOST     = config.MODBUS_HOST
MODBUS_PORT     = config.MODBUS_PORT
REGS_OF_INTEREST = config.REGS_OF_INTEREST
START_REG       = config.START_REG
END_REG         = config.END_REG
SLEEP_INTERVAL  = config.SLEEP_INTERVAL

local_tz = ZoneInfo("Europe/Berlin")
table_rp = TableConfig().TPDataTable


class DataRetriever:
    """
    Class for retrieving data from the database.

    This class provides methods to fetch data from specific tables in the database based on
    different criteria like time range, sample ID, etc. It uses a DatabaseConnection instance
    for database operations.

    Attributes:
        running (bool): Indicates if the data retrieval process is active.
        logger (Logger): Logger for logging messages.
        database_connection (DatabaseConnection): Database connection instance.

    Methods:
        fetch_latest_records(): Fetches the latest records from a specified table.
        fetch_data_by_time(): Fetches data in a specified time range from a table.
        fetch_data_by_sample_id(): Fetches data based on a sample ID from a table.
        fetch_tp_and_etc_data(): Fetches TP and ETC data based on time range or sample ID.
        fetch_xy_data(): Fetches ETC XY data by time value.
        close_connection(): Closes the database connection.
        fetch_last_state_and_cycle(): Fetches the last state and cycle number from a table.
    """
    def __init__(self):
        self.running = False
        self.qb = qb
        self.config = config
        self.logger = AppLogger().get_logger(__name__)
        self.database_connection = DatabaseConnection()
        self.limit_datapoints = 10000

    def fetch_latest_records(self, cursor, table_name='t_p_data', column_names=None, constraints=None, desc_limit=1, sample_id=None):
        """
        Fetches the latest records from a specified table.
        Parameters:
            table_name (str): Name of the table you want to read from default='t_p_data'

            column_names (str): columns to read (tuple) default= ('time', 'pressure')
            constraints (dict): min max parameters for temp increase and char time

        Valid 'column_names' for each 'table_name' are:
            't_p_data' : ["time", "pressure", "temperature_sample", "setpoint_sample", "temperature_heater",
            "setpoint_heater", "eq_pressure", "de_hyd_state", "cycle_number", "reservoir_volume", "sample_id"]

            'thermal_conductivity_data' : ["\"Time\"", "\"File\"", "\"Description\"", "\"Sample_ID\"", "\"Points\"", "\"Temperature\"",
            "\"ThConductivity\"", "\"ThDiffusivity\"", "\"SpecHeat\"", "\"ThEffusivity\"", "\"PrDepth\"",
            "\"TempIncr\"", "\"TempDrift\"", "\"TotalTempIncr\"", "\"TotalCharTime\"", "\"Time_Corr\"",
            "\"Mean_Dev\"", "\"Disk_Res\"", "\"Calc_settings\"", "\"Temperature_avg\"", "\"ThConductivity_avg\"",
            "\"ThDiffusivity_avg\"", "\"SpecHeat_avg\"", "\"ThEffusivity_avg\"", "\"PrDepth_avg\"",
            "\"TempIncr_avg\"", "\"TempDrift_avg\"", "\"TotalTempIncr_avg\"", "\"TotalCharTime_avg\"",
            "\"Time_Corr_avg\"", "\"Mean_Dev_avg\"", "\"Disk_Res_avg\"", "\"Calc_settings_avg\"",
            "\"Temperature_dvt\"", "\"ThConductivity_dvt\"", "\"ThDiffusivity_dvt\"", "\"SpecHeat_dvt\"",
            "\"ThEffusivity_dvt\"", "\"PrDepth_dvt\"", "\"TempIncr_dvt\"", "\"TempDrift_dvt\"",
            "\"TotalTempIncr_dvt\"", "\"TotalCharTime_dvt\"", "\"Time_Corr_dvt\"", "\"Mean_Dev_dvt\"",
            "\"Disk_Res_dvt\"", "\"Calc_settings_dvt\"", "\"Outppower\"", "\"Meastime\"", "\"Radius\"", "\"TCR\"",
            "\"Disk_Type\"", "\"Tempdrift_rec\"", "\"Notes\"", "\"Rs\""]  # Valid columns for thermal_conductivity_data

            'thermal_conductivity_xy_data': ["point_nr", "time", "t_f_tau","temperature", "time_temperature_increase", "temperature_increase",
            "sqrt_time", "diff_temperature", "time_drift", "temperature_drift"]
        """

        query, values = qb.create_continuous_reading_query(column_names=column_names,
                                                   table_name=table_name,
                                                   constraints=constraints,
                                                   desc_limit=desc_limit,
                                                   sample_id=sample_id)

        df = self.execute_continuous_fetching(query=query,
                                              cursor=cursor,
                                              column_names=column_names,
                                              table_name=table_name,
                                              values=values)
        return df

    def fetch_tp_and_etc_data(self, time_range=None, sample_id=None, column_names_t_p=None, column_names_etc=('\"Time\"', '\"ThConductivity_avg\"'), constraints=None):
        """
        Fetches tp and ETC data based on sample_id or time_range.

        Parameters:
            time_range (datetime): Time range of data that will be imported.
            sample_id (str): Sample ID that should be read.
            column_names_etc (list of str): Columns to read from the ETC table.
            column_names_t_p (list of str): Columns to read from the T p table.
            constraints (dict): min max parameters for temp increase and char time

        Valid 'column_names_t_p' include:
            ["time", "pressure", "temperature_sample", "setpoint_sample",
            "temperature_heater", "setpoint_heater", "eq_pressure", "de_hyd_state",
            "cycle_number", "reservoir_volume", "sample_id"]

        Valid 'column_names_etc' include:
            ["\"Time\"", "\"File\"", "\"Description\"", "\"Sample_ID\"", "\"Points\"",
            "\"Temperature\"", "\"ThConductivity\"", "\"ThDiffusivity\"", "\"SpecHeat\"",
            "\"ThEffusivity\"", "\"PrDepth\"", "\"TempIncr\"", "\"TempDrift\"",
            "\"TotalTempIncr\"", "\"TotalCharTime\"", "\"Time_Corr\"", "\"Mean_Dev\"",
            "\"Disk_Res\"", "\"Calc_settings\"", "\"Temperature_avg\"",
            "\"ThConductivity_avg\"", "\"ThDiffusivity_avg\"", "\"SpecHeat_avg\"",
            "\"ThEffusivity_avg\"", "\"PrDepth_avg\"", "\"TempIncr_avg\"",
            "\"TempDrift_avg\"", "\"TotalTempIncr_avg\"", "\"TotalCharTime_avg\"",
            "\"Time_Corr_avg\"", "\"Mean_Dev_avg\"", "\"Disk_Res_avg\"",
            "\"Calc_settings_avg\"", "\"Temperature_dvt\"", "\"ThConductivity_dvt\"",
            "\"ThDiffusivity_dvt\"", "\"SpecHeat_dvt\"", "\"ThEffusivity_dvt\"",
            "\"PrDepth_dvt\"", "\"TempIncr_dvt\"", "\"TempDrift_dvt\"",
            "\"TotalTempIncr_dvt\"", "\"TotalCharTime_dvt\"", "\"Time_Corr_dvt\"",
            "\"Mean_Dev_dvt\"", "\"Disk_Res_dvt\"", "\"Calc_settings_dvt\"",
            "\"Outppower\"", "\"Meastime\"", "\"Radius\"", "\"TCR\"", "\"Disk_Type\"",
            "\"Tempdrift_rec\"", "\"Notes\"", "\"Rs\""]
        """

        table_name_etc = TableConfig().ETCDataTable.table_name
        table_name_tp = TableConfig().TPDataTable.table_name
        if time_range and not sample_id:
            tp_data = self.fetch_data_by_time_2(time_range=time_range,
                                                column_names=column_names_t_p,
                                                table_name=table_name_tp,
                                                sample_id=sample_id)

        elif sample_id and not time_range:
            tp_data = self.fetch_data_by_sample_id_2(sample_id=sample_id,
                                                     column_names=column_names_t_p,
                                                     table_name=table_name_tp)
            if not tp_data.empty:
                # Get min and max
                min_time = tp_data['time'].min()
                max_time = tp_data['time'].max()
                #print(f"fetched by sample id {min_time}, {max_time}")
                time_range = (min_time, max_time)



        elif sample_id and time_range:
            tp_data = self.fetch_data_by_time_2(time_range=time_range,
                                                column_names=column_names_t_p,
                                                table_name=table_name_tp,
                                                sample_id=sample_id)

        else:
            tp_data = pd.DataFrame()

        if not tp_data.empty:
                etc_data = self.fetch_data_by_time_2(time_range=time_range,
                                                     column_names=column_names_etc,
                                                     table_name=table_name_etc,
                                                     constraints=constraints,
                                                     sample_id=sample_id
                                                     )

        else:
            etc_data = pd.DataFrame()
        return tp_data, etc_data

    def fetch_xy_data(self, time_value, row_package_name='Transient'):
        """
        Fetch ETC_XY data by time value. Should always return 200 lines and 2 rows
        Parameters:
            time_value: time_value of the ETC_XY data that should be loaded
            row_package_name: data that should be loaded

        Valid 'row_package_name' include:

            'Name1' : 'Transient'
            'Name2' : 'Drift'
            'Name3' : 'Residual'
            'Name4' : 'Calculated'
        """
        table = TableConfig().ThermalConductivityXyDataTable
        table_name = table.table_name
        time_column = table.time
        if isinstance(time_value, str):
        # Parse string to datetime and localize
            time_value = datetime.fromisoformat(time_value)
        if isinstance(time_value, datetime):
            # Attach timezone info (assuming UTC if naive)
            if time_value.tzinfo is None:
                time_value = time_value.replace(tzinfo=local_tz)

        print(time_value)


        match row_package_name.lower():
            case 'transient':
                column_names = (table._time_temperature_increase, table.temperature_increase, f"{time_column}")
            case 'drift':
                column_names = (table.drift_time, table.temperature_drift, f"{time_column}")
            case 'calculated':
                column_names = (table.t_f_tau, table.temperature, f"{time_column}")
            case 'residual':
                column_names = (table.sqrt_time, table.temp_diff, f"{time_column}")

        column_names_str = ', '.join(column_names)
        query = f"SELECT {column_names_str} FROM {table_name} WHERE {time_column} = %s ORDER by {time_column}"
        params = (time_value,)
        try:
            with DatabaseConnection() as db_conn:  # Open the connection
                db_conn.cursor.execute(query, params)
                records = db_conn.cursor.fetchall()
                column_names = tuple(s.replace("\"", '') for s in column_names)
                df = pd.DataFrame.from_records(records, columns=column_names)

            if not df.empty:
                df = df.sort_values(by=column_names[0], ascending=True)
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Error occurred while fetching x_y data: {e}")
        return pd.DataFrame()

    def close_connection(self):
        """
        Closes the database connection when done.
        """
        self.database_connection.close_connection()

    @staticmethod
    def fetch_last_state_and_cycle(sample_id=None):
        meta_data = MetaData(sample_id=sample_id)
        return meta_data.last_de_hyd_state, meta_data.total_number_cycles

    def fetch_data_by_sample_id_2(self, sample_id, table_name, column_names=None, constraints=None):
        """

        """
        query, values = qb.create_reading_query(sample_id=sample_id,
                                                table_name=table_name,
                                                column_names=column_names,
                                                limit_data_points=self.limit_datapoints,
                                                constraints=constraints)
        #print(query)
        return self.execute_fetching(query=query, column_names=column_names, table_name=table_name, values = values)

    def fetch_data_by_time_2(self, time_range, table_name, column_names=None, constraints=None, sample_id=None):
        """

        """
        query, values = qb.create_reading_query(table_name=table_name,
                                                time_window=time_range,
                                                limit_data_points=self.limit_datapoints,
                                                column_names=column_names,
                                                constraints=constraints,
                                                sample_id=sample_id)

        return self.execute_fetching(query=query, column_names=column_names,
                                     table_name=table_name, values=values)

    def fetch_data_by_cycle(self, cycle_numbers, sample_id, column_names=None, constraints=None, table=None):
        """
        Fetches data from the database for the specified cycle numbers and sample ID.

        Parameters:
        - cycle_numbers: An integer, float, or a list/tuple of cycle numbers.
        - sample_id: The sample ID to filter by.
        - column_names: Optional list of column names to select.
        - constraints: Optional additional constraints.
        - table: Optional table configuration.

        Returns:
        - The result of the execute_fetching method.
        """
        if not table:
            table = TableConfig().TPDataTable
        table_name = table.table_name
        sample_id_col = table.sample_id
        cycle_col = table.cycle_number

        if isinstance(cycle_numbers, (list, tuple)):
            # Convert any NumPy types to native Python types
            cycle_numbers = [float(cn) for cn in cycle_numbers]
            query_symbol = "IN"
            placeholders = ', '.join(['%s'] * len(cycle_numbers))
            query = f"SELECT * from {table_name} WHERE " \
                    f"{sample_id_col} = %s AND " \
                    f"{cycle_col} {query_symbol} ({placeholders}) ORDER by {table.time}"
            values = [sample_id] + cycle_numbers
        else:
            query_symbol = "="
            query = f"SELECT * from {table_name} WHERE " \
                    f"{sample_id_col} = %s AND " \
                    f"{cycle_col} {query_symbol} %s"
            values = [sample_id, float(cycle_numbers)]
        # print(query, values)
        return self.execute_fetching(query=query, column_names=column_names, table_name=table_name, values=values)

    def fetch_pressures_for_h2_uptake_calc(self, sample_id, cycle_number):
        query = qb.create_pressure_query_for_uptake_calc(sample_id=sample_id, cycle_number=cycle_number)

        try:
            with DatabaseConnection() as db_conn:
                db_conn.cursor.execute(query)
                records = db_conn.cursor.fetchall()
                df = pd.DataFrame.from_records(records, columns=["min_pressure", "max_pressure"])
                return df
        except Exception as e:
            self.logger.error(f"Error occurred while fetching data by cycle number: {e}")
        return None

    def fetch_data_by_time_no_limit(self, table, time_range, col_names=None):
        if not col_names:
            col_names = TableConfig().get_table_column_names(table_class=table)

        col_name_str = ", ".join(col_names)

        query = f"SELECT {col_name_str} "\
                f"FROM {table.table_name} "\
                f"WHERE {table.time} BETWEEN %s AND %s ORDER by {table.time} "

        return self.execute_fetching(query=query, values=time_range, column_names=col_names)

    def execute_fetching(self, query, column_names=None, table_name=None, values=None):
        if column_names is None:
            column_names = TableConfig().get_table_column_names(table_name=table_name)

        try:
            with DatabaseConnection() as db_conn:
                db_conn.cursor.execute(query, values)
                records = db_conn.cursor.fetchall()

            if isinstance(column_names, (tuple, list)):
                column_names_for_df = tuple(s.replace("\"", '') for s in column_names)
            else:
                column_names_for_df = [column_names]
            if records:
                df = pd.DataFrame.from_records(records, columns=column_names_for_df)
                df = self._adjust_df_types_and_times(df)
               # print("df returned")

            else:
                df = pd.DataFrame()
            return df
        except Exception as e:
            self.logger.error(f"Error occurred while execute fetching data: {e}")
            return pd.DataFrame()

    def execute_continuous_fetching(self, query, cursor, column_names=None, table_name=None, values=None):
        if column_names is None:
            column_names = TableConfig().get_table_column_names(table_name=table_name)

        try:

            cursor.execute(query, values)
            records = cursor.fetchall()
            if isinstance(column_names, tuple) or isinstance(column_names, list):
                column_names_for_df = tuple(s.replace("\"", '') for s in column_names)
            else:
                column_names_for_df = [column_names]
            if records:
                df = pd.DataFrame.from_records(records, columns=column_names_for_df)
                df = self._adjust_df_types_and_times(df)
            else:
                df = pd.DataFrame()

            return df

        except Exception as e:
            self.logger.error(f"Error occurred while continuous fetching data: {e}")
        return None

    def _adjust_df_types_and_times(self, df):
        def _handle_light_saving(timestamp_str):
            # Extract timezone offset from the string
            offset = timestamp_str[-5:]
            print(offset)
            # Convert string to datetime
            #dt = pd.to_datetime(timestamp_str[:-3], errors='coerce')

            # Apply timezone offset
            #dt += pd.Timedelta(hours=offset)
            #return dt

        for col in df.columns:

            if "_flag" in col.lower():
                df_col = df[col].apply(lambda x: True if x in ('t', 1, "1", 'True', 'true') else False)
                df[col] = df_col

            #check if time column is str or object
            is_time_col_str = "time" in col.lower() and pd.api.types.is_string_dtype(df[col])
            is_time_col_obj = "time" in col.lower() and pd.api.types.is_object_dtype(df[col])
            if is_time_col_str or is_time_col_obj:
                df[col] = pd.to_datetime(df[col], utc=True)
            #converted = pd.to_datetime(df[col], errors='coerce')
            #datetime_check = converted.notna().any()
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if not df[col].dt.tz:
                    df[col] = df[col].dt.tz_localize('UTC')
                    df[col] = df[col].dt.tz_convert(local_tz)
                else:
                    df[col] = df[col].dt.tz_convert(local_tz)
        return df

    @staticmethod
    def remove_timezone(df):
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)
        return df


class DataBaseManipulator:
    """
    Handles batch updates and manipulations of the database.
    """
    def __init__(self):
        """
        Initializes the DataBaseManipulator with a logger.
        """
        self.running = False
        self.logger = AppLogger().get_logger(__name__)
        self.database_connection = DatabaseConnection()
        self.config = GetConfig()

    def execute_updating(self, query, values, many_bool=True):
        with DatabaseConnection() as db_conn:
            try:
                if many_bool:
                    db_conn.cursor.executemany(query, values)
                else:
                    db_conn.cursor.execute(query, values)
                db_conn.cursor.connection.commit()

            except Exception as e:
                db_conn.cursor.connection.rollback()
                self.logger.error(f"An error occurred during executing update: {e}")


    def batch_update_data(self, sample_id: str=None, table: TableConfig()=None,
                          df_vals_to_update: pd.DataFrame() = None,
                          col_to_match: str = None,
                          other_col_to_match: str = None,
                          other_col_to_match_values: pd.Series = None,
                          update_between_min_list: pd.Series = None,
                          update_between_max_list: pd.Series = None):
        """
        Performs batch updates using executemany for multiple rows.

        Args:
            sample_id (str): The sample identifier.
            table (TableConfig): The table configuration.
            df_vals_to_update (DataFrame): DataFrame containing values to update.
            col_to_match (str): Column name to match in the WHERE clause.
            update_between_min_list (Series): List of minimum values for the BETWEEN condition.
            update_between_max_list (Series): List of maximum values for the BETWEEN condition.

        """
        update_between_max_list = update_between_max_list.tolist()
        update_between_min_list = update_between_min_list.tolist()
        table_name = table.table_name
        sample_id_col = table.sample_id
        query_part_cols_to_update = [f"{col} = %s" for col in df_vals_to_update.columns.tolist()]

        query = f"UPDATE {table_name} SET {', '.join(query_part_cols_to_update)} " \
                f"WHERE {sample_id_col} = %s AND {col_to_match} BETWEEN %s AND %s"

        values_to_update = []


        for time_start, time_end, update_values in zip(update_between_min_list,
                                                        update_between_max_list,
                                                        df_vals_to_update.values.tolist()
                                                        ):
            # Prepare values for each update
            values = tuple(update_values) + (sample_id, time_start, time_end)
            values_to_update.append(values)
        if other_col_to_match and not other_col_to_match_values.empty:
            columns = list(df_vals_to_update.columns) + [table.sample_id, TableConfig().CycleDataTable.time_start, TableConfig().CycleDataTable]
            df_values = pd.DataFrame(values_to_update, columns=columns)
            df_values[table.de_hyd_state] = other_col_to_match_values  # or assign a constant value
            values_to_update = [tuple(row) for row in df_values.itertuples(index=False, name=None)]

            optional_query_part = f" AND {other_col_to_match} = %s"
            query += optional_query_part

            self.execute_updating(query=query, values=values_to_update)
            self.logger.info(f"Batch updated {len(values_to_update)} records in {table_name}")


    def update_data(self, sample_id: str = None, table: TableConfig() = None,
                    update_df: pd.DataFrame() = None, col_to_match: str = None,
                    update_between_vals=None):
            """
            """
            table_name = table.table_name
            sample_id_col = table.sample_id
            if isinstance(update_df, pd.DataFrame):
                cols_to_update = [f"{col} = %s" for col in update_df.columns]
                tuple_values_to_update = tuple(update_df.iloc[0])
            elif isinstance(update_df, pd.Series):
                cols_to_update = [f"{col} = %s" for col in update_df.index.tolist()]
                tuple_values_to_update = tuple(update_df.values)

            if not isinstance(update_between_vals, tuple):
                # Convert a list or any non-tuple value to a tuple
                if isinstance(update_between_vals, list):
                    update_between_vals = tuple(update_between_vals)
                else:
                    update_between_vals = (update_between_vals,)

            query_part_cols_to_update = ', '.join(cols_to_update)
            query = f"UPDATE {table_name} SET {query_part_cols_to_update} " \
                f"WHERE {sample_id_col} = %s AND {col_to_match} BETWEEN %s and %s"

            values = tuple_values_to_update + (sample_id,) + update_between_vals

            with DatabaseConnection() as db_conn:
                try:
                    db_conn.cursor.execute(query, values)
                    db_conn.cursor.connection.commit()
                    self.logger.info(f"Updated {cols_to_update} in {table_name}")
                    return True
                except Exception as e:
                    db_conn.cursor.connection.rollback()
                    self.logger.error(f"An error occured while update_data {e}")
                    return False

    def _update_cycle_count_flag(self, sample_id=None, min_cycling_temperature=None):
        tp_table = TableConfig().TPDataTable
        table_name = tp_table.table_name
        column_names = TableConfig().get_table_column_names()
        for col_name in column_names:
            if "cycle" in col_name.lower() and "flag" in col_name.lower():
                column_to_update = col_name
            if "sample" in col_name.lower() and "id" in col_name.lower():
                sample_id_column = col_name
            if "sample" in col_name.lower() and "temperature" in col_name.lower():
                sample_temperature_column = col_name

        query = f"UPDATE {table_name} SET {column_to_update} = CASE WHEN " \
                f"{sample_temperature_column} < {min_cycling_temperature} THEN False " \
                f"ELSE True END " \
                f"WHERE {sample_id_column} = '{sample_id}'"

        try:
            with self.database_connection as db_conn:
                db_conn.cursor.execute(query)
                db_conn.cursor.connection.commit()
            self.logger.info(f"Cycle count flag updated for {sample_id}")
        except Exception as e:
            self.logger.error("Error updating cycle count flag %s", e)

    def _update_uptake_flag(self, sample_id=None, max_cycling_pressure=None):
        table_name = self.config.TP_DATA_TABLE_NAME
        for col_name in self.config.TP_DATA_COLUMN_NAMES:
            if "uptake" in col_name.lower() and "flag" in col_name.lower():
                column_to_update = col_name
            if "sample" in col_name.lower() and "id" in col_name.lower():
                sample_id_column = col_name
            if "pressure" in col_name.lower() and "eq" not in col_name.lower():
                sample_pressure_column = col_name

        query = f"UPDATE {table_name} SET {column_to_update} = CASE WHEN " \
                f"{sample_pressure_column} < {max_cycling_pressure}  THEN True " \
                f"ELSE False END "\
                f"WHERE {sample_id_column} = '{sample_id}'"

        try:
            with DatabaseConnection() as db_conn:
                db_conn.cursor.execute(query)
                db_conn.cursor.connection.commit()
                self.logger.info(f"Cycle count flag updated for {sample_id}")

        except Exception as e:
            self.logger.error("Error updating cycle count flag %s", e)

    def _update_first_hyd_flag(self, sample_id=None, date_first_hydrogenation=None):
        table_name = self.config.TP_DATA_TABLE_NAME
        for col_name in self.config.TP_DATA_COLUMN_NAMES:
            if "uptake" in col_name.lower() and "flag" in col_name.lower():
                column_to_update_1 = col_name
            if "cycle" in col_name.lower() and "flag" in col_name.lower():
                column_to_update_2 = col_name
            if "sample" in col_name.lower() and "id" in col_name.lower():
                sample_id_column = col_name
            if "time" in col_name.lower():
                time_column = col_name

        query = f"UPDATE {table_name} SET {column_to_update_1} = False, " \
                f"{column_to_update_2} = False " \
                f"WHERE {sample_id_column} = '{sample_id}' AND {time_column} < '{date_first_hydrogenation}'"
        try:
            with DatabaseConnection() as db_conn:
                db_conn.cursor.execute(query)
                db_conn.cursor.connection.commit()
                self.logger.info(f"Cycle count flag updated for {sample_id}")

        except Exception as e:
            self.logger.error("Error updating cycle count flag %s", e)

    def update_flags(self, sample_id=None, min_cycling_temperature=None, max_cycling_pressure=None, date_first_hydrogenation=None):
        """
        Updatest and p data with date of first hydrogenation (everything before will be dehydrogenated) and if
        pressures should be taken into account for uptake estimation (_update_uptake_flag)
        :param sample_id:
        :param min_cycling_temperature:
        :param max_cycling_pressure:
        :param date_first_hydrogenation:
        :return:
        """
        start_time_all = time.time()
        if min_cycling_temperature:
            start_time = time.time()
            self._update_cycle_count_flag(sample_id=sample_id, min_cycling_temperature=min_cycling_temperature)
            passed_time = time.time()-start_time
            print(f"Updating temperature flags takes {passed_time} s")
        if max_cycling_pressure:
            start_time = time.time()
            self._update_uptake_flag(sample_id=sample_id, max_cycling_pressure=max_cycling_pressure)
            passed_time = time.time()-start_time
            print(f"Updating pressure flags takes {passed_time} s")
        if date_first_hydrogenation:
            start_time = time.time()
            self._update_first_hyd_flag(sample_id=sample_id, date_first_hydrogenation=date_first_hydrogenation)
            passed_time = time.time()-start_time
            print(f"Updating first hydrogenation flag takes {passed_time} s in total")
        passed_time = time.time()-start_time_all
        print(f"Updating all flags takes {passed_time} s in total")

    def get_column_names(self, table_name):

        if "t_p" in table_name.lower():
            column_names = self.config.TP_DATA_COLUMN_NAMES
        elif "conductivity" in table_name.lower() and "xy" in table_name.lower():
            column_names = self.config.THERMAL_CONDUCTIVITY_XY_COLUMN_NAMES
        elif "conductivity" in table_name.lower():
            column_names = self.config.THERMAL_CONDUCTIVITY_COLUMN_NAMES
        elif "meta" in table_name.lower():
            column_names = self.config.META_DATA_COLUMN_NAMES
        return column_names


class ExcelDataProcessor:
    etc_table = TableConfig().ETCDataTable
    xy_table = TableConfig().ThermalConductivityXyDataTable
    etc_column_attribute_mapping = {etc_table.time: 'Time',
                                    etc_table.file: 'File',
                                    etc_table.description: 'Description',
                                    etc_table.sample_id: 'Sample ID',
                                    etc_table.points: 'Points',
                                    etc_table.temperature: 'Temperature',
                                    etc_table.th_conductivity: 'ThConductivity',
                                    etc_table.th_diffusivity: 'ThDiffusivity',
                                    etc_table.specific_heat: 'SpecHeat',
                                    etc_table.th_effusivity: 'ThEffusivity',
                                    etc_table.probing_depth: 'PrDepth',
                                    etc_table.temperature_increase: 'TempIncr',
                                    etc_table.temperature_drift: 'TempDrift',
                                    etc_table.total_temperature_increase: 'TotalTempIncr',
                                    etc_table.total_to_characteristic_time: 'TotalCharTime',
                                    etc_table.time_correction: 'Time Corr',
                                    etc_table.mean_deviation: 'Mean Dev',
                                    etc_table.disk_resistance: 'Disk Res',
                                    etc_table.calculation_settings: 'Calc settings',
                                    etc_table.temperature_average: 'Temperature_avg',
                                    etc_table.thermal_conductivity_average: 'ThConductivity_avg',
                                    etc_table.thermal_diffusivity_average: 'ThDiffusivity_avg',
                                    etc_table.specific_heat_average: 'SpecHeat_avg',
                                    etc_table.thermal_effusivity_average: 'ThEffusivity_avg',
                                    etc_table.probing_depth_average: 'PrDepth_avg',
                                    etc_table.temperature_increase_average: 'TempIncr_avg',
                                    etc_table.temperature_drift_average: 'TempDrift_avg',
                                    etc_table.total_temperature_increase_average: 'TotalTempIncr_avg',
                                    etc_table.total_to_characteristic_time_average: 'TotalCharTime_avg',
                                    etc_table.time_correction_average: 'Time Corr_avg',
                                    etc_table.mean_deviation_average: 'Mean Dev_avg',
                                    etc_table.disk_resistance_average: 'Disk Res_avg',
                                    etc_table.calculation_settings_average: 'Calc settings_avg',
                                    etc_table.temperature_deviation: 'Temperature_dvt',
                                    etc_table.thermal_conductivity_deviation: 'ThConductivity_dvt',
                                    etc_table.thermal_diffusivity_deviation: 'ThDiffusivity_dvt',
                                    etc_table.specific_heat_deviation: 'SpecHeat_dvt',
                                    etc_table.thermal_effusivity_deviation: 'ThEffusivity_dvt',
                                    etc_table.probing_depth_deviation: 'PrDepth_dvt',
                                    etc_table.temperature_increase_deviation: 'TempIncr_dvt',
                                    etc_table.temperature_drift_deviation: 'TempDrift_dvt',
                                    etc_table.total_temperature_increase_deviation: 'TotalTempIncr_dvt',
                                    etc_table.total_to_characteristic_time_deviation: 'TotalCharTime_dvt',
                                    etc_table.time_correction_deviation: 'Time Corr_dvt',
                                    etc_table.mean_deviation_deviation: 'Mean Dev_dvt',
                                    etc_table.disk_resistance_deviation: 'Disk Res_dvt',
                                    etc_table.calculation_settings_deviation: 'Calc settings_dvt',
                                    etc_table.output_power: 'Outppower',
                                    etc_table.measurement_time: 'Meastime',
                                    etc_table.disk_radius: 'Radius',
                                    etc_table.tcr: 'TCR',
                                    etc_table.disk_type: 'Disk Type',
                                    etc_table.temperature_drift_rec: 'Tempdrift rec',
                                    etc_table.notes:'Notes',
                                    etc_table.resistance:'Rs',
                                    etc_table.sample_id_small:'"Sample_ID"',
                                    etc_table.pressure: 'pressure',
                                    etc_table.temperature_sample: 'temperature_sample',
                                    etc_table.cycle_number: 'cycle_number',
                                    etc_table.cycle_number_flag: 'cycle_number_flag',
                                    etc_table.de_hyd_state: 'de_hyd_state'}
    etc_xy_column_attribute_mapping ={  xy_table.point_number: 'point_nr',
                                        xy_table.time: 'time',
                                        xy_table.t_f_tau: 't_f_tau',
                                        xy_table.temperature: 'temperature',
                                        xy_table._time_temperature_increase: 'time_temperature_increase',
                                        xy_table.temperature_increase: 'temperature_increase',
                                        xy_table.sqrt_time: 'sqrt_time',
                                        xy_table.temp_diff: 'diff_temperature',
                                        xy_table.drift_time: 'time_drift',
                                        xy_table.temperature_drift: 'temperature_drift'}

    def __init__(self, file_path='dummy', results_sheet_name='Results',
                 parameters_sheet_name='Parameters', log_directory='logs',
                 log_file='process.log', sample_id=None, meta_data=MetaData()):
        self.file_path = file_path
        self.results_sheet_name = results_sheet_name
        self.parameters_sheet_name = parameters_sheet_name
        self.log_directory = log_directory
        self.log_file = log_file
        self.logger = AppLogger().get_logger(__name__)


        if sample_id:
            self.meta_data = MetaData(sample_id=sample_id)
        else:
            self.meta_data = meta_data
        self._test_mode = False

    def _update_xlsx_file(self):
        try:
            excel = win32.gencache.EnsureDispatch('Excel.Application')
            workbook = excel.Workbooks.Open(self.file_path)
            workbook.RefreshAll()
            excel.Calculate()
            workbook.Save()
            workbook.Close()
            excel.Quit()
            self.logger.info("Excel file updated successfully.")
        except Exception as e:
            self.logger.error(f"Error updating Excel file: %s", e)

    def _read_and_process_sheets(self):
        try:
            dtype_spec = {
             'Temp.drift rec.': 'float'  # Replace 'YourColumnName' with the actual column name
                         }

            df_parameters = pd.read_excel(self.file_path, sheet_name=self.parameters_sheet_name, header=1, dtype=dtype_spec)
            df_parameters = df_parameters.dropna(subset=['Description'])
            df_parameters.columns = df_parameters.columns.str.replace(r'[^\w\s]', '', regex=True)
            df_parameters = df_parameters.dropna(subset=['Description'])

            df_results = pd.read_excel(self.file_path, sheet_name=self.results_sheet_name, header=1)
            df_results.columns = df_results.columns.str.replace(r'[^\w\s]', '', regex=True)
            df_results = self._process_results_sheet_for_table(df_results)

            merged_df = self._merge_data(df_results, df_parameters)
            return merged_df
        except Exception as e:
            self.logger.error(f"Error reading and processing sheets: %s", e)
            return None

    @staticmethod
    def _process_results_sheet_for_table(df):
        df_copy = df.copy()
        keys = [('Average', '_avg'), ('StandardDeviation', '_dvt')]

        # Store the original column names to avoid adding suffixes to new columns
        original_columns = [col for col in df_copy.columns if col not in ['File', 'Description', 'Sample ID', 'Points']]

        for key, column_addition in keys:
            # Create new columns for the specified key in advance
            for col in original_columns:
                new_col_name = col + column_addition
                if new_col_name not in df_copy.columns:
                    df_copy[new_col_name] = np.nan

            key_rows = df_copy[df_copy['Description'].str.contains(key, na=False)]

            for index, row in key_rows.iterrows():
                target_index = index - 1 if key == 'Average' else index - 2
                if target_index < 0:
                    continue

                for col in original_columns:
                    new_col_name = col + column_addition
                    df_copy.at[target_index, new_col_name] = row[col]

            df_copy = df_copy[~df_copy.index.isin(key_rows.index)]

        return df_copy

    def _merge_data(self, results_sheet, parameters_sheet):
        # Reset index for both DataFrames
        results_sheet = results_sheet.reset_index(drop=True)
        parameters_sheet = parameters_sheet.reset_index(drop=True)

        # Ensure that both DataFrames have the same number of rows
        if len(results_sheet) == len(parameters_sheet):
            # Compare 'Description' columns
            description_matches = results_sheet['Description'] == parameters_sheet['Description']

            # Filter for matching rows
            matching_df_results = results_sheet[description_matches]
            matching_df_parameters = parameters_sheet[description_matches]

            # Concatenate side by side
            combined_sheet = pd.concat([matching_df_results, matching_df_parameters], axis=1)

            # Remove duplicate columns
            combined_sheet = combined_sheet.loc[:, ~combined_sheet.columns.duplicated()]
            # Move 'Time' column to the first position
            time_col = combined_sheet.pop('Time')
            combined_sheet.insert(0, 'Time', time_col)

            # Convert 'Time' column to proper datetime format
            # Convert 'Time' column to proper datetime format for PostgreSQL
            # Assuming 'combined_sheet' is your DataFrame
            combined_sheet['Time'] = pd.to_datetime(combined_sheet['Time'])
            # Format the 'Time' column to include milliseconds
            combined_sheet['Time'] = combined_sheet['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            combined_sheet["Time"] = pd.to_datetime(combined_sheet["Time"])
            combined_sheet["Time"] = combined_sheet["Time"].dt.tz_localize(local_tz, ambiguous='NaT')

            return combined_sheet
        else:
            #print("The DataFrames have different lengths and cannot be concatenated directly.")
            self.logger.error("The DataFrames have different lengths and cannot be concatenated directly.")
            return None

    def save_combined_data(self, combined_df, output_file_path):
        try:
            cols = combined_df.columns.tolist()
            cols.insert(0, cols.pop(cols.index('Time')))
            combined_df = combined_df[cols]

            combined_df.to_csv(output_file_path, sep=';', index=False)
            self.logger.info(f"Data saved to {output_file_path}")
        except Exception as e:
            self.logger.error(f"Error saving combined data: %s", e)

    def _t_f_tau_t_t_diff_reader(self, param_res_combined_df, sheet_name):
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, header=3)
        if sheet_name == 'T-f(Tau)':
            x_col_name = 't_f_tau'
            y_col_name = 'temperature'
            point_nr_name = 'point_nr'
            time_name = 'time'
        elif sheet_name == 'T-t':
            x_col_name = 'time_temperature_increase'
            y_col_name = 'temperature_increase'
            point_nr_name = 'point_nr_t_t'
            time_name = 'time_t_t'
        elif sheet_name == 'Diff':
            x_col_name = 'sqrt_time'
            y_col_name = 'diff_temperature'
            point_nr_name = 'point_nr_diff'
            time_name = 'time_diff'
        elif sheet_name == 'T(drift)':
            x_col_name = 'time_drift'
            y_col_name = 'temperature_drift'
            point_nr_name = 'point_nr_drift'
            time_name = 'time_total_drift'

        if sheet_name == 'T(drift)':
            # Extend the 'Point nr.' column to include values up to 200
            current_length = df['Point nr.'].shape[0]
            if current_length < 200:
                additional_points = range(current_length + 1, 201)
                df = df._append(
                    [{'Point nr.': i} for i in additional_points],
                    ignore_index=True
                )

            # Correct the DataFrame to include the missing value 101
            df.loc[100, 'Point nr.'] = 101
            df['Point nr.'] = df['Point nr.'].sort_values().values

        # Drop the 'Unnamed' columns from the t_f_tau_df
        df = df.dropna(axis=1, how='all')
        if x_col_name not in df.columns:
            # Add the column with NaN (or another default value) as its values
            df[x_col_name] = np.nan  # You can use None or a specific default value instead of np.nan
        if y_col_name not in df.columns:
            # Add the column with NaN (or another default value) as its values
            df[y_col_name] = np.nan  # You can use None or a specific default value instead of np.nan

        # Check if the data points length is exactly 200
        if len(df['Point nr.']) > 200:
            # Handle the case where the length is not 200
            df = self._adjust_and_trim_df(df, original_length=len(df['Point nr.']), target_length=200)
            #print(f"Data length in sheet '{sheet_name}' is not 200. Found: {len(df['Point nr.'])} points.")

        # Extracting the time column from combined_df
        time_column = param_res_combined_df.iloc[:, 0]  # Assuming first column is time
        # Extract the "Point nr." column
        point_nr_column = df.iloc[:, 0]
        # Calculate the number of f(Tau) and Temp pairs
        num_pairs = min((len(df.columns) - 1) // 2, len(time_column))
        # Initialize a dictionary to hold the new data with time
        xy_time_data = {}
        # Iterate over the number of pairs
        for i in range(num_pairs):
            # Add a copy of the "Point nr." column
            xy_time_data[f'{point_nr_name}_{i}'] = point_nr_column

            # Columns indices for f(Tau) and Temp
            x_index = i * 2 + 1  # Adjust index to skip "Point nr."
            y_index = x_index + 1

            # Extract the pair of f(Tau) and Temp columns
            x_column = df.iloc[:, x_index]
            y_column = df.iloc[:, y_index]

            # Assign the time value
            xy_time_data[f'{time_name}_{i}'] = time_column[i]
            xy_time_data[f'{x_col_name}_{i}'] = x_column
            xy_time_data[f'{y_col_name}_{i}'] = y_column

        # Convert the dictionary to a DataFrame
        xy_time_df = pd.DataFrame(xy_time_data)
        xy_time_df.columns = [col.replace(' ', '_').replace('.', '').replace('(', '_').replace(')','') for col in xy_time_df.columns]
        xy_time_df = self._delete_duplicates_from_hot_disk_export(param_res_combined_df, xy_time_df)
        #todo das geht so nicht! 2 fälle machen. einmal mit drift messung import einmal ohne drift messung import. andere query nehmen einfach
        if xy_time_df.empty:
            df_filler = pd.DataFrame(columns=[x_col_name, y_col_name])
            df_filler[x_col_name] = [1, 2, 3]
            return df_filler

        df_for_table = self._vertically_concatenate(xy_time_df, 4)

        return df_for_table
        # Save the DataFrame to a .txt file with tab delimiters

    @staticmethod
    def _adjust_and_trim_df(df, original_length=201, target_length=200):
        # Shift all columns except the first one up by n rows
        n = original_length - target_length
        df.iloc[:, 1:] = df.iloc[:, 1:].shift(-n)
        # Truncate the DataFrame to have exactly target_length rows
        df = df.head(target_length)
        return df

    @staticmethod
    def _vertically_concatenate(df, row_number=4):
        # List to hold the individual dataframes to be concatenated
        dfs_to_concatenate = []
        # Determine the number of sets of data based on the row_number
        num_sets = len(df.columns) // row_number
        row_prefixes = df.columns

        # Iterate through each set and create individual dataframes
        for i in range(num_sets):
            # Calculate start and end indices for slicing columns
            start_index = i * row_number
            end_index = start_index + row_number
            # Extracting a subset of columns for each set
            subset_df = df.iloc[:, start_index:end_index]
            subset_df = subset_df.rename(columns=lambda x: re.sub(r'_(\d+)$', '', x))
            # Append to the list
            dfs_to_concatenate.append(subset_df)

        # Concatenate all dataframes vertically
        concatenated_df = pd.concat(dfs_to_concatenate, ignore_index=True)
        try:
            concatenated_df = concatenated_df.drop(columns='point_nr_drift')
        except KeyError:
            pass
        return concatenated_df

    def _get_measurement_xy_data(self, combined_df):
        t_f_tau = self._t_f_tau_t_t_diff_reader(combined_df, 'T-f(Tau)')
        t_t = self._t_f_tau_t_t_diff_reader(combined_df, 'T-t')
        diff = self._t_f_tau_t_t_diff_reader(combined_df, 'Diff')
        drift = self._t_f_tau_t_t_diff_reader(combined_df, 'T(drift)')
        measurement_xy_data = pd.concat([t_f_tau, t_t, diff, drift], axis=1)
        measurement_xy_data = measurement_xy_data.T.drop_duplicates().T

        return measurement_xy_data

    def _write_to_database(self, insert_query: str, values: tuple, table_name: str = ""):

        with DatabaseConnection() as db_conn:
            self.logger.info("Starting writing thermal conductivity data to %s data base...", table_name)
            #print("Starting writing thermal conductivity data to data base...")
            try:
                # Iterate over DataFrame rows as tuples
                db_conn.cursor.executemany(insert_query, values)
                db_conn.cursor.connection.commit()

                self.logger.info(f"Thermal conductivity data inserted: {self.file_path}")
                #print("Thermal conductivity data inserted")
                return False

            except IntegrityError as e:
                self.logger.error("Error occurred while inserting thermal conductivity data: %s. Trying to delete and writing again", e)
                # print("Error while inserting thermal conductivity data", e)
                db_conn.cursor.connection.rollback()
                return True

            except Exception as e:
                #todo: delete return maybe since integrity error is raised separate now
                self.logger.error("Error occurred while inserting thermal conductivity data: %s", e)
                # print("Error while inserting thermal conductivity data", e)
                db_conn.cursor.connection.rollback()
                return True

    def _delete_data_from_table(self, data_to_delete):
        with DatabaseConnection() as db_conn:
            self.logger.info("Starting deletion of data from %s data base...", self.etc_table.table_name)
            data_to_delete = [(time_vals,) for time_vals in data_to_delete['Time']]
            # Assuming data_to_delete is a DataFrame or list of identifiers (like primary keys) of the rows to be deleted
            try:
                delete_query = f"DELETE FROM {self.etc_table.table_name} WHERE \"Time\" = %s"  # Replace with your table and column names
                delete_query_xy = f"DELETE FROM {self.xy_table.table_name} WHERE \"time\" = %s"
                db_conn.cursor.executemany(delete_query, data_to_delete)
                db_conn.cursor.executemany(delete_query_xy, data_to_delete)
                db_conn.cursor.connection.commit()
                self.logger.info("Data deleted successfully")
            except Exception as e:
                self.logger.error("Error occurred while deleting data: %s", e)
                db_conn.cursor.connection.rollback()

    @staticmethod
    def _delete_duplicates_from_hot_disk_export(df, xy_df):
        # Add a unique identifier to the original DataFrame
           # Save original indices
        original_indices = set(df.index)
        # Perform deduplication
        deduped_df = df.drop_duplicates(subset='Time', keep='last')
        # Get indices after deduplication
        deduped_indices = set(deduped_df.index)
        # Find indices of removed rows
        removed_indices = original_indices - deduped_indices
        multiplied_indices = set(map(lambda x: x * 4, removed_indices))
        # Create a set to store all indices to be removed from df2
        indices_to_remove = set()
        # Add each original index and the next three indices
        for index in sorted(multiplied_indices):
            new_indices = set(range(index, min(index + 4, len(xy_df.columns))))
            indices_to_remove = indices_to_remove.union(new_indices)
        # Convert set to sorted list and validate indices
        valid_indices = sorted(index for index in indices_to_remove if index < len(xy_df.columns))
        columns_to_remove = xy_df.columns[valid_indices]
        xy_df = xy_df.drop(columns=columns_to_remove)
        return xy_df

    def _find_corresponding_t_p(self, df_etc):
        t_p_table = TableConfig().TPDataTable
        etc_table = TableConfig().ETCDataTable
        start_time = min(df_etc[self.etc_table.get_clean("time")])
        end_time = max(df_etc[self.etc_table.get_clean("time")])
        time_range = (start_time, end_time)

        cols = (t_p_table.time,
                t_p_table.pressure,
                t_p_table.temperature_sample,
                t_p_table.cycle_number,
                t_p_table.cycle_number_flag,
                t_p_table.de_hyd_state)
        cols_etc = [self.etc_table.get_clean('time'),
                    t_p_table.pressure,
                    t_p_table.temperature_sample,
                    t_p_table.cycle_number,
                    t_p_table.cycle_number_flag,
                    t_p_table.de_hyd_state]

        db_retriever = DataRetriever()
        df_tp = db_retriever.fetch_data_by_time_no_limit(table=t_p_table, time_range=time_range, col_names=cols)
        if df_tp.empty:
            self.logger.info("No corresponding t_p_data found")
            return pd.DataFrame()

        df_etc = df_etc.sort_values(self.etc_table.get_clean('time'))
        df_tp = df_tp.rename(columns={t_p_table.time : self.etc_table.get_clean('time')})
        df_tp = df_tp.sort_values(self.etc_table.get_clean('time'))
        df_tp[self.etc_table.get_clean('time')] = df_tp[self.etc_table.get_clean('time')].dt.tz_convert(local_tz)

        df_tp_etc = pd.merge_asof(df_etc, df_tp, on=self.etc_table.get_clean('time'),
                                      direction='nearest',
                                      suffixes=('_etc', '_tp'))
        df_merged = df_tp_etc[cols_etc]

        return df_merged

    def _update_sample_id_col(self, time_range):
        time_start = min(time_range)
        time_end = max(time_range)
        etc_table = TableConfig().ETCDataTable
        table_name_etc = etc_table.table_name
        time_col_etc = etc_table.time
        xy_table = TableConfig().ThermalConductivityXyDataTable
        table_name_xy = xy_table.table_name
        time_col_xy = xy_table.time
        values =  (self.meta_data.sample_id, time_start, time_end)


        update_etc_query = f"Update {table_name_etc} " \
                           f"SET " \
                           f"sample_id = %s" \
                           f" WHERE {time_col_etc} " \
                           f"BETWEEN %s AND %s"
        update_xy_query =  f"Update {table_name_xy} " \
                            f"SET " \
                            f"sample_id = %s" \
                            f" WHERE {time_col_xy} " \
                            f"BETWEEN %s AND %s"

        with DatabaseConnection() as db_conn:
            try:
                db_conn.cursor.execute(update_etc_query, values)
                db_conn.cursor.execute(update_xy_query, values)
            except Exception as e:
                self.logger.error(f"Error occurred while updating sample_id col for ETC and ETC_XY: %s", e)
                db_conn.cursor.connection.rollback()

    def execute(self):
        table = TableConfig.ETCDataTable
        self._update_xlsx_file()
        combined_df = self._read_and_process_sheets()

        if combined_df is not None:
            if self._test_mode:
                combined_df = self._overwrite_with_example_data(df=combined_df)

            thermal_conductivity_xy_data = self._get_measurement_xy_data(combined_df)
            combined_df = combined_df.drop_duplicates(subset=table.get_clean("time"), keep='last')

            combined_df[self.etc_table.sample_id] = self.meta_data.sample_id
            thermal_conductivity_xy_data[self.xy_table.sample_id] = self.meta_data.sample_id
            combined_df = combined_df.dropna(subset=[table.get_clean("time")])
            thermal_conductivity_xy_data = thermal_conductivity_xy_data.dropna(subset=[self.xy_table.time])
            df_t_p = self._find_corresponding_t_p(combined_df)
            if not combined_df.empty and not df_t_p.empty:
                combined_df = pd.merge(combined_df, df_t_p, on=self.etc_table.get_clean('time'), how='inner')
            else:
                combined_df[self.etc_table.pressure] = None
                combined_df[self.etc_table.temperature_sample] = None
                combined_df[self.etc_table.cycle_number] = None
                combined_df[self.etc_table.cycle_number_flag] = None

            pd.set_option('future.no_silent_downcasting', True)
            combined_df.replace('(no corr.)', 0, inplace=True)
            ETC_insert_query, ETC_values = TableConfig().writing_query_from_df(df=combined_df,
                                                                               map=self.etc_column_attribute_mapping,
                                                                               table_name=self.etc_table.table_name)
            xy_insert_query, xy_values = TableConfig().writing_query_from_df(df=thermal_conductivity_xy_data,
                                                                             map=self.etc_xy_column_attribute_mapping,
                                                                             table_name=self.xy_table.table_name)

            #output_file_path = file_path.replace('.xlsx', '_combined.csv')
            #processor.save_combined_data(combined_df, output_file_path)
            error_checker = self._write_to_database(insert_query=ETC_insert_query,
                                                    values=ETC_values,
                                                    table_name=self.etc_table.table_name)

            if error_checker:
                self._delete_data_from_table(combined_df)
                self._write_to_database(insert_query=ETC_insert_query,
                                        values=ETC_values,
                                        table_name=self.etc_table.table_name)
                self._write_to_database(insert_query=xy_insert_query,
                                        values=xy_values,
                                        table_name=self.xy_table.table_name)

            else:
                self._write_to_database(insert_query=xy_insert_query,
                                        values=xy_values,
                                        table_name=self.xy_table.table_name)
        time_range = (min(combined_df[table.get_clean("time")]), max(combined_df[table.get_clean("time")]))
        return time_range

    def _overwrite_with_example_data(self, df):
        start_time = datetime.now(tz=local_tz)
        duration = timedelta(minutes=1)  # Example duration of 2 hours
        x = len(df["Time"])  # Number of equidistant points
        # Calculate the end time
        end_time = start_time - duration
        # Generate x equidistant time points between start_time and end_time
        equidistant_points = np.linspace(0, 1, x)
        time_points = [start_time - (start_time - end_time) * point for point in equidistant_points]
        time_points = [tp.replace(microsecond=0) for tp in time_points]

        df["Time"] = time_points
        #print(df['Time'])
        self.logger.info("Data times overwritten for testing purpose")
        return df


def test_update_first_hydr():
    db_manipulator = DataBaseManipulator()

    date_first_hyd = datetime(2023, 5, 23, 10, 20, 00)
    sample_id = "WAE-WA-040"
    db_manipulator.update_flags(sample_id=sample_id, date_first_hydrogenation=date_first_hyd)


def test_data_retriever():
   qb = QueryBuilder()
   data_retriever = DataRetriever()

   sample_id = 'WAE-WA-040'
   table_name = TableConfig().ETCDataTable.table_name
   #table_name = TableConfig().TPDataTable.table_name
   df = data_retriever.fetch_data_by_sample_id_2(column_names=None, table_name=table_name, sample_id=sample_id)
   print(df)


def test_excel_data_processor(etc_dir, sample_id):
    ETC_dir= etc_dir
    sample_id = sample_id
    etc_processor = ExcelDataProcessor(sample_id=sample_id, file_path=ETC_dir)
    etc_processor.execute()


def process_ETC_file(args):
    file_path, sample_id, logger = args
    try:
        logger.info(f"Start writing {os.path.basename(file_path)} to database")
        ETC_processor = ExcelDataProcessor(file_path=file_path, sample_id=sample_id)
        ETC_processor.execute()
        logger.info(f"{os.path.basename(file_path)} written to database")
    except Exception as e:
        logger.error(f"Error processing {os.path.basename(file_path)}: {e}")


def write_ETC_in_parallel(dir_etc_folder, sample_id, logger):
    time_start = time.time()
    directory_path = dir_etc_folder

    # Collect all file paths
    file_paths = [
        os.path.join(directory_path, filename)
        for filename in os.listdir(directory_path)
        if filename.endswith('.xlsx') and "$" not in filename
    ]

    # Prepare arguments for processing
    args = [(file_path, sample_id, logger) for file_path in file_paths]

    # Process files in parallel using multiprocessing Pool
    with Pool(processes=4) as pool:
        pool.map(process_ETC_file, args)

    print(f"Import took {(time.time() - time_start) / 3600} hours")


if __name__ == "__main__":
    sample_id = 'WAE-WA-030'
    dir_etc = r"C:\Daten\Kiki\WAE-WA-030-Mg2NiH4\WAE-WA-030-All\WAE-WA-030-044-320-420C.xlsx"
    dir_etc = r"C:\Daten\Kiki\WAE-WA-030-Mg2NiH4\WAE-WA-030-All\WAE-WA-030-045-400C-ParameterTest.xlsx"

    test_excel_data_processor(etc_dir=dir_etc, sample_id=sample_id)
