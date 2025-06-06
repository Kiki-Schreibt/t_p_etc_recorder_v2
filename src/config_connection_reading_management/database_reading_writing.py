##database_reading_writing.py
import numpy as np
import win32com.client as win32
import re
import os
import time
from zoneinfo import ZoneInfo
from multiprocessing import Pool
from datetime import datetime
from typing import Optional, Tuple, Union, List
import math


import pandas as pd
from psycopg2 import IntegrityError

from src.infrastructure.connections.connections import DatabaseConnection
try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging

from src.config_connection_reading_management.query_builder import QueryBuilder
from src.infrastructure.meta_data.meta_data_handler import MetaData
from src.infrastructure.core.table_config import TableConfig


local_tz = ZoneInfo("Europe/Berlin")
LIMIT_DATA_POINTS = 5000

class DataRetriever:
    """
    Class for retrieving data from the database.
    """
    xy_table = TableConfig().ThermalConductivityXyDataTable
    etc_table = TableConfig().ETCDataTable

    etc_column_attribute_mapping = {
        etc_table.time: 'Time',
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
        etc_table.notes: 'Notes',
        etc_table.resistance: 'Rs',
        etc_table.sample_id_small: '"sample_id"',
        etc_table.pressure: 'pressure',
        etc_table.temperature_sample: 'temperature_sample',
        etc_table.cycle_number: 'cycle_number',
        etc_table.cycle_number_flag: 'cycle_number_flag',
        etc_table.de_hyd_state: 'de_hyd_state',
        etc_table.test_info: 'test_info',
        etc_table.is_isotherm_flag: 'is_isotherm_flag'
    }

    def __init__(self, db_conn_params):
        self.running = False
        self.db_conn_params = db_conn_params
        self.qb = QueryBuilder(db_conn_params=self.db_conn_params)
        self.logger = logging.getLogger(__name__)
        self.limit_datapoints = LIMIT_DATA_POINTS


    def fetch_latest_records(
        self,
        cursor,
        table_name: str = 't_p_data',
        column_names: Optional[Union[List[str], Tuple[str, ...]]] = None,
        constraints: Optional[dict] = None,
        desc_limit: int = 1,
        sample_id: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Fetches the latest records from a specified table.
        """
        query, values = self.qb.create_continuous_reading_query(
            column_names=column_names,
            table_name=table_name,
            constraints=constraints,
            desc_limit=desc_limit,
            sample_id=sample_id
        )
        df = self.execute_continuous_fetching(
            query=query,
            cursor=cursor,
            column_names=column_names,
            table_name=table_name,
            values=values
        )
        return df

    def fetch_tp_and_etc_data(
        self,
        time_range: Optional[Tuple[datetime, datetime]] = None,
        sample_id: Optional[str] = None,
        column_names_t_p: Optional[Union[List[str], Tuple[str, ...]]] = None,
        column_names_etc: Union[List[str], Tuple[str, ...]] = ('"Time"', '\"ThConductivity_avg\"'),
        constraints: Optional[dict] = None,
        join_table: str = None,
        join_on: list[tuple[str,str]] = None,
        join_constraints: dict = None
            ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetches TP and ETC data based on sample_id or time_range.
        """
        table_name_etc = TableConfig().ETCDataTable.table_name
        table_name_tp = TableConfig().TPDataTable.table_name

        if time_range and not sample_id:
            tp_data = self.fetch_data_by_time_2(
                time_range=time_range,
                column_names=column_names_t_p,
                table_name=table_name_tp,
                sample_id=sample_id,
                join_table=join_table,
                join_on=join_on,
                join_constraints=join_constraints
                )

        elif sample_id and not time_range:
            tp_data = self.fetch_data_by_sample_id_2(
                sample_id=sample_id,
                column_names=column_names_t_p,
                table_name=table_name_tp,
                join_table=join_table,
                join_on=join_on,
                join_constraints=join_constraints
            )

            if not tp_data.empty:
                min_time = tp_data['time'].min()
                max_time = tp_data['time'].max()
                time_range = (min_time, max_time)

        elif sample_id and time_range:
            tp_data = self.fetch_data_by_time_2(
                time_range=time_range,
                column_names=column_names_t_p,
                table_name=table_name_tp,
                sample_id=sample_id,
                join_table=join_table,
                join_on=join_on,
                join_constraints=join_constraints
            )

        else:
            tp_data = pd.DataFrame()

        if not tp_data.empty:
            etc_data = self.fetch_data_by_time_2(
                time_range=time_range,
                column_names=column_names_etc,
                table_name=table_name_etc,
                constraints=constraints,
                sample_id=sample_id,
                join_table=join_table,
                join_on=join_on,
                join_constraints=join_constraints
            )

        elif sample_id:
            etc_data = self.fetch_data_by_sample_id_2(
                sample_id=sample_id,
                column_names=column_names_etc,
                table_name=table_name_etc,
                constraints=constraints,
                join_table=join_table,
                join_on=join_on,
                join_constraints=join_constraints
            )

        elif time_range:
            etc_data = self.fetch_data_by_time_2(
                time_range=time_range,
                column_names=column_names_etc,
                table_name=table_name_etc,
                constraints=constraints,
                join_table=join_table,
                join_on=join_on,
                join_constraints=join_constraints
            )

        else:
            etc_data = pd.DataFrame()

        return tp_data, etc_data

    def fetch_data_by_sample_id_2(
        self,
        sample_id: str,
        table_name: str,
        column_names: Optional[Union[List[str], Tuple[str, ...]]] = None,
        constraints: Optional[dict] = None,
        join_table: str = None,
        join_on: list[tuple[str,str]] = None,
        join_constraints: dict = None
    ) -> pd.DataFrame:
        query, values = self.qb.create_reading_query(
            sample_id=sample_id,
            table_name=table_name,
            column_names=column_names,
            limit_data_points=self.limit_datapoints,
            constraints=constraints,
            join_table=join_table,
            join_on=join_on,
            join_constraints=join_constraints
        )
#        print("by ID: "+query)
#        print(f"With Values: {values}")
        return self.execute_fetching(
            query=query,
            column_names=column_names,
            table_name=table_name,
            values=values
        )

    def fetch_data_by_time_2(
        self,
        time_range: Tuple[datetime, datetime],
        table_name: str,
        column_names: Optional[Union[List[str], Tuple[str, ...]]] = None,
        constraints: Optional[dict] = None,
        sample_id: Optional[str] = None,
        join_table: str = None,
        join_on: list[tuple[str,str]] = None,
        join_constraints: dict = None
    ) -> pd.DataFrame:
        query, values = self.qb.create_reading_query(
            table_name=table_name,
            time_window=time_range,
            limit_data_points=self.limit_datapoints,
            column_names=column_names,
            constraints=constraints,
            sample_id=sample_id,
            join_table=join_table,
            join_on=join_on,
            join_constraints=join_constraints
        )
#        print("by time: "+query)
#        print(f"With Values: {values}")
        return self.execute_fetching(
            query=query,
            column_names=column_names,
            table_name=table_name,
            values=values
        )

    def fetch_xy_data(
        self,
        time_value: Union[str, datetime],
        row_package_name: str = 'Transient'
    ) -> pd.DataFrame:
        """
        Fetch ETC_XY data by time value. Returns 200 lines and 2 rows.
        """
        table = TableConfig().ThermalConductivityXyDataTable
        table_name = table.table_name
        time_column = table.time

        if isinstance(time_value, str):
            time_value = datetime.fromisoformat(time_value)
        if isinstance(time_value, datetime):
            if time_value.tzinfo is None:
                time_value = time_value.replace(tzinfo=local_tz)

        # Use match-case (Python 3.10+) for clarity
        match = row_package_name.lower()
        if match == 'transient':
            column_names = (table.transient_x, table.transient_y, time_column)
        elif match == 'drift':
            column_names = (table.drift_x, table.drift_y, time_column)
        elif match == 'calculated':
            column_names = (table.calculated_x, table.calculated_y, time_column)
        elif match == 'residual':
            column_names = (table.residual_x, table.residual_y, time_column)
        else:
            self.logger.error("Invalid row package name provided: %s", row_package_name)
            return pd.DataFrame()

        column_names_str = ', '.join(column_names)
        query = f"SELECT {column_names_str} FROM {table_name} WHERE {time_column} = %s ORDER BY {time_column}"
        params = (time_value,)

        try:
            with DatabaseConnection(**self.db_conn_params) as db_conn:
                db_conn.cursor.execute(query, params)
                records = db_conn.cursor.fetchall()
                col_names_clean = tuple(s.replace("\"", "") for s in column_names)
                df = pd.DataFrame.from_records(records, columns=col_names_clean)
            if not df.empty:
                df = df.explode([column_names[0], column_names[1]]).reset_index(drop=True)
                df = df.dropna()
                df = df.sort_values(by=col_names_clean[0], ascending=True)
                return df
            return pd.DataFrame()
        except Exception as e:
            self.logger.error("Error occurred while fetching xy data: %s", e)
            return pd.DataFrame()

    def fetch_last_state_and_cycle(self, meta_data) -> Tuple:
        if meta_data.last_de_hyd_state and meta_data.total_number_cycles:
            return meta_data.last_de_hyd_state, meta_data.total_number_cycles
        else:
            return "Dehydrogenated", 0

    def fetch_data_by_cycle(
        self,
        cycle_numbers: Union[int, float, List[Union[int, float]]],
        sample_id: str,
        column_names: Optional[Union[List[str], Tuple[str, ...]]] = None,
        constraints: Optional[dict] = None,
        table: Optional[TableConfig] = None,
        join_table: str = None,
        join_on: list[tuple[str,str]] = None,
        join_constraints: dict = None
    ) -> pd.DataFrame:
        if not table:
            table = TableConfig().TPDataTable
        table_name = table.table_name
        sample_id_col = table.sample_id
        if not column_names:
            column_names = TableConfig().get_table_column_names(table_name=table_name, table_class=table)
        column_name_str = ', '.join(column_names) if column_names else None

        if not column_name_str:
            self.logger.error("Couldn't find column names")
            return pd.DataFrame()

        if isinstance(cycle_numbers, (list, tuple)):
            # Convert possible numpy types to native float
            cycle_numbers = [float(cn) for cn in cycle_numbers]
            placeholders = ', '.join(['%s'] * len(cycle_numbers))
            query = (f"SELECT {column_name_str} FROM {table_name} WHERE {sample_id_col} = %s AND "
                     f"{table.cycle_number} IN ({placeholders}) ORDER BY {table.time}")
            values = [sample_id] + cycle_numbers
        else:
            query = (f"SELECT {column_name_str} FROM {table_name} WHERE {sample_id_col} = %s AND "
                     f"{table.cycle_number} = %s")
            values = [sample_id, float(cycle_numbers)]
        df = self.execute_fetching(query=query, column_names=column_names, table_name=table_name, values=values)
        return df

    def fetch_data_by_time_no_limit(self, table: TableConfig,
                                    time_range: Tuple[datetime, datetime],
                                    col_names: Optional[List[str]] = None) -> pd.DataFrame:
        if not col_names:
            col_names = TableConfig().get_table_column_names(table_class=table)
        col_name_str = ", ".join(col_names)
        query = (f"SELECT {col_name_str} FROM {table.table_name} WHERE {table.time} BETWEEN %s AND %s "
                 f"ORDER BY {table.time}")
        return self.execute_fetching(query=query, values=time_range, column_names=col_names)

    def execute_fetching(
        self,
        query: str,
        column_names: Optional[Union[List[str], Tuple[str, ...]]] = None,
        table_name: Optional[str] = None,
        values: Optional[Union[tuple, list]] = None
    ) -> pd.DataFrame:
        if column_names is None:
            column_names = TableConfig().get_table_column_names(table_name=table_name)
        try:
            with DatabaseConnection(**self.db_conn_params) as db_conn:
                db_conn.cursor.execute(query, values)
                records = db_conn.cursor.fetchall()
            col_names_clean = (tuple(s.replace("\"", "") for s in column_names)
                               if isinstance(column_names, (list, tuple)) else [column_names])
            if records:
                df = pd.DataFrame.from_records(records, columns=col_names_clean)
                df = self._adjust_df_types_and_times(df)
            else:
                df = pd.DataFrame()
            return df
        except Exception as e:
            self.logger.error("Error occurred while fetching data: %s", e)
            return pd.DataFrame()

    def execute_continuous_fetching(
        self,
        query: str,
        cursor,
        column_names: Optional[Union[List[str], Tuple[str, ...]]] = None,
        table_name: Optional[str] = None,
        values: Optional[Union[tuple, list]] = None
    ) -> Optional[pd.DataFrame]:
        if column_names is None:
            column_names = TableConfig().get_table_column_names(table_name=table_name)
        try:
            cursor.execute(query, values)
            records = cursor.fetchall()
            col_names_clean = (tuple(s.replace("\"", "") for s in column_names)
                               if isinstance(column_names, (list, tuple)) else [column_names])
            if records:
                df = pd.DataFrame.from_records(records, columns=col_names_clean)
                df = self._adjust_df_types_and_times(df)
            else:
                df = pd.DataFrame()
            return df
        except Exception as e:
            self.logger.error("Error occurred during continuous fetching: %s", e)
            return None

    def _adjust_df_types_and_times(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adjust DataFrame columns for flags and timezones.
        """
        for col in df.columns:
            if "_flag" in col.lower():
                df[col] = df[col].apply(lambda x: str(x).lower() in ('t', '1', 'true'))
            if "time" in col.lower():
                if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    if df[col].dt.tz is None:
                        df[col] = df[col].dt.tz_localize('UTC').dt.tz_convert(local_tz)
                    else:
                        df[col] = df[col].dt.tz_convert(local_tz)
        return df

    @staticmethod
    def remove_timezone(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)
        return df

    def close_connection(self) -> None:
        pass


class DataBaseManipulator:
    """
    Handles batch updates and manipulations of the database.
    """
    tp_table = TableConfig().TPDataTable

    def __init__(self, db_conn_params=None):
        self.running = False
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params or {}

    def execute_updating(self, query: str, values: list, many_bool: bool = True) -> None:
        with DatabaseConnection(**self.db_conn_params) as db_conn:
            try:
                if many_bool:
                    db_conn.cursor.executemany(query, values)
                else:
                    db_conn.cursor.execute(query, values)
                db_conn.cursor.connection.commit()
            except Exception as e:
                db_conn.cursor.connection.rollback()
                self.logger.error("Error during update execution: %s", e)

    def batch_update_data(
        self,
        sample_id: Optional[str] = None,
        table: Optional[TableConfig] = None,
        df_vals_to_update: Optional[pd.DataFrame] = None,
        col_to_match: Optional[str] = None,
        other_col_to_match: Optional[str] = None,
        other_col_to_match_values: Optional[pd.Series] = None,
        update_between_min_list: Optional[pd.Series] = None,
        update_between_max_list: Optional[pd.Series] = None
    ) -> None:
        if table is None or df_vals_to_update is None or update_between_min_list is None or update_between_max_list is None:
            self.logger.error("Insufficient data provided for batch update.")
            return

        update_between_max_list = update_between_max_list.tolist()
        update_between_min_list = update_between_min_list.tolist()
        table_name = table.table_name
        sample_id_col = table.sample_id
        query_part = ", ".join([f"{col} = %s" for col in df_vals_to_update.columns.tolist()])
        query = (f"UPDATE {table_name} SET {query_part} WHERE {sample_id_col} = %s "
                 f"AND {col_to_match} BETWEEN %s AND %s")
        values_to_update = []
        for time_start, time_end, update_values in zip(update_between_min_list, update_between_max_list, df_vals_to_update.values.tolist()):
            values = tuple(update_values) + (sample_id, time_start, time_end)
            values_to_update.append(values)
        if other_col_to_match and other_col_to_match_values is not None and not other_col_to_match_values.empty:
            optional_query_part = f" AND {other_col_to_match} = %s"
            query += optional_query_part
            # Append the extra matching column value for each row
            values_to_update = [vals + (other_col_to_match_values.iloc[i],) for i, vals in enumerate(values_to_update)]
        self.execute_updating(query=query, values=values_to_update)
        self.logger.info("Batch updated %d records in %s", len(values_to_update), table_name)

    def update_data(
        self,
        sample_id: Optional[str] = None,
        table: object = None,
        update_df: Optional[Union[pd.DataFrame, pd.Series]] = None,
        col_to_match: Optional[str] = None,
        update_between_vals: Optional[Union[Tuple, List]] = None,
    ) -> bool:
        """
        Update one or more columns in a database table for rows matching a sample ID
        and a value range on another column.

        :param sample_id: The sample identifier to match in `table.sample_id` column.
        :type sample_id: Optional[str]
        :param table:      A table‐metadata object exposing `table_name` and `sample_id`
                           attributes. e.g. `TableConfig().TPDataTable`.
        :type table:       object
        :param update_df:  A single‐row pandas DataFrame or pandas Series. Its columns
                           (or index) name the columns to update; its values are the new
                           values to write.
        :type update_df:   Optional[Union[pd.DataFrame, pd.Series]]
        :param col_to_match: Name of a second column in the WHERE clause. Only rows where
                             this column is BETWEEN the two values in `update_between_vals`
                             will be updated.
        :type col_to_match: Optional[str]
        :param update_between_vals: A 2‐element tuple or list giving the inclusive lower
                                    and upper bounds for `col_to_match`. e.g. (`start_ts`, `end_ts`).
        :type update_between_vals: Optional[Union[Tuple, List]]

        :returns: True if the UPDATE executed and committed successfully; False otherwise.
        :rtype: bool

        :example:
        >>> df = pd.DataFrame([{'pressure': 1.23, 'temperature': 300.0}])
        >>> success = updater.update_data(
        ...     sample_id="WAE-WA-028",
        ...     table=TableConfig().TPDataTable,
        ...     update_df=df,
        ...     col_to_match='timestamp',
        ...     update_between_vals=(1600000000, 1600003600)
        ... )
        >>> if success:
        ...     print("Rows updated")
        ... else:
        ...     print("Update failed")
        """

        if table is None or update_df is None or col_to_match is None or update_between_vals is None:
            self.logger.error("Missing parameters for update_data")
            return False

        table_name = table.table_name
        if table_name == TableConfig().ETCDataTable.table_name:
            sample_id_col = table.sample_id_small
        else:
            sample_id_col = table.sample_id

        if isinstance(update_df, pd.DataFrame):
            cols_to_update = [f"{col} = %s" for col in update_df.columns]
            tuple_values = tuple(update_df.iloc[0])
        elif isinstance(update_df, pd.Series):
            cols_to_update = [f"{col} = %s" for col in update_df.index.tolist()]
            tuple_values = tuple(update_df.values)
        else:
            self.logger.error("update_df must be a DataFrame or Series")
            return False

        if not isinstance(update_between_vals, tuple):
            update_between_vals = tuple(update_between_vals) if isinstance(update_between_vals, list) else (update_between_vals,)

        query_part = ', '.join(cols_to_update)



        if len(update_between_vals) == 1:
            query = (f"UPDATE {table_name} SET {query_part} WHERE {sample_id_col} = %s "
                     f"AND {col_to_match} = %s")
        elif len(update_between_vals) == 2:
            query = (f"UPDATE {table_name} SET {query_part} WHERE {sample_id_col} = %s "
                     f"AND {col_to_match} BETWEEN %s and %s")
        else:
            self.logger.error("update_between_vals must be length 1 or 2")
            return False

        values = tuple_values + (sample_id,) + update_between_vals
        values = tuple(_to_native(v) for v in values)

        with DatabaseConnection(**self.db_conn_params) as db_conn:
            try:
                #print(query)
                #print(f"Values: {values}")
                db_conn.cursor.execute(query, values)
                db_conn.cursor.connection.commit()
                # Extract just the column names (drop the " = %s")
                col_names = [col_clause.split(" =")[0] for col_clause in cols_to_update]

                #tuple_values hold the new values in the same order:
                #    (pressure, temperature, …)
                #    so zip them together
                assignments = [
                    f"{name} = {val!r}"
                    for name, val in zip(col_names, tuple_values)
                ]
                if len(update_between_vals) == 1:
                    # 3) Log the joined assignments
                    self.logger.info(
                        "Updated %s in %s where %s = %s",
                        ", ".join(assignments),
                        table_name,
                        col_to_match,
                        update_between_vals[0]
                    )
                elif len(update_between_vals) == 2:
                    # 3) Log the joined assignments
                    self.logger.info(
                        "Updated %s in %s where %s between %s and %s",
                        ", ".join(assignments),
                        table_name,
                        col_to_match,
                        update_between_vals[0],
                        update_between_vals[1]
                    )
                return True
            except Exception as e:
                db_conn.cursor.connection.rollback()
                self.logger.error("Error in update_data: %s", e)
                return False


class ExcelDataProcessor:
    """
    Processes Excel files and writes data to the database.
    """
    etc_table = TableConfig().ETCDataTable
    xy_table = TableConfig().ThermalConductivityXyDataTable

    etc_column_attribute_mapping = DataRetriever.etc_column_attribute_mapping

    def __init__(
        self,
        file_path: str = 'dummy',
        results_sheet_name: str = 'Results',
        parameters_sheet_name: str = 'Parameters',
        sample_id: Optional[str] = None,
        meta_data: Optional[object] = None,
        db_conn_params=None
    ):
        self.file_path = file_path
        self.results_sheet_name = results_sheet_name
        self.parameters_sheet_name = parameters_sheet_name
        self.db_conn_params = db_conn_params or {}
        self.logger = logging.getLogger(__name__)
        self.meta_data = MetaData(sample_id=sample_id, db_conn_params=db_conn_params) if (sample_id and db_conn_params) else (meta_data or MetaData(db_conn_params=db_conn_params))
        self._test_mode = False

    def _update_xlsx_file(self) -> None:
        max_retries = 30
        delay = 0.1  # start with 100 ms delay
        for attempt in range(max_retries):
            try:
                excel = win32.gencache.EnsureDispatch('Excel.Application')
                workbook = excel.Workbooks.Open(self.file_path)
                workbook.RefreshAll()
                excel.Calculate()
                workbook.Save()
                workbook.Close()
                excel.Quit()
                self.logger.info("Excel file updated successfully.")
                return
            except Exception as e:
                error_str = str(e)
                # Check if the error message contains the OLE busy error code.
                if "0x800ac472" in error_str:
                    self.logger.warning("Excel busy (attempt %d/%d): %s", attempt + 1, max_retries, e)
                    time.sleep(delay)
                    delay *= 2  # Optional: increase delay with each attempt.
                else:
                    self.logger.error("Error updating Excel file: %s", e)
                    return
        self.logger.error("Failed to update Excel file after %d attempts.", max_retries)

    def _read_and_process_sheets(self) -> pd.DataFrame:
        try:
            dtype_spec = {'Temp.drift rec.': 'float'}
            df_parameters = pd.read_excel(
                self.file_path,
                sheet_name=self.parameters_sheet_name,
                header=1,
                dtype=dtype_spec
            )
            df_parameters = df_parameters.dropna(subset=['Description'])
            df_parameters.columns = df_parameters.columns.str.replace(r'[^\w\s]', '', regex=True)
            df_parameters = df_parameters.dropna(subset=['Description'])

            df_results = pd.read_excel(self.file_path, sheet_name=self.results_sheet_name, header=1)
            df_results.columns = df_results.columns.str.replace(r'[^\w\s]', '', regex=True)
            df_results = self._process_results_sheet_for_table(df_results)

            merged_df = self._merge_data(df_results, df_parameters)
            return merged_df
        except Exception as e:
            self.logger.error("Error reading and processing sheets: %s", e)
            return pd.DataFrame()

    @staticmethod
    def _process_results_sheet_for_table(df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        keys = [('Average', '_avg'), ('StandardDeviation', '_dvt')]
        original_columns = [col for col in df_copy.columns if col not in ['File', 'Description', 'Sample ID', 'Points']]

        for key, suffix in keys:
            for col in original_columns:
                new_col_name = col + suffix
                if new_col_name not in df_copy.columns:
                    df_copy[new_col_name] = np.nan

            key_rows = df_copy[df_copy['Description'].str.contains(key, na=False)]
            for index, row in key_rows.iterrows():
                target_index = index - 1 if key == 'Average' else index - 2
                if target_index < 0:
                    continue
                for col in original_columns:
                    new_col_name = col + suffix
                    df_copy.at[target_index, new_col_name] = row[col]
            df_copy = df_copy.drop(key_rows.index)
        return df_copy

    def _merge_data(self, results_sheet: pd.DataFrame, parameters_sheet: pd.DataFrame) -> Optional[pd.DataFrame]:
        results_sheet = results_sheet.reset_index(drop=True)
        parameters_sheet = parameters_sheet.reset_index(drop=True)
        if len(results_sheet) == len(parameters_sheet):
            description_matches = results_sheet['Description'] == parameters_sheet['Description']
            matching_results = results_sheet[description_matches]
            matching_parameters = parameters_sheet[description_matches]
            combined_sheet = pd.concat([matching_results, matching_parameters], axis=1)
            combined_sheet = combined_sheet.loc[:, ~combined_sheet.columns.duplicated()]
            time_col = combined_sheet.pop('Time')
            combined_sheet.insert(0, 'Time', time_col)
            combined_sheet['Time'] = pd.to_datetime(combined_sheet['Time'])
            combined_sheet['Time'] = combined_sheet['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            combined_sheet["Time"] = pd.to_datetime(combined_sheet["Time"])
            combined_sheet["Time"] = combined_sheet["Time"].dt.tz_localize(local_tz, ambiguous='NaT')
            return combined_sheet
        else:
            self.logger.error("The DataFrames have different lengths and cannot be concatenated directly.")
            return None

    #new xy reader
    def _get_measurement_xy_data_as_lists(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a DataFrame with one row per measurement in combined_df,
        columns:
          ['measurement_time',
           'transient_x','transient_y',
           'drift_x','drift_y',
           'calculated_x','calculated_y',
           'residual_x','residual_y']
        where each _x/_y is a list of floats (or None for NaNs).
        """
        # mapping of mode → (sheet name, x‐col, y‐col)
        sheets = {
            'calculated': ('T-f(Tau)', 't_f_tau', 'temperature'),
            'transient':  ('T-t',       'time_temperature_increase', 'temperature_increase'),
            'residual':   ('Diff',      'sqrt_time',                 'diff_temperature'),
            'drift':      ('T(drift)',  'time_drift',                'temperature_drift'),
        }

        # 1) read each sheet once, drop entirely empty columns
        raw = {}
        for mode, (sheet, xcol, ycol) in sheets.items():
            df = pd.read_excel(self.file_path, sheet_name=sheet, header=3)
            df = df.dropna(axis=1, how='all')
            raw[mode] = df

        # 2) build one record per row of combined_df
        times = combined_df['Time'].tolist()
        rows = []
        for j, meas_time in enumerate(times):
            rec = {'time': meas_time}
            for mode, df_mode in raw.items():
                # X columns start at index 1, then alternate: 1,2 → run 0; 3,4 → run 1; etc.
                idx_x = 1 + 2*j
                idx_y = 2 + 2*j
                if idx_x < df_mode.shape[1] and idx_y < df_mode.shape[1]:
                    col_x = df_mode.columns[idx_x]
                    col_y = df_mode.columns[idx_y]
                    xs = df_mode[col_x].astype(float).tolist()
                    ys = df_mode[col_y].astype(float).tolist()
                    # replace NaN with None
                    xs = [None if math.isnan(v) else v for v in xs]
                    ys = [None if math.isnan(v) else v for v in ys]
                else:
                    xs, ys = [], []
                rec[f'{mode}_x'] = xs
                rec[f'{mode}_y'] = ys
            rows.append(rec)

        df = pd.DataFrame(rows)
        return df

    def _write_to_database(self, insert_query: str, values: list, table_name: str = "") -> bool:
        with DatabaseConnection(**self.db_conn_params) as db_conn:
            self.logger.info("Starting writing thermal conductivity data to %s database...", table_name)
            try:
                db_conn.cursor.executemany(insert_query, values)
                db_conn.cursor.connection.commit()
                self.logger.info("Thermal conductivity data inserted from file: %s", self.file_path)
                return False
            except IntegrityError as e:
                self.logger.error("IntegrityError while inserting data: %s. Rolling back and retrying.", e)
                db_conn.cursor.connection.rollback()
                return True
            except Exception as e:
                self.logger.error("Error inserting thermal conductivity data: %s", e)
                db_conn.cursor.connection.rollback()
                return True

    def _delete_data_from_table(self, data_to_delete: pd.DataFrame) -> None:
        with DatabaseConnection(**self.db_conn_params) as db_conn:
            self.logger.info("Starting deletion of data from %s database...", self.etc_table.table_name)
            data_identifiers = [(time_val,) for time_val in data_to_delete['Time']]
            try:
                delete_query = f"DELETE FROM {self.etc_table.table_name} WHERE \"Time\" = %s"
                delete_query_xy = f"DELETE FROM {self.xy_table.table_name} WHERE \"time\" = %s"
                db_conn.cursor.executemany(delete_query, data_identifiers)
                db_conn.cursor.executemany(delete_query_xy, data_identifiers)
                db_conn.cursor.connection.commit()
                self.logger.info("Data deleted successfully")
            except Exception as e:
                self.logger.error("Error occurred while deleting data: %s", e)
                db_conn.cursor.connection.rollback()

    #new duplicate remover
    @staticmethod
    def _delete_duplicates(df, xy_df):
        deduped_df = df.drop_duplicates(subset='Time', keep='last').reset_index(drop=True)
        deduped_xy_df = xy_df.drop_duplicates(subset='time', keep='last').reset_index(drop=True)
        return deduped_df, deduped_xy_df

    def _find_corresponding_t_p(self, df_etc: pd.DataFrame) -> pd.DataFrame:
        t_p_table = TableConfig().TPDataTable
        etc_table = TableConfig().ETCDataTable
        start_time = min(df_etc[etc_table.get_clean("time")])
        end_time = max(df_etc[etc_table.get_clean("time")])
        time_range = (start_time, end_time)
        cols = (t_p_table.time,
                t_p_table.pressure,
                t_p_table.temperature_sample,
                t_p_table.cycle_number,
                t_p_table.cycle_number_flag,
                t_p_table.de_hyd_state)
        cols_etc = [etc_table.get_clean('time'),
                    t_p_table.pressure,
                    t_p_table.temperature_sample,
                    t_p_table.cycle_number,
                    t_p_table.cycle_number_flag,
                    t_p_table.de_hyd_state]
        db_retriever = DataRetriever(db_conn_params=self.db_conn_params)
        df_tp = db_retriever.fetch_data_by_time_no_limit(table=t_p_table, time_range=time_range, col_names=list(cols))
        if df_tp.empty:
            self.logger.info("No corresponding t_p_data found")
            return pd.DataFrame()
        df_etc = df_etc.sort_values(etc_table.get_clean('time'))
        df_tp = df_tp.rename(columns={t_p_table.time: etc_table.get_clean('time')})
        df_tp = df_tp.sort_values(etc_table.get_clean('time'))
        df_tp[etc_table.get_clean('time')] = df_tp[etc_table.get_clean('time')].dt.tz_convert(local_tz)
        df_tp_etc = pd.merge_asof(df_etc, df_tp, on=etc_table.get_clean('time'),
                                  direction='nearest',
                                  suffixes=('_etc', '_tp'))
        df_merged = df_tp_etc[cols_etc]
        return df_merged

    def execute(self) -> Optional[Tuple[datetime, datetime]]:
        table = TableConfig().ETCDataTable
        self._update_xlsx_file()
        df_etc = self._read_and_process_sheets()
        if df_etc is None or df_etc.empty:
            return None
        df_etc_xy = self._get_measurement_xy_data_as_lists(df_etc)
        df_etc[self.etc_table.sample_id] = self.meta_data.sample_id
        df_etc_xy[self.xy_table.sample_id] = self.meta_data.sample_id
        df_etc = df_etc.dropna(subset=[table.get_clean("time")])
        df_etc_xy = df_etc_xy.dropna(subset=[self.xy_table.time])
        df_etc, df_etc_xy = self._delete_duplicates(df=df_etc, xy_df=df_etc_xy)
        df_t_p = self._find_corresponding_t_p(df_etc) # df containing corresponding tp values
        if not df_etc.empty and not df_t_p.empty:
            df_etc = pd.merge(df_etc, df_t_p, on=table.get_clean('time'), how='inner')
        else:
            df_etc[self.etc_table.pressure] = None
            df_etc[self.etc_table.temperature_sample] = None
            df_etc[self.etc_table.cycle_number] = None
            df_etc[self.etc_table.cycle_number_flag] = None
            df_etc[self.etc_table.is_isotherm_flag] = False
            df_etc[self.etc_table.test_info] = None
        pd.set_option('future.no_silent_downcasting', True)
        df_etc.replace('(no corr.)', 0, inplace=True)

        #create insert query and prepare data for insert
        ETC_insert_query, ETC_values = TableConfig().writing_query_from_df(
            df=df_etc,
            map=self.etc_column_attribute_mapping,
            table_name=self.etc_table.table_name
        )

        xy_insert_query, xy_values = TableConfig().writing_query_from_df(
            df=df_etc_xy,
            map=None,
            table_name=TableConfig().ThermalConductivityXyDataTable.table_name
        )

        #start insertion
        error_checker = self._write_to_database(
            insert_query=ETC_insert_query,
            values=ETC_values,
            table_name=self.etc_table.table_name
        )
        if error_checker:
            self._delete_data_from_table(df_etc)
            self._write_to_database(
                insert_query=ETC_insert_query,
                values=ETC_values,
                table_name=self.etc_table.table_name
            )
            self._write_to_database(
                insert_query=xy_insert_query,
                values=xy_values,
                table_name=self.xy_table.table_name
            )
        else:
            self._write_to_database(
                insert_query=xy_insert_query,
                values=xy_values,
                table_name=self.xy_table.table_name
            )
        time_range = (min(df_etc[table.get_clean("time")]), max(df_etc[table.get_clean("time")]))
        return time_range

    def save_combined_data(self, combined_df: pd.DataFrame, output_file_path: str) -> None:
        try:
            cols = combined_df.columns.tolist()
            if 'Time' in cols:
                cols.insert(0, cols.pop(cols.index('Time')))
            combined_df = combined_df[cols]
            combined_df.to_csv(output_file_path, sep=';', index=False)
            self.logger.info("Data saved to %s", output_file_path)
        except Exception as e:
            self.logger.error("Error saving combined data: %s", e)



def _to_native(val):
    # unwrap numpy scalars
    try:
        # numpy scalar, or pandas NA-scalar, etc.
        return val.item()
    except AttributeError:
        pass
    # pandas Timestamp → datetime.datetime
    if hasattr(val, 'to_pydatetime'):
        return val.to_pydatetime()
    return val


def test_data_retriever() -> None:
    from src.infrastructure.core.config_reader import GetConfig

    data_retriever = DataRetriever(db_conn_params=GetConfig().db_conn_params)
    sample_id = 'WAE-WA-030'
    table_name = TableConfig().TPDataTable.table_name
    df = data_retriever.fetch_data_by_sample_id_2(
        column_names=None,
        table_name=table_name,
        sample_id=sample_id
    )
    print(df)


def test_excel_data_processor(etc_dir: str, sample_id: str) -> None:
    from src.infrastructure.core.config_reader import GetConfig
    config = GetConfig()
    etc_processor = ExcelDataProcessor(sample_id=sample_id, file_path=etc_dir, db_conn_params=config.db_conn_params)
    etc_processor.execute()


def process_ETC_file(args: Tuple[str, str, logging.getLogger]) -> None:
    file_path, sample_id, logger_inst, config = args
    try:
        logger_inst.info("Start writing %s to database", os.path.basename(file_path))
        etc_processor = ExcelDataProcessor(file_path=file_path, sample_id=sample_id, db_conn_params=config.db_conn_params)
        etc_processor.execute()
        logger_inst.info("%s written to database", os.path.basename(file_path))
    except Exception as e:
        logger_inst.error("Error processing %s: %s", os.path.basename(file_path), e)


def write_ETC_in_parallel(dir_etc_folder: str, sample_id: str, logger_inst, config) -> None:
    start_time = time.time()
    file_paths = [
        os.path.join(dir_etc_folder, filename)
        for filename in os.listdir(dir_etc_folder)
        if filename.endswith('.xlsx') and "$" not in filename
    ]
    args = [(file_path, sample_id, logger_inst, config) for file_path in file_paths]
    with Pool(processes=4) as pool:
        pool.map(process_ETC_file, args)
    logger_inst.info("Import took %.2f hours", (time.time() - start_time) / 3600)


def write_ETC_folder(dir_etc_folder: str, sample_id: str, logger_inst, config) -> None:
    start_time = time.time()
    file_paths = [
        os.path.join(dir_etc_folder, filename)
        for filename in os.listdir(dir_etc_folder)
        if filename.endswith('.xlsx') and "$" not in filename
    ]
    for file_path in file_paths:
        args = (file_path, sample_id, logger_inst, config)

        process_ETC_file(args)
    logger_inst.info("Import took %.2f hours", (time.time() - start_time) / 3600)


def main():
    sample_id = 'WAE-WA-040'
    dir_etc = r"C:\Daten\Kiki\WAE-WA-030-Mg2NiH4\WAE-WA-030-All\WAE-WA-030-045-400C-ParameterTest.xlsx"
    dir_etc = r"C:\Daten\Kiki\WAE-WA-040-MgFe5wt\WAE-WA-040-038-350-420C-Cyc\WAE-WA-040-038-02.xlsx"

    test_excel_data_processor(etc_dir=dir_etc, sample_id=sample_id)

if __name__ == "__main__":
    main()
