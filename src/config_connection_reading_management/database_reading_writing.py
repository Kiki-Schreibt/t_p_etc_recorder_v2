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
        etc_table.sample_id_small: 'sample_id',
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

