##database_reading_writing.py
from zoneinfo import ZoneInfo
from datetime import datetime
from typing import Optional, Tuple, Union, List

import pandas as pd

import recorder_app.infrastructure.core.global_vars
from recorder_app.infrastructure.connections.connections import DatabaseConnection
try:
    import recorder_app.infrastructure.core.logger as logging
except ImportError:
    import logging

from recorder_app.config_connection_reading_management.query_builder import QueryBuilder
from recorder_app.infrastructure.core.table_config import TableConfig
from recorder_app.infrastructure.core.global_vars import data_point_reading_limit
from recorder_app.infrastructure.core import global_vars

local_tz = ZoneInfo("Europe/Berlin")
LIMIT_DATA_POINTS = data_point_reading_limit


class DataRetriever:
    """
    Centralized data access layer for retrieving and preprocessing experimental
    and thermal conductivity-related datasets from a relational database.

    This class provides a high-level interface for executing SQL queries against
    multiple domain-specific tables (e.g., TP data, ETC data, XY data), while
    abstracting query construction, execution, and post-processing of results
    into pandas DataFrames.

    It supports flexible querying by:
    - sample identifiers
    - time ranges
    - cycle numbers
    - combined filtering with joins and constraints
    - streaming/continuous cursor-based retrieval

    Additionally, it performs standardized post-processing on retrieved data,
    including:
    - timezone normalization for datetime columns
    - boolean conversion for flag-like fields
    - flattening and cleaning of structured or array-like database outputs

    Attributes
    ----------
    xy_table : TableConfig.Table
        Configuration object for the thermal conductivity XY data table.

    etc_table : TableConfig.Table
        Configuration object for the ETC (Effective Thermal Conductivity) table.

    etc_column_attribute_mapping : dict
        Mapping between database column identifiers and human-readable attribute
        names used for downstream processing or UI representation.

    db_conn_params : dict
        Database connection parameters used to initialize connections.

    qb : QueryBuilder
        Internal query builder responsible for generating SQL statements.

    logger : logging.Logger
        Logger instance used for error reporting and debugging.

    limit_datapoints : int
        Maximum number of datapoints returned by bounded queries.

    Key Responsibilities
    ---------------------
    - Construction and execution of SQL queries via `QueryBuilder`
    - Retrieval of dataset slices based on:
        * sample_id
        * time_range
        * cycle_number
    - Joining and filtering across multiple tables
    - Standardization of returned data into pandas DataFrames
    - Post-processing of database output (types, timezones, flags)

    Notes
    -----
    - All database access is performed through `DatabaseConnection` or
      externally provided cursors for continuous fetching.
    - Time columns are automatically localized and converted to a configured
      local timezone.
    - Flag columns are heuristically interpreted based on naming conventions.
    - The class assumes a relational schema defined by `TableConfig`.
    - Query generation logic is delegated to `QueryBuilder`.

    Error Handling
    --------------
    Most methods catch database and execution errors internally, logging them
    and returning empty DataFrames or None where appropriate. Exceptions are
    generally not propagated.

    Thread Safety
    -------------
    Instances are not guaranteed to be thread-safe due to shared state such
    as connection parameters and logging configuration.

    Examples
    --------
    >>> retriever = DataRetriever(db_conn_params)
    >>> df = retriever.fetch_data_by_sample_id_2(
    ...     sample_id="S123",
    ...     table_name="t_p_data"
    ... )
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
                join_constraints=join_constraints,
                sample_id=sample_id
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
        """
        Retrieve records from a database table filtered by a given sample ID,
        with optional column selection, constraints, and join operations.

        This method constructs a SQL query via the internal query builder and
        executes it using the configured database connection. It supports
        filtering, limiting, and joining additional tables.

        Parameters
        ----------
        sample_id : str
            Identifier used to filter the primary dataset. Typically corresponds
            to a column such as `sample_id` in the target table.

        table_name : str
            Name of the primary database table to query.

        column_names : list of str or tuple of str, optional
            Specific columns to retrieve. If None, all columns are selected.

        constraints : dict, optional
            Additional filtering conditions applied to the primary table.
            Expected format depends on the query builder implementation
            (e.g., {"column": value} or more complex expressions).

        join_table : str, optional
            Name of an additional table to join with the primary table.

        join_on : list of tuple of (str, str), optional
            Join conditions between the primary and join table.
            Each tuple represents a pair of columns:
            (primary_table_column, join_table_column).

        join_constraints : dict, optional
            Additional filtering conditions applied to the joined table.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the query results. Column names correspond
            to the selected or inferred schema.

        Notes
        -----
        - The actual SQL query is generated by `self.qb.create_reading_query`.
        - Result size may be limited by `self.limit_datapoints`.
        - Behavior of constraints and joins depends on the query builder logic.

        Raises
        ------
        Exception
            Propagates any database or query execution errors raised by
            `execute_fetching`.
        """

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
        #print("by ID: "+query)
        #print(f"With Values: {values}")
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
        """
        Retrieve records from a database table within a specified time range,
        with optional filtering, column selection, and join operations.

        This method constructs a SQL query via the internal query builder using
        a time window constraint and executes it against the configured database.

        Parameters
        ----------
        time_range : tuple of datetime
            Start and end timestamps defining the time window (inclusive or exclusive
            depends on the query builder implementation).

        table_name : str
            Name of the primary database table to query.

        column_names : list of str or tuple of str, optional
            Specific columns to retrieve. If None, all columns are selected.

        constraints : dict, optional
            Additional filtering conditions applied to the primary table.
            Format depends on the query builder implementation.

        sample_id : str, optional
            Optional sample identifier used as an additional filter condition.

        join_table : str, optional
            Name of an additional table to join with the primary table.

        join_on : list of tuple of (str, str), optional
            Join conditions between the primary and join table.
            Each tuple represents a pair of columns:
            (primary_table_column, join_table_column).

        join_constraints : dict, optional
            Additional filtering conditions applied to the joined table.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the query results.

        Notes
        -----
        - The SQL query is generated by `self.qb.create_reading_query`.
        - The number of returned rows may be limited by `self.limit_datapoints`.
        - Time filtering behavior depends on the query builder implementation.

        Raises
        ------
        Exception
            Propagates any database or query execution errors raised by
            `execute_fetching`.
        """

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
        Retrieve XY data for a specific timestamp from the thermal conductivity
        dataset, returning paired x–y values as a flattened DataFrame.

        The method queries the `ThermalConductivityXyDataTable` for a given
        timestamp and extracts one of several predefined data series
        ("row packages"), such as transient, drift, calculated, or residual data.
        Retrieved array-like columns are expanded into individual rows.

        Parameters
        ----------
        time_value : str or datetime.datetime
            Timestamp used to filter the dataset. If a string is provided, it must
            be in ISO format and will be converted to a `datetime` object.
            Naive datetime objects are automatically assigned the local timezone.

        row_package_name : str, optional
            Specifies which XY data series to retrieve. Supported values are:
            - "transient"
            - "drift"
            - "calculated"
            - "residual"

            Case-insensitive. Defaults to "Transient".

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing two columns (x and y values) and the associated
            timestamp. Array-like database fields are expanded such that each row
            represents a single (x, y) data point. The DataFrame is sorted by the
            x-values.

            Returns an empty DataFrame if:
            - no matching data is found,
            - an invalid `row_package_name` is provided,
            - or a database/query error occurs.

        Notes
        -----
        - Column mappings are defined via `TableConfig().ThermalConductivityXyDataTable`.
        - The query filters by exact timestamp equality (`WHERE time_column = %s`).
        - Retrieved array-like columns are expanded using `pandas.DataFrame.explode`.
        - Missing values are removed after expansion.
        - Sorting is applied to ensure monotonic x-axis ordering.

        Raises
        ------
        None
            All exceptions are caught internally, logged, and result in an empty
            DataFrame being returned.
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
        """
        Retrieve the last hydrogenation/dehydrogenation state and total cycle count
        from metadata, with fallback defaults if values are missing.

        Parameters
        ----------
        meta_data : object
            Metadata object expected to provide the attributes:
            - `last_de_hyd_state` : str or None
            - `total_number_cycles` : int or None

        Returns
        -------
        tuple
            A tuple of the form (state, cycle_count), where:
            - state : str
                The last known hydrogenation/dehydrogenation state.
            - cycle_count : float
                The total number of completed cycles.

            If either attribute is missing or evaluates to False, defaults are returned:
            ("Dehydrogenated", 0).

        Notes
        -----
        - The method relies on truthiness checks (`if meta_data.last_de_hyd_state and ...`),
          meaning values like `0` or empty strings will trigger the fallback even if
          they may be semantically valid.
        """

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
        join_on: list[tuple[str, str]] = None,
        join_constraints: dict = None,
        asc_desc: bool = False,
        avg_cycle_dur: int = None
    ) -> pd.DataFrame:
        """
        Retrieve data for one or multiple cycle numbers associated with a given sample,
        with optional support for partial cycle extraction (ascending/descending windows).

        This method builds and executes SQL queries to fetch data filtered by cycle number(s).
        It supports querying a single cycle, multiple cycles, or extracting boundary segments
        (start and end) of two cycles for comparative analysis.

        Parameters
        ----------
        cycle_numbers : int, float, or list of (int or float)
            Cycle identifier(s) to query. Can be:
            - a single cycle number
            - a list/tuple of cycle numbers (used in an SQL IN clause)
            - a list of exactly two cycle numbers when `asc_desc=True`, interpreted as
              [previous_cycle, current_cycle]

        sample_id : str
            Identifier used to filter the dataset.

        column_names : list of str or tuple of str, optional
            Columns to retrieve. If None, all columns for the given table are used.

        constraints : dict, optional
            Additional filtering constraints (currently not applied in this method,
            but kept for interface consistency).

        table : TableConfig, optional
            Table configuration object defining table and column names.
            Defaults to `TableConfig().TPDataTable`.

        join_table : str, optional
            Name of an additional table to join (currently unused in this method).

        join_on : list of tuple of (str, str), optional
            Join conditions (currently unused).

        join_constraints : dict, optional
            Additional constraints for joined tables (currently unused).

        asc_desc : bool, optional
            If False (default):
                Standard query mode retrieving full cycle data.

            If True:
                Special mode that extracts limited ascending (start) and descending (end)
                segments from two cycles (requires exactly two cycle numbers).

        avg_cycle_dur : int, optional
            Average cycle duration used to estimate the number of rows to fetch
            in `asc_desc=True` mode. Required when `asc_desc=True`.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the requested data.

            - In standard mode:
                Full data for the specified cycle(s), ordered by time.
            - In `asc_desc=True` mode:
                Concatenated subset of start and end segments from two cycles.

            Returns an empty DataFrame if column resolution fails.

        Notes
        -----
        - Cycle numbers are cast to float before querying.
        - For multiple cycles, an SQL IN clause is used.
        - In `asc_desc=True` mode:
            - Data is fetched separately for ascending and descending order.
            - Results are concatenated and duplicates (based on time) are dropped.
            - The number of rows fetched is estimated via:
              `avg_cycle_dur * 3 / global_vars.sleep_interval`.
        - The method relies on `self.execute_fetching` for query execution.

        Raises
        ------
        Exception
            Propagates database/query execution errors raised by `execute_fetching`.
        """

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

        if not asc_desc:
            if isinstance(cycle_numbers, (list, tuple)):
                # Convert possible numpy types to native float
                cycle_numbers = [float(cn) for cn in cycle_numbers]
                placeholders = ', '.join(['%s'] * len(cycle_numbers))
                query = (f"SELECT {column_name_str} FROM {table_name} WHERE {sample_id_col} = %s AND "
                         f"{table.cycle_number} IN ({placeholders}) ORDER BY {table.time}")
                values = [sample_id] + cycle_numbers
            else:
                if table_name == TableConfig().CycleDataTable.table_name:
                    time_col = TableConfig().CycleDataTable.time_start
                else:
                    time_col = TableConfig().TPDataTable.time
                query = (f"SELECT {column_name_str} FROM {table_name} WHERE {sample_id_col} = %s AND "
                         f"{table.cycle_number} = %s ORDER BY {time_col}")
                values = [sample_id, float(cycle_numbers)]
            df = self.execute_fetching(query=query, column_names=column_names, table_name=table_name, values=values)

        elif len(cycle_numbers) == 2:


            asc_desc_limit = avg_cycle_dur * 3 / global_vars.sleep_interval

            query = (f"SELECT {column_name_str} FROM {table_name} WHERE {sample_id_col} = %s AND "
                         f"{table.cycle_number} = %s ORDER BY {table.time} ASC LIMIT {asc_desc_limit} ")
            values = [sample_id, float(min(cycle_numbers))]
            df_prev_start = self.execute_fetching(query=query, column_names=column_names, table_name=table_name, values=values)
            query = (f"SELECT {column_name_str} FROM {table_name} WHERE {sample_id_col} = %s AND "
                         f"{table.cycle_number} = %s ORDER BY {table.time} DESC LIMIT {asc_desc_limit} ")
            values = [sample_id, float(min(cycle_numbers))]
            df_prev_end = self.execute_fetching(query=query, column_names=column_names, table_name=table_name, values=values)
            df_prev = pd.concat([df_prev_start, df_prev_end], axis=0, ignore_index=True)
            df_prev.drop_duplicates(subset=table.time)

            query = (f"SELECT {column_name_str} FROM {table_name} WHERE {sample_id_col} = %s AND "
                         f"{table.cycle_number} = %s ORDER BY {table.time} ASC LIMIT {asc_desc_limit} ")
            values = [sample_id, float(max(cycle_numbers))]
            df_curr_start = self.execute_fetching(query=query, column_names=column_names, table_name=table_name, values=values)
            query = (f"SELECT {column_name_str} FROM {table_name} WHERE {sample_id_col} = %s AND "
                         f"{table.cycle_number} = %s ORDER BY {table.time} DESC LIMIT {asc_desc_limit} ")
            values = [sample_id, float(max(cycle_numbers))]
            df_curr_end = self.execute_fetching(query=query, column_names=column_names, table_name=table_name, values=values)
            df_curr = pd.concat([df_curr_start, df_curr_end], axis=0, ignore_index=True)
            df_curr.drop_duplicates(subset=table.time)

            df = pd.concat([df_prev, df_curr], axis=0, ignore_index=True)


        return df

    def fetch_data_by_time_no_limit(self, table: TableConfig,
                                    time_range: Tuple[datetime, datetime],
                                    col_names: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Retrieve all records from a given table within a specified time range
        without applying any row limit.

        This method constructs and executes a SQL query filtering data by a
        time interval using a BETWEEN clause and returns the full result set.

        Parameters
        ----------
        table : TableConfig
            Table configuration object containing the table name and column
            definitions (including the time column).

        time_range : tuple of datetime
            Start and end timestamps defining the time window for the query.
            The interval is inclusive, as defined by the SQL BETWEEN operator.

        col_names : list of str, optional
            List of column names to retrieve. If None, all columns defined
            in the table configuration are selected.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing all rows within the specified time range,
            ordered by the table's time column in ascending order.

        Notes
        -----
        - No row limiting is applied, in contrast to other query methods in
          this class that may use `self.limit_datapoints`.
        - Column names are resolved via `TableConfig.get_table_column_names`
          if not explicitly provided.
        - The query relies on the `time` attribute of the provided table
          configuration.

        Raises
        ------
        Exception
            Propagates any database or query execution errors raised by
            `execute_fetching`.
        """

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
        """
        Execute a SQL SELECT query and return the results as a pandas DataFrame.

        This method handles query execution, optional column name resolution,
        result transformation, and basic error handling. If column names are not
        provided, they are inferred from the table configuration and injected
        into the query when possible.

        Parameters
        ----------
        query : str
            SQL query string to execute. May contain parameter placeholders
            (e.g., `%s`) for safe substitution via `values`.

        column_names : list of str or tuple of str, optional
            Column names corresponding to the expected query result.
            If None, column names are resolved using `TableConfig` based on
            `table_name`, and any occurrence of `SELECT *` in the query is
            replaced with explicit column names.

        table_name : str, optional
            Name of the database table. Required if `column_names` is None,
            as it is used to resolve column names via `TableConfig`.

        values : tuple or list, optional
            Parameters to bind to the SQL query placeholders.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the query results.

            - If records are returned:
                Data is converted into a DataFrame and post-processed via
                `_adjust_df_types_and_times`.
            - If no records are found or an error occurs:
                An empty DataFrame is returned.

        Notes
        -----
        - If `column_names` is None, the method attempts to replace `SELECT *`
          in the query with explicit column names derived from `TableConfig`.
        - Column names are cleaned by removing double quotes (`"`).
        - Database interaction is handled via `DatabaseConnection` context manager.
        - All exceptions are caught, logged, and result in an empty DataFrame.

        Raises
        ------
        None
            Exceptions are handled internally; errors are logged and an empty
            DataFrame is returned.
        """

        if column_names is None:
            column_names = TableConfig().get_table_column_names(table_name=table_name)
            column_names_str = ', '.join(column_names)
            query = query.replace("SELECT *", f"SELECT {column_names_str}")
            #print(query)
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
        """
        Execute a SQL query using an existing database cursor and return the
        results as a pandas DataFrame.

        This method is intended for continuous or streaming-like data access
        scenarios where a persistent cursor is reused across multiple queries,
        avoiding repeated connection setup overhead.

        Parameters
        ----------
        query : str
            SQL query string to execute. May contain parameter placeholders
            (e.g., `%s`) for safe substitution via `values`.

        cursor : database cursor
            An active database cursor object used to execute the query.
            The cursor must already be associated with an open connection.

        column_names : list of str or tuple of str, optional
            Column names corresponding to the expected query result.
            If None, column names are resolved using `TableConfig` based on
            `table_name`.

        table_name : str, optional
            Name of the database table. Required if `column_names` is None,
            as it is used to resolve column names via `TableConfig`.

        values : tuple or list, optional
            Parameters to bind to the SQL query placeholders.

        Returns
        -------
        pandas.DataFrame or None
            - pandas.DataFrame:
                Returned if the query executes successfully. May be empty if
                no records are found.
            - None:
                Returned if an exception occurs during query execution.

        Notes
        -----
        - Unlike `execute_fetching`, this method does not manage database
          connections and assumes the caller controls the cursor lifecycle.
        - Column names are cleaned by removing double quotes (`"`).
        - Retrieved data is post-processed using `_adjust_df_types_and_times`.
        - Errors are logged and suppressed; no exception is raised.

        Raises
        ------
        None
            Exceptions are handled internally; errors are logged and result
            in `None` being returned.
        """

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
        Normalize DataFrame column types by converting flag-like values to booleans
        and ensuring consistent timezone-aware datetime columns.

        This method inspects column names heuristically to identify:
        - flag columns (containing "_flag")
        - time-related columns (containing "time")

        It then applies appropriate type conversions in-place.

        Parameters
        ----------
        df : pandas.DataFrame
            Input DataFrame whose columns will be inspected and transformed.

        Returns
        -------
        pandas.DataFrame
            The transformed DataFrame with:
            - flag columns converted to boolean dtype
            - time columns converted to timezone-aware datetime (localized to UTC
              and converted to the configured local timezone)

        Notes
        -----
        - Flag detection is based on column names containing "_flag" (case-insensitive).
          Values are interpreted as True if they match one of:
          ('t', '1', 'true') after string normalization.

        - Time detection is based on column names containing "time" (case-insensitive).
          Conversion steps:
            1. String/object columns are parsed via `pandas.to_datetime` with
               `utc=True` and `errors='coerce'`.
            2. Naive datetime columns are localized to UTC.
            3. All datetime columns are converted to `local_tz`.

        - Invalid or unparsable datetime values are converted to `NaT`.

        - Transformations are applied in-place on the provided DataFrame.

        Raises
        ------
        None
            This method does not raise exceptions; conversion errors result in
            `NaT` (for datetime) or False (for flags).
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
        """removes the time zone info from a timezone aware pd.series"""
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)
        return df

    def close_connection(self) -> None:
        pass


class DataBaseManipulator:
    """
    Database write-layer utility for executing parameterized updates and batch
    modifications on relational tables.

    This class provides a structured interface for performing controlled data
    mutations (UPDATE operations) on experiment-related database tables using
    safe SQL execution patterns and transaction management.

    It supports both single-row updates and large-scale batch updates over
    time ranges or grouped conditions, typically used for:
    - assigning computed values (e.g., cycle numbers, derived metrics)
    - correcting or enriching experimental metadata
    - updating time-window-based segments of data
    - applying post-processing results back into persistent storage

    Core Responsibilities
    ---------------------
    - Execution of parameterized UPDATE statements
    - Batch updates across multiple time windows or conditions
    - Single-record updates using flexible filtering logic
    - Transaction management (commit/rollback safety)
    - Logging of update operations and error conditions

    Key Features
    ------------
    - Supports executemany-based batch updates for efficiency
    - Supports conditional updates using:
        * sample_id filtering
        * equality or BETWEEN range filters
        * optional secondary match constraints
    - Handles both pandas DataFrame and Series inputs for update payloads
    - Automatic type-safe parameter binding to prevent SQL injection
    - Integrated logging of executed modifications for traceability

    Attributes
    ----------
    tp_table : TableConfig.Table
        Default TP (transient/processing) table configuration reference.

    db_conn_params : dict
        Database connection parameters used to initialize connections.

    logger : logging.Logger
        Logger instance used for operational and error logging.

    Notes
    -----
    - All SQL statements are parameterized; values are never interpolated
      directly into queries.
    - Column and table identifiers are assumed to originate from trusted
      `TableConfig` metadata.
    - Transactions are explicitly committed on success and rolled back on failure.
    - Batch operations assume aligned input lengths across all vectorized arguments.

    Error Handling
    --------------
    - Errors during update execution are caught internally.
    - Failed operations are rolled back automatically.
    - Errors are logged but not propagated to the caller.

    Thread Safety
    -------------
    Instances are not guaranteed to be thread-safe due to shared connection
    parameters and logger usage.

    Example Use Cases
    -----------------
    - Update cycle numbers over time windows
    - Apply computed thermal properties back into TP/ETC tables
    - Bulk correction of metadata fields
    - Post-processing enrichment of experimental datasets
    """

    tp_table = TableConfig().TPDataTable

    def __init__(self, db_conn_params=None):
        self.running = False
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params or {}

    def execute_updating(self, query: str, values: list, many_bool: bool = True) -> None:
        """
        Execute a SQL data-modification query (INSERT, UPDATE, DELETE) with optional
        support for bulk operations.

        This method manages database transaction control (commit/rollback) and ensures
        safe execution of parameterized SQL statements using a managed database connection.

        Parameters
        ----------
        query : str
            SQL query string to execute. Must be compatible with the underlying database
            driver (e.g., placeholders such as %s for parameter substitution).

        values : list
            Parameter values to bind to the SQL query.
            - If `many_bool` is False: a single sequence of parameters is expected.
            - If `many_bool` is True: a list of parameter sequences is expected for bulk
              execution via `executemany`.

        many_bool : bool, optional
            Determines whether to execute the query in bulk mode.
            - True (default): uses `cursor.executemany()` for batch execution.
            - False: uses `cursor.execute()` for a single statement.

        Returns
        -------
        None

        Notes
        -----
        - A database transaction is explicitly committed on success.
        - If an exception occurs, the transaction is rolled back to preserve consistency.
        - Errors are logged but not propagated to the caller.

        Error Handling
        --------------
        Any exception raised during query execution results in:
        - rollback of the current transaction
        - logging of the error via `self.logger`
        - silent failure (no exception is re-raised)

        Raises
        ------
        None
            All exceptions are handled internally.
        """
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
        """
        Batch-update rows in a partitioned (or plain) table over multiple time ranges.

        Builds a single parameterized `UPDATE` statement of the form:

            UPDATE <table>
            SET col1 = %s, col2 = %s, ...
            WHERE <table>.sample_id = %s
              AND <col_to_match> BETWEEN %s AND %s
              [AND <other_col_to_match> = %s]

        and executes it repeatedly with one parameter tuple per range. This is useful
        when you need to set the same set of columns for many non-overlapping time
        windows (e.g., assign cycle numbers or flags between `time_start` and
        `time_end` for a given `sample_id`, optionally conditioned on an additional
        column like `de_hyd_state`).

        Args:
            sample_id: Value for the table's `sample_id` column used in the WHERE
                clause. Must not be `None`.
            table: A `TableConfig` object for the target table. Must provide
                `.table_name` and `.sample_id`.
            df_vals_to_update: DataFrame whose columns are the target columns to
                update and whose rows hold the new values. Each row corresponds
                1:1 to the time range at the same position in
                `update_between_min_list` / `update_between_max_list`, and (if
                provided) to `other_col_to_match_values`.
            col_to_match: Column name used for the BETWEEN filter (typically a
                timestamp column such as `time`). The BETWEEN is **inclusive** on
                both ends.
            other_col_to_match: Optional additional column to include as an equality
                filter in the WHERE clause (e.g., `de_hyd_state`). Only added if
                `other_col_to_match_values` is provided and non-empty.
            other_col_to_match_values: Optional Series of values for
                `other_col_to_match`. Must align by position with the rows in
                `df_vals_to_update` and the time ranges.
            update_between_min_list: Series of lower bounds for the BETWEEN filter
                (one per row). Values should be of the appropriate dtype for
                `col_to_match` (e.g., timezone-aware timestamps if the column is
                `timestamptz`).
            update_between_max_list: Series of upper bounds for the BETWEEN filter
                (one per row).

        Behavior:
            - Validates minimal inputs; on missing essentials it logs an error and
              returns without performing updates.
            - Constructs a parameterized UPDATE with placeholders for values; table
              and column identifiers come from `TableConfig`/arguments and are
              interpolated into the SQL string (assumed trusted).
            - Converts the min/max Series to Python lists and zips them together
              with each row of `df_vals_to_update` to build the parameter tuples.
            - If `other_col_to_match` and a non-empty `other_col_to_match_values`
              are provided, appends an equality predicate and the corresponding
              value to each parameter tuple.
            - Executes the updates via `self.execute_updating(query, values)`, where
              `values` is a list of tuples (i.e., an executemany-style batch).
            - Logs the number of rows/ranges attempted.

        Returns:
            None. Side effect is updating rows in the database.

        Requirements & Notes:
            - The lengths of `df_vals_to_update`, `update_between_min_list`,
              `update_between_max_list`, and (if provided) `other_col_to_match_values`
              must match; otherwise indexing/zipping will misalign or raise.
            - The `BETWEEN` predicate is inclusive (`min <= col_to_match <= max`).
            - For performance, ensure indexes exist on `(sample_id, col_to_match)`
              and, if used, on `(sample_id, other_col_to_match, col_to_match)`.
            - Column and table names are not quoted here; they should be safe,
              canonical identifiers from `TableConfig`.

        Example:
            >>> t = TableConfig().TPDataTable
            >>> updates = pd.DataFrame({"cycle_number": [0.5, 1.0]})
            >>> mins = pd.Series([ts1, ts2])   # e.g., pandas Timestamps
            >>> maxs = pd.Series([te1, te2])
            >>> states = pd.Series(["Dehydrogenated", "Hydrogenated"])
            >>> dbm.batch_update_data(
            ...     sample_id="WAE-WA-030",
            ...     table=t,
            ...     df_vals_to_update=updates,
            ...     col_to_match=t.time,
            ...     other_col_to_match=t.de_hyd_state,
            ...     other_col_to_match_values=states,
            ...     update_between_min_list=mins,
            ...     update_between_max_list=maxs,
            ... )
        """

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
        Update one or more columns for rows matching a sample and a value/range filter.

        Constructs and executes a parameterized UPDATE on ``table.table_name`` that
        targets rows with a given ``sample_id`` and a constraint on ``col_to_match``.
        The new values come from a **single-row** pandas DataFrame or a pandas Series.
        If an ETC table is passed, ``table.sample_id_small`` is used instead of
        ``table.sample_id``.

        The WHERE predicate is either:
          * equality: ``col_to_match = %s`` when ``update_between_vals`` has length 1, or
          * inclusive range: ``col_to_match BETWEEN %s AND %s`` when length is 2.

        Args:
            sample_id: Sample identifier compared against the table's sample-id column.
                Must not be ``None``.
            table: Table metadata object exposing ``table_name`` and the appropriate
                sample-id attribute (``sample_id`` or, for ETC tables, ``sample_id_small``).
                Example: ``TableConfig().TPDataTable``.
            update_df: A single-row ``pd.DataFrame`` **or** a ``pd.Series`` providing
                the columns to update and their new values.
                - DataFrame: column names are used; the **first row** (``.iloc[0]``) supplies values.
                - Series: index labels are used as column names; values come from the Series.
            col_to_match: Name of the additional column used in the WHERE clause (e.g. a
                timestamp column).
            update_between_vals: One value (equality) **or** two values (inclusive BETWEEN)
                to filter ``col_to_match``. A list is accepted and will be converted to a tuple.

        Returns:
            True if the UPDATE executed and committed; False if validation failed or an
            exception occurred (the transaction is rolled back and an error is logged).

        Notes:
            - The ``BETWEEN`` predicate is **inclusive** on both bounds.
            - Table/column identifiers are interpolated from trusted metadata; values
              are passed as bind parameters (``%s``).
            - Ensure suitable indexes (e.g. on ``(sample_id, col_to_match)``) for performance.
            - If ``update_df`` is a DataFrame with more than one row, only the **first**
              row is used.

        Examples:
            Update by equality:
                >>> s = pd.Series({'pressure': 1.23, 'temperature': 300.0})
                >>> ok = updater.update_data(
                ...     sample_id="WAE-WA-028",
                ...     table=TableConfig().TPDataTable,
                ...     update_df=s,
                ...     col_to_match=TableConfig().TPDataTable.time,
                ...     update_between_vals=[pd.Timestamp('2023-11-01T12:00:00Z')]
                ... )

            Update by inclusive range:
                >>> df = pd.DataFrame([{'cycle_number': 0.5}])
                >>> ok = updater.update_data(
                ...     sample_id="WAE-WA-030",
                ...     table=TableConfig().TPDataTable,
                ...     update_df=df,
                ...     col_to_match=TableConfig().TPDataTable.time,
                ...     update_between_vals=(
                ...         pd.Timestamp('2023-11-01T10:00:00Z'),
                ...         pd.Timestamp('2023-11-01T11:00:00Z'),
                ...     ),
                ... )
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
    from recorder_app.infrastructure.core.config_reader import config

    data_retriever = DataRetriever(db_conn_params=config.db_conn_params)
    sample_id = 'WAE-WA-030'
    table_name = TableConfig().TPDataTable.table_name
    df = data_retriever.fetch_data_by_sample_id_2(
        column_names=None,
        table_name=table_name,
        sample_id=sample_id
    )
    print(df)


if __name__ == '__main__':
    from recorder_app.infrastructure.core.config_reader import config
    from recorder_app.infrastructure.handler.metadata_handler import MetaData
    import time
    sample_id = 'WAE-WA-060'
    meta_data = MetaData(db_conn_params=config.db_conn_params, sample_id=sample_id)
    time_start = time.time()
    db_retriever = DataRetriever(db_conn_params=config.db_conn_params)

    df = db_retriever.fetch_data_by_cycle(cycle_numbers=[0, 0.5],
                                          sample_id=sample_id,
                                          asc_desc=True,
                                          avg_cycle_dur=meta_data.average_cycle_duration.total_seconds())

    print(df)
    #print(meta_data.average_cycle_duration)
    #print(meta_data.average_cycle_duration.total_seconds())
    #print(21600*4)
    print(f"passed time {int(time.time()-time_start)} s")
