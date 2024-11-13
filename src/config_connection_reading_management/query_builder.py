from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import time

from src.config_connection_reading_management.config_reader import GetConfig
from src.config_connection_reading_management.connections_and_logger import DatabaseConnection, AppLogger
from src.table_data import TableConfig
local_tz = ZoneInfo("Europe/Berlin")


class QueryBuilder:

    def __init__(self):
        self.config = GetConfig()
        self.db = DatabaseConnection()
        self.logger = AppLogger().get_logger(__name__)
        self.tp_table = TableConfig().TPDataTable
        self.etc_table = TableConfig().ETCDataTable
        self.etc_xy_table = TableConfig().ThermalConductivityXyDataTable
        self.cycle_data_table = TableConfig().CycleDataTable
        self.meta_data_table = TableConfig().MetaDataTable


    def create_reading_query(self, table_name, column_names=None, constraints=None,
                             time_window=None, sample_id=None, time_list=None,
                             limit_data_points=50000):
        values = tuple()
        query_part = ""
        base_query = self._build_base_query_reading(table_name=table_name, column_names=column_names)
        constraints_query, constraints_values = self._measurement_constraints_for_query(constraints=constraints, base_query=base_query)
        base_query += constraints_query
        if not constraints_values == ():
            values += constraints_values

        if table_name == self.tp_table.table_name:
            query_part, values_part = self._create_tp_reading_query(time_window=time_window,
                                                               time_list=time_list,
                                                               sample_id=sample_id,
                                                               limit_data_points=limit_data_points,
                                                               base_query=base_query)
            query_part += f" ORDER BY {self.tp_table.time}"
            values += values_part

        if table_name == self.etc_table.table_name:
            query_part, values_part = self._create_etc_reading_query(time_window=time_window,
                                                                time_list=time_list,
                                                                sample_id=sample_id,
                                                                limit_data_points=limit_data_points,
                                                                base_query=base_query)
            query_part += f" ORDER BY {self.etc_table.time}"
            values += values_part

        if table_name == self.etc_xy_table.table_name:
            pass
            #query_part += f"ORDER BY {self.etc}"
        if table_name == self.cycle_data_table.table_name:
            query_part, values_part = self._create_cycle_data_reading_query(time_window=time_window,
                                                                       time_list=time_list,
                                                                       sample_id=sample_id,
                                                                       base_query=base_query)
            query_part += f" ORDER BY {self.cycle_data_table.time_start}"
            values += values_part

        if table_name == self.meta_data_table.table_name:
            query_part, values_part = self._create_meta_data_reading_query(sample_id=sample_id)
            values += values_part

        query = base_query + " " + query_part
        return query, values

    def _create_tp_reading_query(self,
                                 base_query=None,
                                 time_window=None,
                                 sample_id=None,
                                 time_list=None,
                                 limit_data_points=50000):

        values = ()
        query_part = ""
        if time_list:
            time_list_query, time_list_vals = self._build_query_part_time_list(time_list=time_list,
                                                                               base_query=base_query)
            query_part = time_list_query
            values += time_list_vals
            if sample_id:
                query_part += f" AND {self.tp_table.sample_id} = %s"
                values += (sample_id,)

        elif time_window:
            time_constraints_query, time_vals = self._build_query_part_time_constraints(time_range=time_window,
                                                                                            base_query=base_query,
                                                                                            limit_amount=limit_data_points)
            query_part = time_constraints_query
            values += time_vals
            if sample_id:
                query_part += f" AND {self.tp_table.sample_id} = %s"
                values += (sample_id,)

        elif sample_id:
            (min_time, max_time) = self._get_times_by_meta_data(sample_id=sample_id)
            time_window = (min_time, max_time)
            query_part, time_vals = self._build_query_part_time_constraints(time_range=time_window, base_query=base_query, limit_amount=limit_data_points)
            values += time_vals


        return query_part, values

    def _create_etc_reading_query(self,
                                  base_query=None,
                                  time_window=None,
                                  sample_id=None,
                                  time_list=None,
                                  limit_data_points=50000
                                  ):
        query_part = ""
        values = ()

        time_constraints_query, time_vals = self._build_query_part_etc(time_range=time_window,
                                                                       time_list=time_list,
                                                                       base_query=base_query)
        query_part += time_constraints_query
        values += time_vals
        if sample_id:
            query_part += f" AND {self.etc_table.sample_id_small} = %s"
            values += (sample_id,)

        return query_part, values

    def _create_etc_xy_reading_query(self,
                                     base_query=None,
                                     time_window=None,
                                     sample_id=None,
                                     time_list=None,
                                     limit_data_points=50000):
        pass

    def _create_cycle_data_reading_query(self,
                                     base_query=None,
                                     time_window=None,
                                     sample_id=None,
                                     time_list=None
                                     ):

        values = ()
        query_part = ""
        if time_window:
            time_constraints_query, time_vals = self._build_query_part_etc(time_range=time_window, base_query=base_query)
            query_part += time_constraints_query
            values += time_vals
            if sample_id:
                query_part += f" AND {self.tp_table.sample_id} = %s"
                values += (sample_id,)
        elif sample_id:
            query_part += f" WHERE {self.tp_table.sample_id} = %s"
            values += (sample_id,)

        return query_part, values

    def _create_meta_data_reading_query(self, sample_id=None):
        values = ()
        query_part = f" WHERE {self.meta_data_table.sample_id} = %s"
        values += (sample_id,)
        return query_part, values

    @staticmethod
    def create_extreme_value_query(table, target_column, min_value=None,
                               max_value=None, sample_id=None, time_range=None):
        """
        Constructs optimized queries to find the minimum and maximum values of a specified column,
        optionally filtered by sample_id, time range, and min/max values.

        Returns:
        - tuple: (query_min, query_max, values)
        """
        temperature_tolerance = 5
        where_clauses = []
        values = []

        sample_id_column = table.sample_id
        time_column = table.time_start if table.table_name == TableConfig().CycleDataTable else table.time

        # Build WHERE clauses
        if sample_id:
            where_clauses.append(f"{sample_id_column} = %s")
            values.append(sample_id)

        if time_range:
            min_time, max_time = min(time_range), max(time_range)
            where_clauses.append(f"{time_column} BETWEEN %s AND %s")
            values.extend([min_time, max_time])

        if min_value is not None:
            where_clauses.append(f"{target_column} >= %s")
            values.append(min_value)

        if max_value is not None:
            where_clauses.append(f"{target_column} <= %s")
            values.append(max_value)

        if table.table_name == TableConfig().TPDataTable:
            where_clauses.append(f"ABS({table.temperature_sample} - {table.setpoint_sample}) <= %s")
            values.append(temperature_tolerance)

        where_clauses.append("h2_uptake_flag = 'True'")

        where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Simplified query without CTE
        base_query = f"""
            SELECT {time_column}, {target_column}, {table.temperature_sample}, {table.reservoir_volume}
            FROM {table.table_name}
            {where_clause}
            ORDER BY {target_column} {{order}}
            LIMIT 1
        """

        query_min = base_query.format(order='ASC')
        query_max = base_query.format(order='DESC')
        return query_min, query_max, tuple(values)

    def create_continuous_reading_query(self, table_name, column_names=None, constraints=None, sample_id=None,desc_limit=1):

        query_part = self._build_base_query_reading(table_name=table_name, column_names=column_names)
        time_column = self._get_time_column(base_query=query_part)
        constraints_query, values = self._measurement_constraints_for_query(constraints=constraints, base_query=query_part)
        query_part += constraints_query
        if sample_id:
            query_part += f" {self._is_constraint(base_query=query_part)} sample_id = %s"
            values += (sample_id,)
        return query_part + f" ORDER by {time_column} DESC Limit {desc_limit}", values

    def create_update_by_sample_id_query(self, table_name, column_to_update, value_to_update, sample_id):

        table_name, column_names_str = self._normalize_table_names(table_name=table_name, column_names="do_nothing")
        sample_id_column = next((meta_key for meta_key in self.config.META_DATA_COLUMN_NAMES if "sample_id" in meta_key), None)
        if isinstance(value_to_update, datetime) or isinstance(value_to_update, str):
            query = f"UPDATE {table_name} SET {column_to_update} = '{value_to_update}' WHERE {sample_id_column} = '{sample_id}'"
            return query
        else:
            query = f"UPDATE {table_name} SET {column_to_update} = '{value_to_update}' WHERE {sample_id_column} = '{sample_id}'"
            return query

    def create_writing_query(self, table_name=None, column_names=None):

        table_name, column_names_str = self._normalize_table_names(table_name=table_name, column_names=column_names)
        # Create the column part of the query

        if not column_names and "t_p" in table_name.lower():
            column_names = self.config.TP_DATA_COLUMN_NAMES
            column_names_str = ", ".join(column_names)
        # Create the placeholders for values
        amount_values_to_insert = ", ".join(["%s"] * len(column_names))

        query = f"INSERT INTO {table_name} ({column_names_str}) " \
                     f"VALUES ({amount_values_to_insert})"
        return query

    def _measurement_constraints_for_query(self, constraints=None, base_query=""):
        """
        creates a part of a reading query that can be added to the base query.

        Parameters:
        - constraints (dict): constraint name and value pair (min and max must be in name)
        Returns:
        - str: The SQL constraint part of the query
        - tuple: The values for the placeholders
        """
        if constraints is None:
            return "", ()
        if self.config.TP_DATA_TABLE_NAME in base_query:
            column_names = self.config.TP_DATA_COLUMN_NAMES
        elif self.config.THERMAL_CONDUCTIVITY_DATA_TABLE_NAME in base_query:
            column_names = self.config.THERMAL_CONDUCTIVITY_COLUMN_NAMES
        elif self.config.THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME in base_query:
            column_names = self.config.THERMAL_CONDUCTIVITY_XY_COLUMN_NAMES

        constraint_clauses = []
        values = []
        query_start = self._is_constraint(base_query)

        # Check if constraints are provided and not empty
        if constraints:
            # Iterate through constraints to build the WHERE clause
            for key, value in constraints.items():
                # Normalize key and try to find a matching column name
                is_min = 'min_' in key.lower()
                normalized_key = key.replace('min_', '').replace('max_', '').replace('Min_', '').replace('Max_', '').lower()

                # Attempt to match with available column names, considering casing differences
                matched_column = next((col for col in column_names if normalized_key in col.lower()), None)
                values.append(value)
                if matched_column:
                    operator = '>=' if is_min else '<='
                    constraint_clauses.append(f"{matched_column} {operator} %s")


            # If there are any constraints, append them to the query
            if constraint_clauses:
                constraints_query = query_start + " AND ".join(constraint_clauses)
                return constraints_query, tuple(values)
            else:
                return "", ()

    def _build_query_part_time_constraints(self, time_range=None, base_query="", limit_amount=None):
        """
        creates a part of a reading query that can be added to the base query.

        Parameters:
        - time_range: constraint name and value pair (min and max must be in name)
        Returns:
        - query_part: query part to add to base query
        - time_values (tuple): reading times
        """

        datetime_list = []

        for item in time_range:
            if item is None:
                return "", ()

            if isinstance(item, str):
                try:
                    # Parse the string to a datetime object
                    dt = datetime.strptime(item, '%Y-%m-%d %H:%M:%S')
                    datetime_list.append(dt)
                except ValueError:
                    # Handle the case where the string is not a valid date
                    print(f"Skipping invalid date string: {item}")
            else:
                datetime_list.append(item)

        time_range = datetime_list
        time_range = [time_val.astimezone(local_tz) for time_val in time_range]
        min_time = min(time_range)
        max_time = max(time_range)

        if min_time is None or max_time is None:
            return "", ()

        query_start = self._is_constraint(base_query)
        time_list_query, time_list_vals = self._calculate_times(first_match_time=min_time, last_match_time=max_time, limit_amount=limit_amount, base_query=base_query)

        query_part = query_start + f" ({time_list_query})"

        return query_part, time_list_vals

    def _build_query_part_time_list(self, time_list=None, base_query=""):
        """
        Creates a part of a reading query that can be added to the base query using placeholders.
        This method ensures datetime objects are handled correctly, and strings are converted to datetime.

        Parameters:
        - time_list (list): List with time values, expected to be datetime objects or ISO formatted strings.
        - base_query (str): The base SQL query to which this segment will be appended.

        Returns:
        - str: SQL query part with placeholders.
        - tuple: Tuple containing datetime values for the placeholders.
        """
        if not isinstance(time_list, list) or not time_list:
            return "", ()

        # Convert strings to datetime objects if necessary
        formatted_time_list = []
        for time_val in time_list:
            if isinstance(time_val, str):
                # Assuming the string is in ISO format, e.g., 'YYYY-MM-DD HH:MM:SS'
                try:
                    converted_datetime = datetime.strptime(time_val, '%Y-%m-%d %H:%M:%S')
                    formatted_time_list.append(converted_datetime)
                except ValueError as e:
                    self.logger.error(f"Error converting string to datetime: {e}")
                    continue
            elif isinstance(time_val, datetime):
                formatted_time_list.append(time_val)
            else:
                self.logger.error(f"Unsupported type for time list: {type(time_val)}")
                continue
        formatted_time_list = [time.astimezone(local_tz) for time in formatted_time_list]
        # Create placeholders for the query
        placeholders = ', '.join(['%s'] * len(formatted_time_list))
        query_start = self._is_constraint(base_query)
        time_column_name = self._get_time_column(base_query=base_query)

        query_part = f"{query_start}{time_column_name} IN ({placeholders})"
        return query_part, tuple(formatted_time_list)

    def _build_base_query_reading(self, table_name, column_names=None):
        table_name, column_names_str = self._normalize_table_names(table_name=table_name, column_names=column_names)
        query = f" SELECT {column_names_str} from {table_name} "
        return query

    def _get_times_by_meta_data(self, sample_id):
        from src.meta_data.meta_data_handler import MetaData
        meta_data = MetaData(sample_id=sample_id)
        meta_data.read()

        if not meta_data.start_time and not meta_data.end_time:
            (min_time, max_time) = self._fetch_first_and_last_match_by_sample_id(sample_id=sample_id)
            if min_time and max_time:
                min_time.astimezone(local_tz)
                max_time.astimezone(local_tz)
                meta_data.start_time = min_time
                meta_data.end_time = max_time
                meta_data.write()
                return min_time, max_time
            else:
                self.logger.error("Start and end time couldnt be found in meta data. T p does not seem to exist")
                return None, None
        else:
            return meta_data.start_time, meta_data.end_time

    def _get_time_column(self, base_query):
        if self.config.TP_DATA_TABLE_NAME in base_query:
            return self.config.TP_DATA_COLUMN_NAMES[0]
        if self.config.THERMAL_CONDUCTIVITY_DATA_TABLE_NAME in base_query:
            return self.config.THERMAL_CONDUCTIVITY_COLUMN_NAMES[0]
        if self.config.THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME in base_query:
            return self.config.THERMAL_CONDUCTIVITY_XY_COLUMN_NAMES[0]
        if self.config.CYCLE_DATA_TABLE_NAME in base_query:
            return self.config.CYCLE_DATA_COLUMN_NAMES[1]

    def _get_sample_id_column(self, table_name):
        """
        Helper method to get the sample_id column name based on the table.
        This is a placeholder method - implement based on your schema.
        """
        # Placeholder: Return a generic column name or customize based on the table
        return "sample_id"

    def _build_query_part_etc(self, time_range=None, base_query="", time_list=None):
        """
        Creates a part of a reading query that can be added to the base query.
        This version uses placeholders for safer and more flexible queries.

        Parameters:
        - time_range (tuple): Tuple containing the start and end times.
        - base_query (str): The base SQL query to which this segment will be appended.

        Returns:
        - str: SQL query part with placeholders.
        - tuple: Tuple containing values for the placeholders.
        """

        if time_range is None or len(time_range) != 2:
            return "", ()
        if time_range:
            min_time = min(time_range)
            max_time = max(time_range)
        if time_list:
            min_time = min(time_list)
            max_time = max(time_list)

        if min_time is None or max_time is None:
            return "", ()

        query_start = self._is_constraint(base_query)
        time_column_name = self._get_time_column(base_query=base_query)

        # Build the query part with placeholders
        query_part = f"{query_start}{time_column_name} BETWEEN %s AND %s "

        return query_part, (min_time, max_time)

    @staticmethod
    def _is_constraint(base_query):
        """
        Checks for existing constraints in the base reading query

        Parameters:
        - base_query (str): base reading query
        :returns: query start str
        """
        if " WHERE " in base_query or ' where ' in base_query.lower():
            return " AND "
        else:
            return " WHERE "

    def _calculate_times(self, first_match_time, base_query, last_match_time, limit_amount=500):
        """
        Calculates #limit_amount equidistant time points between first_match_time and last_match_time
        and generates a SQL string using placeholders for psycopg2.

        Parameters:
        - first_match_time (datetime or str): Start time for the calculation.
        - last_match_time (datetime, str or int): End time or number of days to add to start_time.
        - limit_amount (int): Number of time points to generate.

        Returns:
        - str: SQL string with placeholders.
        - tuple: Tuple of datetime values for SQL execution.
        """
        time_column_name = self._get_time_column(base_query=base_query)
        # Define the start date from the input and calculate the end date (half a year later)
        if isinstance(first_match_time, datetime):
            start_date = first_match_time
        else:
            start_date = datetime.strptime(first_match_time, "%Y-%m-%d %H:%M:%S")


        if isinstance(last_match_time, datetime):
            end_date = last_match_time
        elif isinstance(last_match_time, str):
            end_date = datetime.strptime(last_match_time, "%Y-%m-%d %H:%M:%S")
        elif isinstance(last_match_time, int):
            end_date = start_date + timedelta(days=last_match_time)

        # Calculate the total seconds and the interval
        total_seconds = (end_date - start_date).total_seconds()
        step_seconds = total_seconds / (limit_amount - 1)

        # Generate the datetime values
        times = [start_date + timedelta(seconds=step_seconds * i) for i in range(limit_amount)]
        times = [time.astimezone(local_tz) for time in times]
        times_pairs = [(times[i], times[i] + timedelta(seconds=1)) for i in range(len(times))]

        # Prepare the SQL part and the parameters tuple
        times_sql_parts = []
        params = []
        for start, end in times_pairs:
            times_sql_parts.append(f"({time_column_name} BETWEEN %s AND %s)")
            params.extend([start, end])

        # Join the SQL parts and package the parameters
        times_sql = " OR ".join(times_sql_parts)
        return times_sql, tuple(params)

    def _normalize_table_names(self, table_name=None, column_names=None):

        if "t_p" in table_name.lower():
            table_name = self.config.TP_DATA_TABLE_NAME
            if isinstance(column_names, list) or isinstance(column_names, tuple):
                column_names_str = ", ".join(column_names)
            else:
                column_names_str = column_names
        elif "x_y" in table_name.lower():
            table_name = self.config.THERMAL_CONDUCTIVITY_XY_DATA_TABLE_NAME
            if isinstance(column_names, list) or isinstance(column_names, tuple):
                column_names_str = ", ".join(column_names)
        elif "etc" in table_name.lower() or "conductivity" in table_name.lower():
            table_name = self.config.THERMAL_CONDUCTIVITY_DATA_TABLE_NAME
            if isinstance(column_names, list) or isinstance(column_names, tuple):
                column_names_str = ", ".join(column_names)
            else:
                column_names_str = column_names
        elif "meta" in table_name.lower():
            table_name = self.config.META_DATA_TABLE_NAME
            if isinstance(column_names, list) or isinstance(column_names, tuple):
                column_names_str = ", ".join(column_names)
        elif "cycle" in table_name.lower():
            table_name = self.config.CYCLE_DATA_TABLE_NAME
            if isinstance(column_names, list) or isinstance(column_names, tuple):
                column_names_str = ", ".join(column_names)
            else:
                column_names_str = column_names
        if not column_names:
            column_names = TableConfig().get_table_column_names(table_name=table_name)
            column_names_str = ", ".join(column_names)
        if column_names == "do_nothing":
            column_names_str = ""

        return table_name, column_names_str

    def _fetch_first_and_last_match_by_sample_id(self, sample_id=None):
        self.db.open_connection()
        table_name = self.config.TP_DATA_TABLE_NAME
        time_column = self.config.TP_DATA_COLUMN_NAMES[0]
        for column_name in self.config.TP_DATA_COLUMN_NAMES:
            # Check if the column name contains the substring "sample_id"
            if "sample_id" in column_name.lower():
                sample_id_column = column_name
        if sample_id is not None:
            query = f"SELECT MIN({time_column}) AS first_occurrence_datetime, " \
                    f"MAX({time_column}) As last_occurrence_datetime "\
                    f"FROM {table_name} "\
                    f"WHERE {sample_id_column} = '{sample_id}'"
        try:
            self.db.cursor.execute(query)
            occurence_sample_id = self.db.cursor.fetchone()
            self.db.close_connection()


            first_occ = occurence_sample_id[0]
            last_occ = occurence_sample_id[1]
          #  first_occ = min(item[0] for item in occurence_sample_id)
           # last_occ = max(item[0] for item in occurence_sample_id)
            return first_occ, last_occ
        except Exception as e:
            self.logger.error(f"Error occurred while fetching data: {e}")


def test_query_builder():
    qb = QueryBuilder()
    config = GetConfig()
    constraints = {
        'min_TotalTempIncr': 0,
        'max_TotalTempIncr': 20,
        'min_TotalCharTime': 0,
        'max_TotalCharTime': 60
                    }
   # constraints = {
    #    'min_eq': 5,
    constraints = None
 #                   }

    start_time = datetime(2022, 1, 1, 20, 21, 22)
    end_time = datetime(2023, 1, 5, 20, 22, 22)
    time_window = (start_time, end_time)
    table_name_etc = qb.config.THERMAL_CONDUCTIVITY_DATA_TABLE_NAME
    sample_id = "WAE-WA-040"
    #query, values = qb.create_reading_query(table_name=table_name_etc, constraints=constraints, time_window=time_window)
    query, values = qb.create_reading_query(table_name="t_p", column_names="eq_pressure", sample_id=sample_id)

   # query, values = qb.create_reading_query(limit_data_points=50000, table_name=table_name_etc, sample_id=sample_id, constraints=constraints)
    print(query)
    with DatabaseConnection() as db_conn:
        db_conn.cursor.execute(query, values)
        records = db_conn.cursor.fetchall()
        print(records)


if __name__ == "__main__":
    import time
    from src.meta_data.meta_data_handler import MetaData

    qb = QueryBuilder()
    qb.create_reading_query(table_name=TableConfig().TPDataTable.table_name,
                            sample_id='WAE-WA-030',
                            time_window=(datetime(2023, 1, 1), datetime(2023, 1, 2)))
    #query, vals=qb.create_continuous_reading_query(table_name="t_p")
    #print(query)
    #test_query_builder()
    #meta_data_handler.write_meta_data(sample_id=sample_id, time_start=time_min, time_end=time_max)





    #passed_time = time.time()-a_timer
    #print(passed_time)

