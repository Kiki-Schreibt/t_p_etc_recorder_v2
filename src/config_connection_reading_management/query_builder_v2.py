"""
Revised query_builder.py

This module contains dedicated builder classes for constructing SQL queries for different
table types. The design includes a BaseQueryBuilder with common helper methods and separate
builders for TP, ETC, CycleData, and MetaData queries. The QueryBuilder façade routes queries
to the correct builder based on the table name.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

from src.config_connection_reading_management.connections import DatabaseConnection
from src.table_data import TableConfig

local_tz = ZoneInfo("Europe/Berlin")


class BaseQueryBuilder:
    """
    Provides helper methods common to all query builders.
    """
    def __init__(self, db_conn_params=None):
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params or {}
        self.table_config = TableConfig()
        self.tp_table = self.table_config.TPDataTable
        self.etc_table = self.table_config.ETCDataTable
        self.etc_xy_table = self.table_config.ThermalConductivityXyDataTable
        self.cycle_data_table = self.table_config.CycleDataTable
        self.meta_data_table = self.table_config.MetaDataTable

    def _normalize_table_names(self, table_name=None, column_names=None):
        """
        Normalizes the table name and converts a list of column names into a comma‐separated string.
        """
        if "t_p" in table_name.lower():
            table_name = self.tp_table.table_name
        elif "x_y" in table_name.lower():
            table_name = self.etc_xy_table.table_name
        elif "etc" in table_name.lower() or "conductivity" in table_name.lower():
            table_name = self.etc_table.table_name
        elif "meta" in table_name.lower():
            table_name = self.meta_data_table.table_name
        elif "cycle" in table_name.lower():
            table_name = self.cycle_data_table.table_name

        if not column_names:
            column_names = TableConfig().get_table_column_names(table_name=table_name)
        if isinstance(column_names, (list, tuple)):
            column_names_str = ", ".join(column_names)
        else:
            column_names_str = column_names
        return table_name, column_names_str

    def _is_constraint(self, base_query):
        """Determines whether to prepend 'WHERE' or 'AND' to a query segment."""
        if " WHERE " in base_query or " where " in base_query:
            return " AND "
        else:
            return " WHERE "

    def _build_base_query_reading(self, table_name, column_names=None):
        """
        Builds a simple SELECT statement for the given table.
        """
        table_name, column_names_str = self._normalize_table_names(table_name, column_names)
        return f"SELECT {column_names_str} FROM {table_name}"

    def _get_time_column(self, base_query):
        """
        Returns the time column name based on which table is referenced in the base_query.
        """
        if self.tp_table.table_name in base_query:
            return self.tp_table.time
        if self.etc_table.table_name in base_query:
            return self.etc_table.time
        if self.etc_xy_table.table_name in base_query:
            return self.etc_xy_table.time
        if self.cycle_data_table.table_name in base_query:
            return self.cycle_data_table.time_start

    def _measurement_constraints_for_query(self, constraints=None, base_query=""):
        """
        Constructs a constraints portion of a query based on provided min/max pairs.
        Returns a tuple (constraint_sql, params).
        """
        if not constraints:
            return "", ()
        # Decide which columns to use based on the base query:
        if self.tp_table.table_name in base_query:
            columns = TableConfig().get_table_column_names(table_class=self.tp_table)
        elif self.etc_table.table_name in base_query:
            columns = TableConfig().get_table_column_names(table_class=self.etc_table)
        elif self.etc_xy_table.table_name in base_query:
            columns = TableConfig().get_table_column_names(table_class=self.etc_xy_table)
        else:
            columns = []

        clauses = []
        params = []
        prefix = self._is_constraint(base_query)
        for key, value in constraints.items():
            is_min = 'min_' in key.lower()
            normalized_key = key.replace('min_', '').replace('max_', '').lower()
            # Find a matching column (ignoring case)
            matched_column = next((col for col in columns if normalized_key in col.lower()), None)
            if matched_column:
                operator = ">=" if is_min else "<="
                clauses.append(f"{matched_column} {operator} %s")
                params.append(value)
        if clauses:
            return prefix + " AND ".join(clauses), tuple(params)
        return "", ()

    def _build_query_part_time_constraints(self, time_range=None, base_query="", limit_amount=None):
        """
        Builds a query segment for filtering based on a time range.
        Returns (query_segment, params).
        """
        if not time_range or len(time_range) != 2:
            return "", ()
        # Ensure each item is a datetime in local timezone:
        dt_list = []
        for item in time_range:
            if isinstance(item, str):
                try:
                    dt = datetime.strptime(item, '%Y-%m-%d %H:%M:%S')
                    dt_list.append(dt)
                except ValueError:
                    self.logger.error("Invalid date string: %s", item)
            elif isinstance(item, datetime):
                dt_list.append(item)
        if not dt_list:
            return "", ()
        dt_list = [t.astimezone(local_tz) for t in dt_list]
        min_time, max_time = min(dt_list), max(dt_list)
        prefix = self._is_constraint(base_query)
        time_column = self._get_time_column(base_query)
        # Optionally, one can use _calculate_times if you need equidistant sampling
        return f"{prefix}{time_column} BETWEEN %s AND %s", (min_time, max_time)

    def _build_query_part_time_list(self, time_list=None, base_query=""):
        """
        Builds a query segment based on an explicit list of time values.
        Returns (query_segment, params).
        """
        if not time_list or not isinstance(time_list, list):
            return "", ()
        formatted_times = []
        for t in time_list:
            if isinstance(t, str):
                try:
                    dt = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
                    formatted_times.append(dt.astimezone(local_tz))
                except ValueError as e:
                    self.logger.error("Time conversion error: %s", e)
            elif isinstance(t, datetime):
                formatted_times.append(t.astimezone(local_tz))
            else:
                self.logger.error("Unsupported time type: %s", type(t))
        placeholders = ", ".join(["%s"] * len(formatted_times))
        prefix = self._is_constraint(base_query)
        time_column = self._get_time_column(base_query)
        query_part = f"{prefix}{time_column} IN ({placeholders})"
        return query_part, tuple(formatted_times)

    def _calculate_times(self, first_match_time, base_query, last_match_time, limit_amount=500):
        """
        Calculates 'limit_amount' equidistant time points between first_match_time and last_match_time.
        Returns a query string segment (using OR) and a tuple of parameters.
        """
        if not isinstance(first_match_time, datetime):
            first_match_time = datetime.strptime(first_match_time, "%Y-%m-%d %H:%M:%S")
        if not isinstance(last_match_time, datetime):
            last_match_time = datetime.strptime(last_match_time, "%Y-%m-%d %H:%M:%S")
        start_date = first_match_time
        end_date = last_match_time
        total_seconds = (end_date - start_date).total_seconds()
        step_seconds = total_seconds / (limit_amount - 1)
        times = [start_date + timedelta(seconds=step_seconds * i) for i in range(limit_amount)]
        times = [t.astimezone(local_tz) for t in times]
        # Build pairs: each interval as (start, start+1 sec)
        times_pairs = [(t, t + timedelta(seconds=1)) for t in times]
        time_column = self._get_time_column(base_query)
        parts = []
        params = []
        for start, end in times_pairs:
            parts.append(f"({time_column} BETWEEN %s AND %s)")
            params.extend([start, end])
        return " OR ".join(parts), tuple(params)


class TPQueryBuilder(BaseQueryBuilder):
    """
    Builds queries for the TP (temperature/pressure) table.
    """
    def create_reading_query(self, column_names=None, constraints=None,
                             time_window=None, sample_id=None, time_list=None,
                             limit_data_points=50000):
        base_query = self._build_base_query_reading(self.tp_table.table_name, column_names)
        cons_query, cons_vals = self._measurement_constraints_for_query(constraints, base_query)
        base_query += cons_query
        values = cons_vals

        query_part, part_vals = self._create_tp_reading_query(base_query, time_window, sample_id, time_list, limit_data_points)
        query_part += f" ORDER BY {self.tp_table.time}"
        values += part_vals
        query = base_query + " " + query_part
        return query, values

    def _create_tp_reading_query(self, base_query, time_window, sample_id, time_list, limit_data_points):
        values = ()
        query_part = ""
        if time_list:
            time_query, time_vals = self._build_query_part_time_list(time_list, base_query)
            query_part = time_query
            values += time_vals
            if sample_id:
                query_part += f" AND {self.tp_table.sample_id} = %s"
                values += (sample_id,)
        elif time_window:
            time_query, time_vals = self._build_query_part_time_constraints(time_range=time_window, base_query=base_query, limit_amount=limit_data_points)
            query_part = time_query
            values += time_vals
            if sample_id:
                query_part += f" AND {self.tp_table.sample_id} = %s"
                values += (sample_id,)
        elif sample_id:
            min_time, max_time = self._get_times_by_meta_data(sample_id)
            if min_time and max_time:
                time_window = (min_time, max_time)
                query_part, time_vals = self._build_query_part_time_constraints(time_range=time_window, base_query=base_query, limit_amount=limit_data_points)
                values += time_vals
        return query_part, values

    def _get_times_by_meta_data(self, sample_id):
        from src.meta_data.meta_data_handler import MetaData
        meta_data = MetaData(sample_id=sample_id, db_conn_params=self.db_conn_params)

        if not meta_data.start_time and not meta_data.end_time:
            first, last = self._fetch_first_and_last_match_by_sample_id(sample_id)

            if first and last:
                meta_data.start_time = first
                meta_data.end_time = last
                meta_data.write()
                return first, last

            else:
                self.logger.error("Cannot determine time window for sample_id: %s", sample_id)
                return None, None
        else:
            return meta_data.start_time, meta_data.end_time

    def _fetch_first_and_last_match_by_sample_id(self, sample_id):
        print("at least correct method started")
        table_name = self.tp_table.table_name
        time_column = self.tp_table.time
        columns = TableConfig().get_table_column_names(self.tp_table)
        print("made it to here")
        sample_id_column = next((col for col in columns if "sample_id" in col.lower()), None)
        if sample_id is not None:
            query = f"SELECT MIN({time_column}), MAX({time_column}) FROM {table_name} WHERE {sample_id_column} = %s"
            print(query)
        else:
            return None, None
        with DatabaseConnection(**self.db_conn_params) as db_conn:
            try:
                db_conn.cursor.execute(query, (sample_id,))
                result = db_conn.cursor.fetchone()
                first_occ, last_occ = result[0], result[1]
                db_conn.close_connection()
                return first_occ, last_occ
            except Exception as e:
                self.logger.error("Error fetching times for sample_id %s: %s", sample_id, e)
                return None, None


class ETCQueryBuilder(BaseQueryBuilder):
    """
    Builds queries for the ETC (thermal conductivity etc.) table.
    """
    def create_reading_query(self, column_names=None, constraints=None,
                             time_window=None, sample_id=None, time_list=None,
                             limit_data_points=50000):
        base_query = self._build_base_query_reading(self.etc_table.table_name, column_names)
        cons_query, cons_vals = self._measurement_constraints_for_query(constraints, base_query)
        base_query += cons_query
        values = cons_vals

        query_part, part_vals = self._create_etc_reading_query(base_query, time_window, sample_id, time_list)
        query_part += f" ORDER BY {self.etc_table.time}"
        values += part_vals
        query = base_query + " " + query_part
        return query, values

    def _create_etc_reading_query(self, base_query, time_window, sample_id, time_list):
        query_part = ""
        values = ()
        # For ETC, we use a similar approach as TP but with a different sample_id column:
        time_query, time_vals = self._build_query_part_etc(time_range=time_window, base_query=base_query, time_list=time_list)
        query_part += time_query
        values += time_vals
        if sample_id:
            query_part += f" AND {self.etc_table.sample_id_small} = %s"
            values += (sample_id,)
        return query_part, values

    def _build_query_part_etc(self, time_range=None, base_query="", time_list=None):
        """
        Builds the ETC-specific time constraints.
        """
        # For ETC we delegate to the base method (could be adjusted if needed)
        return self._build_query_part_time_constraints(time_range, base_query, limit_amount=None)


class CycleDataQueryBuilder(BaseQueryBuilder):
    """
    Builds queries for the Cycle Data table.
    """
    def create_reading_query(self, column_names=None, time_window=None,
                             sample_id=None, time_list=None):
        base_query = self._build_base_query_reading(self.cycle_data_table.table_name, column_names)
        values = ()
        query_part = ""
        if time_window:
            time_query, time_vals = self._build_query_part_etc(time_range=time_window, base_query=base_query)
            query_part += time_query
            values += time_vals
            if sample_id:
                query_part += f" AND {self.tp_table.sample_id} = %s"
                values += (sample_id,)
        elif sample_id:
            query_part += f" WHERE {self.tp_table.sample_id} = %s"
            values += (sample_id,)
        query_part += f" ORDER BY {self.cycle_data_table.time_start}"
        query = base_query + " " + query_part
        return query, values


class MetaDataQueryBuilder(BaseQueryBuilder):
    """
    Builds queries for the MetaData table.
    """
    def create_reading_query(self, sample_id):
        base_query = self._build_base_query_reading(self.meta_data_table.table_name)
        query_part = f" WHERE {self.meta_data_table.sample_id} = %s"
        query = base_query + " " + query_part
        return query, (sample_id,)


class QueryBuilder:
    """
    Facade class that selects the appropriate builder based on the table name.
    Also includes methods for continuous reading and writing queries.
    """
    def __init__(self, db_conn_params=None):
        self.db_conn_params = db_conn_params or {}
        self.tp_builder = TPQueryBuilder(db_conn_params=self.db_conn_params)
        self.etc_builder = ETCQueryBuilder(db_conn_params=self.db_conn_params)
        self.cycle_builder = CycleDataQueryBuilder(db_conn_params=self.db_conn_params)
        self.meta_builder = MetaDataQueryBuilder(db_conn_params=self.db_conn_params)
        self.base_builder = BaseQueryBuilder(db_conn_params=self.db_conn_params)  # For common methods

    def create_reading_query(self, table_name, **kwargs):
        table_name_lower = table_name.lower()
        if "t_p" in table_name_lower:
            return self.tp_builder.create_reading_query(**kwargs)
        elif "etc" in table_name_lower or "conductivity" in table_name_lower:
            return self.etc_builder.create_reading_query(**kwargs)
        elif "cycle" in table_name_lower:
            return self.cycle_builder.create_reading_query(**kwargs)
        elif "meta" in table_name_lower:
            return self.meta_builder.create_reading_query(kwargs.get("sample_id"))
        else:
            self.base_builder.logger.error("Unknown table name: %s", table_name)
            return None, None

    def create_continuous_reading_query(self, table_name, column_names=None, constraints=None,
                                          sample_id=None, desc_limit=1):
        base_query = self.base_builder._build_base_query_reading(table_name, column_names)
        time_column = self.base_builder._get_time_column(base_query)
        cons_query, values = self.base_builder._measurement_constraints_for_query(constraints, base_query)
        query = base_query + cons_query
        if sample_id:
            query += f" {self.base_builder._is_constraint(base_query)} sample_id = %s"
            values += (sample_id,)
        query += f" ORDER BY {time_column} DESC LIMIT {desc_limit}"
        return query, values

    def create_writing_query(self, table_name, column_names=None):
        table_name, col_str = self.base_builder._normalize_table_names(table_name, column_names)
        if not column_names and "t_p" in table_name.lower():
            column_names = TableConfig().get_table_column_names(table_class=self.base_builder.tp_table)
            col_str = ", ".join(column_names)
        placeholders = ", ".join(["%s"] * len(column_names))
        query = f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})"
        return query


# --- Test function for quick verification ---
def test_query_builder():
    from src.config_connection_reading_management.config_reader import GetConfig
    config = GetConfig()
    qb = QueryBuilder(db_conn_params=config.db_conn_params)
    sample_id = "WAE-WA-040"
    start_time = datetime(2022, 1, 1, 20, 21, 22)
    end_time = datetime(2023, 1, 5, 20, 22, 22)
    time_window = (start_time, end_time)
    # Example: build a reading query for TP data (even if table name is given loosely)
    query, values = qb.create_reading_query(
        table_name="t_p",
        column_names="eq_pressure",
        sample_id=sample_id,
        #time_window=time_window
    )
    print("Generated Query:")
    print(query)
    print("With values:")
    print(values)
    # Optionally, test executing the query:
    with DatabaseConnection(config.db_conn_params) as db_conn:
        db_conn.cursor.execute(query, values)
        records = db_conn.cursor.fetchall()
        print("Records:")
        print(records)


if __name__ == "__main__":
    test_query_builder()


