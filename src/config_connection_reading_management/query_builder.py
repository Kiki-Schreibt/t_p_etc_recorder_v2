#query_builder.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

from src.infrastructure.connections.connections import DatabaseConnection
from src.infrastructure.core.table_config import TableConfig

local_tz = ZoneInfo("Europe/Berlin")


def filter_kwargs(func):
    import functools
    import inspect
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Get the parameters accepted by the function
        sig = inspect.signature(func)
        valid_params = sig.parameters.keys()
        # Filter kwargs to only include keys that are valid parameters
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_params}
        return func(*args, **filtered_kwargs)
    return wrapper


class BaseQueryBuilder:
    """
    Base query builder with common helper methods.
    """
    def __init__(self, db_conn_params):
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params
        self.table_config = TableConfig()
        self.tp_table = self.table_config.TPDataTable
        self.etc_table = self.table_config.ETCDataTable
        self.etc_xy_table = self.table_config.ThermalConductivityXyDataTable
        self.cycle_data_table = self.table_config.CycleDataTable
        self.meta_data_table = self.table_config.MetaDataTable

    def _normalize_table_names(self, table_name=None, column_names=None):
        if "t_p" in table_name.lower():
            table_name = self.tp_table.table_name
        elif "x_y" in table_name.lower():
            table_name = self.etc_xy_table.table_name
            if not column_names:
                column_names = TableConfig().get_xy_array_column_names()
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
        if " WHERE " in base_query or " where " in base_query:
            return " AND "
        else:
            return " WHERE "

    def _build_base_query_reading(self, table_name, column_names=None):
        table_name, column_names_str = self._normalize_table_names(table_name, column_names)
        return f"SELECT {column_names_str} FROM {table_name}"

    def _get_time_column(self, base_query):
        if self.tp_table.table_name in base_query:
            return self.tp_table.time
        if self.etc_table.table_name in base_query:
            return self.etc_table.time
        if self.etc_xy_table.table_name in base_query:
            return self.etc_xy_table.time
        if self.cycle_data_table.table_name in base_query:
            return self.cycle_data_table.time_start

    def _measurement_constraints_for_query(self, constraints=None, base_query=""):
        if not constraints:
            return "", ()
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
            if key.startswith("min_"):
                field_name = key[len("min_"):]
                operator = ">="
            elif key.startswith("max_"):
                field_name = key[len("max_"):]
                operator = "<="
            elif key.startswith("where_"):
                field_name = key[len("where_"):]
                operator = "="
            else:
                # unknown prefix → skip
                continue

            # find the actual column name that contains our field_name
            matched = next(
                (col for col in columns if field_name.lower() in col.lower()),
                None
            )
            if matched:
                clauses.append(f"{matched} {operator} %s")
                params.append(value)

        if clauses:
            # join all clauses with AND, and prepend WHERE/AND as appropriate
            clause_str = prefix + " AND ".join(clauses)
            return clause_str, tuple(params)

        return "", ()

    def _build_query_part_time_constraints(self, time_range=None, base_query="", limit_amount=None):
        """
        For a given time_range, this method either returns a simple BETWEEN clause
        or, if limit_amount is provided, returns a downsampled set of time constraints.
        """
        if not time_range or len(time_range) != 2:
            return "", ()
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
        if limit_amount is not None:
            # Downsample the time window using equidistant time intervals
            time_list_query, time_list_vals = self._calculate_times(
                first_match_time=min_time,
                base_query=base_query,
                last_match_time=max_time,
                limit_amount=limit_amount
            )
            return f"{prefix} ({time_list_query})", time_list_vals
        else:
            return f"{prefix}{time_column} BETWEEN %s AND %s", (min_time, max_time)

    def _build_query_part_time_list(self, time_list=None, base_query=""):
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
        Returns a downsampled set of equidistant time constraints.
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
        # Build pairs: each interval as (start, start + 1 sec)
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
    Query builder for TP (temperature/pressure) data.
    Now uses downsampling for time windows.
    """
    def __init__(self, db_conn_params):
        super().__init__(db_conn_params=db_conn_params)

    @filter_kwargs
    def create_reading_query(self, column_names=None, constraints=None,
                             time_window=None, sample_id=None, time_list=None,
                             limit_data_points=50000):
        base_query = self._build_base_query_reading(self.tp_table.table_name, column_names)
        cons_query, cons_vals = self._measurement_constraints_for_query(constraints, base_query)
        base_query += cons_query
        values = cons_vals

        query_part, part_vals = self._create_tp_reading_query(
            base_query, time_window, sample_id, time_list, limit_data_points
        )
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
            # Downsample the TP data query using limit_data_points
            time_query, time_vals = self._build_query_part_time_constraints(
                time_range=time_window,
                base_query=base_query,
                limit_amount=limit_data_points
            )
            query_part = time_query
            values += time_vals
            if sample_id:
                query_part += f" AND {self.tp_table.sample_id} = %s"
                values += (sample_id,)
        elif sample_id:
            min_time, max_time = self._get_times_by_meta_data(sample_id)
            if min_time and max_time:
                time_window = (min_time, max_time)
                query_part, time_vals = self._build_query_part_time_constraints(
                    time_range=time_window,
                    base_query=base_query,
                    limit_amount=limit_data_points
                )
                values += time_vals
        return query_part, values

    def _get_times_by_meta_data(self, sample_id):
        from src.infrastructure.meta_data.meta_data_handler import MetaData

        meta_data = MetaData(sample_id=sample_id, db_conn_params=self.db_conn_params)
        if not meta_data.start_time or not meta_data.end_time:
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
        table_name = self.tp_table.table_name
        time_column = self.tp_table.time
        columns = TableConfig().get_table_column_names(self.tp_table)
        sample_id_column = next((col for col in columns if "sample_id" in col.lower()), None)
        if sample_id is not None:
            query = f"SELECT MIN({time_column}), MAX({time_column}) FROM {table_name} WHERE {sample_id_column} = %s"
        else:
            return None, None
        with DatabaseConnection(**self.db_conn_params) as db_conn:
            try:
                db_conn.cursor.execute(query, (sample_id,))
                result = db_conn.cursor.fetchone()
                first_occ, last_occ = result[0], result[1]
                return first_occ, last_occ
            except Exception as e:
                self.logger.error("Error fetching times for sample_id %s: %s", sample_id, e)
                return None, None


class ETCQueryBuilder(BaseQueryBuilder):
    """
    Query builder for ETC (thermal conductivity, etc.) data.
    """
    def __init__(self, db_conn_params):
        super().__init__(db_conn_params=db_conn_params)

    @filter_kwargs
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
        time_query, time_vals = self._build_query_part_time_constraints(time_range=time_window, base_query=base_query)
        query_part += time_query
        values += time_vals
        if sample_id:
            prefix = self._is_constraint(base_query + query_part)
            query_part += f"{prefix}{self.etc_table.sample_id_small} = %s"
            values    += (sample_id,)
        return query_part, values


class CycleDataQueryBuilder(BaseQueryBuilder):
    """
    Query builder for Cycle Data.
    """
    def __init__(self, db_conn_params):
        super().__init__(db_conn_params=db_conn_params)

    @filter_kwargs
    def create_reading_query(self, column_names=None, time_window=None,
                             sample_id=None):
        base_query = self._build_base_query_reading(self.cycle_data_table.table_name, column_names)
        values = ()
        query_part = ""
        if time_window:
            time_query, time_vals = self._build_query_part_time_constraints(time_range=time_window, base_query=base_query)
            query_part += time_query
            values += time_vals
            if sample_id:
                query_part += f" AND {self.cycle_data_table.sample_id} = %s"
                values += (sample_id,)
        elif sample_id:
            query_part += f" WHERE {self.cycle_data_table.sample_id} = %s"
            values += (sample_id,)
        query_part += f" ORDER BY {self.cycle_data_table.time_start}"
        query = base_query + " " + query_part
        return query, values


class MetaDataQueryBuilder(BaseQueryBuilder):
    """
    Query builder for MetaData.
    """
    def __init__(self, db_conn_params):
        super().__init__(db_conn_params=db_conn_params)

    def create_reading_query(self, sample_id):
        base_query = self._build_base_query_reading(self.meta_data_table.table_name)
        query_part = f" WHERE {self.meta_data_table.sample_id} = %s"
        query = base_query + " " + query_part
        return query, (sample_id,)


class QueryBuilder:
    """
    Facade that selects the appropriate query builder based on table name.
    Now uses a stricter match for TP tables.
    """
    def __init__(self, db_conn_params=None):
        self.db_conn_params = db_conn_params or {}
        self.tp_builder = TPQueryBuilder(db_conn_params=self.db_conn_params)
        self.etc_builder = ETCQueryBuilder(db_conn_params=self.db_conn_params)
        self.cycle_builder = CycleDataQueryBuilder(db_conn_params=self.db_conn_params)
        self.meta_builder = MetaDataQueryBuilder(db_conn_params=self.db_conn_params)
        self.base_builder = BaseQueryBuilder(db_conn_params=self.db_conn_params)

    def create_reading_query(self, table_name, **kwargs):
        table_name_lower = table_name.lower()
        # Use strict equality if table_name exactly matches the TP table name;
        # otherwise, fall back to substring matching.
        if table_name_lower == self.tp_builder.tp_table.table_name.lower() or "t_p" in table_name_lower:
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
    from src.infrastructure.core.config_reader import GetConfig
    config = GetConfig()
    qb = QueryBuilder(db_conn_params=config.db_conn_params)
    sample_id = "WAE-WA-040"
    start_time = datetime(2022, 1, 1, 20, 21, 22)
    end_time = datetime(2023, 1, 5, 20, 22, 22)
    time_window = (start_time, end_time)
    query, values = qb.create_reading_query(
        table_name="t_p",  # This will now match strictly to the TP table
        column_names="eq_pressure",
        sample_id=sample_id,
        time_window=time_window
    )
    print("Generated Query:")
    print(query)
    print("With values:")
    print(values)
    with DatabaseConnection(config.db_conn_params) as db_conn:
        db_conn.cursor.execute(query, values)
        records = db_conn.cursor.fetchall()
        print("Records:")
        print(records)


if __name__ == "__main__":
    test_query_builder()


