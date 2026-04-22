#query_builder.py

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging
import re

from connections import DatabaseConnection
from table_config import TableConfig


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
    def __init__(self, db_conn_params, join_time_precision=""):
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params
        self.table_config = TableConfig()
        self.tp_table = self.table_config.TPDataTable
        self.etc_table = self.table_config.ETCDataTable
        self.etc_xy_table = self.table_config.ThermalConductivityXyDataTable
        self.cycle_data_table = self.table_config.CycleDataTable
        self.meta_data_table = self.table_config.MetaDataTable
        self.join_time_precision = join_time_precision

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

    def _build_base_query_reading(self,
                                  table_name: str,
                                  column_names=None,
                                  join_table: str = None,
                                  join_on: list[tuple[str,str]] = None):
        """
        If join_table is given, we will emit:
            FROM main_table AS m
            JOIN join_table AS j ON m.col1 = j.colA AND m.col2 = j.colB
        """

        #print(self.join_time_precision)
        main_table, col_str = self._normalize_table_names(table_name, column_names)
        if not join_table:
            return f"SELECT {col_str} FROM {main_table}"

        join_table_norm, _ = self._normalize_table_names(join_table, None)
        alias_m = "m"; alias_j = "j"
        # build ON clauses from join_on pairs
        on_clauses = []

        for mcol, jcol in (join_on or []):
            if self.join_time_precision == "second":
                left  = f"date_trunc('second', {alias_m}.{mcol})"
                right = f"date_trunc('second', {alias_j}.{jcol})"
            else:
                left  = f"{alias_m}.{mcol}"
                right = f"{alias_j}.{jcol}"
            on_clauses.append(f"{left} = {right}")

        on_sql = " AND ".join(on_clauses) if on_clauses else "1=1"

        # re‐prefix all selected columns with m.
        col_list = [c.strip() for c in col_str.split(",")]
        prefixed = []
        for c in col_list:
            # if someone passed e.g. "etc.th_conductivity AS ThConductivity" you may need more parsing;
            # for simplicity we just prefix bare names:
            prefixed.append(f"{alias_m}.{c}")
        col_str = ", ".join(prefixed)

        sql = (
            f"SELECT {col_str}\n"
            f"  FROM {main_table} AS {alias_m}\n"
            f"  JOIN {join_table_norm}  AS {alias_j}\n"
            f"    ON {on_sql}"
        )
        return sql

    def _main_alias(self, base_query: str) -> str:
        # If you've used _build_base_query_reading with a join, you alias the main table as "m"
        # otherwise no alias (i.e. empty string)
        return "m" if " AS m" in base_query else ""

    def _get_time_column(self, base_query: str) -> str:
        """
        Determine which table is the “main” one in this query,
        whether aliased or not, and return its properly cased time column,
        prefixed with the alias if there is one.
        """
        # 1) Look for an explicit alias: FROM table_name AS alias
        m = re.search(r"FROM\s+(\S+)\s+AS\s+(\w+)", base_query, flags=re.IGNORECASE)
        if m:
            table_in_from, alias = m.group(1), m.group(2)
        else:
            table_in_from, alias = None, ""

        prefix = f"{alias}." if alias else ""

        # 2) If aliased, use that exact table_name → column mapping
        if table_in_from:
            if table_in_from == self.tp_table.table_name:
                return prefix + self.tp_table.time
            if table_in_from == self.etc_table.table_name:
                return prefix + self.etc_table.time
            if table_in_from == self.etc_xy_table.table_name:
                return prefix + self.etc_xy_table.time
            if table_in_from == self.cycle_data_table.table_name:
                return prefix + self.cycle_data_table.time_start

        # 3) Otherwise (no alias), detect by substring but still return the real column name
        if self.tp_table.table_name in base_query:
            return prefix + self.tp_table.time
        if self.etc_table.table_name in base_query:
            return prefix + self.etc_table.time
        if self.etc_xy_table.table_name in base_query:
            return prefix + self.etc_xy_table.time
        if self.cycle_data_table.table_name in base_query:
            return prefix + self.cycle_data_table.time_start

        # 4) If somehow nothing matched, fall back to TP’s time column
        return prefix + self.tp_table.time

    def _measurement_constraints_for_query(self, constraints=None, base_query=""):
        if not constraints:
            return "", ()

        # find explicit alias
        m = re.search(r"FROM\s+(\S+)\s+AS\s+(\w+)", base_query, flags=re.IGNORECASE)
        table_in_from = m.group(1) if m else None
        alias         = m.group(2) if m else ""
        prefix        = self._is_constraint(base_query)

        # pick columns by explicit alias
        if   table_in_from == self.tp_table.table_name:
            columns = TableConfig().get_table_column_names(table_class=self.tp_table)
        elif table_in_from == self.etc_table.table_name:
            columns = TableConfig().get_table_column_names(table_class=self.etc_table)
        elif table_in_from == self.etc_xy_table.table_name:
            columns = TableConfig().get_table_column_names(table_class=self.etc_xy_table)
        elif table_in_from == self.cycle_data_table.table_name:
            columns = TableConfig().get_table_column_names(table_class=self.cycle_data_table)
        else:
            # fallback to substring detection when there's no alias
            if self.tp_table.table_name in base_query:
                columns = TableConfig().get_table_column_names(table_class=self.tp_table)
            elif self.etc_table.table_name in base_query:
                columns = TableConfig().get_table_column_names(table_class=self.etc_table)
            elif self.etc_xy_table.table_name in base_query:
                columns = TableConfig().get_table_column_names(table_class=self.etc_xy_table)
            elif self.cycle_data_table.table_name in base_query:
                columns = TableConfig().get_table_column_names(table_class=self.cycle_data_table)
            else:
                columns = []

        # build your predicates
        clauses, params = [], []
        for key, value in constraints.items():
            if key.startswith("min_"):
                field_name, operator = key[4:], ">="
            elif key.startswith("max_"):
                field_name, operator = key[4:], "<="
            elif key.startswith("where_"):
                field_name, operator = key[6:], "="
            else:
                continue

            matched = next((c for c in columns if field_name.lower() in c.lower()), None)
            if matched:
                col_ref = f"{alias}.{matched}" if alias else matched
                clauses.append(f"{col_ref} {operator} %s")
                params.append(value)

        if not clauses:
            return "", ()
        return prefix + " AND ".join(clauses), tuple(params)

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

    def _aliased_column_name(self, column_name: str, base_query: str) -> str:
        """
        If the main table was aliased as "m", returns "m.column_name",
        otherwise just "column_name".
        """
        alias = self._main_alias(base_query)
        return f"{alias}.{column_name}" if alias else column_name


class TPQueryBuilder(BaseQueryBuilder):
    """
    Query builder for TP (temperature/pressure) data.
    Now uses downsampling for time windows.
    """
    def __init__(self, db_conn_params):
        super().__init__(db_conn_params=db_conn_params)

    @filter_kwargs
    def create_reading_query(self,
                             column_names=None,
                             constraints=None,
                             time_window=None,
                             sample_id=None,
                             time_list=None,
                             limit_data_points=50000,
                             join_table: str = None,
                             join_on: list[tuple[str,str]] = None,
                             join_constraints: dict = None):

        # 1) SELECT … FROM [tp]  (with optional JOIN)
        base_query = self._build_base_query_reading(
            table_name=self.tp_table.table_name,
            column_names=column_names,
            join_table=join_table,
            join_on=join_on
        )

        # 2) apply any TP‐table constraints
        cons_query, cons_vals = self._measurement_constraints_for_query(constraints, base_query)
        base_query += cons_query
        values    = cons_vals

        # 3) apply any filters on the joined table
        if join_constraints:
            sep = " WHERE " if " WHERE " not in base_query.upper() else " AND "
            clauses = []
            for col, v in join_constraints.items():
                clauses.append(f"j.{col} = %s")
                values += (v,)
            base_query += sep + " AND ".join(clauses)

        # 4) now add your time/sample‐id logic exactly as before
        tp_part, tp_vals = self._create_tp_reading_query(
            base_query,
            time_window,
            sample_id,
            time_list,
            limit_data_points
        )
        time_col = self._get_time_column(base_query)
        base_query += tp_part + f" ORDER BY {time_col}"
        values     += tp_vals

        return base_query, values

    def _create_tp_reading_query(self, base_query, time_window, sample_id, time_list, limit_data_points):
        values = ()
        query_part = ""
        if time_list:
            time_query, time_vals = self._build_query_part_time_list(time_list, base_query)
            query_part = time_query
            values += time_vals
            if sample_id:
                prefix = self._is_constraint(base_query + query_part)
                query_part += f" {prefix} {self.tp_table.sample_id} = %s"
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
                prefix = self._is_constraint(base_query + query_part)
                query_part += f" {prefix} {self.tp_table.sample_id} = %s"
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
        from metadata_handler import MetaData

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
    def create_reading_query(self,
                             column_names=None,
                             constraints=None,
                             time_window=None,
                             sample_id=None,
                             time_list=None,
                             limit_data_points=50000,
                             join_table: str = None,
                             join_on: list[tuple[str,str]] = None,
                             join_constraints: dict = None
                             ):

         # 1) build the SELECT ... FROM [etc] (with optional join)
        base_query = self._build_base_query_reading(
            table_name=self.etc_table.table_name,
            column_names=column_names,
            join_table=join_table,
            join_on=join_on
        )

        # 2) measurement‐style constraints on the etc table only:
        cons_query, cons_vals = self._measurement_constraints_for_query(constraints, base_query)
        base_query += cons_query
        values = cons_vals

        # 3) if the user also passed some constraints on the joined table:
        if join_constraints:
            # assume keys are exact column names on the joined table,
            # and we always prefix with "j." for the alias
            parts = []
            for col, val in join_constraints.items():
                parts.append(f"j.{col} = %s")
                values += (val,)
            sep = " WHERE " if " WHERE " not in base_query.upper() else " AND "
            base_query += sep + " AND ".join(parts)

        # 4) time/window/sample_id logic stays the same,
        #    you’ll call _build_query_part_time_constraints etc.
        #    just remember time_column comes from the *etc* table by default.
        time_query, time_vals = self._build_query_part_time_constraints(
            time_range=time_window, base_query=base_query
        )
        values += time_vals
        base_query += time_query
        if sample_id:
            sample_id_col_name = self._aliased_column_name(column_name=self.etc_table.sample_id_small, base_query=base_query)
            prefix           = self._is_constraint(base_query)
            values += (sample_id,)
            base_query += f" {prefix} {sample_id_col_name} = %s"


        # 5) finish with ORDER BY
        time_col = self._get_time_column(base_query)
        base_query += f" ORDER BY {time_col}"
        return base_query, values

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
    def create_reading_query(self,
                             column_names=None,
                             time_window=None,
                             sample_id=None,
                             join_table: str = None,
                             join_on: list[tuple[str,str]] = None,
                             join_constraints: dict = None):
        # 1) SELECT … FROM [cycle]  (with optional JOIN)
        base_query = self._build_base_query_reading(
            table_name=self.cycle_data_table.table_name,
            column_names=column_names,
            join_table=join_table,
            join_on=join_on
        )
        values = ()
        # 2) any joined‐table filters
        if join_constraints:
            sep = " WHERE " if " WHERE " not in base_query.upper() else " AND "
            clauses = []
            for col, v in join_constraints.items():
                clauses.append(f"j.{col} = %s")
                values += (v,)
            base_query += sep + " AND ".join(clauses)

        # 3) time_window / sample_id logic (as before)
        query_part = ""
        if time_window:
            t_q, t_v = self._build_query_part_time_constraints(time_range=time_window,
                                                                base_query=base_query)
            query_part += t_q
            values     += t_v
            if sample_id:
                prefix = self._is_constraint(base_query + query_part)
                query_part += f" {prefix} {self.cycle_data_table.sample_id} = %s"
                values     += (sample_id,)
        elif sample_id:
            query_part += f" WHERE {self.cycle_data_table.sample_id} = %s"
            values     += (sample_id,)

        time_col = self._get_time_column(base_query)
        base_query += query_part
        base_query += f" ORDER BY {time_col}"
        return base_query, values


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
    from config_reader import config
    qb = QueryBuilder(db_conn_params=config.db_conn_params)
    sample_id = "WAE-WA-040"
    start_time = datetime(2023, 1, 1, 23, 21, 22)
    end_time = datetime(2023, 1, 5, 20, 22, 22)
    time_window = (start_time, end_time)

    constraints = {
    "min_TotalCharTime": 0.33,
    "max_TotalCharTime": 1,
    "min_TotalTempIncr": 2,
    "max_TotalTempIncr": 5
}
    query, values = qb.create_reading_query(
        table_name="etc",  # This will now match strictly to the TP table
        column_names=TableConfig().TPDataTable.pressure,
        sample_id=sample_id,
        constraints=constraints,
        limit_data_points=5
    )
    print("Generated Query:")
    print(query)
    print("With values:")
    print(values)
    tp_table = TableConfig().TPDataTable
    etc_table = TableConfig().ETCDataTable

    join_table = tp_table.table_name
    join_on = [(etc_table.time, tp_table.time)]
    join_constraints = {tp_table.is_isotherm_flag: False}

    query, values = qb.create_reading_query(
        table_name="etc",  # This will now match strictly to the TP table
        column_names=TableConfig().ETCDataTable.temperature_sample,
        sample_id=sample_id,
        time_window=time_window,
        limit_data_points=5,
        constraints=constraints,
        join_on=join_on,
        join_table=join_table,
        join_constraints=join_constraints
    )
    print("Generated Query:")
    print(query)
    print("With values:")
    print(values)



    #with DatabaseConnection(**config.db_conn_params) as db_conn:
    #    db_conn.cursor.execute(query, values)
    #    records = db_conn.cursor.fetchall()
    #    print("Records:")
    #    print(records)


if __name__ == "__main__":
    test_query_builder()


