#table_partitioner.py
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
from src.infrastructure.connections.connections import DatabaseConnection
from src.infrastructure.core.table_config import TableConfig
from src.table_creator import TableCreator


class Partitioner:
    """
    Utility class for converting tables to monthly range partitions
    and ensuring future or arbitrary-range partitions exist.
    """
    def __init__(self, db_conn_params: dict):
        self.db_conn_params = db_conn_params

    def convert_table_to_monthly_partitions(self,
                                            parent_schema: str,
                                            parent_table_class):
        """
        Convert a non-partitioned table into a partitioned parent with
        monthly child tables spanning the full data range.
        """
        table = parent_table_class.table_name
        time_col = parent_table_class.time
        full_old = f"{parent_schema}.{table}"
        archive = f"{parent_schema}.{table}_old"
        full_new = f"{parent_schema}.{table}_new"

        # 1) rename existing
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"ALTER TABLE {full_old} RENAME TO {table}_old;"
            )
            conn.conn.commit()

        # 2) create new partitioned parent
        TableCreator(db_conn_params=self.db_conn_params) \
            .create_table(parent_table_class)
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"ALTER TABLE {table} RENAME TO {table}_new;"
            )
            conn.conn.commit()

        # 3) find data range
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"SELECT MIN({time_col})::date, MAX({time_col})::date FROM {archive};"
            )
            min_date, max_date = conn.cursor.fetchone()
            if min_date is None:
                return

        # 4) create monthly partitions
        start = min_date.replace(day=1)
        step = relativedelta(months=1)
        end_boundary = max_date.replace(day=1) + step
        current = start
        with DatabaseConnection(**self.db_conn_params) as conn:
            while current < end_boundary:
                suffix = f"{current.year:04d}_{current.month:02d}"
                self._create_partition(conn, parent_schema, table + "_new",
                                       suffix, current, current + step)
                current += step
            conn.conn.commit()

        # 5) bulk copy
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"INSERT INTO {full_new} SELECT * FROM {archive};"
            )
            conn.conn.commit()

        # 6) drop old
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(f"DROP TABLE {archive};")
            conn.conn.commit()

        # 7) rename back
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"ALTER TABLE {full_new} RENAME TO {table};"
            )
            conn.conn.commit()

    def get_monthly_partition_ranges(self, start_date: date, months_ahead: int):
        current = start_date.replace(day=1)
        target = current + relativedelta(months=months_ahead)
        ranges = []
        while current <= target:
            suffix = f"{current.year:04d}_{current.month:02d}"
            ranges.append((suffix, current, current + relativedelta(months=1)))
            current += relativedelta(months=1)
        return ranges

    def get_monthly_partition_ranges_between(self, start_date: date, end_date: date):
        start = start_date.replace(day=1)
        end_boundary = end_date.replace(day=1)
        ranges = []
        current = start
        while current <= end_boundary:
            suffix = f"{current.year:04d}_{current.month:02d}"
            ranges.append((suffix, current, current + relativedelta(months=1)))
            current += relativedelta(months=1)
        return ranges

    def _create_partition(self, conn, schema: str, parent_table: str,
                          suffix: str, from_date: date, to_date: date):
        # always qualify partition name with schema and parent_table
        full_part = f"{schema}.{parent_table}_{suffix}"
        conn.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {full_part}
              PARTITION OF {schema}.{parent_table}
              FOR VALUES FROM ('{from_date}') TO ('{to_date}');
            """
        )

    def ensure_future_monthly_partitions(self, parent_schema: str,
                                         parent_table_class,
                                         months_ahead: int = 3):
        table = parent_table_class.table_name
        today = date.today()
        ranges = self.get_monthly_partition_ranges(today, months_ahead)
        with DatabaseConnection(**self.db_conn_params) as conn:
            for suffix, start, end in ranges:
                self._create_partition(conn, parent_schema, table,
                                       suffix, start, end)
            conn.conn.commit()

    def ensure_monthly_partitions_between(self, parent_schema: str,
                                          parent_table_class,
                                          start_date: date,
                                          end_date: date):
        table = parent_table_class.table_name
        ranges = self.get_monthly_partition_ranges_between(start_date, end_date)
        with DatabaseConnection(**self.db_conn_params) as conn:
            for suffix, start, end in ranges:
                self._create_partition(conn, parent_schema, table,
                                       suffix, start, end)
            conn.conn.commit()


# Example usage:
if __name__ == '__main__':
    # Prepare your db connection params dict from your config
    from src.infrastructure.core.config_reader import GetConfig
    from src.infrastructure.core.table_config import TableConfig
    config = GetConfig()

    start_date = datetime.datetime(2021, 1, 1)
    end_date = datetime.datetime(2024, 12, 25)

    db_conn_params = config.db_conn_params
    schema = 'public'
    table = TableConfig().TPDataTable
    partitioner = Partitioner(db_conn_params=db_conn_params)
    partitioner.ensure_monthly_partitions_between(parent_schema=schema,
                                                  parent_table_class=table,
                                                  start_date=start_date,
                                                  end_date=end_date)

