from datetime import date
from dateutil.relativedelta import relativedelta
from src.infrastructure.connections.connections import DatabaseConnection
from src.infrastructure.core.table_config import TableConfig
from src.table_creator import TableCreator, PARTITIONING_KEYS


try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging


class Partitioner:
    """
    Utility class for converting tables to monthly range partitions
    and ensuring future or arbitrary-range partitions exist.
    """
    def __init__(self, db_conn_params: dict, schema: str = 'public'):
        self.db_conn_params = db_conn_params
        self.schema = schema
        self.logger = logging.getLogger(__name__)

    def convert_table_to_monthly_partitions(self, parent_table_class):
        """
        Convert a non-partitioned table into a partitioned parent with
        monthly child tables spanning the full data range.
        """
        table_name = parent_table_class.table_name
        time_col = PARTITIONING_KEYS.get(table_name)
        full_old = f"{self.schema}.{table_name}"
        archive = f"{self.schema}.{table_name}_old"
        full_new = f"{self.schema}.{table_name}_new"

        # 1) rename existing
        self.logger.info(f"Renaming {full_old} to {table_name}_old")
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"ALTER TABLE {full_old} RENAME TO {table_name}_old;"
            )
            conn.conn.commit()

        # 2) create new partitioned parent
        self.logger.info(f"Creating new parent table for {table_name}")
        TableCreator(db_conn_params=self.db_conn_params).create_table(parent_table_class)
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"ALTER TABLE {self.schema}.{table_name} RENAME TO {table_name}_new;"
            )
            conn.conn.commit()

        # 3) find data range
        self.logger.info(f"Fetching date range from {archive}")
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"SELECT MIN({time_col})::date, MAX({time_col})::date FROM {archive};"
            )
            min_date, max_date = conn.cursor.fetchone()
            if min_date is None:
                self.logger.info("No data found; skipping partition creation.")
                return

        # 4) create monthly partitions
        start = min_date.replace(day=1)
        step = relativedelta(months=1)
        end_boundary = max_date.replace(day=1) + step
        current = start
        self.logger.info(
            f"Creating partitions from {start} to {end_boundary} by {step}"
        )
        with DatabaseConnection(**self.db_conn_params) as conn:
            while current < end_boundary:
                suffix = f"{current.year:04d}_{current.month:02d}"
                self._create_partition(
                    conn,
                    f"{table_name}_new",
                    suffix,
                    current,
                    current + step
                )
                current += step
            conn.conn.commit()

        # 5) bulk copy with explicit columns
        self.logger.info(f"Copying data from {archive} to {full_new}")
        with DatabaseConnection(**self.db_conn_params) as conn:
            # fetch ordered column list
            conn.cursor.execute(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = %s
                   AND table_name = %s
                 ORDER BY ordinal_position
                """, (self.schema, f"{table_name}_old")
            )
            cols = [row[0] for row in conn.cursor.fetchall()]
            # quote each identifier to preserve capitalization
            quoted_cols = [f'"{col}"' for col in cols]
            col_list = ", ".join(quoted_cols)
            insert_sql = (
                f"INSERT INTO {full_new} ({col_list}) SELECT {col_list} FROM {archive};"
            )
            conn.cursor.execute(insert_sql)
            conn.conn.commit()

        # 6) drop old
        self.logger.info(f"Dropping archive table {archive}")
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(f"DROP TABLE {archive};")
            conn.conn.commit()

        # 7) rename back
        self.logger.info(f"Renaming {full_new} back to {table_name}")
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(
                f"ALTER TABLE {full_new} RENAME TO {table_name};"
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

    def _create_partition(self, conn, parent_table: str,
                          suffix: str, from_date: date, to_date: date):
        full_part = f"{self.schema}.{parent_table}_{suffix}"
        conn.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {full_part}
              PARTITION OF {self.schema}.{parent_table}
              FOR VALUES FROM ('{from_date}') TO ('{to_date}');
            """
        )

    def ensure_future_monthly_partitions(self, parent_table_class,
                                         months_ahead: int = 3):
        table_name = parent_table_class.table_name
        today = date.today()
        ranges = self.get_monthly_partition_ranges(today, months_ahead)
        with DatabaseConnection(**self.db_conn_params) as conn:
            for suffix, start, end in ranges:
                self._create_partition(conn, table_name, suffix, start, end)
            conn.conn.commit()

    def ensure_monthly_partitions_between(self, parent_table_class,
                                          start_date: date,
                                          end_date: date):
        table_name = parent_table_class.table_name
        ranges = self.get_monthly_partition_ranges_between(start_date, end_date)
        with DatabaseConnection(**self.db_conn_params) as conn:
            for suffix, start, end in ranges:
                self._create_partition(conn, table_name, suffix, start, end)
            conn.conn.commit()


def convert_existing_to_partitioned_table(config, table_class, schema='public'):
    partitioner = Partitioner(db_conn_params=config.db_conn_params, schema=schema)
    partitioner.convert_table_to_monthly_partitions(parent_table_class=table_class)


def partition_by_time(config, table_class, start_date, end_date, schema='public'):
    partitioner = Partitioner(db_conn_params=config.db_conn_params, schema=schema)
    partitioner.ensure_monthly_partitions_between(
        parent_table_class=table_class,
        start_date=start_date,
        end_date=end_date
    )


def partition_ahead(config, table_class, months_ahead, schema='public'):
    partitioner = Partitioner(db_conn_params=config.db_conn_params, schema=schema)
    partitioner.ensure_future_monthly_partitions(
        parent_table_class=table_class,
        months_ahead=months_ahead
    )

# Example usage:
if __name__ == '__main__':
    from src.infrastructure.core.config_reader import GetConfig
    config = GetConfig()
    table_classes = [
        #TableConfig().CycleDataTable,
        TableConfig().ETCDataTable,
        TableConfig().ThermalConductivityXyDataTable,
        TableConfig().TPDataTable
    ]
    for tc in table_classes:
        convert_existing_to_partitioned_table(config, tc)
