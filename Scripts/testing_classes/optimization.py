import time
from zoneinfo import ZoneInfo

from src.config_connection_reading_management.connections import DatabaseConnection
from src.config_connection_reading_management.logger import AppLogger

from src.config_connection_reading_management.query_builder import QueryBuilder
from src.table_data import TableConfig

qb = QueryBuilder()

local_tz = ZoneInfo("Europe/Berlin")
table_rp = TableConfig().TPDataTable


class Problem:

    def __init__(self):
        self.running = False
        self.qb = qb
        self.logger = AppLogger().get_logger(__name__)
        self.database_connection = DatabaseConnection()
       # self.database_connection.open_connection()  # Open the connection
        self.limit_datapoints = 10000


    def fetch_last_state_and_cycle(self, table=TableConfig().TPDataTable, sample_id=None):

        table_name = table.table_name
        sample_id_column = table.sample_id
        #todo: implement total cycles to meta data table and read last cycle from there. Or index columns to speed up
        query = f"SELECT de_hyd_state, cycle_number FROM {table_name} WHERE {sample_id_column} = %s ORDER BY cycle_number DESC LIMIT 1"
        #query = f"""SELECT de_hyd_state, cycle_number
        #            FROM {table_name}
        #            WHERE {sample_id_column} = %s
        #              AND cycle_number = (
        #                  SELECT MAX(cycle_number)
        #                  FROM {table_name}
        #                  WHERE {sample_id_column} = %s
        #             )"""
        try:
            time_start_query_exec = time.time()
            self.database_connection.open_connection()
            self.database_connection.cursor.execute(query, (sample_id,))
            record = self.database_connection.cursor.fetchone()
            self.database_connection.close_connection()
            print(f"Executing reading query took: {time.time()-time_start_query_exec}s")
            if record:
                return record[0], record[1]  # Return de_hyd_state and cycle_number
            else:
                return None, 0  # Default values if no records found
        except Exception as e:
            self.logger.error(f"Error occurred while fetching last state and cycle: {e}")
            return None, 0



def main():

    sample_id = "WAE-WA-030"
    test_fetcher = Problem()
    a, b = test_fetcher.fetch_last_state_and_cycle(sample_id=sample_id)
    print(a, b)

if __name__ == "__main__":
    time_start = time.time()
    main()
    print(f"{time.time()-time_start}s" )




