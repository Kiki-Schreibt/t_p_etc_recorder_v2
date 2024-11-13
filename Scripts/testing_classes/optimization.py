import threading
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import win32com.client as win32
import re
import os
import struct
import time
from zoneinfo import ZoneInfo

from src.config_connection_reading_management.connections_and_logger import AppLogger, DatabaseConnection, GetConfig, ModbusConnection
from pymodbus.exceptions import ModbusException, ConnectionException
from pymodbus.pdu import ExceptionResponse
from src.calculations.eq_p_calculation import VantHoffCalcEq as EqCalculator
from src.config_connection_reading_management.query_builder import QueryBuilder
from src.meta_data.meta_data_handler import MetaData
from src.table_data import TableConfig

qb = QueryBuilder()
# Load the configuration
config = GetConfig()
# Accessing the variables
LOG_DIRECTORY   = config.LOG_DIRECTORY
LOG_FILE        = config.LOG_FILE
DB_SERVER       = config.DB_SERVER
DB_DATABASE     = config.DB_DATABASE
DB_USERNAME     = config.DB_USERNAME
DB_PASSWORD     = config.DB_PASSWORD
DB_PORT         = config.DB_PORT
MODBUS_HOST     = config.MODBUS_HOST
MODBUS_PORT     = config.MODBUS_PORT
REGS_OF_INTEREST = config.REGS_OF_INTEREST
START_REG       = config.START_REG
END_REG         = config.END_REG
SLEEP_INTERVAL  = config.SLEEP_INTERVAL

local_tz = ZoneInfo("Europe/Berlin")
table_rp = TableConfig().TPDataTable



class Problem:

    def __init__(self):
        self.running = False
        self.qb = qb
        self.config = config
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




