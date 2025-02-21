#data_loader.py
import pandas as pd
import numpy as np
import logging
from src.config_connection_reading_management.database_reading_writing import DataRetriever
from src.table_data import TableConfig

# Define standard constraints as a constant
STANDARD_CONSTRAINTS = {
    "min_TotalCharTime": 0.33,
    "max_TotalCharTime": 1,
    "min_TotalTempIncr": 2,
    "max_TotalTempIncr": 5
}

def remove_order_term(input_string):
    """Remove the order clause from an SQL string."""
    keyword = "order"
    index = input_string.lower().find(keyword)
    return input_string[:index] if index != -1 else input_string

class DataLoader:
    """
    Loads experimental data either from a CSV file or a database query.
    """
    def __init__(self, file_path=None, sample_id=None, cycle_number=None, temperature=None, db_conn_params=None):
        self.logger = logging.getLogger(__name__)
        self.file_path = file_path
        self.db_conn_params = db_conn_params or {}
        self.data_retriever = DataRetriever(db_conn_params=self.db_conn_params)
        self.constraints = STANDARD_CONSTRAINTS
        self.table_config = TableConfig().ETCDataTable
        self.sample_id = sample_id
        self.temperature = temperature
        self.cycle_number = cycle_number

    def read_data(self):
        """Read CSV data if a file path is provided."""
        if self.file_path:
            return pd.read_csv(self.file_path, delimiter=',')
        return None

    def get_isotherm(self, sample_id=None, cycle_number=None, temperature=None):
        """
        Retrieve the isotherm data along with the mean temperature and de-hydrogenation state.
        """
        isotherm = self._read_isotherm(sample_id, cycle_number, temperature)
        mean_temperature, de_hyd_state = self._process_isotherm(isotherm)
        return isotherm, mean_temperature, de_hyd_state

    def _read_isotherm(self, sample_id=None, cycle_number=None, temperature=None):
        sample_id = sample_id or self.sample_id
        cycle_number = cycle_number or self.cycle_number
        temperature = temperature or self.temperature

        cols = (self.table_config.temperature_sample, self.table_config.pressure,
                self.table_config.th_conductivity, self.table_config.de_hyd_state)
        query, values = self.data_retriever.qb.create_reading_query(
            table_name=self.table_config.table_name,
            column_names=cols,
            constraints=self.constraints
        )
        query = remove_order_term(query)
        query += f" AND {self.table_config.sample_id_small} = %s "
        query += f" AND {self.table_config.cycle_number} = %s "
        query += f" AND {self.table_config.temperature} = %s "
        query += f" ORDER by {self.table_config.pressure}"
        values += (sample_id, cycle_number, temperature)
        return self.data_retriever.execute_fetching(query=query, column_names=cols, values=values)

    def _process_isotherm(self, df_isotherm):
        if df_isotherm.empty:
            return None, None
        mean_temp = np.mean(df_isotherm[self.table_config.temperature_sample])
        # Check if de-hyd state is consistent across the dataset
        if df_isotherm[self.table_config.de_hyd_state].iloc[0] == df_isotherm[self.table_config.de_hyd_state].iloc[-1]:
            de_hyd_state = df_isotherm[self.table_config.de_hyd_state].iloc[0]
        else:
            self.logger.info("No consistent de_hyd_state")
            de_hyd_state = None
        return mean_temp, de_hyd_state

    @staticmethod
    def write_example_data():
        """Write example data to a text file."""
        example_data = (
            "Druck\tThAverage\tSampleTemp\tTh_Conductivity\n"
            "0.1\t0.02\t30\t0.018\n"
            "0.2\t0.025\t30\t0.019\n"
            "0.3\t0.028\t30\t0.0185\n"
            "0.4\t0.032\t30\t0.0195\n"
            "0.5\t0.03\t30\t0.020\n"
            "1.0\t0.035\t30\t0.022\n"
            "2.0\t0.04\t30\t0.024\n"
            "3.0\t0.038\t30\t0.023\n"
            "5.0\t0.045\t30\t0.026\n"
            "7.0\t0.048\t30\t0.027\n"
            "10.0\t0.05\t30\t0.028\n"
            "20.0\t0.055\t30\t0.030\n"
        )
        with open("example_data.txt", "w") as file:
            file.write(example_data)
