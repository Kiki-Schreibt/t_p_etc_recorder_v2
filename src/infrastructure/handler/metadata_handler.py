#metadata_handler.py
import time
from datetime import timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from src.infrastructure.connections.connections import DatabaseConnection
from src.config_connection_reading_management.query_builder import QueryBuilder
from src.infrastructure.core.table_config import TableConfig
from src.infrastructure.core import global_vars

try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging
meta_table = TableConfig().MetaDataTable

#todo: implement used sensor type radius and tcr values
# (maybe exists already in ETC data)
#       self.sensor_radius = None
#       self.sensor_insulation = None
#       self.sensor_coil = None
LOCAL_TZ = global_vars.local_tz_qt


class MetaData:

    column_attribute_mapping = {
        meta_table.sample_id: 'sample_id',
        meta_table.material: 'sample_material',
        meta_table.mass: 'sample_mass',
        meta_table.measurement_cell: 'measurement_cell',
        meta_table.time_start: 'start_time',
        meta_table.time_end: 'end_time',
        meta_table.time_first_hydrogenation: 'first_hydrogenation',
        meta_table.max_pressure_cycling: 'max_pressure_cycling',
        meta_table.min_temperature_cycling: 'min_temperature_cycling',
        meta_table.enthalpy: 'enthalpy',
        meta_table.entropy: 'entropy',
        meta_table.average_cycle_duration: 'average_cycle_duration',
        meta_table.reservoir_volume: 'reservoir_volume',
        meta_table.total_number_cycles: 'total_number_cycles',
        meta_table.last_de_hyd_state: 'last_de_hyd_state'
    }

    def __init__(self, db_conn_params, sample_id=None):
        self.db_conn_params = db_conn_params or {}

        self.qb = QueryBuilder()
        self.logger = logging.getLogger(__name__)
        self.table_name = TableConfig().MetaDataTable.table_name
        self.sample_id = sample_id
        self.start_time = None
        self.volume_measurement_cell = None
        self.end_time = None
        self.sample_mass = None
        self.reservoir_volume = None
        self.measurement_cell = None
        self.sample_material = None
        self.measurement_time = None
        self.first_hydrogenation = None
        self.max_pressure_cycling = None
        self.min_temperature_cycling = None
        self.enthalpy = None
        self.entropy = None
        self.average_cycle_duration = None
        self.theoretical_uptake = None
        self.total_number_cycles = None
        self.last_de_hyd_state = None
        self.sensor_radius = None
        self.sensor_insulation = None
        self.sensor_coil = None
        self.retry_counter = 1
        self.read()

    def reading_thread(self, column_names=None, mode="read"):
        if self.sample_id:
            if not column_names:
                column_names = TableConfig().get_table_column_names(table_name=self.table_name)

            if self._sample_id_exists():
                query, values = self.qb.create_reading_query(table_name=self.table_name,
                                                             sample_id=self.sample_id,
                                                             column_names=column_names)

                with DatabaseConnection(**self.db_conn_params) as conn:
                   # print(f"meta_data query = {query} meta_data values = {values}")
                    conn.cursor.execute(query, values)
                    records = conn.cursor.fetchall()
                if isinstance(column_names, tuple) or isinstance(column_names, list):
                    column_names_for_df = tuple(s.replace("\"", '') for s in column_names)
                else:
                    column_names_for_df = [column_names]
                df = pd.DataFrame.from_records(records, columns=column_names_for_df)
                self._assign_column_names_and_values(column_names=column_names, df=df)
            elif mode == 'create':  # If no records are returned, sample_id does not exist
                self.logger.info(f"Test {self.sample_id} does not exist yet. Will be created")
                self._create_new_line_meta_data()
                self.read()
            elif mode == 'read':  # If no records are returned, sample_id does not exist
                self.logger.info(f"Test {self.sample_id} does not exist yet. You may want to create a new test")
        else:
            self.sample_id = None

    def read(self, column_names=None):
        #time_start_reading = time.time()
        if self.sample_id:
            self.reading_thread(column_names)
        #thread = threading.Thread(target=self.reading_thread, args=(column_names,))
        #thread.start()
        #thread.join()
        #time_passed = time.time()-time_start_reading
        #if time_passed > 1:
        #    print(f"Reading meta data took longer than 1 s: {time_passed} s")

    def write(self, quiet=False):
        if not self._sample_id_exists():
            if not self.sample_id:
                return
            self._create_new_line_meta_data()

        # Build a mapping from column names to attribute values
        column_value_mapping = {}
        for column_name, attribute_name in self.column_attribute_mapping.items():
            value = getattr(self, attribute_name, None)
            column_value_mapping[column_name] = value

        # Remove 'sample_id' from the mapping if it's the primary key and shouldn't be updated
        if 'sample_id' in column_value_mapping:
            del column_value_mapping['sample_id']

        # Optionally, filter out None values if you don't want to update columns to None
        column_value_mapping = {col: val for col, val in column_value_mapping.items() if val is not None}

        if self._sample_id_exists():
            with DatabaseConnection(**self.db_conn_params) as conn:
                # Construct the SET clause
                set_clauses = ', '.join([f'"{col}" = %s' for col in column_value_mapping.keys()])
                values = list(column_value_mapping.values())
                values.append(self.sample_id)  # For the WHERE clause

                # Build the UPDATE query
                update_query = f'UPDATE "{self.table_name}" SET {set_clauses} WHERE "sample_id" = %s'
                conn.cursor.execute(update_query, values)
                conn.cursor.connection.commit()
                if not quiet:
                    self.logger.info(f"Meta data updated for sample_id: {self.sample_id}")

    def _assign_column_names_and_values(self, column_names, df):
        updated_using_tp_data = False
        for col_name in column_names:
            if "sample_id" in col_name.lower():
                self.sample_id = df[col_name].iloc[0]

            if "start" in col_name.lower() and "time" in col_name.lower():
                self.start_time = df[col_name].iloc[0]

            if "end" in col_name.lower() and "time" in col_name.lower():
                self.end_time = df[col_name].iloc[0]

            if "mass" in col_name.lower():
                self.sample_mass = float(df[col_name].iloc[0]) if df[col_name].iloc[0] else None

            if "cell" in col_name.lower() and "volume" not in col_name.lower():
                self.measurement_cell = df[col_name].iloc[0]
                if self.measurement_cell:
                    if "2" in self.measurement_cell:
                        self.volume_measurement_cell = 30.24  #[ml]
                    if "3" in self.measurement_cell:
                        self.volume_measurement_cell = 44.37  #[ml]

            if "material" in col_name.lower():
                self.sample_material = df[col_name].iloc[0]

            if "hydrogenation" in col_name.lower():
                self.first_hydrogenation = df[col_name].iloc[0]
                #self.first_hydrogenation = self._make_tz_aware(value=self.first_hydrogenation)

            if "max" in col_name.lower() and "pressure" in col_name.lower():
                self.max_pressure_cycling = float(df[col_name].iloc[0]) if df[col_name].iloc[0] else None

            if "min" in col_name.lower() and "temperature" in col_name.lower():
                self.min_temperature_cycling = float(df[col_name].iloc[0]) if df[col_name].iloc[0] else None

            if "duration" in col_name.lower():
                if df[col_name].iloc[0]:
                    value = df[col_name].iloc[0]
                    if isinstance(df[col_name].iloc[0], pd.Timestamp):
                        df[col_name].iloc[0] = df[col_name].iloc[0].astype(str)
                        # Assuming the string format is hours:minutes:seconds
                        # Split the string by ":"
                    if isinstance(value, pd.Timedelta):
                        # It's a Timedelta, extract components directly
                        hours = value.components.hours
                        minutes = value.components.minutes
                        seconds = value.components.seconds
                    elif isinstance(value, str):
                        # It's a string, proceed with split
                        hours, minutes, seconds = map(int, value.split(":"))

                    # Calculate the duration in hours
                    duration_in_hours = hours + (minutes / 60) + (seconds / 3600)
                    avg_dur = timedelta(hours=duration_in_hours)
                    self.average_cycle_duration = avg_dur

            if "reservoir" in col_name.lower():
                self.reservoir_volume = float(df[col_name].iloc[0]) if df[col_name].iloc[0] else None #[l]

            if "number_cycles" in col_name.lower():
                v = df[col_name].iloc[0]
                self.total_number_cycles = None if pd.isna(v) else float(v)  #
                if pd.isna(self.total_number_cycles) and self.retry_counter > 0:
                    updated_using_tp_data = True
                    self.last_de_hyd_state, self.total_number_cycles = self.fetch_last_state_and_cycle()
                    self.retry_counter -= 1

            if "de_hyd_state" in col_name.lower() and not updated_using_tp_data:
                self.last_de_hyd_state = df[col_name].iloc[0]  #

            if self.sample_material:
                (self.enthalpy, self.entropy, self.theoretical_uptake) = self._get_enthalpy_entropy_wt_theoretical()

        if updated_using_tp_data:
            self.write(quiet=True)

    def _create_new_line_meta_data(self):
        # Create a new row with the given sample_id
        table_name = TableConfig().MetaDataTable.table_name
        column_name = TableConfig().MetaDataTable.sample_id
        value = (self.sample_id,)
        query = f"INSERT INTO {table_name} ({column_name}) VALUES (%s)"
        with DatabaseConnection(**self.db_conn_params) as conn:
            conn.cursor.execute(query, value)
            conn.cursor.connection.commit()

        self.logger.info(f"Sample ID does not exist. New entry was created for: {self.sample_id} ")

    def _sample_id_exists(self):
        # Check if the sample_id exists in the table
        table_name = self.table_name
        column_name = TableConfig().MetaDataTable.sample_id
        query = f"SELECT 1 FROM {table_name} WHERE {column_name} = '{self.sample_id}'"
        with DatabaseConnection(**self.db_conn_params) as conn:

            conn.cursor.execute(query)
            record = conn.cursor.fetchone() is not None

        return record

    def print(self):
        """
        Print the current attribute values of the MetaDataBusiness instance.
        """
        print("Sample ID:", self.sample_id)
        print("Start Time:", self.start_time)
        print("End Time:", self.end_time)
        print("Sample Mass:", self.sample_mass)
        print("Reservoir Volume:", self.reservoir_volume, 'l') if self.reservoir_volume else print("No Reservoir")
        print("Measurement Cell:", self.measurement_cell)
        print("Sample Material:", self.sample_material)
        print("Measurement Time:", self.measurement_time)
        print("Volume Measurement Cell:", self.volume_measurement_cell*1e6, "ml") if self.volume_measurement_cell else print("No cell volume")
        print("Enthalpy:", self.enthalpy*1e-3, " kJ/mol") if self.enthalpy else print("No Enthalpy")
        print("Entropy:", self.entropy*1e-3, " kJ/mol/K")if self.entropy else print("No Entropy")
        print("Theoretical uptake:", self.theoretical_uptake, ' wt-%') if self.theoretical_uptake else print("No uptake")
        print("Average Cycle Duration:", self.average_cycle_duration) if self.average_cycle_duration else print("No average cycle duration")
        print("Total number cycle:", self.total_number_cycles) if self.total_number_cycles else print("No cycles yet")
        print("Last de_hyd state:", self.last_de_hyd_state) if self.last_de_hyd_state else print("No defined de_hyd state")

    def _get_enthalpy_entropy_wt_theoretical(self):
        from src.infrastructure.handler.hydride_handler import MetalHydrideDatabase
        if self.enthalpy is not None and self.entropy is not None and self.theoretical_uptake is not None:
            return self.enthalpy, self.entropy, self.theoretical_uptake

        mh_database = MetalHydrideDatabase()
        (enthalpy, entropy) = mh_database.get_enthalpy_entropy(self.sample_material)
        wt_theoretical = mh_database.get_capacity(self.sample_material)

        if enthalpy and entropy and wt_theoretical:
            self.enthalpy = enthalpy
            self.entropy = entropy
            self.theoretical_uptake = wt_theoretical
            #self.write()
            return enthalpy, entropy, wt_theoretical

        elif wt_theoretical:
            return None, None, wt_theoretical
        else:
            return None, None, None

    def remove_sample_id(self):
        if self._sample_id_exists():
            query = f"DELETE from {meta_table.table_name} WHERE {meta_table.sample_id} = %s"
            values = (self.sample_id,)
            with DatabaseConnection() as db_conn:
                db_conn.cursor.execute(query, values)
                self.logger.info(f"{self.sample_id} removed from database")

    def _make_tz_aware(self, value):

        if not pd.isna(value):  # Make sure it's not NaN
            if not isinstance(value, pd.Timestamp):
                # Convert to Timestamp if not already
                self.first_hydrogenation = pd.Timestamp(value)

            if self.first_hydrogenation.tzinfo is None:
                # If no timezone info, localize to a specific timezone, e.g., UTC
                self.first_hydrogenation = value.tz_localize(LOCAL_TZ)
            else:
                # If already timezone-aware, you may want to convert to another timezone
                self.first_hydrogenation = value.tz_convert(LOCAL_TZ)
        return value

    def fetch_last_state_and_cycle(self):
        from src.infrastructure.core.table_config import TableConfig
        table = TableConfig().TPDataTable
        table_name = table.table_name
        sample_id_column = table.sample_id
        sample_id = self.sample_id
        # query = f"SELECT de_hyd_state, cycle_number FROM {table_name} WHERE {sample_id_column} = %s ORDER BY cycle_number DESC LIMIT 1"

        query = f"""
                    SELECT de_hyd_state, cycle_number
                    FROM {table_name}
                    WHERE {sample_id_column} = %s
                      AND cycle_number IS NOT NULL
                    ORDER BY
                      CASE WHEN de_hyd_state IS NOT NULL THEN 0 ELSE 1 END,
                      cycle_number DESC
                    LIMIT 1
                """
        try:
            with DatabaseConnection(**self.db_conn_params) as conn:
                conn.cursor.execute(query, (sample_id,))
                record = conn.cursor.fetchone()

         # print(f"Executing reading query took: {time.time()-time_start_query_exec}s")
            if record:
                de_hyd_state, cycle_number = record
                if de_hyd_state is not None:
                    # Both values are not None
                    return de_hyd_state, cycle_number
                else:
                    # de_hyd_state is None, cycle_number is not None
                    return None, cycle_number
            else:
                return None, 0  # Default values if no records found
        except Exception as e:
            self.logger.error(f"Error occurred while fetching last state and cycle: {e}")
            return None, 0


def test_meta_data_handler(sample_id):
    from src.infrastructure.core.config_reader import GetConfig
    config = GetConfig()
    meta_data_instance = MetaData(sample_id=sample_id,
                                  db_conn_params=config.db_conn_params)
    #meta_data_instance.print()
    #meta_data_instance._create_new_line_meta_data()


if __name__ == "__main__":
    #test_meta_data_handler()
    from datetime import datetime
    start = datetime.now()
    test_meta_data_handler(sample_id="WAE-WA-040")
    first_finished = (datetime.now()-start).total_seconds()
    print(f"WAE-WA-040 took: {first_finished} s")
    test_meta_data_handler(sample_id="WAE-WA-060")
    second_finished = (datetime.now()-start).total_seconds()
    print(f"WAE-WA-060 took: {second_finished} s")

