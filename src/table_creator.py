from src.table_data import TableConfig
from src.config_connection_reading_management.connections_and_logger import DatabaseConnection, AppLogger


class TableCreator:
    def __init__(self):
        self.db_connection = DatabaseConnection()
        self.logger = AppLogger().get_logger(__name__)

    def create_all_tables(self):
        for attr_name, attr_value in vars(TableConfig).items():
            # Exclude any special or private attributes (those starting with '__')
            if not attr_name.startswith('__') and isinstance(attr_value, type):
                self.create_table(attr_value)
        self.create_index()

    def create_table(self, table_class, table_name=None):
        # Check if the table already exists
        if not table_name:
            table_name = table_class.table_name

        check_table_exists_query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '{table_name}'
        );
        """

        with DatabaseConnection() as db_conn:
            db_conn.cursor.execute(check_table_exists_query)
            table_exists = db_conn.cursor.fetchone()[0]

        if not table_exists:
            columns_data_types = self._extract_columns_and_assign_data_types(table_class)

            # Start building the SQL statement
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            columns_sql = [f"    {col_name} {col_type}" for col_name, col_type in columns_data_types.items()]
            create_table_sql += ",\n".join(columns_sql)
            create_table_sql += "\n);"
            self._execute(create_table_sql)
            self.logger.info(f"New Table Created: {table_name}")
        else:
            self.logger.info(f"{table_name} table exists in database. All good")

    def create_index(self):
        tp_table = TableConfig().TPDataTable
        queries = []
        queries.append(f'CREATE INDEX IF NOT EXISTS idx_time ON {tp_table.table_name}({tp_table.time})')
        queries.append(f'CREATE INDEX IF NOT EXISTS idx_sample_id ON {tp_table.table_name}({tp_table.sample_id})')

        with DatabaseConnection() as db_conn:
            for query in queries:
                try:
                    db_conn.cursor.execute(query)
                    self.logger.info(f"Executed query: {query}")
                except Exception as e:
                    self.logger.error(f"Error executing query {query}: {e}")

    @staticmethod
    def _extract_columns_and_assign_data_types(table_class):
        def _get_mapping(table_class):
            table_name = table_class.table_name.lower()

            if "t_p" in table_name:
                data_map = {'cycle_length_flag': 'boolean',
                            'cycle_number': 'real',
                            'cycle_number_flag': 'boolean',
                            'de_hyd_state': 'text',
                            'eq_pressure': 'real',
                            'h2_uptake': 'real',
                            'h2_uptake_flag': 'boolean',
                            'pressure': 'real',
                            'reservoir_volume': 'real',
                            'sample_id': 'text',
                            'setpoint_heater': 'real',
                            'setpoint_sample': 'real',
                            'temperature_heater': 'real',
                            'temperature_sample': 'real',
                            'time': 'timestamp with time zone',
                            '_default': 'TEXT',
                            'eq_pressure_real': 'real'# Default data type
                            }

            if "conductivity" in table_name:
                data_map = {'\"Calc_settings\"': 'character varying',
                               '\"Calc_settings_avg\"': 'character varying',
                               '\"Calc_settings_dvt\"': 'character varying',
                               '\"Description\"': 'character varying',
                               '\"Disk_Res\"': 'double precision',
                               '\"Disk_Res_avg\"': 'double precision',
                               '\"Disk_Res_dvt\"': 'double precision',
                               '\"Disk_Type\"': 'character varying',
                               '\"File\"': 'character varying',
                               '\"Mean_Dev\"': 'double precision',
                               '\"Mean_Dev_avg\"': 'double precision',
                               '\"Mean_Dev_dvt\"': 'double precision',
                               '\"Meastime\"': 'double precision',
                               '\"Notes\"': 'text',
                               '\"Outppower\"': 'double precision',
                               '\"Points\"': 'character varying',
                               '\"PrDepth\"': 'double precision',
                               '\"PrDepth_avg\"': 'double precision',
                               '\"PrDepth_dvt\"': 'double precision',
                               '\"Radius\"': 'double precision',
                               '\"Rs\"': 'double precision',
                               '\"Sample_ID\"': 'character varying',
                               '\"SpecHeat\"': 'double precision',
                               '\"SpecHeat_avg\"': 'double precision',
                               '\"SpecHeat_dvt\"': 'double precision',
                               '\"TCR\"': 'double precision',
                               '\"TempDrift\"': 'double precision',
                               '\"TempDrift_avg\"': 'double precision',
                               '\"TempDrift_dvt\"': 'double precision',
                               '\"TempIncr\"': 'double precision',
                               '\"TempIncr_avg\"': 'double precision',
                               '\"TempIncr_dvt\"': 'double precision',
                               '\"Tempdrift_rec\"': 'double precision',
                               '\"Temperature\"': 'double precision',
                               '\"Temperature_avg\"': 'double precision',
                               '\"Temperature_dvt\"': 'double precision',
                               '\"ThConductivity\"': 'double precision',
                               '\"ThConductivity_avg\"': 'double precision',
                               '\"ThConductivity_dvt\"': 'double precision',
                               '\"ThDiffusivity\"': 'double precision',
                               '\"ThDiffusivity_avg\"': 'double precision',
                               '\"ThDiffusivity_dvt\"': 'double precision',
                               '\"ThEffusivity\"': 'double precision',
                               '\"ThEffusivity_avg\"': 'double precision',
                               '\"ThEffusivity_dvt\"': 'double precision',
                               '\"Time\"': 'timestamp with time zone',
                               '\"Time_Corr\"': 'double precision',
                               '\"Time_Corr_avg\"': 'double precision',
                               '\"Time_Corr_dvt\"': 'double precision',
                               '\"TotalCharTime\"': 'double precision',
                               '\"TotalCharTime_avg\"': 'double precision',
                               '\"TotalCharTime_dvt\"': 'double precision',
                               '\"TotalTempIncr\"': 'double precision',
                               '\"TotalTempIncr_avg\"': 'double precision',
                               '\"TotalTempIncr_dvt\"': 'double precision',
                               'cycle_number': 'real',
                               'cycle_number_flag': 'boolean',
                               'de_hyd_state': 'text',
                               'pressure': 'real',
                               'sample_id': 'text',
                               'temperature_sample': 'real',
                               '_default': 'TEXT'  # Default data type
                             }

            if "xy" in table_name:
                data_map = {'diff_temperature': 'double precision',
                                  'point_nr': 'integer',
                                  'sample_id': 'text',
                                  'sqrt_time': 'double precision',
                                  't_f_tau': 'double precision',
                                  'temperature': 'double precision',
                                  'temperature_drift': 'double precision',
                                  'temperature_increase': 'double precision',
                                  'time': 'timestamp with time zone',
                                  'time_drift': 'double precision',
                                  'time_temperature_increase': 'double precision',
                                  '_default': 'TEXT'  # Default data type
                            }

            if "meta" in table_name:
                data_map = {'average_cycle_duration': 'interval',
               'enthalpy': 'real',
               'entropy': 'real',
               'first_hydrogenation': 'timestamp with time zone',
               'mass': 'real',
               'material': 'text',
               'max_pressure_cycling': 'real',
               'measurement_cell': 'text',
               'min_temperature_cycling': 'real',
               'reservoir_volume': 'real',
               'sample_id': 'text',
               'time_end': 'timestamp with time zone',
               'time_start': 'timestamp with time zone',
               '_default': 'TEXT'  # Default data type
                            }

            if "cycle" in table_name:
                data_map = {'cycle_duration': 'interval',
                'cycle_number': 'real',
                'de_hyd_state': 'text',
                'h2_uptake': 'real',
                'pressure_max': 'real',
                'pressure_min': 'real',
                'sample_id': 'text',
                'temperature_max': 'real',
                'temperature_min': 'real',
                'time_end': 'timestamp with time zone',
                'time_max': 'timestamp with time zone',
                'time_min': 'timestamp with time zone',
                'time_start': 'timestamp with time zone',
                'volume_cell': 'real',
                'volume_reservoior': 'real',
                '_default': 'TEXT'  # Default data type
                            }

            return data_map

        # Initial mappings of column name fragments to data types
        # This is a simplified example; adjust the logic as needed
        data_type_mapping = _get_mapping(table_class)
        # Extract class attributes and create column-to-data-type mappings
        columns_data_types = {}
        for attr, value in vars(table_class).items():
            #print(f"{attr}, {value}")
            if not attr.startswith("__") and attr != 'table_name' and attr != 'get_clean':
                # Determine the data type based on the attribute name
                for key_fragment, data_type in data_type_mapping.items():
                    if key_fragment == value:
                        columns_data_types[value] = data_type
                        break
                else:  # If no specific data type was found, assign the default
                    columns_data_types[value] = data_type_mapping['_default']

        return columns_data_types

    def _execute(self, query):
        self.db_connection.open_connection()
        # Execute the SQL statement to create the table
        self.db_connection.cursor.execute(query)
        # Commit the changes and close the connection
        self.db_connection.conn.commit()
        self.db_connection.close_connection()

    def __delete_table__(self, table_name):
        deletion_query = f"DROP TABLE IF EXISTS {table_name}"
        self._execute(deletion_query)
        self.logger.info(f"Table deleted: {table_name}")


def test_create_table_from_class():
    # Example: Generate SQL for TPDataTable class
    test_table = TableConfig.CycleDataTable
    table_name = "asdf"
    TableCreator().create_table(test_table, test_table.table_name)
    #TableCreator().__delete_table__('asdf')

    # Example Usage


if __name__ == "__main__":
    test_create_table_from_class()
        # Accessing class attributes with and without escaped quotes

