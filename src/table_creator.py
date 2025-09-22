from psycopg2 import sql, errors
from src.infrastructure.core.table_config import TableConfig
from src.infrastructure.connections.connections import DatabaseConnection
try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging
t_p_table = TableConfig().TPDataTable
cycle_table = TableConfig().CycleDataTable
etc_table = TableConfig().ETCDataTable
etc_xy_table = TableConfig().ThermalConductivityXyDataTable
meta_table = TableConfig().MetaDataTable
kinetics_table = TableConfig().KineticsTable

PRIMARY_KEYS = {
                        t_p_table.table_name:    [t_p_table.time, t_p_table.sample_id, t_p_table.cycle_number],
                        cycle_table.table_name:  [cycle_table.time_start, cycle_table.sample_id],
                        meta_table.table_name:   meta_table.sample_id,
                        etc_table.table_name:    [etc_table.time, etc_table.sample_id_small, etc_table.cycle_number],
                        etc_xy_table.table_name: [etc_xy_table.time, etc_xy_table.sample_id],
                        kinetics_table.table_name: [kinetics_table.sample_id, kinetics_table.cycle_number]
                    }

PARTITIONING_KEYS = {
                    t_p_table.table_name:    t_p_table.sample_id,
                    cycle_table.table_name:  cycle_table.sample_id,
                    etc_table.table_name:    etc_table.sample_id_small,
                    etc_xy_table.table_name: etc_xy_table.sample_id,
                    kinetics_table.table_name: kinetics_table.sample_id
                    }


class TableCreator:



    def __init__(self, db_conn_params):
        self.db_conn_params = db_conn_params
        self.logger = logging.getLogger(__name__)

    def create_all_tables(self):
        for attr_name, attr_value in vars(TableConfig).items():
            # Exclude any special or private attributes (those starting with '__')
            if not attr_name.startswith('__') and isinstance(attr_value, type):
                self.create_table(attr_value)

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

        with DatabaseConnection(**self.db_conn_params) as db_conn:
            db_conn.cursor.execute(check_table_exists_query)
            table_exists = db_conn.cursor.fetchone()[0]

        if not table_exists:
            columns_data_types = self._extract_columns_and_assign_data_types(table_class)

           # Build the column definitions list
            columns_sql = [f"    {col} {dtype}" for col, dtype in columns_data_types.items()]

            # If the table_class has a primary_key attribute, use it
            pk = PRIMARY_KEYS.get(table_name)
            partitioning_key = PARTITIONING_KEYS.get(table_name) or None
            if pk:
                if isinstance(pk, (list, tuple)):
                    pk_cols = ", ".join(pk)
                else:
                    pk_cols = pk
                columns_sql.append(f"    PRIMARY KEY ({pk_cols})")

            if 'time' in partitioning_key:
                query_part_partition = f"PARTITION BY RANGE ({partitioning_key})"
            elif 'sample' in partitioning_key:
                query_part_partition = f"PARTITION BY LIST ({partitioning_key})"
            else:
                query_part_partition = ""
            create_table_sql = (
            f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            + ",\n".join(columns_sql)
            + f"\n) {query_part_partition} ;"
            )

            self._execute(create_table_sql)
            self.logger.info(f"New Table Created: {table_name} with primary key {pk_cols}")
            self.create_index(table_name, pk_cols)

        else:
            self.logger.info(f"{table_name} table exists in database. All good")

    def create_index(self, table_name, pk_cols):
        #tp_table = TableConfig().TPDataTable
        #queries = []
       # queries.append(f'CREATE INDEX IF NOT EXISTS idx_time ON {tp_table.table_name}({tp_table.time})')
       # queries.append(f'CREATE INDEX IF NOT EXISTS idx_sample_id ON {tp_table.table_name}({tp_table.sample_id})')
        query = f"""CREATE INDEX idx_{table_name}_time_sample_id
                    ON {table_name} ({pk_cols})"""
        try:
            with DatabaseConnection(**self.db_conn_params) as db_conn:
                db_conn.conn.autocommit = True
                db_conn.cursor.execute(query)
                db_conn.conn.autocommit = False

        except Exception as e:
            self.logger.info(f"Couldn't create index: {e}")
       # with DatabaseConnection(**self.db_conn_params) as db_conn:
       #     for query in queries:
        #        try:
         #           db_conn.cursor.execute(query)
          #          self.logger.info(f"Executed query: {query}")
           #     except Exception as e:
            #        self.logger.error(f"Error executing query {query}: {e}")

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
                            'eq_pressure_real': 'real',
                            'is_isotherm_flag': 'boolean',
                            'test_info': 'text'# Default data type
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
                               '_default': 'TEXT',
                               'is_isotherm_flag': 'boolean',
                               'test_info': 'text'# Default data type
                             }

            if "xy" in table_name:
                data_map = {
                                'sample_id':        'TEXT',
                                'time':             'TIMESTAMPTZ',
                                'transient_x':      'DOUBLE PRECISION[]',
                                'transient_y':      'DOUBLE PRECISION[]',
                                'drift_x':          'DOUBLE PRECISION[]',
                                'drift_y':          'DOUBLE PRECISION[]',
                                'calculated_x':     'DOUBLE PRECISION[]',
                                'calculated_y':     'DOUBLE PRECISION[]',
                                'residual_x':       'DOUBLE PRECISION[]',
                                'residual_y':       'DOUBLE PRECISION[]',
                                '_default':         'TEXT[]'
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

            if "kinetics" in table_name:
                data_map = {
                                'sample_id':           'TEXT',
                                'time':                'TIMESTAMPTZ[]',
                                'cycle_number':        'real',
                                'pressure':            'DOUBLE PRECISION[]',
                                'temperature_res':     'DOUBLE PRECISION[]',
                                'temperature_sample':  'DOUBLE PRECISION[]',
                                'uptake_wt_percent':   'DOUBLE PRECISION[]',
                                'uptake_kg':           'DOUBLE PRECISION[]',
                                'rate_wt_p_min':       'DOUBLE PRECISION[]',
                                'rate_kg_min':         'DOUBLE PRECISION[]',
                                'time_delta_min':      'DOUBLE PRECISION[]',
                                'm_gas_kg':            'DOUBLE PRECISION[]',
                                'v_cell':              'real',
                                'v_res':               'real',
                                'max_rate_wt_p_min':   'real',
                                'max_rate_kg_min':     'real',
                                '_default':            'TEXT[]'
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
        with DatabaseConnection(**self.db_conn_params) as db_conn:
            # Execute the SQL statement to create the table
            #print(query)
            db_conn.cursor.execute(query)
            # Commit the changes and close the connection
            db_conn.cursor.connection.commit()

    def __delete_table__(self, table_name):
        deletion_query = f"DROP TABLE IF EXISTS {table_name}"
        self._execute(deletion_query)
        self.logger.info(f"Table deleted: {table_name}")


def create_database(config):

    query = f"CREATE DATABASE {config.db_conn_params["DB_DATABASE"]} WITH OWNER = {config.db_conn_params["DB_USERNAME"]}"
    #config.db_conn_params['DB_DATABASE'] = 'test'
    try:
        with DatabaseConnection(**config.db_conn_params) as db_conn:
            db_conn.conn.autocommit = True
            db_conn.cursor.execute(query)
            db_conn.conn.autocommit = False
    except errors.DuplicateDatabase:
        print("DataBase already exists. Skipping creation")


def test_create_table_from_class():
    # Example: Generate SQL for TPDataTable class
    test_table = TableConfig.CycleDataTable
    table_name = "asdf"
    TableCreator().create_table(test_table, test_table.table_name)
    #TableCreator().__delete_table__('asdf')

    # Example Usage


if __name__ == "__main__":
    from src.infrastructure.core.config_reader import config
    create_database(config)
    creator = TableCreator(db_conn_params=config.db_conn_params)
    #creator.__delete_table__(table_name="thermal_conductivity_xy_data")
    #creator.__delete_table__(table_name="thermal_conductivity_data")
    creator.create_all_tables()
        # Accessing class attributes with and without escaped quotes

