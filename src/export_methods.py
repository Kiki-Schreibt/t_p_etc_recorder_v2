import pandas as pd
import os
import numpy as np
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging
from src.table_data import TableConfig
from src.config_connection_reading_management.database_reading_writing import DataRetriever
from src.standard_paths import standard_export_path
from src.meta_data.meta_data_handler import MetaData


class QuickExport:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.tp_table = TableConfig().TPDataTable
        self.etc_table = TableConfig().ETCDataTable
        self.cycle_table = TableConfig().CycleDataTable
        self.db_retriever = DataRetriever()
        self.meta_data = MetaData()

    def export_all(self, sample_id, subfolder_name_str=""):
        self.meta_data = MetaData(sample_id=sample_id)
        self.export_etc_data(sample_id=sample_id, subfolder_name_str=subfolder_name_str)
        self.export_capacity_data(sample_id=sample_id, subfolder_name_str=subfolder_name_str)
        self.export_t_p_data(sample_id=sample_id, subfolder_name_str=subfolder_name_str)

    def export_etc_data(self, sample_id, subfolder_name_str=""):
        if not self.meta_data.sample_id:
            self.meta_data = MetaData(sample_id=sample_id)
        self.logger.info(f"Starting ETC data export for {sample_id}")
        etc_data, file_edition_etc = self._get_etc_data_full_test(sample_id=sample_id)
        self._write_data(sample_id=sample_id,
                        data=etc_data,
                        file_addition=file_edition_etc,
                        subfolder_name=subfolder_name_str)

    def export_capacity_data(self, sample_id, subfolder_name_str=""):
        if not self.meta_data.sample_id:
            self.meta_data = MetaData(sample_id=sample_id)
        self.logger.info(f"Starting capacity over cycles data export for {sample_id}")
        capacity_data, file_edition_cap = self._get_capacity_data_full_test(sample_id=sample_id)
        self._write_data(sample_id=sample_id,
                        data=capacity_data,
                        file_addition=file_edition_cap,
                        subfolder_name=subfolder_name_str)

    def _get_etc_data_full_test(self, sample_id):
        cols_to_export = (self.etc_table.time,
                          self.etc_table.pressure,
                          self.etc_table.temperature_sample,
                          self.etc_table.th_conductivity,
                          self.etc_table.thermal_conductivity_average,
                          self.etc_table.thermal_conductivity_deviation,
                          self.etc_table.total_temperature_increase,
                          self.etc_table.total_to_characteristic_time,
                          self.etc_table.cycle_number,
                          self.etc_table.cycle_number_flag,
                          self.etc_table.output_power,
                          self.etc_table.measurement_time,
                          self.etc_table.resistance)
        constraints_dict = {
                                "min_TotalCharTime": 0.3,
                                "max_TotalCharTime": 1.1,
                                "min_TotalTempIncr": 1.9,
                                "max_TotalTempIncr": 5.2
                            }
        data_to_export = self.db_retriever.fetch_data_by_sample_id_2(table_name=self.etc_table.table_name,
                                                            column_names=cols_to_export,
                                                            constraints=constraints_dict,
                                                                     sample_id=sample_id)


        data_to_export = data_to_export.dropna(subset=self.etc_table.get_clean('time'))
        #calculate relative times
        time_start = self.meta_data.start_time
        time_shift = (data_to_export[self.etc_table.get_clean('time')].iloc[0] - time_start)
        time_shift = time_shift.total_seconds()/3600
        print(time_shift)
        time_intervall = data_to_export[self.etc_table.get_clean('time')].diff().dt.total_seconds() / 3600
        time_intervall[0] = 0

        # Calculate the cumulative sum of the intervals
        data_to_export['hours'] = time_intervall.cumsum()
        data_to_export['hours'] = data_to_export['hours'] + time_shift


        return data_to_export, "_Conductivity_Data"

    def _get_capacity_data_full_test(self, sample_id):
        cols_to_export = (self.cycle_table.cycle_number,

                          self.cycle_table.h2_uptake)

        data_to_export = self.db_retriever.fetch_data_by_sample_id_2(table_name=self.cycle_table.table_name,
                                                                     column_names=cols_to_export,
                                                                     sample_id=sample_id)
        min_value = data_to_export[self.cycle_table.cycle_number].min()
        max_value = data_to_export[self.cycle_table.cycle_number].max()
        expected_values = np.arange(min_value, max_value + 0.1, 0.5)

        # Create a dataframe with the expected values
        expected_df = pd.DataFrame({self.cycle_table.cycle_number: expected_values})

        # Merge with the original dataframe to identify missing values
        merged_df = pd.merge(expected_df, data_to_export, on=self.cycle_table.cycle_number, how='left')

        # Fill missing values with NaN
        merged_df.fillna(np.nan, inplace=True)

        return merged_df, "_Capacity_Data"

    def _write_data(self, sample_id="", data=pd.DataFrame(), file_addition="", subfolder_name=""):

        file_name = sample_id + file_addition + ".txt"
        # Combine them into a full file path
        full_file_path = os.path.join(standard_export_path,
                                      sample_id,
                                      subfolder_name,
                                      file_name)

        # Ensure the directory exists
        os.makedirs(standard_export_path, exist_ok=True)
        os.makedirs(os.path.join(standard_export_path, sample_id, subfolder_name), exist_ok=True)
        try:
            # Write the data to the file
            data.to_csv(full_file_path, sep=';', index=False)

            self.logger.info(f"Data exported to {full_file_path}")
        except Exception as e:
            self.logger.error(f"Error exporting data: {e}")

    def export_t_p_data(self, sample_id, subfolder_name_str=""):
        if not self.meta_data.sample_id:
            self.meta_data = MetaData(sample_id=sample_id)
        self.logger.info(f"Starting temperature-pressure data export for {sample_id}")
        t_p_data, file_edition_etc = self._get_tp_data(sample_id=sample_id)
        self._write_data(sample_id=sample_id,
                        data=t_p_data,
                        file_addition=file_edition_etc,
                        subfolder_name=subfolder_name_str)

    def _get_tp_data(self, sample_id):
        table = self.tp_table
        column_names_first = [self.tp_table.time,
                              self.tp_table.temperature_sample,
                              self.tp_table.temperature_heater,
                              self.tp_table.pressure,
                              self.tp_table.setpoint_sample]
                            #Hours
        column_names_last = [self.tp_table.eq_pressure,
                             self.tp_table.cycle_number,
                             self.tp_table.de_hyd_state]
        column_names_all = column_names_first + ['hours'] + column_names_last
        print(column_names_all)

        query = f"""
                WITH time_series AS (
                    SELECT generate_series(
                        date_trunc('minute', min({table.time})),
                        date_trunc('minute', max({table.time})),
                        '1 minute'::interval
                    ) as minute
                    FROM {table.table_name}
                    WHERE {table.sample_id} = %s
                )
                SELECT {', m.'.join(column_names_first)}, 
                            ts.minute, 
                        {', m.'.join(column_names_last)}
                FROM time_series ts
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM {table.table_name}
                    WHERE {table.sample_id} = %s
                    AND {table.time} >= ts.minute
                    AND {table.time} < ts.minute + interval '1 minute'
                    ORDER BY {table.time}
                    LIMIT 1
                ) m ON true
                ORDER BY ts.minute;
                """

        values = (sample_id, sample_id)
        df = self.db_retriever.execute_fetching(query=query,
                                                values=values,
                                                table_name=table.table_name,
                                                column_names=column_names_all)
        #calculate relative times
        df = df.dropna(subset=table.time)
        if self.meta_data.start_time:
            time_start = self.meta_data.start_time
        else:
            time_start = df[table.time].iloc[0]

        time_shift = (df[table.time].iloc[0] - time_start)
        time_shift = time_shift.total_seconds()/3600
        time_intervall = df[table.time].diff().dt.total_seconds() / 3600
        time_intervall[0] = 0


        column_names_export = [self.tp_table.time,
                              self.tp_table.temperature_sample,
                              self.tp_table.temperature_heater,
                              self.tp_table.pressure,
                              self.tp_table.setpoint_sample,
                              'hours', self.tp_table.eq_pressure,
                              self.tp_table.cycle_number,
                              'de_hyd_oi',
                              self.tp_table.de_hyd_state,
                              ]

        # Calculate the cumulative sum of the intervals
        df['hours'] = time_intervall.cumsum()
        df['hours'] = df['hours'] + time_shift
        df['de_hyd_oi'] = df[table.de_hyd_state] == "Hydriert"
        df_to_export = df[column_names_export]

        if not df.empty:
            return df_to_export, "_t-p-data"
        else:
            return pd.DataFrame(), ""


if __name__ == '__main__':
    #sample_id = '028-test-simulator_2'
    sample_id = 'WAE-WA-030'
    exporter = QuickExport()
    exporter.export_all(sample_id=sample_id)
    #5344,289444522186 origin
    #5345,30476 python
    #31-03-2022 15:25:05 origin
    #31-03-2022 13:41:43 python
    #origin_first = datetime(2022, 3, 31, 15, 25, 5)
    #python_first = datetime(2022, 3, 31, 13, 41, 43)
    #print(origin_first-python_first)
    #print(0.72278*60)
    #print(0.3668*60)

