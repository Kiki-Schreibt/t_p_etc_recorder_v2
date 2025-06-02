#table_config.py
import pandas as pd


class TableConfig:

    class TPDataTable:
        table_name = 't_p_data'
        time = 'time'
        pressure = 'pressure'
        temperature_sample = 'temperature_sample'
        setpoint_sample = 'setpoint_sample'
        temperature_heater = 'temperature_heater'
        setpoint_heater = 'setpoint_heater'
        eq_pressure = 'eq_pressure'
        de_hyd_state = 'de_hyd_state'
        cycle_number = 'cycle_number'
        reservoir_volume = 'reservoir_volume'
        sample_id = 'sample_id'
        h2_uptake = 'h2_uptake'
        cycle_number_flag = 'cycle_number_flag'
        h2_uptake_flag = 'h2_uptake_flag'
        cycle_length_flag = 'cycle_length_flag'
        eq_pressure_real = 'eq_pressure_real'
        is_isotherm_flag = 'is_isotherm_flag'
        test_info = 'test_info'

    class ETCDataTable:
        table_name = 'thermal_conductivity_data'
        time = "\"Time\""
        file =  "\"File\""
        description = "\"Description\""
        sample_id = "\"Sample_ID\""
        points =  "\"Points\""
        temperature = "\"Temperature\""
        th_conductivity = "\"ThConductivity\""
        th_diffusivity = "\"ThDiffusivity\""
        specific_heat = "\"SpecHeat\""
        th_effusivity = "\"ThEffusivity\""
        probing_depth = "\"PrDepth\""
        temperature_increase = "\"TempIncr\""
        temperature_drift = "\"TempDrift\""
        total_temperature_increase = "\"TotalTempIncr\""
        total_to_characteristic_time = "\"TotalCharTime\""
        time_correction = "\"Time_Corr\""
        mean_deviation = "\"Mean_Dev\""
        disk_resistance = "\"Disk_Res\""
        calculation_settings = "\"Calc_settings\""
        temperature_average = "\"Temperature_avg\""
        thermal_conductivity_average = "\"ThConductivity_avg\""
        thermal_diffusivity_average = "\"ThDiffusivity_avg\""
        specific_heat_average = "\"SpecHeat_avg\""
        thermal_effusivity_average = "\"ThEffusivity_avg\""
        probing_depth_average = "\"PrDepth_avg\""
        temperature_increase_average = "\"TempIncr_avg\""
        temperature_drift_average = "\"TempDrift_avg\""
        total_temperature_increase_average = "\"TotalTempIncr_avg\""
        total_to_characteristic_time_average = "\"TotalCharTime_avg\""
        time_correction_average = "\"Time_Corr_avg\""
        mean_deviation_average = "\"Mean_Dev_avg\""
        disk_resistance_average = "\"Disk_Res_avg\""
        calculation_settings_average = "\"Calc_settings_avg\""
        temperature_deviation = "\"Temperature_dvt\""
        thermal_conductivity_deviation = "\"ThConductivity_dvt\""
        thermal_diffusivity_deviation = "\"ThDiffusivity_dvt\""
        specific_heat_deviation = "\"SpecHeat_dvt\""
        thermal_effusivity_deviation = "\"ThEffusivity_dvt\""
        probing_depth_deviation = "\"PrDepth_dvt\""
        temperature_increase_deviation = "\"TempIncr_dvt\""
        temperature_drift_deviation = "\"TempDrift_dvt\""
        total_temperature_increase_deviation = "\"TotalTempIncr_dvt\""
        total_to_characteristic_time_deviation = "\"TotalCharTime_dvt\""
        time_correction_deviation = "\"Time_Corr_dvt\""
        mean_deviation_deviation = "\"Mean_Dev_dvt\""
        disk_resistance_deviation = "\"Disk_Res_dvt\""
        calculation_settings_deviation = "\"Calc_settings_dvt\""
        output_power = "\"Outppower\""
        measurement_time = "\"Meastime\""
        disk_radius = "\"Radius\""
        tcr = "\"TCR\""
        disk_type = "\"Disk_Type\""
        temperature_drift_rec = "\"Tempdrift_rec\""
        notes = "\"Notes\""
        resistance = "\"Rs\""
        sample_id_small = "sample_id"
        pressure = "pressure"
        temperature_sample = "temperature_sample"
        cycle_number = "cycle_number"
        cycle_number_flag = "cycle_number_flag"
        de_hyd_state = 'de_hyd_state'
        test_info = 'test_info'

        @classmethod
        def get_clean(cls, attribute_name):
            """Return the value of a class attribute without escaped quotes."""
            value = getattr(cls, attribute_name, None)
            if value is not None and isinstance(value, str):
                return value.replace('\"', '')
            return value

    class ThermalConductivityXyDataTable:
        table_name = 'thermal_conductivity_xy_data'
        sample_id        = 'sample_id'
        time = 'time'
        # parallel numeric arrays for XY pairs
        transient_x      = 'transient_x'
        transient_y      = 'transient_y'
        drift_x          = 'drift_x'
        drift_y          = 'drift_y'
        calculated_x     = 'calculated_x'
        calculated_y     = 'calculated_y'
        residual_x       = 'residual_x'
        residual_y       = 'residual_y'

    class MetaDataTable:
        table_name = "meta_data"
        sample_id = "sample_id"
        material = "material"
        mass = "mass"
        measurement_cell = "measurement_cell"
        time_start = "time_start"
        time_end = "time_end"
        time_first_hydrogenation = "first_hydrogenation"
        max_pressure_cycling = "max_pressure_cycling"
        min_temperature_cycling = "min_temperature_cycling"
        reservoir_volume = "reservoir_volume"
        average_cycle_duration = "average_cycle_duration"
        enthalpy = "enthalpy"
        entropy = "entropy"
        total_number_cycles = "total_number_cycles"
        last_de_hyd_state = "last_de_hyd_state"

    class CycleDataTable:
        table_name = 'cycle_data'
        sample_id = 'sample_id'
        time_start = 'time_start'
        time_end = 'time_end'
        cycle_number = 'cycle_number'
        cycle_duration = 'cycle_duration'
        de_hyd_state = 'de_hyd_state'
        pressure_min = 'pressure_min'
        pressure_max = 'pressure_max'
        temperature_min = 'temperature_min'
        temperature_max = 'temperature_max'
        volume_reservoir = 'volume_reservoior'
        volume_cell = 'volume_cell'
        h2_uptake = 'h2_uptake'
        time_min = 'time_min'
        time_max = 'time_max'


    def get_table_column_names(self, table_class=None, table_name=None):
        """
        Returns a list of all column names in the table class.
        """
        if not table_class  and table_name:
            if "t_p" in table_name.lower():
                table_class = self.TPDataTable
            elif "conductivity" in table_name.lower() and "xy" in table_name.lower():
                table_class = self.ThermalConductivityXyDataTable
            elif "conductivity" in table_name.lower():
                table_class = self.ETCDataTable
            elif "meta" in table_name.lower():
                table_class = self.MetaDataTable
            elif "cycle" in table_name.lower():
                table_class = self.CycleDataTable

        if table_class:
            column_names = []
            for attr_name in dir(table_class):
                # Skip special attributes and methods
                if not attr_name.startswith('__') and attr_name != 'table_name':
                    attr_value = getattr(table_class, attr_name)
                    if not callable(attr_value):
                        column_names.append(attr_value)
            return column_names
        else:
            print(f'Could not find table class {table_class}')
            return None


    def writing_query_from_df(self, df, table_name: str, map: dict = None):
        if map:
            db_columns, df_columns = self._map_attr_col_to_list(df_cols_all=df.columns,
                                                            column_attribute_mapping=map)

            insert_query, data_values = self._writing_query(df=df,
                                                            db_columns=db_columns,
                                                            df_columns=df_columns,
                                                            table_name=table_name)
        else:
            insert_query, data_values = self._writing_query(df=df,
                                                            db_columns=df.columns.to_list(),
                                                            df_columns=df.columns.to_list(),
                                                            table_name=table_name)


        return insert_query, data_values

    @staticmethod
    def _map_attr_col_to_list(df_cols_all,
                              column_attribute_mapping: dict):
        """
        :param df:
        :param column_attribute_mapping:
        :param table_name:
        :return: columns_str, placeholders
        """
        db_columns = []
        df_columns = []

        # Iterate over the mapping
        for db_col, df_col in column_attribute_mapping.items():
            if df_col in df_cols_all:
                db_columns.append(db_col)
                df_columns.append(df_col)


        return db_columns, df_columns

    @staticmethod
    def _writing_query(df,
                      db_columns: list,
                      df_columns: list,
                      table_name: str):
        """
        :param df:
        :param column_attribute_mapping:
        :param table_name:
        :param placeholders
        :return: insert_query, data_values
        """

        columns_str = ', '.join(db_columns)
        placeholders = ', '.join(['%s'] * len(db_columns))
        insert_query = (f"INSERT INTO {table_name} ({columns_str}) "
                        f"VALUES ({placeholders})")
        data_values = df[df_columns].values.tolist()
        return insert_query, data_values

    def get_xy_array_column_names(self):
        """
        Return the ordered list of column names for the xy-array schema.
        """
        tbl = self.ThermalConductivityXyDataTable
        return [
            tbl.sample_id,
            tbl.time,
            tbl.transient_x,
            tbl.transient_y,
            tbl.drift_x,
            tbl.drift_y,
            tbl.calculated_x,
            tbl.calculated_y,
            tbl.residual_x,
            tbl.residual_y,
        ]

if __name__ == '__main__':
    tablemaster = TableConfig()
    table = TableConfig().TPDataTable
    names = tablemaster.get_table_column_names(table)
    print(names)

