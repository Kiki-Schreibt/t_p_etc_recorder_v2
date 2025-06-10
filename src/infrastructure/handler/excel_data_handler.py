##excel_data_handler.py

import numpy as np
import win32com.client as win32
import re
import os
import time
from zoneinfo import ZoneInfo
from multiprocessing import Pool
from datetime import datetime
from typing import Optional, Tuple, Union, List
import math


import pandas as pd
from psycopg2 import IntegrityError

from src.infrastructure.connections.connections import DatabaseConnection
try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging

from src.infrastructure.meta_data.meta_data_handler import MetaData
from src.infrastructure.core.table_config import TableConfig
from src.config_connection_reading_management.database_reading_writing import DataRetriever, DataBaseManipulator

local_tz = ZoneInfo("Europe/Berlin")
LIMIT_DATA_POINTS = 5000


class ExcelDataProcessor:
    """
    Processes Excel files and writes data to the database.
    """
    etc_table = TableConfig().ETCDataTable
    xy_table = TableConfig().ThermalConductivityXyDataTable

    etc_column_attribute_mapping = DataRetriever.etc_column_attribute_mapping

    def __init__(
        self,
        file_path: str = 'dummy',
        results_sheet_name: str = 'Results',
        parameters_sheet_name: str = 'Parameters',
        sample_id: Optional[str] = None,
        meta_data: Optional[object] = None,
        db_conn_params=None
    ):
        self.file_path = file_path
        self.results_sheet_name = results_sheet_name
        self.parameters_sheet_name = parameters_sheet_name
        self.db_conn_params = db_conn_params or {}
        self.logger = logging.getLogger(__name__)
        self.meta_data = MetaData(sample_id=sample_id, db_conn_params=db_conn_params) if (sample_id and db_conn_params) else meta_data
        self._test_mode = False

    def _update_xlsx_file(self) -> None:
        max_retries = 30
        delay = 0.1  # start with 100 ms delay
        for attempt in range(max_retries):
            try:
                excel = win32.gencache.EnsureDispatch('Excel.Application')
                workbook = excel.Workbooks.Open(self.file_path)
                workbook.RefreshAll()
                excel.Calculate()
                workbook.Save()
                workbook.Close()
                excel.Quit()
                self.logger.info("Excel file updated successfully.")
                return
            except Exception as e:
                error_str = str(e)
                # Check if the error message contains the OLE busy error code.
                if "0x800ac472" in error_str:
                    self.logger.warning("Excel busy (attempt %d/%d): %s", attempt + 1, max_retries, e)
                    time.sleep(delay)
                    delay *= 2  # Optional: increase delay with each attempt.
                else:
                    self.logger.error("Error updating Excel file: %s", e)
                    return
        self.logger.error("Failed to update Excel file after %d attempts.", max_retries)

    def _read_and_process_sheets(self) -> pd.DataFrame:
        try:
            dtype_spec = {'Temp.drift rec.': 'float'}
            df_parameters = pd.read_excel(
                self.file_path,
                sheet_name=self.parameters_sheet_name,
                header=1,
                dtype=dtype_spec
            )
            df_parameters = df_parameters.dropna(subset=['Description'])
            df_parameters.columns = df_parameters.columns.str.replace(r'[^\w\s]', '', regex=True)
            df_parameters = df_parameters.dropna(subset=['Description'])

            df_results = pd.read_excel(self.file_path, sheet_name=self.results_sheet_name, header=1)
            df_results.columns = df_results.columns.str.replace(r'[^\w\s]', '', regex=True)
            df_results = self._process_results_sheet_for_table(df_results)

            merged_df = self._merge_data(df_results, df_parameters)
            return merged_df
        except Exception as e:
            self.logger.error("Error reading and processing sheets: %s", e)
            return pd.DataFrame()

    @staticmethod
    def _process_results_sheet_for_table(df: pd.DataFrame) -> pd.DataFrame:
        df_copy = df.copy()
        keys = [('Average', '_avg'), ('StandardDeviation', '_dvt')]
        original_columns = [col for col in df_copy.columns if col not in ['File', 'Description', 'Sample ID', 'Points']]

        for key, suffix in keys:
            for col in original_columns:
                new_col_name = col + suffix
                if new_col_name not in df_copy.columns:
                    df_copy[new_col_name] = np.nan

            key_rows = df_copy[df_copy['Description'].str.contains(key, na=False)]
            for index, row in key_rows.iterrows():
                target_index = index - 1 if key == 'Average' else index - 2
                if target_index < 0:
                    continue
                for col in original_columns:
                    new_col_name = col + suffix
                    df_copy.at[target_index, new_col_name] = row[col]
            df_copy = df_copy.drop(key_rows.index)
        return df_copy

    def _merge_data(self, results_sheet: pd.DataFrame, parameters_sheet: pd.DataFrame) -> Optional[pd.DataFrame]:
        results_sheet = results_sheet.reset_index(drop=True)
        parameters_sheet = parameters_sheet.reset_index(drop=True)
        if len(results_sheet) == len(parameters_sheet):
            description_matches = results_sheet['Description'] == parameters_sheet['Description']
            matching_results = results_sheet[description_matches]
            matching_parameters = parameters_sheet[description_matches]
            combined_sheet = pd.concat([matching_results, matching_parameters], axis=1)
            combined_sheet = combined_sheet.loc[:, ~combined_sheet.columns.duplicated()]
            time_col = combined_sheet.pop('Time')
            combined_sheet.insert(0, 'Time', time_col)
            combined_sheet['Time'] = pd.to_datetime(combined_sheet['Time'])
            combined_sheet['Time'] = combined_sheet['Time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            combined_sheet["Time"] = pd.to_datetime(combined_sheet["Time"])
            combined_sheet["Time"] = combined_sheet["Time"].dt.tz_localize(local_tz, ambiguous='NaT')
            return combined_sheet
        else:
            self.logger.error("The DataFrames have different lengths and cannot be concatenated directly.")
            return None

    #new xy reader
    def _get_measurement_xy_data_as_lists(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a DataFrame with one row per measurement in combined_df,
        columns:
          ['measurement_time',
           'transient_x','transient_y',
           'drift_x','drift_y',
           'calculated_x','calculated_y',
           'residual_x','residual_y']
        where each _x/_y is a list of floats (or None for NaNs).
        """
        # mapping of mode → (sheet name, x‐col, y‐col)
        sheets = {
            'calculated': ('T-f(Tau)', 't_f_tau', 'temperature'),
            'transient':  ('T-t',       'time_temperature_increase', 'temperature_increase'),
            'residual':   ('Diff',      'sqrt_time',                 'diff_temperature'),
            'drift':      ('T(drift)',  'time_drift',                'temperature_drift'),
        }

        # 1) read each sheet once, drop entirely empty columns
        raw = {}
        for mode, (sheet, xcol, ycol) in sheets.items():
            df = pd.read_excel(self.file_path, sheet_name=sheet, header=3)
            df = df.dropna(axis=1, how='all')
            raw[mode] = df

        # 2) build one record per row of combined_df
        times = combined_df['Time'].tolist()
        rows = []
        for j, meas_time in enumerate(times):
            rec = {'time': meas_time}
            for mode, df_mode in raw.items():
                # X columns start at index 1, then alternate: 1,2 → run 0; 3,4 → run 1; etc.
                idx_x = 1 + 2*j
                idx_y = 2 + 2*j
                if idx_x < df_mode.shape[1] and idx_y < df_mode.shape[1]:
                    col_x = df_mode.columns[idx_x]
                    col_y = df_mode.columns[idx_y]
                    xs = df_mode[col_x].astype(float).tolist()
                    ys = df_mode[col_y].astype(float).tolist()
                    # replace NaN with None
                    xs = [None if math.isnan(v) else v for v in xs]
                    ys = [None if math.isnan(v) else v for v in ys]
                else:
                    xs, ys = [], []
                rec[f'{mode}_x'] = xs
                rec[f'{mode}_y'] = ys
            rows.append(rec)

        df = pd.DataFrame(rows)
        return df

    def _write_to_database(self, insert_query: str, values: list, table_name: str = "") -> bool:
        with DatabaseConnection(**self.db_conn_params) as db_conn:
            self.logger.info("Starting writing thermal conductivity data to %s database...", table_name)
            try:
                db_conn.cursor.executemany(insert_query, values)
                db_conn.cursor.connection.commit()
                self.logger.info("Thermal conductivity data inserted from file: %s", self.file_path)
                return False
            except IntegrityError as e:
                self.logger.error("IntegrityError while inserting data: %s. Rolling back and retrying.", e)
                db_conn.cursor.connection.rollback()
                return True
            except Exception as e:
                self.logger.error("Error inserting thermal conductivity data: %s", e)
                db_conn.cursor.connection.rollback()
                return True

    def _delete_data_from_table(self, data_to_delete: pd.DataFrame) -> None:
        with DatabaseConnection(**self.db_conn_params) as db_conn:
            self.logger.info("Starting deletion of data from %s database...", self.etc_table.table_name)
            data_identifiers = [(time_val,) for time_val in data_to_delete['Time']]
            try:
                delete_query = f"DELETE FROM {self.etc_table.table_name} WHERE \"Time\" = %s"
                delete_query_xy = f"DELETE FROM {self.xy_table.table_name} WHERE \"time\" = %s"
                db_conn.cursor.executemany(delete_query, data_identifiers)
                db_conn.cursor.executemany(delete_query_xy, data_identifiers)
                db_conn.cursor.connection.commit()
                self.logger.info("Data deleted successfully")
            except Exception as e:
                self.logger.error("Error occurred while deleting data: %s", e)
                db_conn.cursor.connection.rollback()

    #new duplicate remover
    @staticmethod
    def _delete_duplicates(df, xy_df):
        deduped_df = df.drop_duplicates(subset='Time', keep='last').reset_index(drop=True)
        deduped_xy_df = xy_df.drop_duplicates(subset='time', keep='last').reset_index(drop=True)
        return deduped_df, deduped_xy_df

    def _find_corresponding_t_p(self, df_etc: pd.DataFrame) -> pd.DataFrame:
        t_p_table = TableConfig().TPDataTable
        etc_table = TableConfig().ETCDataTable
        start_time = min(df_etc[etc_table.get_clean("time")])
        end_time = max(df_etc[etc_table.get_clean("time")])
        time_range = (start_time, end_time)
        cols = (t_p_table.time,
                t_p_table.pressure,
                t_p_table.temperature_sample,
                t_p_table.cycle_number,
                t_p_table.cycle_number_flag,
                t_p_table.de_hyd_state,
                t_p_table.is_isotherm_flag,
                t_p_table.test_info)
        cols_etc = [etc_table.get_clean('time'),
                    t_p_table.pressure,
                    t_p_table.temperature_sample,
                    t_p_table.cycle_number,
                    t_p_table.cycle_number_flag,
                    t_p_table.de_hyd_state,
                    etc_table.is_isotherm_flag,
                    etc_table.test_info]
        db_retriever = DataRetriever(db_conn_params=self.db_conn_params)
        df_tp = db_retriever.fetch_data_by_time_no_limit(table=t_p_table, time_range=time_range, col_names=list(cols))
        if df_tp.empty:
            self.logger.info("No corresponding t_p_data found")
            return pd.DataFrame()
        df_etc = df_etc.sort_values(etc_table.get_clean('time'))
        df_tp = df_tp.rename(columns={t_p_table.time: etc_table.get_clean('time')})
        df_tp = df_tp.sort_values(etc_table.get_clean('time'))
        df_tp[etc_table.get_clean('time')] = df_tp[etc_table.get_clean('time')].dt.tz_convert(local_tz)
        df_tp_etc = pd.merge_asof(df_etc, df_tp, on=etc_table.get_clean('time'),
                                  direction='nearest',
                                  suffixes=('_etc', '_tp'))
        df_merged = df_tp_etc[cols_etc]
        return df_merged

    def execute(self) -> Optional[Tuple[datetime, datetime]]:
        table = TableConfig().ETCDataTable
        self._update_xlsx_file()
        df_etc = self._read_and_process_sheets()
        if df_etc is None or df_etc.empty:
            return None
        df_etc_xy = self._get_measurement_xy_data_as_lists(df_etc)
        df_etc[self.etc_table.sample_id_small] = self.meta_data.sample_id
        df_etc_xy[self.xy_table.sample_id] = self.meta_data.sample_id
        df_etc = df_etc.dropna(subset=[table.get_clean("time")])
        df_etc_xy = df_etc_xy.dropna(subset=[self.xy_table.time])
        df_etc, df_etc_xy = self._delete_duplicates(df=df_etc, xy_df=df_etc_xy)
        df_t_p = self._find_corresponding_t_p(df_etc) # df containing corresponding tp values
        if not df_etc.empty and not df_t_p.empty:
            df_etc = pd.merge(df_etc, df_t_p, on=table.get_clean('time'), how='inner')
        else:
            df_etc[self.etc_table.pressure] = None
            df_etc[self.etc_table.temperature_sample] = None
            df_etc[self.etc_table.cycle_number] = None
            df_etc[self.etc_table.cycle_number_flag] = None
            df_etc[self.etc_table.is_isotherm_flag] = False
            df_etc[self.etc_table.test_info] = None
        pd.set_option('future.no_silent_downcasting', True)
        df_etc.replace('(no corr.)', 0, inplace=True)

        #create insert query and prepare data for insert
        ETC_insert_query, ETC_values = TableConfig().writing_query_from_df(
            df=df_etc,
            map=self.etc_column_attribute_mapping,
            table_name=self.etc_table.table_name
        )

        xy_insert_query, xy_values = TableConfig().writing_query_from_df(
            df=df_etc_xy,
            map=None,
            table_name=TableConfig().ThermalConductivityXyDataTable.table_name
        )

        #start insertion
        error_checker = self._write_to_database(
            insert_query=ETC_insert_query,
            values=ETC_values,
            table_name=self.etc_table.table_name
        )
        if error_checker:
            self._delete_data_from_table(df_etc)
            self._write_to_database(
                insert_query=ETC_insert_query,
                values=ETC_values,
                table_name=self.etc_table.table_name
            )
            self._write_to_database(
                insert_query=xy_insert_query,
                values=xy_values,
                table_name=self.xy_table.table_name
            )
        else:
            self._write_to_database(
                insert_query=xy_insert_query,
                values=xy_values,
                table_name=self.xy_table.table_name
            )
        time_range = (min(df_etc[table.get_clean("time")]), max(df_etc[table.get_clean("time")]))
        return time_range

    def save_combined_data(self, combined_df: pd.DataFrame, output_file_path: str) -> None:
        try:
            cols = combined_df.columns.tolist()
            if 'Time' in cols:
                cols.insert(0, cols.pop(cols.index('Time')))
            combined_df = combined_df[cols]
            combined_df.to_csv(output_file_path, sep=';', index=False)
            self.logger.info("Data saved to %s", output_file_path)
        except Exception as e:
            self.logger.error("Error saving combined data: %s", e)


def test_excel_data_processor(etc_dir: str, sample_id: str) -> None:
    from src.infrastructure.core.config_reader import GetConfig
    config = GetConfig()
    etc_processor = ExcelDataProcessor(sample_id=sample_id, file_path=etc_dir, db_conn_params=config.db_conn_params)
    etc_processor.execute()


def process_ETC_file(args: Tuple[str, str, logging.getLogger]) -> None:
    file_path, sample_id, logger_inst, config = args
    try:
        logger_inst.info("Start writing %s to database", os.path.basename(file_path))
        etc_processor = ExcelDataProcessor(file_path=file_path, sample_id=sample_id, db_conn_params=config.db_conn_params)
        etc_processor.execute()
        logger_inst.info("%s written to database", os.path.basename(file_path))
    except Exception as e:
        logger_inst.error("Error processing %s: %s", os.path.basename(file_path), e)


def write_ETC_in_parallel(dir_etc_folder: str, sample_id: str, logger_inst, config) -> None:
    start_time = time.time()
    file_paths = [
        os.path.join(dir_etc_folder, filename)
        for filename in os.listdir(dir_etc_folder)
        if filename.endswith('.xlsx') and "$" not in filename
    ]
    args = [(file_path, sample_id, logger_inst, config) for file_path in file_paths]
    with Pool(processes=4) as pool:
        pool.map(process_ETC_file, args)
    logger_inst.info("Import took %.2f hours", (time.time() - start_time) / 3600)


def write_ETC_folder(dir_etc_folder: str, sample_id: str, logger_inst, config) -> None:
    start_time = time.time()
    file_paths = [
        os.path.join(dir_etc_folder, filename)
        for filename in os.listdir(dir_etc_folder)
        if filename.endswith('.xlsx') and "$" not in filename
    ]
    for file_path in file_paths:
        args = (file_path, sample_id, logger_inst, config)

        process_ETC_file(args)
    logger_inst.info("Import took %.2f hours", (time.time() - start_time) / 3600)


def main():
    sample_id = 'WAE-WA-060'
    dir_etc = r"C:\Daten\Kiki\WAE-WA-060-Mg5wtFe\WAE-WA-060-All\WAE-WA-060-000-AngleTest\WAE-WA-060-000-01-0dlong_0dtrans\WAE-WA-060-000-01.xlsx"
    test_excel_data_processor(etc_dir=dir_etc, sample_id=sample_id)

if __name__ == "__main__":
    main()
