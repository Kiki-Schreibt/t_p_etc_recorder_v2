#recording_busines_v2
#!/usr/bin/env python
"""
Revised recording application
----------------------------------
This module contains the revised business logic (data recording) and main-window UI code.
The plotting “reader” code in plot_window_reader_basic.py is assumed to be structured well
and remains unchanged.
"""

# ===============================
#         BUSINESS LOGIC
# ===============================
import sys
import threading
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyqtgraph as pg

from PySide6.QtCore import QThread, Signal, QObject
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging

from src.config_connection_reading_management.database_reading_writing import DataRetriever
from src.config_connection_reading_management.hot_disk_log_file_tracker import LogFileTracker
from src.meta_data.meta_data_handler import MetaData
from src.config_connection_reading_management.modbus_handler import ModbusProcessor
from src.table_data import TableConfig
# Import plot window base classes (unchanged)
from src.GUI.recording_gui.plot_window_reader_basic import (
    PlotBaseWindow, ReadStatic, ReadContinuous, DateAxisItem, AxisLabel
)


# Global colors (if needed)
COLORS = [
    "#FF0000", "#00FF00", "#0000FF", "#FFFF00", "#00FFFF", "#FF00FF",
    "#800000", "#808000", "#008000", "#800080", "#008080", "#000080",
    "#FFA500", "#A52A2A", "#20B2AA", "#778899", "#D2691E", "#DC143C",
    "#7FFF00", "#6495ED"
]
COLORS_SCATTER = COLORS.copy()


class DataRecorder(QObject):
    """
    Encapsulates data recording via Modbus (T-p data) and log file tracking (ETC data).
    Provides methods to start/stop recording and to update configuration.
    """
    newEtcDataWritten = Signal(pd.DataFrame)

    def __init__(self, meta_data: MetaData = MetaData(), reservoir_volume: float = None):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.meta_data = meta_data
        self._update_lock = threading.Lock()
        self.mb_processor = self._create_mb_processor(meta_data)
        self.log_tracker = self._create_log_tracker(meta_data)
        self._mb_thread = None
        self._log_tracker_thread = None
        self.etc_constraints = None

    def _create_mb_processor(self, meta_data):
        return ModbusProcessor(meta_data=meta_data)

    def _create_log_tracker(self, meta_data):
        return LogFileTracker(meta_data=meta_data)

    def start_all_recording(self):
        """Start both T-p and ETC recording threads."""
        self.start_tp_recording()
        self.start_etc_recording()

    def stop_all_recording(self):
        """Stop both T-p and ETC recording threads."""
        self.stop_tp_recording()
        self.stop_etc_recording()

    def start_tp_recording(self):
        self.logger.info("Starting T-p recording thread...")
        if self._mb_thread is None:
            self._mb_thread = threading.Thread(target=self.mb_processor.run, daemon=True)
            self._mb_thread.start()
            self.logger.info("T-p recording thread started.")

    def stop_tp_recording(self):
        self.logger.info("Stopping T-p recording thread...")
        if self._mb_thread is not None:
            self.mb_processor.stop()
            self._mb_thread.join()
            self._mb_thread = None
            self.logger.info("T-p recording thread stopped.")

    def start_etc_recording(self):
        self.logger.info("Starting ETC recording thread...")
        if self._log_tracker_thread is None:
            self._log_tracker_thread = threading.Thread(target=self.log_tracker.start, daemon=True)
            self._log_tracker_thread.start()
            self.log_tracker.time_range_etc_import.connect(self._emit_etc_data)
            self.logger.info("ETC recording thread started.")

    def stop_etc_recording(self):
        self.logger.info("Stopping ETC recording thread...")
        if self._log_tracker_thread is not None:
            self._log_tracker_thread.join()
            self._log_tracker_thread = None
            self.logger.info("ETC recording thread stopped.")

    def update_sample_id(self, new_sample_id: str):
        with self._update_lock:
            self.mb_processor.on_sample_id_change(new_val=new_sample_id)
            self.log_tracker.update_sample_id(new_val=new_sample_id)
        self.logger.info(f"Updated sample ID to: {new_sample_id}")

    def update_meta_data(self, new_meta_data: MetaData):
        with self._update_lock:
            self.mb_processor.on_sample_id_change(new_val=new_meta_data, mode="meta")
            self.log_tracker.update_sample_id(new_val=new_meta_data, mode="meta")
        self.logger.info(f"Meta data updated: {new_meta_data.sample_id}")

    def update_reservoir_volume(self, new_volume: float):
        with self._update_lock:
            self.mb_processor.reservoir_volume = new_volume
        self.logger.info(f"Reservoir volume updated to: {new_volume}")

    def update_cycling_flag(self, flag: bool):
        with self._update_lock:
            self.mb_processor.cycling_flag = flag
            self.mb_processor.mb_data_handler.cycling_flag = flag
        self.logger.info(f"Cycling flag set to: {flag}")

    def update_h2_uptake_flag(self, flag: bool):
        with self._update_lock:
            self.mb_processor.h2_uptake_flag = flag
            self.mb_processor.mb_data_handler.h2_uptake_flag = flag
        self.logger.info(f"H2 uptake flag set to: {flag}")

    def _emit_etc_data(self, time_range):
        etc_table = TableConfig().ETCDataTable
        columns = (etc_table.time, etc_table.th_conductivity, etc_table.thermal_conductivity_average)
        db_reader = DataRetriever()
        df_etc = db_reader.fetch_data_by_time_2(
            time_range=time_range,
            table_name=etc_table.table_name,
            constraints=self.etc_constraints,
            column_names=columns
        )
        if not df_etc.empty:
            self.newEtcDataWritten.emit(df_etc)


# For the presentation layer, we derive our plot windows from PlotBaseWindow.
# Here are two example subclasses for continuous and static plotting.

class ContinuousPlotWindow(PlotBaseWindow):
    def __init__(self, parent=None, y_axis='', meta_data: MetaData = MetaData()):
        self.reader = ReadContinuous(meta_data=meta_data)
        super().__init__(parent=parent, y_axis=y_axis)
        # Relay key signals to be used by the controller
        self.reader.current_cycle_sig.connect(self.current_cycle_sig.emit)
        self.reader.current_state_sig.connect(self.current_state_sig.emit)
        self.reader.current_uptake_sig.connect(self.current_uptake_sig.emit)
        self.enableAutoRange()


class StaticPlotWindow(PlotBaseWindow):
    def __init__(self, parent=None, y_axis='', meta_data: MetaData = MetaData()):
        self.reader = ReadStatic(meta_data=meta_data)
        super().__init__(parent=parent, y_axis=y_axis)
        self.enableAutoRange(axis=pg.ViewBox.XYAxes)
        self.reader.start()
        self.reader.whole_test_emited_sig.connect(self._init_on_x_range_changed)

    def _init_on_x_range_changed(self):
        self.disableAutoRange()
        self.plotItem.sigXRangeChanged.connect(self._on_x_range_changed)


# An example of a specialized plot widget for hydrogen uptake:
class UptakePlot(pg.PlotWidget):
    """
    Displays H2 capacity versus cycle.
    """
    uptakeDataReceived = Signal(pd.DataFrame)

    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.logger = logging.getLogger(__name__)
        self._init_ui()
        self.df_uptake = pd.DataFrame()
        self.uptakeDataReceived.connect(self.plot_uptake)
        self.cycle_table = TableConfig().CycleDataTable
        self.symbol_size = 12
        self.scatter_hyd = pg.ScatterPlotItem(pen=None, symbol='o', size=self.symbol_size,
                                               brush=pg.mkBrush('r'), name="Hydrogenated")
        self.scatter_dehyd = pg.ScatterPlotItem(pen=None, symbol='x', size=self.symbol_size,
                                                 brush=pg.mkBrush('b'), name="Dehydrogenated")
        self.plotItem.addLegend(offset=(0, 1))

    def _init_ui(self):
        axis_font = {'color': 'white', 'font-size': '12pt'}
        self.plotItem.getAxis('bottom').setLabel(AxisLabel.create_axis_label("cycle"), **axis_font)
        self.plotItem.getAxis('left').setLabel(AxisLabel.create_axis_label("uptake"), **axis_font)

    def load_data(self, meta_data: MetaData, time_range=None):
        db_reader = DataRetriever()
        if time_range:
            df = db_reader.fetch_data_by_time_2(sample_id=meta_data.sample_id,
                                                table_name=self.cycle_table.table_name,
                                                time_range=time_range)
        else:
            df = db_reader.fetch_data_by_sample_id_2(sample_id=meta_data.sample_id,
                                                     table_name=self.cycle_table.table_name)
        if not df.empty:
            self.uptakeDataReceived.emit(df)

    def plot_uptake(self, df: pd.DataFrame):
        self.plotItem.clear()
        df = df.dropna(subset=[self.cycle_table.h2_uptake])
        if self.plotItem.legend() is None:
            self.plotItem.addLegend()
        df_hyd = df[df[self.cycle_table.de_hyd_state] == "Hydrogenated"]
        df_dehyd = df[df[self.cycle_table.de_hyd_state] == "Dehydrogenated"]

        x_hyd = df_hyd[self.cycle_table.cycle_number]
        y_hyd = df_hyd[self.cycle_table.h2_uptake]
        x_dehyd = df_dehyd[self.cycle_table.cycle_number]
        y_dehyd = -df_dehyd[self.cycle_table.h2_uptake]

        self.scatter_hyd.setData(x_hyd, y_hyd)
        self.scatter_dehyd.setData(x_dehyd, y_dehyd)
        self.plotItem.addItem(self.scatter_hyd)
        self.plotItem.addItem(self.scatter_dehyd)
        if not x_hyd.empty and not y_hyd.empty:
            self.plotItem.setXRange(min(x_hyd), max(x_hyd))
            self.plotItem.setYRange(-max(y_hyd), max(y_hyd))
        self.update()


class ReadPlotTpDependent(pg.PlotWidget):
    """
    A custom PlotWidget for displaying Effective Thermal Conductivity data(ETC) data pressure or temperature dependent.

    Attributes:
        tp_etc_data (Signal): PyQt signal that emits a DataFrame for plotting.
        tp_table (TableConfig.TPDataTable): Configuration for TP data table attributes.
        etc_table (TableConfig.ETCDataTable): Configuration for ETC data table attributes.
        df_etc_storage (DataFrame): Temporary storage for ETC data fetched from the database.
        scatter (ScatterPlotItem): Scatter plot item for raw ETC data.
        scatter_avg (ScatterPlotItem): Scatter plot item for average ETC data.

    Args:
        parent (Optional[QWidget]): The parent widget for this plot widget.
    """
    class ReadTpDependent(QThread):
        tp_etc_data = Signal(pd.DataFrame, str, str)

        def __init__(self, parent=None, constraints=None):
            """
            Initializes the plot widget with two scatter plots and a legend.
            """
            super().__init__(parent=parent)
            self.logger = logging.getLogger(__name__)
            self.db_reader = DataRetriever()
            self.constraints = constraints
            # Customize plot appearance (you can adjust these settings as needed)
            # self.plotItem.showGrid(x=True, y=True)
            self.tp_table = TableConfig().TPDataTable
            self.etc_table = TableConfig().ETCDataTable
            self.df_etc_storage = None
            self.time_range = None
            self.x_col = None

        def run(self):
            """
            Fetches TP and ETC data from the database within the specified
            time range and initiates plotting.

            """
            try:
                tp_table_name = self.tp_table.table_name
                etc_table_name = self.etc_table.table_name

                df_tp_etc = self.db_reader.fetch_data_by_time_2(time_range=self.time_range,
                                                                table_name=etc_table_name,
                                                                constraints=self.constraints)
                self.df_etc_storage = df_tp_etc

                if df_tp_etc[self.etc_table.pressure].isnull().all() or df_tp_etc[self.etc_table.temperature_sample].isnull().all():
                    df_tp = self.db_reader.fetch_data_by_time_2(time_range=self.time_range, table_name=tp_table_name)
                    df_etc = self.db_reader.fetch_data_by_time_2(time_range=self.time_range, table_name=etc_table_name)
                    self.df_etc_storage = df_etc
                    df_for_plot, x_axis_label, y_axis_label = self._sort_data(df_tp=df_tp, df_etc=df_etc, x_col=self.x_col)

                else:
                    df_for_plot, x_axis_label, y_axis_label = self._sort_data(df_tp_etc=df_tp_etc, x_col=self.x_col)

                #self.db_reader.close_connection()
                if not df_for_plot.empty:
                    self.tp_etc_data.emit(df_for_plot, x_axis_label, y_axis_label)
                else:
                    return
            except Exception as e:
                 self.logger.error(f"Exception in loading t_p_dependent data: {e}")

        def _sort_data(self, df_tp=pd.DataFrame(), df_etc=pd.DataFrame(), x_col="", df_tp_etc=pd.DataFrame()):
            """
            Sorts and merges TP and ETC data based on the TP data's time column.

            Args:
                df_tp (DataFrame): The dataframe containing TP data.
                df_etc (DataFrame): The dataframe containing ETC data.
                x_col (str): The column name to sort by ('pressure' or 'temperature').

            Returns:
                DataFrame: The merged and sorted dataframe ready for plotting.
            """
            df_tp = df_tp.copy()
            df_etc = df_etc.copy()
            df_tp_etc = df_tp_etc.copy()
            if 'pressure' in x_col.lower():
                columns_for_plot = [self.tp_table.pressure,
                                    self.etc_table.get_clean('th_conductivity'),
                                    self.etc_table.get_clean('thermal_conductivity_average'),
                                    self.tp_table.de_hyd_state,
                                    self.etc_table.temperature_sample]
                x_axis_label = AxisLabel.create_axis_label(column_name="pressure")
            elif 'temperature' in x_col.lower():
                columns_for_plot = [self.tp_table.temperature_sample,
                                    self.etc_table.get_clean('th_conductivity'),
                                    self.etc_table.get_clean('thermal_conductivity_average'),
                                    self.tp_table.de_hyd_state,
                                    self.tp_table.pressure]
                x_axis_label = AxisLabel.create_axis_label(column_name="temperature")
            else:
                return pd.DataFrame(), "", ""

            y_axis_label = AxisLabel.create_axis_label(column_name="conductivity")

            # Sort the dataframes by time
            if not df_etc.empty and not df_tp.empty:
                df_etc = df_etc.sort_values(self.etc_table.get_clean('time'))
                df_etc = df_etc.rename(columns={self.etc_table.get_clean('time'): self.tp_table.time})
                df_tp = df_tp.sort_values(self.tp_table.time)

                # Perform an asof merge
                df_tp_etc = pd.merge_asof(df_etc, df_tp, on=self.tp_table.time,
                                              direction='nearest',
                                              suffixes=('_etc', '_tp'))
                df_tp_etc = df_tp_etc.rename(columns={self.tp_table.time: self.etc_table.get_clean('time')})

                df_for_plot = df_tp_etc[columns_for_plot]
                df_for_plot = df_for_plot.rename(columns={df_for_plot.columns[-1]: "color_col"})
                return df_for_plot, x_axis_label, y_axis_label

            elif not df_tp_etc.empty:
                df_for_plot = df_tp_etc[columns_for_plot].copy()
                df_for_plot = df_for_plot.rename(columns={df_for_plot.columns[-1]: "color_col"})
                return df_for_plot, x_axis_label, y_axis_label
            else:
                return pd.DataFrame(), "", ""

        def _set_params(self, time_range, x_col):
            """
            Set parameters for data loading.
            """
            self.time_range = time_range
            self.x_col = x_col

    class PlotTpDependent(pg.PlotWidget):

        def __init__(self, parent=None, constraints=None):
            """
            Initializes the plot widget with two scatter plots and a legend.
            """
            super().__init__(parent=parent)
            self.logger = logging.getLogger(__name__)
            self.reader = ReadPlotTpDependent.ReadTpDependent(parent=parent, constraints=constraints)
            # Customize plot appearance (you can adjust these settings as needed)
            # self.plotItem.showGrid(x=True, y=True)
            self.legend = self.plotItem.addLegend(offset=(0, 1))
            self.reader.tp_etc_data.connect(self.plot_ETC)
            self.tp_table = TableConfig().TPDataTable
            self.etc_table = TableConfig().ETCDataTable
            self.df_etc_storage = None
            self.scatter_hyd = pg.ScatterPlotItem(pen=None, symbol='o',
                                                  size=5, brush=pg.mkBrush('w'))
            self.scatter_hyd_avg = pg.ScatterPlotItem(pen=None, symbol='o', size=5,
                                                      brush=pg.mkBrush('r'))
            self.scatter_dehyd = pg.ScatterPlotItem(pen=None, symbol='x',
                                                  size=5, brush=pg.mkBrush('w'))
            self.scatter_dehyd_avg = pg.ScatterPlotItem(pen=None, symbol='x',
                                                        size=5, brush=pg.mkBrush('r'))


            self.addItem(self.scatter_hyd)  # Add scatter plot item to the plot widget
            self.addItem(self.scatter_hyd_avg)  # Add scatter plot item to the plot widget
            self.addItem(self.scatter_dehyd)  # Add scatter plot item to the plot widget
            self.addItem(self.scatter_dehyd_avg)  # Add scatter plot item to the plot widget


            self.legend.addItem(self.scatter_hyd, 'ETC Hydrogenated')
            self.legend.addItem(self.scatter_hyd_avg, 'ETC Hydrogenated Average')
            self.legend.addItem(self.scatter_dehyd, 'ETC Dehydrogenated')
            self.legend.addItem(self.scatter_dehyd_avg, 'ETC Dehydrogenated Average')

        def load_data(self, time_range, x_col):
            self.reader._set_params(time_range, x_col)
            self.reader.start()

        def plot_ETC(self, df, x_axis_label, y_axis_label):
            """
            Plots ETC and average ETC data from the provided DataFrame.

            Args:
                df (DataFrame): The dataframe containing ETC data to plot.
            """
            def split_by_state(df, state):
                mask = df[self.tp_table.de_hyd_state] == state
                return df.loc[mask]

            def create_color_map(color_vals):
                # Create a color map
                c_map = pg.colormap.get('CET-L4')  # You can choose different colormaps available in pyqtgraph
                normalized_vals = (color_vals - color_vals.min()) / (color_vals.max() - color_vals.min())
                colors = c_map.map(normalized_vals, mode='qcolor')  # Map temperatures to colors
                return colors

            def label_last_points(color_col, x, y):
                #todo: split by color_col_vals (done) sort each coloc_col after color_co_vals (increasing).

                color_col = color_col.copy()
                rounded_values = color_col.round()
                x = x.round()
                # Find the indices where the value changes from the previous entry
                change_indices = rounded_values.ne(rounded_values.shift()).index[rounded_values.ne(rounded_values.shift())]
                last_point_list = change_indices.tolist()

                # Label the corresponding data points
                for idx in last_point_list:
                    x_val = x.loc[idx]  # or 'Temperature', depending on your x-axis
                    y_val = y.loc[idx]
                    color_val = color_col.loc[idx]
                    text = pg.TextItem(f'{color_val}', anchor=(1, 1))
                    text.setPos(x_val, y_val)
                    return text

            self.plotItem.clear()
            if not df.empty:
                self.plotItem.getAxis('bottom').setLabel(x_axis_label)
                self.plotItem.getAxis('left').setLabel(y_axis_label)

                color = pd.DataFrame()
                df_hyd = split_by_state(df, "Hydrogenated")
                df_dehyd = split_by_state(df, "Dehydrogenated")
                y_hyd = df_hyd[self.etc_table.get_clean('th_conductivity')]
                y_hyd_avg = df_hyd[self.etc_table.get_clean('thermal_conductivity_average')]
                y_dehyd = df_dehyd[self.etc_table.get_clean('th_conductivity')]
                y_dehyd_avg = df_dehyd[self.etc_table.get_clean('thermal_conductivity_average')]

                for col in df.columns:
                    if 'pressure' in col.lower():
                        x = df[self.tp_table.pressure]
                        x_hyd = df_hyd[self.tp_table.pressure]
                        x_dehyd = df_dehyd[self.tp_table.pressure]

                    elif 'temperature' in col.lower():
                        x = df[self.tp_table.temperature_sample]
                        x_hyd = df_hyd[self.tp_table.temperature_sample]
                        x_dehyd = df_dehyd[self.tp_table.temperature_sample]

                color_hyd = create_color_map(df_hyd["color_col"])
                color_dehyd = create_color_map(df_dehyd["color_col"])


                y = df[self.etc_table.get_clean('th_conductivity')]
                #y_avg = df[self.etc_table.get_clean('thermal_conductivity_average')]

                self.scatter_hyd.setData(x_hyd, y_hyd, brush=color_hyd)
                self.scatter_hyd_avg.setData(x_hyd, y_hyd_avg, brush=color_hyd)
                self.scatter_dehyd.setData(x_dehyd, y_dehyd, brush=color_dehyd)
                self.scatter_dehyd_avg.setData(x_dehyd, y_dehyd_avg, brush=color_dehyd)

                self.addItem(self.scatter_hyd)
                self.addItem(self.scatter_hyd_avg)  # Add scatter plot item to the plot widget
                self.addItem(self.scatter_dehyd)
                self.addItem(self.scatter_dehyd_avg)


                label_hyd = label_last_points(df_hyd["color_col"], x_hyd, y_hyd)
                label_dehyd = label_last_points(df_dehyd["color_col"], x_dehyd, y_dehyd)
                if label_hyd:
                    self.plotItem.addItem(label_hyd)
                if label_dehyd:
                    self.plotItem.addItem(label_dehyd)

                self.plotItem.setXRange(min(x), max(x))
                self.plotItem.setYRange(min(y), max(y))
                self.update()

        def closeEvent(self, event):
            """
            This method is called when the window is closed.
            """
            self.logger.info("Window is being closed")
            # Perform any cleanup or save tasks
            # Continue with the event#

            super().closeEvent(event)


class XYPlot(pg.PlotWidget):
    plot_cleared = Signal()
    cycle_number_sig = Signal(float)

    def __init__(self, parent=None):
        super().__init__(parent=parent)
        self.logger = logging.getLogger(__name__)
        # Initialize legend
        self.plotItem.addLegend(offset=(0, 1))
        self.table = TableConfig().ThermalConductivityXyDataTable
        self.last_click_time = 0
        self.debounce_interval = 0.5
        self.db_reader = DataRetriever()

    def _load_data(self, time_value, xy_data_to_load):
        """
        Loads xy_data_to_load from ETC xy table
        Parameters:
            xy_data_to_load (str): Name of the measurement
        Valid 'xy_data_to_load' include:
            'Name1' : 'Transient'
            'Name2' : 'Residual'
            'Name3' : 'Calculated'
            'Name4' : 'Drift'
        """
        current_time = time.time()
        if current_time - self.last_click_time > self.debounce_interval:

            self.last_click_time = current_time
            x_y = self.db_reader.fetch_xy_data(time_value=time_value, row_package_name=xy_data_to_load)
            self.db_reader.close_connection()
            load_cycle_number_thread = threading.Thread(target=self.load_cycle_number,
                                                        args=(time_value,))
            load_cycle_number_thread.start()
            return x_y
        else:
            return pd.DataFrame()

    def load_cycle_number(self, time_value):

        etc_table = TableConfig().ETCDataTable
        query = f"SELECT {etc_table.cycle_number} from {etc_table.table_name} WHERE {etc_table.time} = %s"

        cycle_number = self.db_reader.execute_fetching(query=query, values=(time_value,), column_names=etc_table.cycle_number)
        if not cycle_number.empty:
            self.cycle_number_sig.emit(cycle_number.iloc[0, 0])

    def add_curve_to_plot(self, time_value, color, xy_data_to_load):

        df = self._load_data(time_value=time_value, xy_data_to_load=xy_data_to_load)
        #print(df)
        if not df.empty:
            try:
                # Convert 'time' to timestamp if necessary
                if df.columns[0].lower() == self.table.time.lower():
                    df[df.columns[0]] = df[df.columns[0]].apply(lambda x: x.timestamp())

                # Extract new data points
                new_x_data = df[df.columns[0]].tolist()
                new_y_data = df[df.columns[1]].tolist()

                # Create a new plot data item for the new dataset
                new_plot_data_item = pg.PlotDataItem(new_x_data, new_y_data, pen=pg.mkPen(color=color, width=2))

                # Add the new plot data item to the plot
                self.plotItem.addItem(new_plot_data_item)

                # Optionally, update axis labels if necessary
                self.plotItem.setLabel('left', df.columns[1])  # Update Y-axis label
                self.plotItem.setLabel('bottom', df.columns[0])  # Update X-axis label

            except Exception as e:
                self.logger.error(f"Exception in add_curve_to_plot: {e}")

    def adjust_plot_size(self):
        # Adjust the plot size as needed when the parent widget is resized
        self.setGeometry(self.parent().rect())

    def closeEvent(self, event):
        """
        This method is called when the window is closed.
        """
        self.logger.info("Window is being closed.")
        # Perform any cleanup or save tasks
        # Continue with the event
        self.db_reader.close_connection()
        super().closeEvent(event)

    def clear_plot(self):
        global color_index_scatter
        color_index_scatter = 0
        try:
            # Remove all items from the plot
            self.plotItem.clear()

            # Reset stored plot data (if you're maintaining any)
            self.plot_data = {'x': [], 'y': []}
            self.plot_cleared.emit()

        except Exception as e:
            self.logger.error(f"Exception in clear_plot: {e}")



def test_read_plot_uptake():
    meta_data = MetaData(sample_id='WAE-WA-040')
    uptake_win = UptakePlot()
    uptake_win.load_data(meta_data=meta_data)
    return uptake_win


def test_read_plot_tp_dependent():
    time_start = datetime(2022, 5, 10, 5, 00, 00, tzinfo=local_tz_reg)
    time_end = datetime(2022, 5, 15, 5, 00, 00, tzinfo=local_tz_reg)
    time_range = (time_start, time_end)
    Tp_dependent_plot = ReadPlotTpDependent.PlotTpDependent()
    Tp_dependent_plot.load_data(time_range=time_range, x_col="pressure")
    return Tp_dependent_plot


def test_plots():
    app = QApplication(sys.argv)
    uptake_win = test_read_plot_uptake()
    uptake_win.show()
    tp_dependent_plot = test_read_plot_tp_dependent()
    tp_dependent_plot.show()

    sys.exit(app.exec())


def test_reading():
    import threading
    reader = ReadContinuous()
    thread = threading.Thread(target=reader.run, daemon=True)
    thread.start()
    time.sleep(5)
    reader.running = False
    thread.join()


def test_xy_read_plot():
    time_plot = PlotStaticWindow(y_axis="Temperature")
    time_plot.reader.is_test = True
    xy_plot = XYPlot()
    return time_plot, xy_plot


if __name__ == '__main__':
    app = QApplication([])
    meta_data = MetaData('WAE-WA-030')
    win = PlotStaticWindow(y_axis='temperature')
    win.point_clicked_time_received.connect(print)
    #win2 = PlotStaticWindow(y_axis='pressure')
    win.reader.meta_data = meta_data

    win.show()
    sys.exit(app.exec())



