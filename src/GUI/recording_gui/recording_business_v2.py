#recording_busines_v2.py
#!/usr/bin/env python
#!/usr/bin/env python
"""
--------------------------------------------------------------------------------
This module contains:
    - Business logic for data recording (T-p via Modbus and ETC log tracking)
    - Plotting window classes for continuous and static plotting
    - Specialized plot widgets for hydrogen uptake, TP-dependent ETC data, and XY data.
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
from src.config_connection_reading_management.database_reading_writing import DataRetriever
from src.config_connection_reading_management.hot_disk_log_file_tracker import LogFileTracker
from src.meta_data.meta_data_handler import MetaData
from src.config_connection_reading_management.modbus_handler import ModbusProcessor
from src.table_data import TableConfig
# Import plot window base classes (assumed to be unchanged)
from src.GUI.recording_gui.plot_window_reader_basic import (
    PlotBaseWindow, ReadStatic, ReadContinuous, DateAxisItem, AxisLabel
)
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging

# Global colors for plotting
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

    Attributes:
        newEtcDataWritten (Signal): Emits a DataFrame when new ETC data is available.

    Methods:
        start_all_recording() / stop_all_recording(): Start/stop both recording threads.
        update_sample_id(), update_meta_data(), update_reservoir_volume(), update_cycling_flag(),
            update_h2_uptake_flag(): Update configuration dynamically.
    """
    newEtcDataWritten = Signal(pd.DataFrame)

    def __init__(self, meta_data: MetaData = MetaData(), reservoir_volume: float = None):
        """
        Initialize the DataRecorder with metadata and optional reservoir volume.
        """
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
        """
        Create and return a ModbusProcessor for T-p data recording.
        """
        try:
            return ModbusProcessor(meta_data=meta_data)
        except Exception as e:
            self.logger.exception("Error creating ModbusProcessor:")
            raise

    def _create_log_tracker(self, meta_data):
        """
        Create and return a LogFileTracker for ETC log file tracking.
        """
        try:
            return LogFileTracker(meta_data=meta_data)
        except Exception as e:
            self.logger.exception("Error creating LogFileTracker:")
            raise

    def start_all_recording(self):
        """
        Start both T-p and ETC recording threads.
        """
        try:
            self.start_tp_recording()
            self.start_etc_recording()
        except Exception as e:
            self.logger.exception("Exception starting all recording:")

    def stop_all_recording(self):
        """
        Stop both T-p and ETC recording threads.
        """
        try:
            self.stop_tp_recording()
            self.stop_etc_recording()
        except Exception as e:
            self.logger.exception("Exception stopping all recording:")

    def start_tp_recording(self):
        """
        Start the T-p recording thread using the ModbusProcessor.
        """
        self.logger.info("Starting T-p recording thread...")
        try:
            if self._mb_thread is None:
                self._mb_thread = threading.Thread(target=self.mb_processor.run, daemon=True)
                self._mb_thread.start()
                self.logger.info("T-p recording thread started.")
        except Exception as e:
            self.logger.exception("Error starting T-p recording thread:")

    def stop_tp_recording(self):
        """
        Stop the T-p recording thread and wait for it to finish.
        """
        self.logger.info("Stopping T-p recording thread...")
        try:
            if self._mb_thread is not None:
                self.mb_processor.stop()
                self._mb_thread.join()
                self._mb_thread = None
                self.logger.info("T-p recording thread stopped.")
        except Exception as e:
            self.logger.exception("Error stopping T-p recording thread:")

    def start_etc_recording(self):
        """
        Start the ETC recording thread using the LogFileTracker.
        """
        self.logger.info("Starting ETC recording thread...")
        try:
            if self._log_tracker_thread is None:
                self._log_tracker_thread = threading.Thread(target=self.log_tracker.start, daemon=True)
                self._log_tracker_thread.start()
                self.log_tracker.time_range_etc_import.connect(self._emit_etc_data)
                self.logger.info("ETC recording thread started.")
        except Exception as e:
            self.logger.exception("Error starting ETC recording thread:")

    def stop_etc_recording(self):
        """
        Stop the ETC recording thread.
        """
        self.logger.info("Stopping ETC recording thread...")
        try:
            if self._log_tracker_thread is not None:
                self._log_tracker_thread.join()
                self._log_tracker_thread = None
                self.logger.info("ETC recording thread stopped.")
        except Exception as e:
            self.logger.exception("Error stopping ETC recording thread:")

    def update_sample_id(self, new_sample_id: str):
        """
        Update the sample ID in both the ModbusProcessor and LogFileTracker.
        """
        try:
            with self._update_lock:
                self.mb_processor.on_sample_id_change(new_val=new_sample_id)
                self.log_tracker.update_sample_id(new_val=new_sample_id)
            self.logger.info(f"Updated sample ID to: {new_sample_id}")
        except Exception as e:
            self.logger.exception("Error updating sample ID:")

    def update_meta_data(self, new_meta_data: MetaData):
        """
        Update metadata information.
        """
        try:
            with self._update_lock:
                self.mb_processor.on_sample_id_change(new_val=new_meta_data, mode="meta")
                self.log_tracker.update_sample_id(new_val=new_meta_data, mode="meta")
            self.logger.info(f"Meta data updated: {new_meta_data.sample_id}")
        except Exception as e:
            self.logger.exception("Error updating meta data:")

    def update_reservoir_volume(self, new_volume: float):
        """
        Update the reservoir volume used in T-p processing.
        """
        try:
            with self._update_lock:
                self.mb_processor.reservoir_volume = new_volume
            self.logger.info(f"Reservoir volume updated to: {new_volume}")
        except Exception as e:
            self.logger.exception("Error updating reservoir volume:")

    def update_cycling_flag(self, flag: bool):
        """
        Update the cycling flag in the processors.
        """
        try:
            with self._update_lock:
                self.mb_processor.cycling_flag = flag
                self.mb_processor.mb_data_handler.cycling_flag = flag
            self.logger.info(f"Cycling flag set to: {flag}")
        except Exception as e:
            self.logger.exception("Error updating cycling flag:")

    def update_h2_uptake_flag(self, flag: bool):
        """
        Update the hydrogen uptake flag in the processors.
        """
        try:
            with self._update_lock:
                self.mb_processor.h2_uptake_flag = flag
                self.mb_processor.mb_data_handler.h2_uptake_flag = flag
            self.logger.info(f"H2 uptake flag set to: {flag}")
        except Exception as e:
            self.logger.exception("Error updating H2 uptake flag:")

    def _emit_etc_data(self, time_range):
        """
        Fetch ETC data within the specified time range and emit it via the newEtcDataWritten signal.
        """
        try:
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
        except Exception as e:
            self.logger.exception("Error emitting ETC data:")

    def is_running(self):
        return True if self._mb_thread else False

# -------------------------------
# Presentation Layer / Plotting
# -------------------------------
class ContinuousPlotWindow(PlotBaseWindow):
    """
    A PlotBaseWindow subclass for continuous plotting.

    Signals:
        current_cycle_sig, current_state_sig, current_uptake_sig: Relayed signals from the reader.
    """
    def __init__(self, parent=None, y_axis='', meta_data: MetaData = MetaData()):
        try:
            self.reader = ReadContinuous(meta_data=meta_data)
            super().__init__(parent=parent, y_axis=y_axis)
            # Relay key signals to be used by the controller
            self.reader.current_cycle_sig.connect(self.current_cycle_sig.emit)
            self.reader.current_state_sig.connect(self.current_state_sig.emit)
            self.reader.current_uptake_sig.connect(self.current_uptake_sig.emit)
            self.enableAutoRange()
        except Exception as e:
            logging.getLogger(__name__).exception("Error initializing ContinuousPlotWindow:")


class StaticPlotWindow(PlotBaseWindow):
    """
    A PlotBaseWindow subclass for static plotting.
    """
    def __init__(self, parent=None, y_axis='', meta_data: MetaData = MetaData()):
        try:
            self.reader = ReadStatic(meta_data=meta_data)
            super().__init__(parent=parent, y_axis=y_axis)
            self.enableAutoRange(axis=pg.ViewBox.XYAxes)
            self.reader.start()
            self.reader.whole_test_emited_sig.connect(self._init_on_x_range_changed)
        except Exception as e:
            logging.getLogger(__name__).exception("Error initializing StaticPlotWindow:")

    def _init_on_x_range_changed(self):
        """
        Initialize x-axis range change behavior after static data is loaded.
        """
        try:
            self.disableAutoRange()
            self.plotItem.sigXRangeChanged.connect(self._on_x_range_changed)
        except Exception as e:
            logging.getLogger(__name__).exception("Error in _init_on_x_range_changed:")


class UptakePlot(pg.PlotWidget):
    """
    Specialized PlotWidget to display H2 uptake versus cycle.

    Signals:
        uptakeDataReceived (Signal): Emits a DataFrame with uptake data.
    """
    uptakeDataReceived = Signal(pd.DataFrame)

    def __init__(self, parent=None):
        """
        Initialize the UptakePlot widget and its UI elements.
        """
        try:
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
        except Exception as e:
            logging.getLogger(__name__).exception("Error initializing UptakePlot:")

    def _init_ui(self):
        """
        Initialize the axes labels for the uptake plot.
        """
        try:
            axis_font = {'color': 'white', 'font-size': '12pt'}
            self.plotItem.getAxis('bottom').setLabel(AxisLabel.create_axis_label("cycle"), **axis_font)
            self.plotItem.getAxis('left').setLabel(AxisLabel.create_axis_label("uptake"), **axis_font)
        except Exception as e:
            self.logger.exception("Error initializing UI in UptakePlot:")

    def load_data(self, meta_data: MetaData, time_range=None):
        """
        Load uptake data from the database based on the sample ID and time range.
        """
        try:
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
        except Exception as e:
            self.logger.exception("Error loading data in UptakePlot:")

    def plot_uptake(self, df: pd.DataFrame):
        """
        Process and plot the uptake data.
        """
        try:
            self.plotItem.clear()
            df = df.dropna(subset=[self.cycle_table.h2_uptake])
            if self.plotItem.legend is None:
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
        except Exception as e:
            self.logger.exception("Error plotting uptake data:")


class ReadPlotTpDependent(pg.PlotWidget):
    """
    Custom PlotWidget for displaying Effective Thermal Conductivity (ETC) data
    that depends on either pressure or temperature.
    """

    class ReadTpDependent(QThread):
        """
        QThread subclass to load and sort TP and ETC data for plotting.

        Signals:
            tp_etc_data (Signal): Emits a tuple (DataFrame, x_axis_label, y_axis_label).
        """
        tp_etc_data = Signal(pd.DataFrame, str, str)

        def __init__(self, parent=None, constraints=None):
            """
            Initialize the data loading thread.
            """
            super().__init__(parent=parent)
            self.logger = logging.getLogger(__name__)
            self.db_reader = DataRetriever()
            self.constraints = constraints
            self.tp_table = TableConfig().TPDataTable
            self.etc_table = TableConfig().ETCDataTable
            self.df_etc_storage = None
            self.time_range = None
            self.x_col = None

        def run(self):
            """
            Execute data fetching and sorting, then emit the data for plotting.
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

                if not df_for_plot.empty:
                    self.tp_etc_data.emit(df_for_plot, x_axis_label, y_axis_label)
            except Exception as e:
                self.logger.exception("Exception in loading tp-dependent data:")

        def _sort_data(self, df_tp=pd.DataFrame(), df_etc=pd.DataFrame(), x_col="", df_tp_etc=pd.DataFrame()):
            """
            Sort and merge TP and ETC data based on the specified x-axis column.

            Returns:
                tuple: (DataFrame for plot, x_axis_label, y_axis_label)
            """
            try:
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

                if not df_etc.empty and not df_tp.empty:
                    df_etc = df_etc.sort_values(self.etc_table.get_clean('time'))
                    df_etc = df_etc.rename(columns={self.etc_table.get_clean('time'): self.tp_table.time})
                    df_tp = df_tp.sort_values(self.tp_table.time)
                    # Perform asof merge to align TP and ETC data based on time
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
            except Exception as e:
                self.logger.exception("Error sorting data in ReadTpDependent:")
                return pd.DataFrame(), "", ""

        def _set_params(self, time_range, x_col):
            """
            Set the parameters for data fetching.
            """
            try:
                self.time_range = time_range
                self.x_col = x_col
            except Exception as e:
                self.logger.exception("Error setting parameters in ReadTpDependent:")

    class PlotTpDependent(pg.PlotWidget):
        """
        PlotWidget subclass to display ETC data that depends on either pressure or temperature.
        """
        def __init__(self, parent=None, constraints=None):
            try:
                super().__init__(parent=parent)
                self.logger = logging.getLogger(__name__)
                self.reader = ReadPlotTpDependent.ReadTpDependent(parent=parent, constraints=constraints)
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

                self.addItem(self.scatter_hyd)
                self.addItem(self.scatter_hyd_avg)
                self.addItem(self.scatter_dehyd)
                self.addItem(self.scatter_dehyd_avg)

                self.legend.addItem(self.scatter_hyd, 'ETC Hydrogenated')
                self.legend.addItem(self.scatter_hyd_avg, 'ETC Hydrogenated Average')
                self.legend.addItem(self.scatter_dehyd, 'ETC Dehydrogenated')
                self.legend.addItem(self.scatter_dehyd_avg, 'ETC Dehydrogenated Average')
            except Exception as e:
                logging.getLogger(__name__).exception("Error initializing PlotTpDependent:")

        def load_data(self, time_range, x_col):
            """
            Trigger the data loading thread with the provided time range and x-axis column.
            """
            try:
                self.reader._set_params(time_range, x_col)
                self.reader.start()
            except Exception as e:
                self.logger.exception("Error loading data in PlotTpDependent:")

        def plot_ETC(self, df, x_axis_label, y_axis_label):
            """
            Process and plot the ETC data.
            """
            try:
                def split_by_state(df, state):
                    mask = df[self.tp_table.de_hyd_state] == state
                    return df.loc[mask]

                def create_color_map(color_vals):
                    c_map = pg.colormap.get('CET-L4')
                    normalized_vals = (color_vals - color_vals.min()) / (color_vals.max() - color_vals.min())
                    colors = c_map.map(normalized_vals, mode='qcolor')
                    return colors

                def label_last_points(color_col, x, y):
                    color_col = color_col.copy()
                    rounded_values = color_col.round()
                    x = x.round()
                    change_indices = rounded_values.ne(rounded_values.shift()).index[rounded_values.ne(rounded_values.shift())]
                    last_point_list = change_indices.tolist()
                    for idx in last_point_list:
                        x_val = x.loc[idx]
                        y_val = y.loc[idx]
                        color_val = color_col.loc[idx]
                        text = pg.TextItem(f'{color_val}', anchor=(1, 1))
                        text.setPos(x_val, y_val)
                        return text

                self.plotItem.clear()
                if not df.empty:
                    self.plotItem.getAxis('bottom').setLabel(x_axis_label)
                    self.plotItem.getAxis('left').setLabel(y_axis_label)

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

                    self.scatter_hyd.setData(x_hyd, y_hyd, brush=color_hyd)
                    self.scatter_hyd_avg.setData(x_hyd, y_hyd_avg, brush=color_hyd)
                    self.scatter_dehyd.setData(x_dehyd, y_dehyd, brush=color_dehyd)
                    self.scatter_dehyd_avg.setData(x_dehyd, y_dehyd_avg, brush=color_dehyd)

                    self.addItem(self.scatter_hyd)
                    self.addItem(self.scatter_hyd_avg)
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
            except Exception as e:
                self.logger.exception("Error plotting ETC data in PlotTpDependent:")

        def closeEvent(self, event):
            """
            Handle widget close event and perform necessary cleanup.
            """
            try:
                self.logger.info("Window is being closed")
                super().closeEvent(event)
            except Exception as e:
                self.logger.exception("Error during closeEvent in PlotTpDependent:")


class XYPlot(pg.PlotWidget):
    """
    PlotWidget for displaying XY curves (e.g., for thermal conductivity data).

    Signals:
        plot_cleared (Signal): Emitted when the plot is cleared.
        cycle_number_sig (Signal): Emits the cycle number on data load.
    """
    plot_cleared = Signal()
    cycle_number_sig = Signal(float)

    def __init__(self, parent=None):
        """
        Initialize the XYPlot widget.
        """
        try:
            super().__init__(parent=parent)
            self.logger = logging.getLogger(__name__)
            self.plotItem.addLegend(offset=(0, 1))
            self.table = TableConfig().ThermalConductivityXyDataTable
            self.last_click_time = 0
            self.debounce_interval = 0.5
            self.db_reader = DataRetriever()
        except Exception as e:
            logging.getLogger(__name__).exception("Error initializing XYPlot:")

    def _load_data(self, time_value, xy_data_to_load):
        """
        Load XY data from the database for the given time value and package name.
        """
        try:
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
        except Exception as e:
            self.logger.exception("Error loading data in XYPlot:")
            return pd.DataFrame()

    def load_cycle_number(self, time_value):
        """
        Load and emit the cycle number for a given time value.
        """
        try:
            etc_table = TableConfig().ETCDataTable
            query = f"SELECT {etc_table.cycle_number} from {etc_table.table_name} WHERE {etc_table.time} = %s"
            cycle_number = self.db_reader.execute_fetching(query=query, values=(time_value,), column_names=etc_table.cycle_number)
            if not cycle_number.empty:
                self.cycle_number_sig.emit(cycle_number.iloc[0, 0])
        except Exception as e:
            self.logger.exception("Error in load_cycle_number in XYPlot:")

    def add_curve_to_plot(self, time_value, color, xy_data_to_load):
        """
        Load data and add a new curve to the plot.
        """
        try:
            df = self._load_data(time_value=time_value, xy_data_to_load=xy_data_to_load)
            if not df.empty:
                try:
                    if df.columns[0].lower() == self.table.time.lower():
                        df[df.columns[0]] = df[df.columns[0]].apply(lambda x: x.timestamp())
                    new_x_data = df[df.columns[0]].tolist()
                    new_y_data = df[df.columns[1]].tolist()
                    new_plot_data_item = pg.PlotDataItem(new_x_data, new_y_data, pen=pg.mkPen(color=color, width=2))
                    self.plotItem.addItem(new_plot_data_item)
                    self.plotItem.setLabel('left', df.columns[1])
                    self.plotItem.setLabel('bottom', df.columns[0])
                except Exception as inner_e:
                    self.logger.exception("Error processing data in add_curve_to_plot:")
        except Exception as e:
            self.logger.exception("Error adding curve to plot in XYPlot:")

    def adjust_plot_size(self):
        """
        Adjust the plot size based on the parent's geometry.
        """
        try:
            self.setGeometry(self.parent().rect())
        except Exception as e:
            self.logger.exception("Error adjusting plot size in XYPlot:")

    def closeEvent(self, event):
        """
        Cleanup when the plot widget is closed.
        """
        try:
            self.logger.info("Window is being closed.")
            self.db_reader.close_connection()
            super().closeEvent(event)
        except Exception as e:
            self.logger.exception("Error during closeEvent in XYPlot:")

    def clear_plot(self):
        """
        Clear all items from the plot and reset stored plot data.
        """
        try:
            global color_index_scatter
            color_index_scatter = 0
            self.plotItem.clear()
            self.plot_data = {'x': [], 'y': []}
            self.plot_cleared.emit()
        except Exception as e:
            self.logger.exception("Error clearing plot in XYPlot:")


# -------------------------------
# Test functions for standalone execution
# -------------------------------
def test_read_plot_uptake():
    """
    Test function for UptakePlot.
    """
    try:
        meta_data = MetaData(sample_id='WAE-WA-040')
        uptake_win = UptakePlot()
        uptake_win.load_data(meta_data=meta_data)
        return uptake_win
    except Exception as e:
        logging.getLogger(__name__).exception("Error in test_read_plot_uptake:")

def test_read_plot_tp_dependent():
    """
    Test function for TP-dependent plot.
    """
    try:
        from zoneinfo import ZoneInfo
        local_tz_reg = ZoneInfo("Europe/Berlin")
        time_start = datetime(2022, 5, 10, 5, 0, 0, tzinfo=local_tz_reg)
        time_end = datetime(2022, 5, 15, 5, 0, 0, tzinfo=local_tz_reg)
        time_range = (time_start, time_end)
        Tp_dependent_plot = ReadPlotTpDependent.PlotTpDependent()
        Tp_dependent_plot.load_data(time_range=time_range, x_col="pressure")
        return Tp_dependent_plot
    except Exception as e:
        logging.getLogger(__name__).exception("Error in test_read_plot_tp_dependent:")

def test_plots():
    """
    Test function to display uptake and TP-dependent plots.
    """
    try:
        from PySide6.QtWidgets import QApplication
        app = QApplication(sys.argv)
        uptake_win = test_read_plot_uptake()
        uptake_win.show()
        tp_dependent_plot = test_read_plot_tp_dependent()
        tp_dependent_plot.show()
        sys.exit(app.exec())
    except Exception as e:
        logging.getLogger(__name__).exception("Error in test_plots:")

def test_reading():
    """
    Test function for ReadContinuous.
    """
    try:
        import threading
        reader = ReadContinuous()
        thread = threading.Thread(target=reader.run, daemon=True)
        thread.start()
        time.sleep(5)
        reader.running = False
        thread.join()
    except Exception as e:
        logging.getLogger(__name__).exception("Error in test_reading:")

def test_xy_read_plot():
    """
    Test function for XY plot.
    """
    try:
        from src.GUI.recording_gui.plot_window_reader_basic import PlotStaticWindow
        time_plot = PlotStaticWindow(y_axis="Temperature")
        time_plot.reader.is_test = True
        xy_plot = XYPlot()
        return time_plot, xy_plot
    except Exception as e:
        logging.getLogger(__name__).exception("Error in test_xy_read_plot:")

if __name__ == '__main__':
    try:
        from PySide6.QtWidgets import QApplication
        app = QApplication([])
        from src.GUI.recording_gui.plot_window_reader_basic import PlotStaticWindow
        meta_data = MetaData('WAE-WA-030')
        win = PlotStaticWindow(y_axis='temperature')
        win.point_clicked_time_received.connect(print)
        win.reader.meta_data = meta_data
        win.show()
        sys.exit(app.exec())
    except Exception as e:
        logging.getLogger(__name__).exception("Error in main block of recording_busines_v2.py:")
