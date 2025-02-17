import sys
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import numpy as np

import pandas as pd
import pyqtgraph as pg
from PySide6.QtCore import (QDateTime, QThread, QTimeZone, QTimer, QObject,
                            Signal, Slot, QRunnable, QThreadPool)
from PySide6.QtWidgets import QApplication

from src.config_connection_reading_management.connections import DatabaseConnection
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging
from src.config_connection_reading_management.database_reading_writing import DataRetriever
from src.config_connection_reading_management.hot_disk_log_file_tracker import LogFileTracker
from src.meta_data.meta_data_handler import MetaData
from src.config_connection_reading_management.modbus_handler import ModbusProcessor
from src.table_data import TableConfig


time_format_str = "yyyy-MM-dd HH:mm:ss"
local_tz = QTimeZone(b'Europe/Berlin')
local_tz_reg = ZoneInfo('Europe/Berlin')
colors = [
    "#FF0000",  # Red
    "#00FF00",  # Lime
    "#0000FF",  # Blue
    "#FFFF00",  # Yellow
    "#00FFFF",  # Cyan
    "#FF00FF",  # Magenta
    "#800000",  # Maroon
    "#808000",  # Olive
    "#008000",  # Green
    "#800080",  # Purple
    "#008080",  # Teal
    "#000080",  # Navy
    "#FFA500",  # Orange
    "#A52A2A",  # Brown
    "#20B2AA",  # Light Sea Green
    "#778899",  # Light Slate Gray
    "#D2691E",  # Chocolate
    "#DC143C",  # Crimson
    "#7FFF00",  # Chartreuse
    "#6495ED"   # Cornflower Blue
]
colors_scatter = colors.copy()
READING_MODE_FULL_TEST = 'full_test'
READING_MODE_BY_TIME = 'by_time'


#class DateAxisItem(pg.AxisItem):
  #  def tickStrings(self, values, scale, spacing):
  #      global time_format_str
  #      return [QDateTime.fromSecsSinceEpoch(int(value)).toString(time_format_str) for value in values]

###############################################################################
#                         Data Reading Threads                                #
###############################################################################
class ReadData(QThread):
    """
    Base class for reading data and emitting signals.
    """
    T_data_sig = Signal(pd.DataFrame)
    p_data_sig = Signal(pd.DataFrame)
    etc_data_sig = Signal(pd.DataFrame)
    meta_data_sig = Signal(MetaData)
    current_cycle_sig = Signal(float)
    current_state_sig = Signal(str)
    current_uptake_sig = Signal(float)
    cycles_full_test_sig = Signal(pd.DataFrame)
    auto_update_x_range_sig = Signal()


    def __init__(self, meta_data=MetaData()):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.db_retriever = DataRetriever()
        self.limit_amount_storage = self.db_retriever.limit_datapoints
        self.running = False
        self.T_max = 1000
        self.p_max = 500
        self.T_data = pd.DataFrame()
        self.p_data = pd.DataFrame()
        self.etc_data = pd.DataFrame()
        self.current_cycle = None
        self.current_state = None
        self.current_uptake = None
        self.meta_data = meta_data
        self.constraints_t_p = None
        self.constraints_etc = self.standard_constraints("etc")
        self.time_range_to_read = None

    def run(self):
        pass  # Implemented in subclasses

    def stop(self):
        self.running = False
        self.quit()
        self.wait()

    def _standard_constraints(self, mode="etc"):
        """Return standard constraints based on the mode."""
        if mode == "etc":
            return {
                "min_TotalCharTime": 0.33,
                "max_TotalCharTime": 1,
                "min_TotalTempIncr": 2,
                "max_TotalTempIncr": 5
            }
        return None

    # Common methods used by both reading modes
    def _read_t_p(self, cursor=None, desc_limit=1, time_range=None):
        table = TableConfig().TPDataTable
        table_name = table.table_name

        try:
            if not cursor:
                if time_range:
                    previous_desc_limit = self.db_retriever.limit_datapoints
                    self.db_retriever.limit_datapoints = desc_limit
                    df = self.db_retriever.fetch_data_by_time_2(
                        time_range=time_range,
                        table_name=table.table_name)
                    self.db_retriever.limit_datapoints = previous_desc_limit
                    return df
                else:
                    return pd.DataFrame()

            df = self.db_retriever.fetch_latest_records(
                table_name=table_name,
                cursor=cursor,
                constraints=self.constraints_t_p,
                desc_limit=desc_limit,
                sample_id=self.meta_data.sample_id)
            if not df.empty:
                df = df.sort_values(by=table.time, ascending=True)

            if not df[table.sample_id].iloc[-1] == self.meta_data.sample_id:
                self.meta_data = MetaData(sample_id=df[table.sample_id].iloc[-1])
                self.meta_data_sig.emit(self.meta_data)
                self.logger.info(f"Sample ID changed to {self.meta_data.sample_id}")

            return df
        except Exception as e:
            self.logger.error(f"Error while loading T p data: {e}")
            return pd.DataFrame()

    def _read_data_by_time(self):
        """Read T-p and ETC data for a given time range and emit signals."""
        self.T_data = pd.DataFrame()
        self.p_data = pd.DataFrame()
        self.etc_data = pd.DataFrame()

        tp_table = TableConfig().TPDataTable
        # Read T-p data
        df_t_p = self.db_retriever.fetch_data_by_time_2(
                                                        time_range=self.time_range_to_read,
                                                        table_name=tp_table.table_name,
                                                        sample_id=self.meta_data.sample_id)
        self._separate_and_append_t_p(df_t_p=df_t_p)
        # Read ETC data
        self.etc_data = self._read_etc(time_range=self.time_range_to_read)

        if not self.T_data.empty:
            self.T_data_sig.emit(self.T_data)
        if not self.p_data.empty:
            self.p_data_sig.emit(self.p_data)
        if not self.etc_data.empty:
            self.etc_data_sig.emit(self.etc_data)

        self._read_emit_cycles_full_test_thread()

    def _separate_and_append_t_p(self, df_t_p):
        """
        Separate T-p data into temperature and pressure subsets,
        remove outliers, append to storage, and trim excess data.
        """
        table = TableConfig().TPDataTable

        time_temperature_columns = [col for col in df_t_p.columns if "time" in col.lower()
                                    or "temperature" in col.lower()
                                    or ("setpoint" in col.lower() and "sample" in col.lower())
                                    or "state" in col.lower()]
        time_pressure_columns = [col for col in df_t_p.columns if "time" in col.lower()
                                 or "pressure" in col.lower()
                                 or "state" in col.lower()]

        temperature_data = df_t_p[time_temperature_columns].copy()
        pressure_data = df_t_p[time_pressure_columns].copy()
        temperature_data = self._remove_outliers(temperature_data)
        pressure_data = self._remove_outliers(pressure_data)

        # Append data to storage
        if not temperature_data.empty:
            self.T_data = pd.concat([self.T_data, temperature_data], ignore_index=True)

        if not pressure_data.empty:
            self.p_data = pd.concat([self.p_data, pressure_data], ignore_index=True)

        # Limit storage to a fixed number of points
        for data in [self.T_data, self.p_data]:
            if len(data) > self.limit_amount_storage:
                drop_count = len(data) - self.limit_amount_storage
                data.drop(data.index[:drop_count], inplace=True)

    def _remove_outliers(self, df):
        """
        Remove outliers from numeric columns.
        Currently replaces values above T_max with NaN.
        """
        numeric_cols = df.select_dtypes(include=[np.number]).columns.difference(['time', 'state'])
        for col in numeric_cols:
            df.loc[df[col] > self.T_max, col] = np.nan
        return df

    def _read_etc(self, time_range):
        """
        Read ETC data for a given time range.
        """
        table = TableConfig().ETCDataTable
        cols = (table.time,
                        table.th_conductivity,
                        table.thermal_conductivity_average)
        try:
            df = self.db_retriever.fetch_data_by_time_2(
                time_range=time_range,
                table_name=table.table_name,
                column_names=cols,
                constraints=self.constraints_etc,
                sample_id=self.meta_data.sample_id)
            return df
        except Exception as e:
            self.logger.error(f"Error while loading ETC data: {e}")
            return pd.DataFrame()

    def update_constraints_etc(self, new_constraints: dict):
        self.constraints_etc = new_constraints
        self.logger.info("Updated ETC constraints: {}".format(self.constraints_etc))

    def update_constraints_t_p(self, new_constraints: dict):
        self.constraints_t_p = new_constraints
        self.logger.info("Updated T p constraints: {}".format(self.constraints_t_p))

    def _read_emit_uptake_last_cycle(self):
        table = TableConfig().CycleDataTable
        if self.current_cycle:

            df_one_cycle = self.db_retriever.fetch_data_by_cycle(
                cycle_numbers=self.current_cycle - 0.5,
                sample_id=self.meta_data.sample_id,
                table=table)

            if not df_one_cycle.empty:
                self.current_uptake = df_one_cycle[TableConfig().CycleDataTable.h2_uptake].iloc[-1]
                self.current_uptake_sig.emit(self.current_uptake)

    def _read_emit_cycles_full_test_thread(self):
        """
        Reads and emits cycle data for the full test.
        """
        cycle_table = TableConfig().CycleDataTable
        column_names = (cycle_table.time_min, cycle_table.pressure_min, cycle_table.temperature_min,
                        cycle_table.time_max, cycle_table.pressure_max, cycle_table.temperature_max)
        if self.meta_data.sample_id:
            if not self.time_range_to_read:
                df_cycles = self.db_retriever.fetch_data_by_sample_id_2(
                    sample_id=self.meta_data.sample_id,
                    table_name=cycle_table.table_name,
                    column_names=column_names)
            else:
                df_cycles = self.db_retriever.fetch_data_by_time_2(
                    time_range=self.time_range_to_read,
                    table_name=cycle_table.table_name,
                    column_names=column_names,
                    sample_id=self.meta_data.sample_id)
            if not df_cycles.empty:
                self.cycles_full_test_sig.emit(df_cycles)

    def on_meta_data_changed(self, new_meta_data=MetaData()):
        if new_meta_data.sample_id:
            self.meta_data = new_meta_data

    def standard_constraints(self, mode="etc"):
        if mode == "etc":
            constraints_dict = {
                "min_TotalCharTime": 0.33,
                "max_TotalCharTime": 1,
                "min_TotalTempIncr": 2,
                "max_TotalTempIncr": 5
            }
        else:
            constraints_dict = None
        return constraints_dict


class ReadContinuous(ReadData):
    """
    Thread class for continuously reading data and updating plots.
    Uses QTimer to schedule periodic data reads.
    """

    def __init__(self, meta_data=MetaData()):
        super().__init__(meta_data)
        self.reading_mode = "continuous"
        self.db_connection = None  # Initialize the database connection to None

        #self.tp_timer.timeout.connect(print("fire"))

    def run(self):
        try:
            # Open the database connection
            self.running = True
            self.db_connection = DatabaseConnection()
            self.db_connection.open_connection()
            self.cursor = self.db_connection.cursor

            # Set up timers for T-p and ETC data reads
            self.etc_timer = QTimer()
            self.etc_timer.setInterval(15 * 60 * 1000)  # 15 minutes in milliseconds
            self.etc_timer.timeout.connect(self._read_emit_etc_data)

            self.tp_timer = QTimer()
            self.tp_timer.setInterval(1000)  # 1 second in milliseconds
            self.tp_timer.timeout.connect(self._read_emit_tp_data)
            # Start the timers

            self.etc_timer.start()
            self.tp_timer.start()

            self.exec()  # Start the event loop
        finally:
            # Ensure the connection is closed when the thread finishes
            self.stop()

    def _read_emit_tp_data(self):

        if not self.running:
            return
        try:
            t_p_df = self._read_t_p(self.cursor)
            if t_p_df.empty:
                self.logger.debug("No new T-p data available.")
                return

            self.logger.debug(f"Read T-p data: {t_p_df.head()}")
            self._separate_and_append_t_p(t_p_df)
            last_row = t_p_df.iloc[-1]

            # Check for cycle or state change
            if (self.current_cycle != last_row[TableConfig().TPDataTable.cycle_number] or
                    self.current_state != last_row[TableConfig().TPDataTable.de_hyd_state]):
                self.current_cycle = last_row[TableConfig().TPDataTable.cycle_number]
                self.current_state = last_row[TableConfig().TPDataTable.de_hyd_state]
                self.current_cycle_sig.emit(self.current_cycle)
                self.current_state_sig.emit(self.current_state)
                self._read_emit_uptake_last_cycle()

            if not self.T_data.empty:
                self.T_data_sig.emit(self.T_data)
            if not self.p_data.empty:
                self.p_data_sig.emit(self.p_data)
        except Exception as e:
            self.logger.error(f"Error in _read_emit_tp_data: {e}")
            # Attempt to reconnect
            self._attempt_reconnect()

    def _read_emit_etc_data(self):
        """Read ETC data and emit signal."""
        #todo: only works if t_p_data exists in time_range of etc recording.....
        if not self.running:
            return
        try:
            if self.T_data.empty:
                return
            time_min = self.T_data[TableConfig().TPDataTable.time].min()
            time_max = self.T_data[TableConfig().TPDataTable.time].max()
            time_range = (time_min, time_max)
            self.etc_data = self._read_etc(time_range)
            if not self.etc_data.empty:
                self.etc_data_sig.emit(self.etc_data)
        except Exception as e:
            self.logger.error(f"Error in _read_emit_etc_data: {e}")
            # Attempt to reconnect
            self._attempt_reconnect()

    def _attempt_reconnect(self):
        """
        Attempt to reconnect to the database if the connection is lost.
        """
        self.logger.info("Attempting to reconnect to the database...")
        try:
            if self.db_connection:
                self.db_connection.close_connection()
            self.db_connection = DatabaseConnection()
            self.cursor = self.db_connection.cursor
            self.logger.info("Reconnected to the database.")
        except Exception as e:
            self.logger.error(f"Failed to reconnect to the database: {e}")
            self.stop()

    def stop(self):
        self.running = False
        # Stop the timers
        if hasattr(self, 'tp_timer'):
            self.tp_timer.stop()
        if hasattr(self, 'etc_timer'):
            self.etc_timer.stop()
        # Close the database connection
        if self.db_connection:
            self.db_connection.close_connection()
            self.db_connection = None
        self.quit()
        self.wait()


class ReadStatic(ReadData):
    """
    Thread class for static data reading (e.g., for full-test plots).
    """
    whole_test_emited_sig = Signal()

    def __init__(self, meta_data=MetaData()):
        super().__init__(meta_data)
        self.reading_mode = READING_MODE_FULL_TEST  # Could be "full test" or "by time"

    def start(self, reading_mode: str=None):
        if reading_mode is not None:
            self._previous_reading_mode = self.reading_mode
            self.reading_mode = reading_mode
        super().start()
        #if reading_mode is not None:
        #    self.reading_mode = reading_mode_buffer

    def run(self):
        if not self.reading_mode:
            return
        self.running = True
        if self.reading_mode.lower() == "full_test":
            self._read_full_test()

        elif self.reading_mode.lower() == "by_time":
            self._read_data_by_time()

        if hasattr(self, '_previous_reading_mode') and self._previous_reading_mode is not None:
            self.reading_mode = self._previous_reading_mode
            del self._previous_reading_mode  # Clean up
        self.running = False

    def _read_full_test(self):
        """
        Reads full test data.
        """
        previous_limit = self.db_retriever.limit_datapoints
        self.db_retriever.limit_datapoints = self.limit_amount_storage
        self.T_data = pd.DataFrame()
        self.p_data = pd.DataFrame()
        self.etc_data = pd.DataFrame()
        etc_table = TableConfig().ETCDataTable
        etc_cols = (etc_table.time,
                    etc_table.th_conductivity,
                    etc_table.thermal_conductivity_average)

        df_t_p, self.etc_data = self.db_retriever.fetch_tp_and_etc_data(
            sample_id=self.meta_data.sample_id,
            column_names_t_p=None,
            column_names_etc=etc_cols,
            constraints=self.constraints_etc)
        self._separate_and_append_t_p(df_t_p=df_t_p)
        self.db_retriever.limit_datapoints = previous_limit

        if not self.T_data.empty:
            self.T_data_sig.emit(self.T_data)
        if not self.p_data.empty:
            self.p_data_sig.emit(self.p_data)
        if not self.etc_data.empty:
            self.etc_data_sig.emit(self.etc_data)
            self.whole_test_emited_sig.emit()
            self.auto_update_x_range_sig.emit()
        self._read_emit_cycles_full_test_thread()

    def _change_reading_mode(self, new_reading_mode: str):
        self.reading_mode = new_reading_mode


###############################################################################
#                        Plotting Classes                                     #
###############################################################################
class PlotBaseStyle(pg.PlotWidget):
    """
    Base style for plotting windows.
    Sets up left and right axes, legends, and tick fonts.
    """

    def __init__(self, parent=None, y_axis=''):
        super().__init__(parent=parent)
        self.logger = logging.getLogger(__name__)
        self.axis_font = {'color': 'white', 'font-size': '12pt'}
        self.font_color = 'white'
        self.font_size = 10
        self.y_axis_str = y_axis
        self.point_colors = colors_scatter
        self.color_index_scatter = 0
        self._init_left_axis(y_axis=y_axis)
        self._init_col_names(y_axis=y_axis)
        self._init_right_axis()

    ###style init
    def _init_col_names(self, y_axis):
        """Set column names based on the chosen y-axis."""
        self.y_axis = y_axis
        self.t_p_table = TableConfig().TPDataTable
        self.de_hyd_state_col = (self.t_p_table.time, self.t_p_table.de_hyd_state)
        self.etc_table = TableConfig().ETCDataTable
        if "temperature" in y_axis.lower():
            self.column_names_left = (self.t_p_table.time,
                                      self.t_p_table.temperature_sample,
                                      self.t_p_table.setpoint_sample,
                                      self.t_p_table.temperature_heater)
        elif "pressure" in y_axis.lower():
            self.column_names_left = (self.t_p_table.time,
                                      self.t_p_table.pressure,
                                      self.t_p_table.eq_pressure)

    def _init_left_axis(self, y_axis):

        x_axis = DateAxisItem(orientation='bottom')
        self.plotItem.setAxisItems({'bottom': x_axis})
        x_axis_label = AxisLabel.create_axis_label(column_name="Time")
        self.plotItem.getAxis('bottom').setLabel(x_axis_label, **self.axis_font)

        self.plotItem.addLegend(offset=(0, 1))
        y_axis_label = AxisLabel.create_axis_label(column_name=y_axis)
        self.plotItem.getAxis('left').setLabel(y_axis_label, **self.axis_font)
        self._set_tick_fonts(self.plotItem.getAxis('bottom'))
        self._set_tick_fonts(self.plotItem.getAxis('left'))

    def _init_right_axis(self):
        """Set up a secondary (right) axis and link it to a separate view box."""
        self.rightViewBox = pg.ViewBox()
        self.plotItem.showAxis('right')
        self.plotItem.scene().addItem(self.rightViewBox)
        self.plotItem.getAxis('right').linkToView(self.rightViewBox)
        self.rightViewBox.setXLink(self.plotItem)
        self.plotItem.getAxis('right').setZValue(-10000)
        self.plotItem.getAxis('right').setVisible(True)
        self.plotItem.vb.sigResized.connect(self._update_view)
        self.color_index_scatter = 0
        self._set_tick_fonts(self.plotItem.getAxis('right'))
        axis_label = AxisLabel.create_axis_label(column_name="conductivity")
        x_axis_label = AxisLabel.create_axis_label(column_name="time")
        self.plotItem.getAxis('right').setLabel(axis_label, **self.axis_font)
        self.plotItem.getAxis('bottom').setLabel(x_axis_label, **self.axis_font)
        self._set_tick_fonts(self.plotItem.getAxis('right'))

    def _set_tick_fonts(self, axis):
        """Set fonts and colors for the axis ticks."""
        font = pg.QtGui.QFont('Arial', self.font_size)
        axis.setTickFont(font)
        axis.setStyle(tickTextOffset=10)
        axis.setPen(pg.mkPen(self.font_color))  # Set the color of the axis line
        axis.setTextPen(pg.mkPen(self.font_color))  # Set the color of the tick labels

    def _customize_legend(self, legend):

        # todo: does not work yet
        font = pg.QtGui.QFont()
        font.setPointSize(12)  # Set the font size
        font.setItalic(True)  # Set the font to italic
        font.setFamily('Arial')  # Set the font family

        # Iterate through the legend items and set their font
        for sample, label in legend.items:
            label.setFont(font)

    def _update_view(self):
        """Update the geometry of the right view box to match the main view."""
        self.rightViewBox.setGeometry(self.plotItem.vb.sceneBoundingRect())
        self.rightViewBox.linkedViewChanged(self.plotItem.vb, self.rightViewBox.XAxis)

    def adjust_plot_size(self):
        """Adjust the plot size to fill the parent window."""
        self.setGeometry(self.parent().rect())

    ###create plot items
    def _create_plot_items_left(self, df, x):
        """Create plot items for the left axis based on the DataFrame columns."""
        self.plot_items = {}
        for idx, col in enumerate(self.column_names_left):
            if col == self.t_p_table.time:
                continue
            if col not in df.columns or df[col].isna().all():
                continue
            color = colors[idx % len(colors)]
            y = df[col].values
            plot_item = self.plotItem.plot(
                x=x,
                y=y,
                name=col,
                pen=pg.mkPen(color=color, width=2)
            )
            self.plot_items[col] = plot_item

    def _create_plot_item_right(self, col, x, y):
        """Create a scatter plot item for the right axis."""
        if "_avg" in col.lower():
            brush_color = "r"
        else:
            brush_color = "w"
        scatter_plot_item = pg.ScatterPlotItem(
            x=x,
            y=y,
            pen=pg.mkPen(None),
            brush=pg.mkBrush(color=brush_color),
            name=col
        )
        if "_avg" not in col.lower():
            scatter_plot_item.sigClicked.connect(self._on_point_clicked)
            x_data, y_data = scatter_plot_item.getData()
            self.point_colors = [pg.mkBrush(brush_color) for _ in range(len(x_data))]
            self.scatter_plot_item = scatter_plot_item  # Keep reference to this item
        self.rightViewBox.addItem(scatter_plot_item)
        self.rightViewBox.setXLink(self.plotItem)
        if not hasattr(self, 'scatter_plot_items_dict'):
            self.scatter_plot_items_dict = {}
        self.scatter_plot_items_dict[col] = scatter_plot_item


        self._customize_legend(self.plotItem.legend)

    def _create_min_max_plot(self, x, y, mode):
        if mode == 'max':
            self.cycle_data_plot_item_max = pg.ScatterPlotItem(
                x=x,
                y=y,
                pen=pg.mkPen(None),
                brush=pg.mkBrush(color="m"),
                name="Max values per cycle",
                symbol='x'
            )
            self.plotItem.addItem(self.cycle_data_plot_item_max)
        elif mode == 'min':
            self.cycle_data_plot_item_min = pg.ScatterPlotItem(
                x=x,
                y=y,
                pen=pg.mkPen(None),
                brush=pg.mkBrush(color="c"),
                name="Min values per cycle",
                symbol='x'
            )
            self.plotItem.addItem(self.cycle_data_plot_item_min)

    def closeEvent(self, event):
        self.logger.info("Window is being closed.")
        # Perform any cleanup or save tasks
        if hasattr(self, 'reader'):
            self.reader.stop()
        super().closeEvent(event)


class PlotBaseWindow(PlotBaseStyle):
    """
    Base plotting window for displaying data.
    """
    point_clicked_time_received = Signal(object, object)
    current_cycle_sig = Signal(float)
    current_state_sig = Signal(str)
    current_uptake_sig = Signal(float)

    def __init__(self, parent=None, y_axis=''):
        super().__init__(parent=parent, y_axis=y_axis)

        self.range_change_timer = QTimer(self)
        self.current_time_range = [datetime.now(tz=local_tz_reg),
                                   (datetime.now(tz=local_tz_reg)+timedelta(days=2))]  # Placeholder
        self.plotItem.setXRange(min(self.current_time_range).timestamp(),
                                max(self.current_time_range).timestamp())
        self.plotItem.vb.sigResized.connect(self._update_view)
        self.range_change_timer = QTimer()
        self.range_change_timer.setSingleShot(True)

        self._init_connections(y_axis=y_axis)

    def _init_connections(self, y_axis):
        self.range_change_timer.timeout.connect(self._on_range_change_timeout)

        if hasattr(self, 'reader'):
            self.reader.cycles_full_test_sig.connect(self.update_min_max_plot)
            self.reader.etc_data_sig.connect(self.update_plot_right)
            self.reader.auto_update_x_range_sig.connect(self.plotItem.autoRange)

            if y_axis == 'pressure':
                self.reader.p_data_sig.connect(self.update_plot_left)
            elif y_axis == 'temperature':

                self.reader.T_data_sig.connect(self.update_plot_left)

    ###updating plot items
    def update_plot_left(self, df):

        if df.empty:
            return
        x = [t.timestamp() for t in df[self.t_p_table.time]]
        # If plot items already exist, update them
        if hasattr(self, 'plot_items') and self.plot_items:
            for col, plot_item in self.plot_items.items():
                if col not in df.columns:
                    continue
                if df[col].isna().all():
                    continue
                y = df[col].values
                plot_item.setData(x=x, y=y)
        else:
            self._create_plot_items_left(df=df, x=x)

        self._customize_legend(self.plotItem.legend)

    def update_plot_right(self, df):
        if df.empty:
            return

        x = [t.timestamp() for t in df[self.etc_table.get_clean("time")]]
        # Initialize scatter_plot_items_dict if it doesn't exist
        if not hasattr(self, 'scatter_plot_items_dict'):
            self.scatter_plot_items_dict = {}

        # Remove plot items for columns that are no longer present
        existing_cols = set(self.scatter_plot_items_dict.keys())
        new_cols = set(df.columns) - {'time'}
        cols_to_remove = existing_cols - new_cols
        for col in cols_to_remove:
            plot_item = self.scatter_plot_items_dict.pop(col)
            self.rightViewBox.removeItem(plot_item)

        # Update existing plot items and create new ones as needed
        for col in df.columns:
            if col.lower() == 'time':
                continue
            if df[col].isna().all():
                continue
            y = df[col].values
            if col in self.scatter_plot_items_dict:
                scatter_plot_item = self.scatter_plot_items_dict[col]
                scatter_plot_item.setData(x=x, y=y)
                if "_avg" not in col.lower():
                    # Re-initialize point colors if necessary
                    x_data, y_data = scatter_plot_item.getData()
                    self.point_colors = [pg.mkBrush('w') for _ in range(len(x_data))]

            else:
                # Plot item for this column does not exist, create it
                self._create_plot_item_right(col, x, y)
                #self._update_x_range(x)

    def update_min_max_plot(self, df_cycles=pd.DataFrame()):
        if df_cycles.empty:
            return

        cycle_table = TableConfig().CycleDataTable
        df_cycles = df_cycles.dropna(subset=[cycle_table.time_min, cycle_table.time_max])

        if 'pressure' in self.y_axis_str.lower():
            x_min = df_cycles[cycle_table.time_min]
            y_min = df_cycles[cycle_table.pressure_min]
            x_max = df_cycles[cycle_table.time_max]
            y_max = df_cycles[cycle_table.pressure_max]
        elif 'temperature' in self.y_axis_str.lower():
            x_min = df_cycles[cycle_table.time_min]
            y_min = df_cycles[cycle_table.temperature_min]
            x_max = df_cycles[cycle_table.time_max]
            y_max = df_cycles[cycle_table.temperature_max]
        else:
            return

        x_min = [t.timestamp() for t in x_min]
        x_max = [t.timestamp() for t in x_max]

        # Update existing plot items or create them if they don't exist
        if hasattr(self, 'cycle_data_plot_item_min') and self.cycle_data_plot_item_min is not None:
            # Update existing plot item for min values
            self.cycle_data_plot_item_min.setData(x=x_min, y=y_min)
        else:
            # Create plot item for min values
            self._create_min_max_plot(x=x_min, y=y_min, mode='min')


        if hasattr(self, 'cycle_data_plot_item_max') and self.cycle_data_plot_item_max is not None:
            # Update existing plot item for max values
            self.cycle_data_plot_item_max.setData(x=x_max, y=y_max)
        else:
            # Create plot item for max values
            self._create_min_max_plot(x=x_max, y=y_max, mode='max')

    ### handle zooming in plot
    @Slot()
    def _on_x_range_changed(self):
        self.range_change_timer.start(500)

    def _on_range_change_timeout(self):
        view_range = self.plotItem.viewRange()
        if view_range[0][0] < -65 or view_range[0][1] < -65:
            return
        current_x_range = [datetime.fromtimestamp(ts, tz=local_tz_reg) for ts in view_range[0]]
        start_time, end_time = current_x_range
        if not self._is_data_covered(start_time, end_time):
            self.load_visible_data(start_time, end_time)

    def _is_data_covered(self, start_time, end_time):
        if self.current_time_range is None:
            return False

        current_start, current_end = self.current_time_range
        current_range = current_end - current_start
        new_range = end_time - start_time

        # Threshold for significant change (e.g., 10%)
        threshold = 0.1  # 10%

        # Calculate the relative change in range
        range_change = abs(new_range - current_range) / current_range

        # Check if the range change exceeds the threshold
        if range_change > threshold:
            return False  # Significant zoom in or zoom out detected

        # Now check if the new range is within the buffered current range
        buffer = current_range * threshold
        is_covered = (start_time >= (current_start - buffer) and
                      end_time <= (current_end + buffer))

        return is_covered

    def load_visible_data(self, start_time, end_time):
        self.current_time_range = (start_time, end_time)
        if hasattr(self, 'reader'):
            self.reader.time_range_to_read = self.current_time_range
            self.reader.start(reading_mode=READING_MODE_BY_TIME)

    ### updating reader contraints
    def update_constraints_etc(self, new_constraints):
        if hasattr(self, 'reader'):
            self.reader.update_constraints_etc(new_constraints)

    def update_constraints_t_p(self, new_constraints):
        if hasattr(self, 'reader'):
            self.reader.update_constraints_t_p(new_constraints)
    ###

    ### handle point clicking
    def _on_point_clicked(self, plot, points):
        if len(points) > 0:
            # Take only the first point
            point = points[0]
            point_datetime = datetime.fromtimestamp(point.pos().x(), tz=local_tz_reg)
            #print('clicked a point', point_datetime)
            self._change_point_color(point.pos().x())
            self.point_clicked_time_received.emit(point_datetime,  self.current_color_scatter)

    def _change_point_color(self, target_timestamp):
        # Retrieve the data from the scatter plot item
        x_data, y_data = self.scatter_plot_item.getData()
        # Find the index of the point with the target timestamp
        point_index = None
        for i, x in enumerate(x_data):
            if x == target_timestamp:
                point_index = i
                break

        if point_index is not None:
            # Update the color of the specific point
            self.current_color_scatter = colors_scatter[self.color_index_scatter % len(colors_scatter)]
            self.color_index_scatter += 1
            self.point_colors[point_index] = pg.mkBrush(self.current_color_scatter)

            # Update the scatter plot item with the new color
            self.scatter_plot_item.setData(x=x_data, y=y_data, brush=self.point_colors)

    def _reset_point_colors(self):
        # Define the default color for resetting
        self.current_color_scatter = []
        self.color_index_scatter = 0
        default_color = pg.mkBrush('w')  # Replace 'w' with your desired default color
        # Reset the color of each point to the default color
        if not self.point_colors:
            return
        self.point_colors = [default_color for _ in self.point_colors]
        # Retrieve the data from the scatter plot item
        x_data, y_data = self.scatter_plot_item.getData()
        # Update the scatter plot item with the default colors
        self.scatter_plot_item.setData(x=x_data, y=y_data, brush=self.point_colors)
    ###

    def _update_x_range(self, time_range):
        self.plotItem.setXRange(min(time_range), max(time_range))


class DateAxisItem(pg.AxisItem):
    def tickStrings(self, values, scale, spacing):
        if not values:
            return []
        span = max(values) - min(values)
        if span < 3600:  # less than an hour
            fmt = "HH:mm:ss"
        elif span < 86400:  # less than a day
            fmt = "HH:mm"
        else:
            fmt = "yyyy MM dd"
        tick_labels = []
        for value in values:
            qdt = QDateTime.fromSecsSinceEpoch(int(value))
            tick_labels.append(qdt.toString(fmt))
        return tick_labels


class AxisLabel:

    @staticmethod
    def create_axis_label(column_name):
        """
        :param column_name: (str) name of the parameter that is plotted
        :return: axis title str
        """
        if "temperature" in column_name.lower():
            unit_str = "°C"
            variable_name = "Temperature"
        elif "pressure" in column_name.lower():
            unit_str = "bar"
            variable_name = "Pressure"
        elif "conductivity" in column_name.lower():
            unit_str = "Wm⁻¹K⁻¹"
            variable_name = "ETC"
        elif "time" in column_name.lower():
            unit_str = "Y-M-D H:M:s"
            variable_name = "Time"
        elif "uptake" in column_name.lower():
            unit_str = 'wt-%'
            variable_name = 'Capacity'
        elif "cycle" in column_name.lower():
            unit_str = '#'
            variable_name = 'Cycle Number'
        else:
            variable_name = ""
            unit_str = " "  # Default unit if neither temperature nor pressure

        return f"{variable_name} ({unit_str})"


if __name__ == '__main__':
    app = QApplication()
    win = PlotBaseStyle()
    win.show()
    meta_data = MetaData('WAE-WA-030')
    app.exec()



