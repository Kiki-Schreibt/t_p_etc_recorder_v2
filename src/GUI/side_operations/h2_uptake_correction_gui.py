#h2_uptake_correction.py
import pandas as pd
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTextEdit, QPushButton, QMessageBox, QApplication, QCheckBox
)
from PySide6.QtGui import QFont
from PySide6.QtCore import Signal, QObject, Slot, QThread
from pyqtgraph import LinearRegionItem
from datetime import datetime

from src.config_connection_reading_management.database_reading_writing import DataRetriever, local_tz
from src.infrastructure.core import global_vars

try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging

STYLE_SHEET = global_vars.style


class UptakeCorrectionUi(QMainWindow):
    """
    Base UI for the uptake correction tool, displaying two linked plots
    (temperature and pressure), a linear region selector, and control buttons.

    Args:
        meta_data (MetaData): Sample metadata used for plotting and calculations.
        config (ConfigReader): Application configuration (e.g. DB connection params).
        time_range_to_read (List[datetime, datetime]): Initial time range to display.
    """

    def __init__(self, meta_data, config, time_range_to_read):
        super().__init__()
        self.setStyleSheet(STYLE_SHEET)
        self._init_plots(meta_data=meta_data,
                         config=config,
                         time_range_to_read=time_range_to_read)
        self.logger = logging.getLogger(__name__)
        self.setWindowTitle("Uptake Correction")
        self.config = config
        self.meta_data = meta_data
        self.current_time_range = []
        self.bottom_plot.setXLink(self.top_plot)
        self.backend = None

        self._setup_ui()
        self._setup_linear_region()

    def _init_plots(self, meta_data, config, time_range_to_read):
        """
        Create and configure the top (temperature) and bottom (pressure) plots.

        Connects the data signals so that pressure data updates with temperature,
        and sets the initial time range for both readers.

        Args:
            meta_data (MetaData): Sample metadata.
            config (ConfigReader): Application configuration.
            time_range_to_read (List[datetime, datetime]): Time window to load.
        """
        from src.GUI.recording_gui.recording_business_v2 import StaticPlotWindow

        self.top_plot = StaticPlotWindow(
            meta_data=meta_data,
            db_conn_params=config.db_conn_params,
            y_axis="temperature",
            read_on_init=False
        )
        self.bottom_plot = StaticPlotWindow(
            meta_data=meta_data,
            db_conn_params=config.db_conn_params,
            y_axis="pressure",
            read_on_init=False
        )
        # Set time range and wiring
        self.top_plot.reader.time_range_to_read = time_range_to_read
        self.bottom_plot.reader.time_range_to_read = time_range_to_read
        self.top_plot.reader.p_data_sig.connect(self.bottom_plot.on_tp_data)
        self.top_plot.reader.etc_data_sig.connect(self.bottom_plot.on_etc_data)
        # Start readers
        self.top_plot.reader.start()
        self.bottom_plot.reader.start()

    def _setup_ui(self):
        """
        Lay out the main window: top buttons, plots, text display, and bottom buttons.
        """
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)

        self._setup_top_buttons(main_layout=main_layout)
        self._setup_text_edit(main_layout=main_layout)
        self._setup_bottom_buttons(main_layout=main_layout)

    def _setup_top_buttons(self, main_layout):
        """
        Create the top row of buttons for region update and cycle browsing.
        """
        btn_layout_top = QHBoxLayout()
        btn_layout_top.setSpacing(20)
        btn_layout_top.addStretch()

        self.linear_region_button = QPushButton("Update Region")
        self.linear_region_button.setFixedWidth(180)
        btn_layout_top.addWidget(self.linear_region_button)

        self.find_uncounted_cycles_button = QPushButton("Find Uncounted Cycles")
        self.find_uncounted_cycles_button.setFixedWidth(180)
        btn_layout_top.addWidget(self.find_uncounted_cycles_button)

        self.prev_cycle_button = QPushButton("Prev Cycle")
        self.prev_cycle_button.setFixedWidth(150)
        btn_layout_top.addWidget(self.prev_cycle_button)

        self.next_cycle_button = QPushButton("Next Cycle")
        self.next_cycle_button.setFixedWidth(150)
        btn_layout_top.addWidget(self.next_cycle_button)

        btn_layout_top.addStretch()
        main_layout.addLayout(btn_layout_top)
        main_layout.addWidget(self.top_plot)
        main_layout.addWidget(self.bottom_plot)

    def _setup_text_edit(self, main_layout):
        """
        Create the read-only text area for informational messages.
        """
        self.info_text_edit = QTextEdit()
        self.info_text_edit.setReadOnly(True)
        self.info_text_edit.setFont(QFont("Arial", 10))
        self.info_text_edit.setPlaceholderText("Information will appear here...")
        self.info_text_edit.setFixedHeight(150)
        main_layout.addWidget(self.info_text_edit)

    def _setup_bottom_buttons(self, main_layout):
        """
        Create the bottom row of buttons for showing info, calculating, and updating.
        """
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(20)
        btn_layout.addStretch()

        self.show_info_button = QPushButton("Show Cycle Info")
        self.show_info_button.setFixedWidth(150)
        btn_layout.addWidget(self.show_info_button)

        self.calc_button = QPushButton("Calculate Uptake")
        self.calc_button.setFixedWidth(150)
        btn_layout.addWidget(self.calc_button)

        self.update_button = QPushButton("Update Data")
        self.update_button.setFixedWidth(150)
        btn_layout.addWidget(self.update_button)

        self.update_isotherm_flag_button = QPushButton("Update Isotherm Flag")
        self.update_isotherm_flag_button.setFixedWidth(160)
        btn_layout.addWidget(self.update_isotherm_flag_button)

        self.isotherm_checkbox = QCheckBox("Set Isotherm Flag")
        self.isotherm_checkbox.setChecked(True)           # default state
        btn_layout.addWidget(self.isotherm_checkbox)

        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)

    def _setup_linear_region(self):
        """
        Create a linked LinearRegionItem on both plots for selecting a time window.
        """
        if not self.top_plot.reader.time_range_to_read:
            return
        t0_dt, t1_dt = self.top_plot.reader.time_range_to_read
        t0, t1 = t0_dt.timestamp(), t1_dt.timestamp()

        self.region_top = LinearRegionItem([t0, t1], brush=(0, 0, 255, 50))
        self.region_bottom = LinearRegionItem([t0, t1], brush=(0, 0, 255, 50))

        self.region_top.sigRegionChanged.connect(self._sync_bottom)
        self.region_bottom.sigRegionChanged.connect(self._sync_top)
        self.region_top.sigRegionChanged.connect(self._on_region_changed)
        self.region_bottom.sigRegionChanged.connect(self._on_region_changed)

        self.top_plot.addItem(self.region_top)
        self.bottom_plot.addItem(self.region_bottom)

    def _sync_bottom(self):
        """
        Sync bottom region to top to keep them identical and avoid loops.
        """
        r = self.region_top.getRegion()
        self.region_bottom.blockSignals(True)
        self.region_bottom.setRegion(r)
        self.region_bottom.blockSignals(False)

    def _sync_top(self):
        """
        Sync top region to bottom to keep them identical and avoid loops.
        """
        r = self.region_bottom.getRegion()
        self.region_top.blockSignals(True)
        self.region_top.setRegion(r)
        self.region_top.blockSignals(False)

    def _on_region_changed(self):
        """
        Update internal current_time_range whenever the region moves.
        """
        t0, t1 = self.region_top.getRegion()
        dt0 = datetime.fromtimestamp(t0, tz=local_tz)
        dt1 = datetime.fromtimestamp(t1, tz=local_tz)
        self.current_time_range = [dt0, dt1]
        if self.backend:
            self.backend.time_range_to_load = self.current_time_range


class UptakeCorrectionWindow(UptakeCorrectionUi):
    """
    Full-featured uptake correction window, adding business logic for:
      - calculating uptake
      - updating database
      - browsing uncounted cycles
      - threading the update operation
    """

    def __init__(self, meta_data, config, time_range_to_read):
        super().__init__(meta_data, config, time_range_to_read)

        # Connect UI signals to handlers
        self.calc_button.clicked.connect(self._on_calculate_uptake)
        self.update_button.clicked.connect(self._on_update_data)
        self.linear_region_button.clicked.connect(self._on_update_linear_region)
        self.find_uncounted_cycles_button.clicked.connect(self._on_find_uncounted_cycles)
        self.prev_cycle_button.clicked.connect(self._on_prev_cycle)
        self.next_cycle_button.clicked.connect(self._on_next_cycle)
        self.show_info_button.clicked.connect(self._on_show_cycle_info)
        self.update_isotherm_flag_button.clicked.connect(self._on_update_isotherm_flag)

        self.df_uncounted_cycles = pd.DataFrame()
        self._uncounted_idx = 0

    def _on_calculate_uptake(self):
        """
        Trigger the backend to load min/max data in the selected range
        and calculate hydrogen uptake.
        """
        self.backend = UptakeCorrectionBackend(
            self.meta_data,
            self.current_time_range,
            self.config
        )
        self.backend.df_uncounted_cycles_sig.connect(self._receive_uncounted_cycles)
        self.backend.cycle_updated_sig.connect(self._delete_cycle_from_uncounted_df)

        try:
            self.backend.load_min_max_data()
            self.backend.calculate_uptake()
            self.info_text_edit.append(
                f"Selected time range: "
                f"{self.current_time_range[0]:%Y-%m-%d %H:%M:%S}"
                f" → {self.current_time_range[1]:%Y-%m-%d %H:%M:%S}"
            )
            result = f"Calculated uptake: {self.backend.h2_uptake:.4f} wt-%"
            self.info_text_edit.append(result)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Calculation failed: {e}")

    def _on_update_data(self):
        """
        Run the backend update_database() in a background thread to avoid GUI freeze.
        """
        try:
            self.info_text_edit.append(
                f"Updating data for range: "
                f"{self.current_time_range[0]:%Y-%m-%d %H:%M:%S}"
                f" → {self.current_time_range[1]:%Y-%m-%d %H:%M:%S}"
            )
            self.update_button.setEnabled(False)

            self.backend.time_range_to_update = list(self.current_time_range)

            self._update_thread = QThread(self)
            self._update_worker = UpdateWorker(self.backend)
            self._update_worker.moveToThread(self._update_thread)

            self._update_thread.started.connect(self._update_worker.run)
            self._update_worker.finished.connect(self._on_update_finished)
            self._update_worker.error.connect(self._on_update_error)
            self._update_worker.progress.connect(self.info_text_edit.append)

            self._update_worker.finished.connect(self._update_thread.quit)
            self._update_worker.finished.connect(self._update_worker.deleteLater)
            self._update_thread.finished.connect(self._update_thread.deleteLater)

            self._update_thread.start()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Update failed: {e}")

    def _on_update_finished(self):
        """Called when the background update task completes successfully."""
        self.info_text_edit.append("Update complete.")
        self.update_button.setEnabled(True)

    def _on_update_error(self, errmsg):
        """Called if the background update task raises an error."""
        QMessageBox.critical(self, "Update Error", errmsg)
        self.update_button.setEnabled(True)

    def _on_update_linear_region(self):
        """
        Set the region selector to the current view’s X-range.
        """
        new_range = self.top_plot.viewRange()[0]
        self.region_top.setRegion(new_range)
        self.region_bottom.setRegion(new_range)
        self._on_region_changed()

    def _on_find_uncounted_cycles(self):
        """
        Ask the backend to load cycles with NULL uptake, then reset index pointer.
        """
        if not self.backend:
            self.backend = UptakeCorrectionBackend(
                self.meta_data,
                self.current_time_range,
                self.config
            )
            self.backend.df_uncounted_cycles_sig.connect(self._receive_uncounted_cycles)
            self.backend.cycle_updated_sig.connect(self._delete_cycle_from_uncounted_df)
        self.backend.load_uncounted_cycles()
        self._uncounted_idx = 0
        if not self.df_uncounted_cycles.empty:
            self.info_text_edit.append(
                f"Found {len(self.df_uncounted_cycles)} un-counted cycles."
            )
        else:
            self.info_text_edit.append("No un-counted cycles found.")

    @Slot(pd.DataFrame)
    def _receive_uncounted_cycles(self, df):
        """
        Slot to receive the DataFrame of un-counted cycles from backend.
        """
        self.df_uncounted_cycles = df

    def _show_uncounted_cycle(self):
        """
        Display the cycle at the current index by updating the region and info text.
        """
        if self.df_uncounted_cycles.empty:
            return
        n = len(self.df_uncounted_cycles)
        self._uncounted_idx %= n

        row = self.df_uncounted_cycles.iloc[self._uncounted_idx]
        cycle = row[self.backend.cycle_table.cycle_number]
        t0 = row[self.backend.cycle_table.time_start].timestamp()
        t1 = row[self.backend.cycle_table.time_end].timestamp()

        self.top_plot.setXRange(t0, t1)
        self.region_top.setRegion((t0, t1))
        self.region_bottom.setRegion((t0, t1))
        self._on_region_changed()

        self.info_text_edit.append(
            f"Cycle #{cycle} of {n}: "
            f"{row[self.backend.cycle_table.time_start]} → "
            f"{row[self.backend.cycle_table.time_end]}"
        )

    def _on_next_cycle(self):
        if self.df_uncounted_cycles.empty:
            return
        old_idx = self._uncounted_idx
        self._uncounted_idx += 1
        n = len(self.df_uncounted_cycles)
        self._uncounted_idx %= n
        self._show_uncounted_cycle()
        # if we wrapped from last back to zero, we’ve seen them all once
        if self._uncounted_idx == 0 and old_idx == n - 1:
            self.info_text_edit.append("🔁 You’ve now browsed through all cycles once.")

    def _on_prev_cycle(self):
        """Go back to the previous un-counted cycle and display it."""
        if self.df_uncounted_cycles.empty:
            return
        self._uncounted_idx -= 1
        self._show_uncounted_cycle()

    def _on_show_cycle_info(self):
        """
        Load min/max data for the current region and show the last row’s cycle info.
        """
        if not self.current_time_range:
            self._on_region_changed()

        if not self.backend:
            self.backend = UptakeCorrectionBackend(
                self.meta_data,
                self.current_time_range,
                self.config
            )
            self.backend.df_uncounted_cycles_sig.connect(self._receive_uncounted_cycles)
            self.backend.cycle_updated_sig.connect(self._delete_cycle_from_uncounted_df)

        self.backend.load_min_max_data()

        last = self.backend.df_all_loaded.iloc[-1]
        current_cycle = last[self.backend.tp_table.cycle_number]
        current_de_hyd_state = last[self.backend.tp_table.de_hyd_state]
        current_uptake = last[self.backend.tp_table.h2_uptake]

        if current_uptake is not None:
            uptake_str = f"{current_uptake:.2f} wt-%"
        else:
            uptake_str = "N/A"

        self.info_text_edit.append(
            f"Info about loaded data:\n"
            f"{self.current_time_range[0]:%Y-%m-%d %H:%M:%S} → "
            f"{self.current_time_range[1]:%Y-%m-%d %H:%M:%S}\n"
            f"Cycle Number: {current_cycle}, "
            f"State: {current_de_hyd_state}, "
            f"H2 Uptake = {uptake_str} wt-%"
        )

    def _delete_cycle_from_uncounted_df(self, cycle_number):
        """
        Remove uncounted cycle after manual uptake calculation.
        """
        self.df_uncounted_cycles = self.df_uncounted_cycles[self.df_uncounted_cycles['cycle_number'] != cycle_number]
        self.info_text_edit.append(
                f"Cycle #{cycle_number} removed from uncounted cycles"
            )
        self._uncounted_idx -= 1

    def _on_update_isotherm_flag(self):
        """
        Start a background thread to run backend.update_isotherm_flag()
        on both TP and ETC tables.
        """
        if not self.backend:
            # If no backend yet, initialize it (using current_time_range or None)
            self.backend = UptakeCorrectionBackend(
                self.meta_data,
                self.current_time_range,
                self.config
            )
            self.backend.df_uncounted_cycles_sig.connect(self._receive_uncounted_cycles)
            self.backend.cycle_updated_sig.connect(self._delete_cycle_from_uncounted_df)

        self.backend.time_range_to_update = list(self.current_time_range)
        self.backend.is_isotherm = self.isotherm_checkbox.isChecked()
        self.info_text_edit.append("Updating isotherm flags…")
        self.update_isotherm_flag_button.setEnabled(False)

        # Create a QThread and worker, just like in _on_update_data:
        self._iso_thread = QThread(self)
        self._iso_worker = UpdateIsothermWorker(self.backend)
        self._iso_worker.moveToThread(self._iso_thread)

        self._iso_thread.started.connect(self._iso_worker.run)
        self._iso_worker.finished.connect(self._on_iso_finished)
        self._iso_worker.error.connect(self._on_iso_error)
        self._iso_worker.progress.connect(self.info_text_edit.append)

        # Clean up when done:
        self._iso_worker.finished.connect(self._iso_thread.quit)
        self._iso_worker.finished.connect(self._iso_worker.deleteLater)
        self._iso_thread.finished.connect(self._iso_thread.deleteLater)

        self._iso_thread.start()

    def _on_iso_finished(self):
        """Called when isotherm‐flag update completes successfully."""
        self.info_text_edit.append("Isotherm‐flag update complete.")
        self.update_isotherm_flag_button.setEnabled(True)

    def _on_iso_error(self, errmsg):
        """Called if isotherm‐flag update raises an error."""
        QMessageBox.critical(self, "Isotherm Update Error", errmsg)
        self.update_isotherm_flag_button.setEnabled(True)

    def closeEvent(self, event):
        """
        Ensure backend cleanup on window close.
        """
        if self.backend:
            self.backend.stop()
        super().closeEvent(event)


class UptakeCorrectionBackend(QObject):
    """
    Business-logic backend for uptake correction: fetches data, finds min/max
    points, calculates uptake, updates the database, and lists un-counted cycles.

    Signals:
        df_uncounted_cycles_sig (pd.DataFrame): Emitted with cycles missing uptake.
    """
    df_uncounted_cycles_sig = Signal(pd.DataFrame)
    cycle_updated_sig = Signal(float)
    time_range_to_update = None
    is_isotherm = None

    def __init__(self, meta_data, time_range_to_load, config):
        """
        Initialize with sample metadata, time window, and DB config.

        Args:
            meta_data (MetaData): Sample metadata object.
            time_range_to_load (List[datetime, datetime]): Time range to fetch.
            config (ConfigReader): Application configuration.
        """
        super().__init__()
        from src.infrastructure.core.table_config import TableConfig

        self.logger = logging.getLogger(__name__)
        self.tp_table = TableConfig().TPDataTable
        self.cycle_table = TableConfig().CycleDataTable
        cols = TableConfig().get_table_column_names(table_class=self.cycle_table)
        self.cycle_table_column_names_str = ", ".join(cols)
        self.etc_table = TableConfig().ETCDataTable
        self.meta_data = meta_data
        self.time_range_to_load = time_range_to_load
        self.db_conn_params = config.db_conn_params
        self.data_retriever = DataRetriever(db_conn_params=self.db_conn_params)

        self.row_min = self.row_max = pd.Series()
        self.h2_uptake = None
        self.cycle_to_update = None
        self.params_uptake = None
        self.time_min_cycle = None
        self.time_max_cycle = None
        self.df_all_loaded = None
        self.uncounted_cycles_emitted = False

    def load_min_max_data(self):
        """
        Fetches TP data over the time range and identifies min/max pressure rows.
        """
        try:
            df = self.data_retriever.fetch_data_by_time_2(
                time_range=self.time_range_to_load,
                sample_id=self.meta_data.sample_id,
                table_name=self.tp_table.table_name
            )
            self.row_min, self.row_max = self._filter_min_max_vals(df)
        except Exception as e:
            self.logger.error("Could not load data from time range: %s", e)
            return pd.DataFrame()

    def _filter_min_max_vals(self, df):
        """
        Internal: find rows of minimum and maximum pressure, record their times.

        Args:
            df (pd.DataFrame): DataFrame of TP data.

        Returns:
            Tuple[pd.Series, pd.Series]: (row_min, row_max)
        """
        self.df_all_loaded = df
        min_idx = df[self.tp_table.pressure].idxmin()
        max_idx = df[self.tp_table.pressure].idxmax()
        row_min = df.loc[min_idx]
        row_max = df.loc[max_idx]

        try:
            self.time_min_cycle = row_min[self.tp_table.time]
            self.time_max_cycle = row_max[self.tp_table.time]
        except Exception as e:
            self.logger.error("Couldn’t determine time min and max: %s", e)

        self.cycle_to_update = float(df[self.tp_table.cycle_number].max())
        return row_min, row_max

    def calculate_uptake(self):
        """
        Perform the Vant Hoff uptake calculation using the min/max rows.
        """
        from src.infrastructure.utils.eq_p_calculation import VantHoffCalcEq

        if self.row_min.empty or self.row_max.empty:
            self.logger.info("No data for uptake calculation provided")
            return

        self.params_uptake = {
            'p_hyd':    self.row_min[self.tp_table.pressure],
            'p_dehyd':  self.row_max[self.tp_table.pressure],
            'T_hyd':    self.row_min[self.tp_table.temperature_sample],
            'T_dehyd':  self.row_max[self.tp_table.temperature_sample],
            'V_res':    self.meta_data.reservoir_volume,
            'V_cell':   self.meta_data.volume_measurement_cell,
            'm_sample': self.meta_data.sample_mass
        }
        eq_calc = VantHoffCalcEq(
            meta_data=self.meta_data,
            db_conn_params=self.db_conn_params
        )
        self.h2_uptake = eq_calc.calc_h2_uptake(**self.params_uptake)

    def update_database(self):
        """
        Write calculated uptake back to both TP and cycle tables for the latest cycle.
        """
        if not self.cycle_to_update:
            self.logger.error("No cycle to update provided")
            return

        from src.config_connection_reading_management.database_reading_writing import DataBaseManipulator
        tp_series, cycle_series = self._create_series_to_update()
        dbm = DataBaseManipulator(db_conn_params=self.db_conn_params)

        dbm.update_data(
            sample_id=self.meta_data.sample_id,
            table=self.tp_table,
            update_df=tp_series,
            col_to_match=self.tp_table.cycle_number,
            update_between_vals=self.cycle_to_update
        )
        dbm.update_data(
            sample_id=self.meta_data.sample_id,
            table=self.cycle_table,
            update_df=cycle_series,
            col_to_match=self.tp_table.cycle_number,
            update_between_vals=self.cycle_to_update
        )

        if self.uncounted_cycles_emitted:
            self.cycle_updated_sig.emit(self.cycle_to_update)
        self.time_range_to_update = None

    def _create_series_to_update(self):
        """
        Build pandas Series for TP and cycle tables from computed uptake and params.

        Returns:
            Tuple[pd.Series, pd.Series]: (tp_update_series, cycle_update_series)
        """
        tp_etc_data = {self.tp_table.h2_uptake: float(self.h2_uptake)}
        series_tp = pd.Series(tp_etc_data)

        cyc_data = {
            self.cycle_table.h2_uptake: float(self.h2_uptake),
            self.cycle_table.pressure_min: float(self.params_uptake['p_hyd']),
            self.cycle_table.pressure_max: float(self.params_uptake['p_dehyd']),
            self.cycle_table.temperature_min: float(self.params_uptake['T_hyd']),
            self.cycle_table.temperature_max: float(self.params_uptake['T_dehyd']),
            self.cycle_table.time_min: self.time_min_cycle,
            self.cycle_table.time_max: self.time_max_cycle
        }
        series_cycle = pd.Series(cyc_data)
        return series_tp, series_cycle

    def load_uncounted_cycles(self):
        """
        Query the cycle table for entries with NULL uptake and emit them as a DataFrame.
        """
        query = (
            f"SELECT {self.cycle_table_column_names_str}"
            f" FROM {self.cycle_table.table_name}"
            f" WHERE {self.cycle_table.sample_id} = %s"
            f" AND {self.cycle_table.h2_uptake} IS NULL"
        )
        df = self.data_retriever.execute_fetching(
            table_name=self.cycle_table.table_name,
            query=query,
            values=(self.meta_data.sample_id,)
        )
        if not df.empty:
            df = df.sort_values(by=self.cycle_table.time_start)
        self.df_uncounted_cycles_sig.emit(df.copy())
        self.uncounted_cycles_emitted = True

    def update_isotherm_flag(self):
        """
        Update the TP and ETC tables’ is_isotherm_flag = TRUE
        for all rows whose timestamp lies in self.time_range_to_update.
        """
        if not self.time_range_to_update:
            raise RuntimeError("No time range selected for isotherm‐flag update.")

        update_df_tp = pd.Series({self.tp_table.is_isotherm_flag: self.is_isotherm})
        update_df_etc = pd.Series({self.etc_table.is_isotherm_flag: self.is_isotherm})

        from src.config_connection_reading_management.database_reading_writing import DataBaseManipulator
        dbm = DataBaseManipulator(db_conn_params=self.db_conn_params)

        dbm.update_data(
            sample_id=self.meta_data.sample_id,
            table=self.tp_table,
            update_df=update_df_tp,
            col_to_match=self.tp_table.time,
            update_between_vals=self.time_range_to_update)

        dbm.update_data(
            sample_id=self.meta_data.sample_id,
            table=self.etc_table,
            update_df=update_df_etc,
            col_to_match=self.etc_table.time,
            update_between_vals=self.time_range_to_update)

        self.is_isotherm = None
        self.time_range_to_update = None

    def stop(self):
        """
        Cleanup any resources (e.g. data retriever) when the UI closes.
        """
        self.data_retriever = None


class UpdateWorker(QObject):
    """
    Worker QObject to run backend.update_database() off the main thread.

    Signals:
        finished (): emitted when update_database completes.
        error (str): emitted with an error message if update_database raises.
        progress (str): emitted with status updates.
    """
    finished = Signal()
    error = Signal(str)
    progress = Signal(str)

    def __init__(self, backend):
        """
        Args:
            backend (UptakeCorrectionBackend): The backend instance to call.
        """
        super().__init__()
        self.backend = backend

    @Slot()
    def run(self):
        """
        Execute the database update, emitting progress and errors as signals.
        """
        try:
            self.progress.emit("Starting update…")
            self.backend.update_database()
            self.progress.emit("Done.")
            self.finished.emit()
        except Exception as e:
            self.error.emit(str(e))


class UpdateIsothermWorker(QObject):
    """
    Worker to run backend.update_isotherm_flag() off the main thread.

    Signals:
        finished (): emitted when update_isotherm_flag() completes.
        error (str): emitted with an error message if update_isotherm_flag() fails.
        progress (str): emitted with intermediate status updates.
    """
    finished = Signal()
    error = Signal(str)
    progress = Signal(str)

    def __init__(self, backend):
        super().__init__()
        self.backend = backend

    @Slot()
    def run(self):
        try:
            self.progress.emit("Starting isotherm‐flag update…")
            self.backend.update_isotherm_flag()
            self.progress.emit("Done updating isotherm flags.")
            self.finished.emit()
        except Exception as e:
            self.error.emit(str(e))


def main():
    """
    Standalone entry point for testing the UptakeCorrectionWindow.

    Opens the window for sample 'WAE-WA-028' over a fixed time range.
    """
    from src.infrastructure.core.config_reader import GetConfig
    from src.infrastructure.handler.metadata_handler import MetaData

    app = QApplication([])
    config = GetConfig()
    meta_data = MetaData(sample_id="WAE-WA-028", db_conn_params=config.db_conn_params)
    from datetime import datetime
    time_range = [datetime(2021, 9, 18), datetime(2021, 9, 21)]
    win = UptakeCorrectionWindow(
        meta_data=meta_data,
        config=config,
        time_range_to_read=time_range
    )
    win.show()
    app.exec()


if __name__ == '__main__':
    main()

