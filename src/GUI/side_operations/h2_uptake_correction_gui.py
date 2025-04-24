import pandas as pd
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTextEdit, QPushButton, QMessageBox, QApplication
)
from PySide6.QtGui import QFont
from PySide6.QtCore import Qt, Signal, QObject, Slot, QThread
from pyqtgraph import LinearRegionItem
from datetime import datetime

from src.config_connection_reading_management.database_reading_writing import DataRetriever, local_tz

try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging


class UptakeCorrectionUi(QMainWindow):
    """
    A window to display a plot, information text, and buttons
    for calculating uptake and updating data.
    """
    def __init__(self, meta_data, config, top_plot, bottom_plot):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.setWindowTitle("Uptake Correction")
        self.config = config
        self.meta_data = meta_data
        self.current_time_range = []
        self.top_plot = top_plot
        self.bottom_plot = bottom_plot
        self.bottom_plot.setXLink(self.top_plot)
        self.backend = None

        self._setup_ui()
        self._setup_linear_region()

    def _setup_ui(self):
        """
        Set up the main UI components: plot area, text edit, and buttons.
        """
        # Central widget and main layout
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)

        self._setup_top_buttons(main_layout=main_layout)
        self._setup_text_edit(main_layout=main_layout)
        self._setup_bottom_buttons(main_layout=main_layout)

    def _setup_top_buttons(self, main_layout):
        # Top Buttons layout
        btn_layout_top = QHBoxLayout()
        btn_layout_top.setSpacing(20)

        btn_layout_top.addStretch()
        self.linear_region_button = QPushButton("Update Region")
        self.linear_region_button.setFixedWidth(150)
        btn_layout_top.addWidget(self.linear_region_button)

        self.find_uncounted_cycles_button = QPushButton("Find Uncounted Cycles")
        self.find_uncounted_cycles_button.setFixedWidth(150)
        btn_layout_top.addWidget(self.find_uncounted_cycles_button)

        self.prev_cycle_button = QPushButton("Prev Cycle")
        self.prev_cycle_button.setFixedWidth(120)
        btn_layout_top.addWidget(self.prev_cycle_button)

        self.next_cycle_button = QPushButton("Next Cycle")
        self.next_cycle_button.setFixedWidth(120)
        btn_layout_top.addWidget(self.next_cycle_button)
        btn_layout_top.addStretch()
        main_layout.addLayout(btn_layout_top)

        main_layout.addWidget(self.top_plot)
        main_layout.addWidget(self.bottom_plot)

    def _setup_text_edit(self, main_layout):
        # Text edit for information display
        self.info_text_edit = QTextEdit()
        self.info_text_edit.setReadOnly(True)
        self.info_text_edit.setFont(QFont("Arial", 10))
        self.info_text_edit.setPlaceholderText("Information will appear here...")
        self.info_text_edit.setFixedHeight(150)
        main_layout.addWidget(self.info_text_edit)

    def _setup_bottom_buttons(self, main_layout):
        # Buttons layout
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

        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)

    def _setup_linear_region(self):
        t0_dt, t1_dt = self.top_plot.reader.time_range_to_read
        t0 = t0_dt.timestamp()
        t1 = t1_dt.timestamp()


        # 1) make two separate regions
        self.region_top    = LinearRegionItem([t0, t1], brush=(0, 0, 255, 50))
        self.region_bottom = LinearRegionItem([t0, t1], brush=(0, 0, 255, 50))

        # wire up your two regions
        self.region_top.sigRegionChanged.connect(self._sync_bottom)
        self.region_bottom.sigRegionChanged.connect(self._sync_top)
        self.region_top.sigRegionChanged.connect(self._on_region_changed)
        self.region_bottom.sigRegionChanged.connect(self._on_region_changed)


        # 3) add each to its own plot
        self.top_plot.addItem(self.region_top)
        self.bottom_plot.addItem(self.region_bottom)

    def _sync_bottom(self):
        r = self.region_top.getRegion()            # r is a (minX, maxX) tuple
        self.region_bottom.blockSignals(True)      # avoid feedback loop
        self.region_bottom.setRegion(r)
        self.region_bottom.blockSignals(False)

    def _sync_top(self):
        r = self.region_bottom.getRegion()
        self.region_top.blockSignals(True)
        self.region_top.setRegion(r)
        self.region_top.blockSignals(False)

    def _on_region_changed(self):
        # getRegion() returns (minX, maxX) in your plot’s x‐units
        t0, t1 = self.region_top.getRegion()
        # convert from your float‐timestamps to datetime objects
        dt0 = datetime.fromtimestamp(t0, tz=local_tz)
        dt1 = datetime.fromtimestamp(t1, tz=local_tz)
        self.current_time_range = [dt0, dt1]


class UptakeCorrectionWindow(UptakeCorrectionUi):
    def __init__(self, meta_data, config, top_plot, bottom_plot):
        super().__init__(meta_data, config, top_plot, bottom_plot)
        # Connect signals
        self.calc_button.clicked.connect(self._on_calculate_uptake)
        self.update_button.clicked.connect(self._on_update_data)
        self.linear_region_button.clicked.connect(self._on_update_linear_region)
        self.find_uncounted_cycles_button.clicked.connect(self._on_find_uncounted_cycles)
        self.prev_cycle_button.clicked.connect(self._on_prev_cycle)
        self.next_cycle_button.clicked.connect(self._on_next_cycle)
        self.show_info_button.clicked.connect(self._on_show_cycle_info)
        self.df_uncounted_cycles = pd.DataFrame()
        self._uncounted_idx      = 0

    def _on_calculate_uptake(self):
        """
        Handler for the 'Calculate Uptake' button.
        Replace with your calculation logic.
        """
        self.backend = UptakeCorrectionBackend(self.meta_data, self.current_time_range, self.config)
        self.backend.df_uncounted_cycles_sig.connect(self._receive_uncounted_cycles)
        try:
            self.backend.load_min_max_data()
            self.backend.calculate_uptake()

            # display to the user
            self.info_text_edit.append(
                f"Selected time range: {self.current_time_range[0]:%Y-%m-%d %H:%M:%S} → {self.current_time_range[1]:%Y-%m-%d %H:%M:%S}"
            )

            # Placeholder computation
            result = f"Calculated uptake: {self.backend.h2_uptake} wt-%"
            self.info_text_edit.append(result)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Calculation failed: {e}")

    def _on_update_data(self):
        """
        Handler for the 'Update Data' button.
        Replace with your update logic.
        """
        try:
            self.info_text_edit.append(f"Updating t_p and cycle table for time range: "
                                       f"{self.current_time_range[0]:%Y-%m-%d %H:%M:%S} → "
                                       f"{self.current_time_range[1]:%Y-%m-%d %H:%M:%S}")

            """Slot called when user clicks “Update Data”."""
            # disable the button so they can’t re-click
            self.update_button.setEnabled(False)

            # create the worker + thread
            self._update_thread = QThread(self)
            self._update_worker = UpdateWorker(self.backend)

            # move worker into its thread
            self._update_worker.moveToThread(self._update_thread)

            # wire up signals
            self._update_thread.started.connect(self._update_worker.run)
            self._update_worker.finished.connect(self._on_update_finished)
            self._update_worker.error.connect(self._on_update_error)
            self._update_worker.progress.connect(self.info_text_edit.append)

            # clean up when done
            self._update_worker.finished.connect(self._update_thread.quit)
            self._update_worker.finished.connect(self._update_worker.deleteLater)
            self._update_thread.finished.connect(self._update_thread.deleteLater)

            # start the background work
            self._update_thread.start()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Update failed: {e}")

    def _on_update_finished(self):
        self.info_text_edit.append("Update complete.")
        self.update_button.setEnabled(True)

    def _on_update_error(self, errmsg):
        QMessageBox.critical(self, "Update Error", errmsg)
        self.update_button.setEnabled(True)

    def _on_update_linear_region(self):
        # get the current visible X-range from the top plot:
        new_range = self.top_plot.viewRange()[0]  # (t0, t1)

        # simply move your existing region items:
        self.region_top.setRegion(new_range)
        self.region_bottom.setRegion(new_range)

        # and of course update your stored time range if you need it immediately
        self._on_region_changed()

    def _on_find_uncounted_cycles(self):
        if not self.backend:
            self.backend = UptakeCorrectionBackend(self.meta_data, self.current_time_range, self.config)
            self.backend.df_uncounted_cycles_sig.connect(self._receive_uncounted_cycles)
        self.backend.load_uncounted_cycles()

        self._uncounted_idx = 0
        if not self.df_uncounted_cycles.empty:
            self.info_text_edit.append(
                f"Found {len(self.df_uncounted_cycles)} un-counted cycles."
            )
        else:
            self.info_text_edit.append("No un-counted cycles found.")

    @Slot()
    def _receive_uncounted_cycles(self, df):
        self.df_uncounted_cycles = df

    def _show_uncounted_cycle(self):

        if self.df_uncounted_cycles.empty:
            return
        # clamp index
        n = len(self.df_uncounted_cycles)
        self._uncounted_idx %= n

        row = self.df_uncounted_cycles.iloc[self._uncounted_idx]
        cycle = row[self.backend.cycle_table.cycle_number]
        t0    = row[self.backend.cycle_table.time_start].timestamp()
        t1    = row[self.backend.cycle_table.time_end].timestamp()
        self.top_plot.setXRange(t0, t1)
        # move the regions to this cycle’s time window
        self.region_top.setRegion((t0, t1))
        self.region_bottom.setRegion((t0, t1))
        # update your internal current_time_range
        self._on_region_changed()

        # display info
        self.info_text_edit.append(
            f"Cycle #{cycle} of {n}: "
            f"{row[self.backend.cycle_table.time_start]} → "
            f"{row[self.backend.cycle_table.time_end]}"
        )

    def _on_next_cycle(self):
        if self.df_uncounted_cycles.empty:
            return
        self._uncounted_idx += 1
        self._show_uncounted_cycle()

    def _on_prev_cycle(self):
        if self.df_uncounted_cycles.empty:
            return
        self._uncounted_idx -= 1
        self._show_uncounted_cycle()

    def _on_show_cycle_info(self):
        if not self.current_time_range:
            self._on_region_changed()

        self.backend = UptakeCorrectionBackend(self.meta_data, self.current_time_range, self.config)
        self.backend.df_uncounted_cycles_sig.connect(self._receive_uncounted_cycles)
        self.backend.load_min_max_data()

        current_cycle = self.backend.df_all_loaded.iloc[-1][self.backend.tp_table.cycle_number]
        current_de_hyd_state = self.backend.df_all_loaded.iloc[-1][self.backend.tp_table.de_hyd_state]



        self.info_text_edit.append(f"Info abot loaded cycle: "
                                   f"{self.current_time_range[0]:%Y-%m-%d %H:%M:%S} → "
                                   f"{self.current_time_range[1]:%Y-%m-%d %H:%M:%S}"
                                   f"\n"
                                   f"Cycle Number: {current_cycle}, {current_de_hyd_state}")

    def closeEvent(self, event):
        super().closeEvent(event)
        if self.backend:
            self.backend.stop()


class UptakeCorrectionBackend(QObject):

    df_uncounted_cycles_sig = Signal(pd.DataFrame)

    def __init__(self, meta_data, time_range_to_load, config):
        super().__init__()
        from src.table_data import TableConfig
        self.logger = logging.getLogger(__name__)
        self.tp_table = TableConfig().TPDataTable
        self.cycle_table = TableConfig().CycleDataTable
        cycle_table_column_names = TableConfig().get_table_column_names(table_class=self.cycle_table)
        self.cycle_table_column_names_str = ", ".join(cycle_table_column_names)
        self.etc_table = TableConfig().ETCDataTable
        self.meta_data = meta_data
        self.time_range_to_load = time_range_to_load
        self.db_conn_params = config.db_conn_params
        self.data_retriever = DataRetriever(db_conn_params=self.db_conn_params)
        self.row_min = self.row_max = pd.Series()
        self.h2_uptake = None
        self.cycle_to_update = None  # latest cycle number
        self.params_uptake = None
        self.time_min_cycle = None
        self.time_max_cycle = None
        self.df_all_loaded = None

    def load_min_max_data(self):
        try:
            df_min_max = self.data_retriever.fetch_data_by_time_2(time_range=self.time_range_to_load,
                                                                  sample_id=self.meta_data.sample_id,
                                                                  table_name=self.tp_table.table_name)

            self.row_min, self.row_max = self._filter_min_max_vals(df_min_max)

        except Exception as e:
            self.logger.error("Could not load data from time range: %s", e)
            return pd.DataFrame()

    def _filter_min_max_vals(self, df):

        self.df_all_loaded = df
        min_idx = df[self.tp_table.pressure].idxmin()
        max_idx = df[self.tp_table.pressure].idxmax()
        row_min = df.loc[min_idx]
        row_max = df.loc[max_idx]

        try:
            self.time_min_cycle = row_min[self.tp_table.time]
                                   # row_max[self.tp_table.time]

            self.time_max_cycle = row_max[self.tp_table.time]


        except Exception as e:
            self.logger.error("Couldnt determine time min and max: %s". e)

        self.cycle_to_update = float(df[self.tp_table.cycle_number].max())

        return row_min, row_max

    def calculate_uptake(self):
        from src.calculations.eq_p_calculation import VantHoffCalcEq

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

        eq_calculator = VantHoffCalcEq(meta_data=self.meta_data, db_conn_params=self.db_conn_params)
        self.h2_uptake = eq_calculator.calc_h2_uptake(**self.params_uptake)

    def update_database(self):
        if not self.cycle_to_update:
            self.logger.error("No cycle to update provided")
            return
        from src.config_connection_reading_management.database_reading_writing import DataBaseManipulator
        series_to_update_tp, series_to_update_cycle = self._create_series_to_update()

        db_manipulator = DataBaseManipulator(db_conn_params=self.db_conn_params)
        #update tp_data table
        db_manipulator.update_data(sample_id=self.meta_data.sample_id,
                                   table=self.tp_table,
                                   update_df=series_to_update_tp,
                                   col_to_match=self.tp_table.cycle_number,
                                   update_between_vals=self.cycle_to_update)

        #update cycle data table
        db_manipulator.update_data(sample_id=self.meta_data.sample_id,
                                   table=self.cycle_table,
                                   update_df=series_to_update_cycle,
                                   col_to_match=self.tp_table.cycle_number,
                                   update_between_vals=self.cycle_to_update)

    def _create_series_to_update(self):
        tp_etc_data = {self.tp_table.h2_uptake: float(self.h2_uptake)}
        series_tp_etc = pd.Series(tp_etc_data)

        cycle_data = {self.cycle_table.h2_uptake: float(self.h2_uptake),
                      self.cycle_table.pressure_min: float(self.params_uptake['p_hyd']),
                      self.cycle_table.pressure_max: float(self.params_uptake['p_dehyd']),
                      self.cycle_table.temperature_min: float(self.params_uptake['T_hyd']),
                      self.cycle_table.temperature_max: float(self.params_uptake['T_dehyd']),
                      self.cycle_table.time_min: self.time_min_cycle,
                      self.cycle_table.time_max: self.time_max_cycle,
                      }

        series_cycle = pd.Series(cycle_data)
        return series_tp_etc, series_cycle

    def load_uncounted_cycles(self):
        query = (f"Select {self.cycle_table_column_names_str} from {self.cycle_table.table_name} WHERE "
                 f"{self.cycle_table.sample_id} = %s"
                 f"AND {self.cycle_table.h2_uptake} IS NULL")

        df_uncounted_cycles = self.data_retriever.execute_fetching(table_name=self.cycle_table.table_name,
                                                  query=query,
                                                  values=(self.meta_data.sample_id,))
        df_uncounted_cycles = df_uncounted_cycles.sort_values(by=self.cycle_table.time_start)
        self.df_uncounted_cycles_sig.emit(df_uncounted_cycles)

    def stop(self):
        self.data_retriever = None


class UpdateWorker(QObject):
    finished = Signal()
    error    = Signal(str)
    progress = Signal(str)

    def __init__(self, backend):
        super().__init__()
        self.backend = backend

    @Slot()
    def run(self):
        try:
            self.progress.emit("Starting update…")
            self.backend.update_database()
            self.progress.emit("Done.")
            self.finished.emit()
        except Exception as e:
            self.error.emit(str(e))


def main():
    from src.GUI.recording_gui.recording_business_v2 import StaticPlotWindow
    from src.config_connection_reading_management.config_reader import GetConfig
    from src.meta_data.meta_data_handler import MetaData
    app = QApplication([])
    config = GetConfig()
    meta_data = MetaData(sample_id="WAE-WA-028", db_conn_params=config.db_conn_params)
    top_plot = StaticPlotWindow(meta_data=meta_data, db_conn_params=config.db_conn_params, y_axis="temperature", read_on_init=False)
    bottom_plot = StaticPlotWindow(meta_data=meta_data, db_conn_params=config.db_conn_params, y_axis="pressure", read_on_init=False)
    top_plot.reader.start()
    bottom_plot.reader.start()
    from datetime import datetime
    time_range = [datetime(2021, 9, 18), datetime(2021, 9, 21)]
    top_plot.reader.time_range_to_read = time_range
    win = UptakeCorrectionWindow(meta_data=meta_data,
                                 config=config,
                                 top_plot=top_plot,
                                 bottom_plot=bottom_plot)
    win.show()
    app.exec()

# Example usage:
if __name__ == '__main__':
    main()
