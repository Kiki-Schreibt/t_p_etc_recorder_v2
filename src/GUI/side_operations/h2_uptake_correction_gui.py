import pandas as pd
from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTextEdit, QPushButton, QMessageBox, QApplication
)
from PySide6.QtGui import QFont
from PySide6.QtCore import Qt
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

        main_layout.addWidget(self.top_plot)
        main_layout.addWidget(self.bottom_plot)


        # Text edit for information display
        self.info_text_edit = QTextEdit()
        self.info_text_edit.setReadOnly(True)
        self.info_text_edit.setFont(QFont("Arial", 10))
        self.info_text_edit.setPlaceholderText("Information will appear here...")
        self.info_text_edit.setFixedHeight(150)
        main_layout.addWidget(self.info_text_edit)

        # Buttons layout
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(20)
        btn_layout.addStretch()

        self.calc_button = QPushButton("Calculate Uptake")
        self.calc_button.setFixedWidth(150)
        btn_layout.addWidget(self.calc_button)

        self.update_button = QPushButton("Update Data")
        self.update_button.setFixedWidth(150)
        btn_layout.addWidget(self.update_button)

        self.linear_region_button = QPushButton("Update Region")
        self.linear_region_button.setFixedWidth(150)
        btn_layout.addWidget(self.linear_region_button)

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

    def _on_calculate_uptake(self):
        """
        Handler for the 'Calculate Uptake' button.
        Replace with your calculation logic.
        """
        self.backend = UptakeCorrectionBackend(self.meta_data, self.current_time_range, self.config)
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
            # Placeholder update
            self.info_text_edit.append("Data updated successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Update failed: {e}")

    def _on_update_linear_region(self):
        # get the current visible X-range from the top plot:
        new_range = self.top_plot.viewRange()[0]  # (t0, t1)

        # simply move your existing region items:
        self.region_top.setRegion(new_range)
        self.region_bottom.setRegion(new_range)

        # and of course update your stored time range if you need it immediately
        self._on_region_changed()


class UptakeCorrectionBackend:
    def __init__(self, meta_data, time_range_to_load, config):

        from src.table_data import TableConfig
        self.logger = logging.getLogger(__name__)
        self.tp_table = TableConfig().TPDataTable
        self.cycle_table = TableConfig().CycleDataTable
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

        min_idx = df[self.tp_table.pressure].idxmin()
        max_idx = df[self.tp_table.pressure].idxmax()
        row_min = df.loc[min_idx]
        row_max = df.loc[max_idx]
        self.time_min_cycle = min(
                                self.row_min[self.tp_table.time],
                                self.row_max[self.tp_table.time]
                            )
        self.time_max_cycle = max(
                                self.row_min[self.tp_table.time],
                                self.row_max[self.tp_table.time]
                            )

        self.cycle_to_update = df[self.tp_table.cycle_number].max()

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
        series_to_update_tp_etc, series_to_update_cycle = self._create_series_to_update()

        db_manipulator = DataBaseManipulator(db_conn_params=self.db_conn_params)
        #update tp_data table
        db_manipulator.update_data(sample_id=self.meta_data.sample_id,
                                   table=self.tp_table,
                                   update_df=series_to_update_tp_etc,
                                   col_to_match=self.tp_table.cycle_number,
                                   update_between_vals=self.cycle_to_update)
        #update etc data table
        db_manipulator.update_data(sample_id=self.meta_data.sample_id,
                                   table=self.etc_table,
                                   update_df=series_to_update_tp_etc,
                                   col_to_match=self.tp_table.cycle_number,
                                   update_between_vals=self.cycle_to_update)
        #update cycle data table
        db_manipulator.update_data(sample_id=self.meta_data.sample_id,
                                   table=self.cycle_table,
                                   update_df=series_to_update_cycle,
                                   col_to_match=self.tp_table.cycle_number,
                                   update_between_vals=self.cycle_to_update)

    def _create_series_to_update(self):
        tp_etc_data = {self.tp_table.h2_uptake: self.h2_uptake}
        series_tp_etc = pd.Series(tp_etc_data)

        cycle_data = {self.cycle_table.h2_uptake: self.h2_uptake,
                      self.cycle_table.pressure_min: self.params_uptake['p_hyd'],
                      self.cycle_table.pressure_max: self.params_uptake['p_dehyd'],
                      self.cycle_table.temperature_min: self.params_uptake['T_hyd'],
                      self.cycle_table.temperature_max: self.params_uptake['T_dehyd'],
                      self.cycle_table.time_min: self.time_min_cycle,
                      self.cycle_table.time_max: self.time_max_cycle,
                      }

        series_cycle = pd.Series(cycle_data)
        return series_tp_etc, series_cycle


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
