#plot_individualizer.py
from datetime import datetime

import pyqtgraph as pg
import pandas as pd

from PySide6.QtCore import Qt, QObject, Signal, Slot, QThread
from PySide6.QtWidgets import (QApplication, QWidget, QVBoxLayout, QPushButton,
                               QLabel, QComboBox, QHBoxLayout, QLineEdit,
                               QMessageBox)
from matplotlib.pyplot import tight_layout

from src.infrastructure.core.table_config import TableConfig
from src.config_connection_reading_management.database_reading_writing import DataRetriever
from src.GUI.recording_gui.recording_business_v2 import StaticPlotWindow
from src.infrastructure.handler.metadata_handler import MetaData
from src.infrastructure.core.global_vars import style, STANDARD_CONSTRAINTS, local_tz

tp_table = TableConfig().TPDataTable
etc_table = TableConfig().ETCDataTable
etc_xy_table = TableConfig().ThermalConductivityXyDataTable
cycle_table = TableConfig().CycleDataTable
meta_table = TableConfig().MetaDataTable

EXCLUDE_TABLES = {meta_table.table_name,
                  etc_xy_table.table_name}       # tables to hide
EXCLUDE_COLUMNS = {
                      etc_table.table_name: {etc_table.file,
                                  etc_table.sample_id,
                                  etc_table.table_name,
                                  etc_table.sample_id_small,
                                  etc_table.test_info,
                                  etc_table.calculation_settings,
                                  etc_table.calculation_settings_average,
                                  etc_table.calculation_settings_deviation,
                                  etc_table.de_hyd_state,
                                  etc_table.description,
                                  etc_table.disk_type,
                                  etc_table.disk_radius,
                                  etc_table.notes
                                  },
                      cycle_table.table_name: {cycle_table.sample_id,
                                               cycle_table.de_hyd_state
                                               },

                      tp_table.table_name: {tp_table.de_hyd_state,
                                            tp_table.sample_id,
                                            tp_table.test_info
                                            }

                    }


class PlotIndividualizerUI(QWidget):

    def __init__(self, config):
        super().__init__()
        self.config = config
        self.setWindowTitle("Table and Column Selector")
        self.resize(400, 200)

        self.meta_data = None
        self.table_classes = [
            TableConfig.TPDataTable,
            TableConfig.ETCDataTable,
            TableConfig.ThermalConductivityXyDataTable,
            TableConfig.MetaDataTable,
            TableConfig.CycleDataTable
        ]

        # Map display names to class or table_name
        self.table_map = {
                            cls.table_name: cls
                            for cls in self.table_classes
                            if cls.table_name not in EXCLUDE_TABLES
                        }
        self._init_ui()
        self.sample_id_edit_field.editingFinished.connect(self._on_sample_id_changed)
        self.setMinimumSize(1200, 450)
        self.setStyleSheet(style)

    def _init_ui(self):
        main_layout = QHBoxLayout(self)
        right_layout = QVBoxLayout(self)

        left_layout = QVBoxLayout(self)
        left_layout.addLayout(self._init_meta_edit_fields())
        left_layout.addLayout(self._init_combo_boxes())
        self.static_plot_layout = self._init_static_plot_layout()
        left_layout.addLayout(self.static_plot_layout)
        self.individual_plot_layout = self._init_individual_plot_layout()
        right_layout.addLayout(self.individual_plot_layout)
        left_layout.addLayout(self._init_constraints_edit_fields())
        main_layout.addLayout(left_layout, stretch=1)
        main_layout.addLayout(right_layout, stretch=1)

    def _init_meta_edit_fields(self):
        meta_layout = QHBoxLayout(self)
        meta_layout.addWidget(QLabel("Sample ID:"))
        self.sample_id_edit_field = QLineEdit()
        meta_layout.addWidget(self.sample_id_edit_field)
        return meta_layout

    def _init_combo_boxes(self):
        layout = QHBoxLayout(self)

        # Table selection
        layout.addWidget(QLabel("Select Table:"))
        self.tableCombo = QComboBox()
        self.tableCombo.addItems(sorted(self.table_map.keys()))
        layout.addWidget(self.tableCombo)

        # First column selection
        layout.addWidget(QLabel("Select X:"))
        self.x_col_combo_box = QComboBox()
        layout.addWidget(self.x_col_combo_box)

        # Second column selection
        layout.addWidget(QLabel("Select Y:"))
        self.y_col_combo_box = QComboBox()
        layout.addWidget(self.y_col_combo_box)

        self.plotButton = QPushButton(text="Plot")
        layout.addWidget(self.plotButton)
        return layout

    def _init_static_plot_layout(self):
        plot_layout = QVBoxLayout()

        # static placeholder:
        self.static_plot_widget = QWidget(self)
        self.static_plot_widget.setObjectName("staticPlotContainer")
        placeholder = QLabel("Waiting for sample ID…", alignment=Qt.AlignCenter)
        w = QVBoxLayout(self.static_plot_widget)
        w.addWidget(placeholder)
        plot_layout.addWidget(self.static_plot_widget, stretch=1)
        return plot_layout

    def _init_individual_plot_layout(self):
        plot_layout = QVBoxLayout()
        # second (individual) plot
        self.individual_plot_widget = QWidget(self)
        self.individual_plot_widget.setObjectName("individualPlotContainer")
        placeholder = QLabel("Waiting for selection…", alignment=Qt.AlignCenter)
        w = QVBoxLayout(self.individual_plot_widget)
        w.addWidget(placeholder)
        plot_layout.addWidget(self.individual_plot_widget, stretch=1)
        return plot_layout

    def _init_constraints_edit_fields(self):
        constraints_layout = QHBoxLayout(self)
        constraints_layout.addWidget(QLabel("Min Char Time:"))
        self.min_char_time_edit_field= QLineEdit()
        constraints_layout.addWidget(self.min_char_time_edit_field)
        constraints_layout.addWidget(QLabel("Max Char Time:"))
        self.max_char_time_edit_field= QLineEdit()
        constraints_layout.addWidget(self.max_char_time_edit_field)
        constraints_layout.addWidget(QLabel("Min Temp Inc:"))
        self.min_temp_inc_edit_field= QLineEdit()
        constraints_layout.addWidget(self.min_temp_inc_edit_field)
        constraints_layout.addWidget(QLabel("Max Temp Inc:"))
        self.max_temp_inc_edit_field= QLineEdit()
        constraints_layout.addWidget(self.max_temp_inc_edit_field)

        self.min_temp_inc_edit_field.setText(str(STANDARD_CONSTRAINTS["min_TotalTempIncr"]))
        self.max_temp_inc_edit_field.setText(str(STANDARD_CONSTRAINTS["max_TotalTempIncr"]))
        self.min_char_time_edit_field.setText(str(STANDARD_CONSTRAINTS["min_TotalCharTime"]))
        self.max_char_time_edit_field.setText(str(STANDARD_CONSTRAINTS["max_TotalCharTime"]))
        return constraints_layout

    def _on_sample_id_changed(self):
        sample_id = self.sample_id_edit_field.text()
        self.meta_data = MetaData(sample_id=sample_id, db_conn_params=self.config.db_conn_params)


class PlotIndividualizerMainWindow(PlotIndividualizerUI):

    def __init__(self, config):
        super().__init__(config=config)

        # Initialize columns for the default table
        self.update_columns(self.tableCombo.currentText())
        # Connect signal
        self.tableCombo.currentTextChanged.connect(self.update_columns)
        self.plotButton.clicked.connect(self._trigger_data_loading)

    def update_columns(self, table_name: str):
        # Get all column names
        cols = TableConfig().get_table_column_names(table_name=table_name) or []

        # Filter out the ones in EXCLUDE_COLUMNS
        ex = EXCLUDE_COLUMNS.get(table_name, set())
        filtered = [c for c in cols if c not in ex]

        # Sort and populate
        filtered.sort()
        self.x_col_combo_box.clear();  self.x_col_combo_box.addItems(filtered)
        self.y_col_combo_box.clear();  self.y_col_combo_box.addItems(filtered)

    def _on_sample_id_changed(self):
        super()._on_sample_id_changed()
        self._init_static_plot_window()
        self._init_individual_plot_window()

    def _init_static_plot_window(self):
        # 1) find the old placeholder by objectName
        old = self.findChild(QWidget, "staticPlotContainer")

        if not old:
            old = self.static_plot_widget

        # 2) instantiate your real plot
        real = StaticPlotWindow(
            y_axis="temperature",
            db_conn_params=self.config.db_conn_params,
            meta_data=self.meta_data
        )
        real.setParent(self)   # parent it so it'll show in the same spot

        # 3) replace in the layout
        self.static_plot_layout.replaceWidget(old, real)
        old.deleteLater()      # clean up the placeholder

        # 4) re-assign for any future reference
        self.static_plot_widget = real

    def _init_individual_plot_window(self):
        # 1) find the old placeholder by objectName
        old = self.findChild(QWidget, "individualPlotContainer")

        if not old:
            return

        # 2) instantiate your real plot
        real = IndividualPlotWindow(config=self.config)
        real.setParent(self)   # parent it so it'll show in the same spot

        # 3) replace in the layout
        self.individual_plot_layout.replaceWidget(old, real)
        old.deleteLater()      # clean up the placeholder

        # 4) re-assign for any future reference
        self.individual_plot_widget = real

    def _trigger_data_loading(self):
        table_name = self.tableCombo.currentText()
        x_col = self.x_col_combo_box
        y_col = self.y_col_combo_box

        view_range = self.static_plot_widget.viewRange()[0]
        dt0 = datetime.fromtimestamp(view_range[0], tz=local_tz)
        dt1 = datetime.fromtimestamp(view_range[1], tz=local_tz)
        time_range = [dt0, dt1]

        sample_id = self.meta_data.sample_id

        constraints = {"min_TotalCharTime": float(self.min_char_time_edit_field.text()),
                       "max_TotalCharTime": float(self.max_char_time_edit_field.text()),
                       "min_TotalTempIncr": float(self.min_temp_inc_edit_field.text()),
                       "max_TotalTempIncr": float(self.max_temp_inc_edit_field.text())
                       }

        self.individual_plot_widget.load_plot_data(table_name=table_name,
                                                   x_col=x_col,
                                                   y_col=y_col,
                                                   sample_id=sample_id,
                                                   time_range=time_range,
                                                   constraints=constraints)


class IndividualPlotWindow(pg.PlotWidget):

    def __init__(self, config, parent=None):
        super().__init__(parent=parent)
        self.config = config
        self._worker = None
        self._thread = None
        self.table_name = None
        self.scatter_plot_item = pg.ScatterPlotItem()
        self.addItem(self.scatter_plot_item)

    def load_plot_data(self,
                       table_name,
                       x_col,
                       y_col,
                       sample_id,
                       time_range=None,
                       constraints=None):

        # if user passed comboboxes, extract text
        if hasattr(x_col, "currentText"):
            x_col = x_col.currentText()
        if hasattr(y_col, "currentText"):
            y_col = y_col.currentText()

        # 1) Tear down any old worker/thread
        if self._thread and self._thread.isRunning():
            self._thread.quit()
            self._thread.wait()
            self._thread = None
        self.clear()
        self.table_name = table_name

        # 2) Make worker + thread
        self._worker = DataLoadWorker(
            db_conn_params=self.config.db_conn_params,
            table_name=table_name,
            x_col=x_col,
            y_col=y_col,
            sample_id=sample_id,
            time_range=time_range,
            constraints=constraints
        )
        self._thread = QThread(self)
        self._worker.moveToThread(self._thread)

        # 3) Wire up signals
        self._thread.started.connect(self._worker.run)
        self._worker.data_loaded.connect(self._on_data_loaded)
        self._worker.error.connect(self._on_data_error)
        # clean up
        self._worker.data_loaded.connect(self._thread.quit)
        self._worker.error.connect(self._thread.quit)
        self._thread.finished.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)

        # 4) Kick it off
        self._thread.start()

    @Slot(pd.DataFrame)
    def _on_data_loaded(self, df):
        # assume df[x_col] and df[y_col] exist
        self._plot_data(df)
        #self.plot(x, y, pen=pg.mkPen(width=2), symbol='o')

    def _plot_data(self, df):
        self._create_axis_labels(df.columns.to_list())
        self.scatter_plot_item.setData(x=df.iloc[0].to_list(), y=df.iloc[1].to_list())

    def _create_axis_labels(self, col_names):
        self.plotItem.getAxis('bottom').setLabel(col_names[0])
        self.plotItem.getAxis('left').setLabel(col_names[1])

    @Slot(str)
    def _on_data_error(self, msg):
        QMessageBox.critical(self, "Data Load Error", msg)


class DataLoadWorker(QObject):
    data_loaded = Signal(pd.DataFrame)
    error       = Signal(str)

    def __init__(self, db_conn_params, table_name, x_col, y_col,
                 sample_id, time_range, constraints):
        super().__init__()
        self.db_conn_params = db_conn_params
        self.table_name     = table_name
        self.x_col          = x_col
        self.y_col          = y_col
        self.sample_id      = sample_id
        self.time_range     = time_range
        self.constraints    = constraints

    @Slot()
    def run(self):
        try:
            retriever = DataRetriever(db_conn_params=self.db_conn_params)
            column_names = [self.x_col, self.y_col]
            df = retriever.fetch_data_by_time_2(
                table_name= self.table_name,
                column_names=column_names,
                sample_id=   self.sample_id,
                time_range=  self.time_range,
                constraints=self.constraints
            )
            self.data_loaded.emit(df)
        except Exception as e:
            self.error.emit(str(e))




if __name__ == '__main__':
    import sys
    from src.infrastructure.core.config_reader import GetConfig
    app = QApplication(sys.argv)
    win = PlotIndividualizerMainWindow(config=GetConfig())
    win.show()
    sys.exit(app.exec())
