#plot_individualizer.py

from PySide6.QtWidgets import (QApplication, QWidget, QVBoxLayout,
                                       QLabel, QComboBox, QHBoxLayout)
import pyqtgraph as pg

from src.infrastructure.core.table_config import TableConfig
from src.config_connection_reading_management.database_reading_writing import DataRetriever

tp_table = TableConfig().TPDataTable
etc_table = TableConfig().ETCDataTable
etc_xy_table = TableConfig().ThermalConductivityXyDataTable
cycle_table = TableConfig().CycleDataTable
meta_table = TableConfig().MetaDataTable

EXCLUDE_TABLES = {meta_table.table_name,
                  etc_xy_table.table_name}       # tables to hide
EXCLUDE_COLUMNS = {
                      etc_table: {etc_table.file},
                    }


class TableSelectorWindow(QWidget):

    def __init__(self, config):
        super().__init__()
        self.setWindowTitle("Table and Column Selector")
        self.resize(400, 200)

        self.config = config

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
        self.setMinimumSize(700, 450)
        # Initialize columns for the default table
        self.update_columns(self.tableCombo.currentText())

        # Connect signal
        self.tableCombo.currentTextChanged.connect(self.update_columns)

    def _init_ui(self):
        main_layout = QHBoxLayout(self)
        main_layout.addLayout(self._init_combo_boxes())
        main_layout.addWidget(self._init_plot_window())


    def _init_combo_boxes(self):
        layout = QVBoxLayout(self)

        # Table selection
        layout.addWidget(QLabel("Select Table:"))
        self.tableCombo = QComboBox()
        self.tableCombo.addItems(sorted(self.table_map.keys()))
        layout.addWidget(self.tableCombo)

        # First column selection
        layout.addWidget(QLabel("Select Column 1:"))
        self.colCombo1 = QComboBox()
        layout.addWidget(self.colCombo1)

        # Second column selection
        layout.addWidget(QLabel("Select Column 2:"))
        self.colCombo2 = QComboBox()
        layout.addWidget(self.colCombo2)
        return layout


    def _init_plot_window(self):

        self.plot_widget = IndividualPlotWindow(config=self.config)
        #self.plot_widget.setBackground('r')  # optional: set a white background
        return self.plot_widget


    def update_columns(self, table_name: str):
        # Get all column names
        cols = TableConfig().get_table_column_names(table_name=table_name) or []

        # Filter out the ones in EXCLUDE_COLUMNS
        ex = EXCLUDE_COLUMNS.get(table_name, set())
        filtered = [c for c in cols if c not in ex]

        # Sort and populate
        filtered.sort()
        self.colCombo1.clear();  self.colCombo1.addItems(filtered)
        self.colCombo2.clear();  self.colCombo2.addItems(filtered)


class IndividualPlotWindow(pg.PlotWidget):

    def __init__(self, config, parent=None):
        super().__init__(parent=parent)
        self.config = config

    def load_data(self, table_name, x_col, y_col, time_range=None, constraints=None):


        pass




if __name__ == '__main__':
    import sys
    from src.infrastructure.core.config_reader import GetConfig
    app = QApplication(sys.argv)
    win = TableSelectorWindow(config=GetConfig())
    win.show()
    sys.exit(app.exec())
