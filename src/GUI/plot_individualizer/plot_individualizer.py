#plot_individualizer.py

from PySide6.QtWidgets import (QApplication, QWidget, QVBoxLayout,
                                       QLabel, QComboBox)
from table_config import TableConfig


EXCLUDE_TABLES = {"meta_data", "cycle_data"}       # tables to hide
EXCLUDE_COLUMNS = {
                      "t_p_data": {"time"},
                      "thermal_conductivity_data": {"\"Time\"", "\"File\""},
                    }


class TableSelectorWindow(QWidget):
               # columns to hide


    def __init__(self):
        super().__init__()
        self.setWindowTitle("Table and Column Selector")
        self.resize(400, 200)

        self.config = TableConfig()
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


        # Initialize columns for the default table
        self.update_columns(self.tableCombo.currentText())

        # Connect signal
        self.tableCombo.currentTextChanged.connect(self.update_columns)

    def _init_ui(self):
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

    def update_columns(self, table_name: str):
        # Get all column names
        cols = self.config.get_table_column_names(table_name=table_name) or []

        # Filter out the ones in EXCLUDE_COLUMNS
        ex = EXCLUDE_COLUMNS.get(table_name, set())
        filtered = [c for c in cols if c not in ex]

        # Sort and populate
        filtered.sort()
        self.colCombo1.clear();  self.colCombo1.addItems(filtered)
        self.colCombo2.clear();  self.colCombo2.addItems(filtered)


if __name__ == '__main__':
    import sys
    app = QApplication(sys.argv)
    win = TableSelectorWindow()
    win.show()
    sys.exit(app.exec())
