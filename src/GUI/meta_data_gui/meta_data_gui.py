import sys
from datetime import timedelta

from PySide6.QtCore import Qt, QAbstractTableModel, QSortFilterProxyModel
from PySide6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QPushButton, QLineEdit, QTableView, QMessageBox, QLabel,
    QDoubleSpinBox, QSpinBox
)

from src.infrastructure.core.config_reader import config
from src.infrastructure.connections.connections import DatabaseConnection
from src.infrastructure.core.table_config import TableConfig
from metadata_handler import MetaData


# --------------------------------------------------
# Duration widget
# --------------------------------------------------

class DurationWidget(QWidget):

    def __init__(self):
        super().__init__()

        layout = QHBoxLayout(self)

        self.hours = QSpinBox()
        self.minutes = QSpinBox()
        self.seconds = QSpinBox()

        self.hours.setRange(0, 999)
        self.minutes.setRange(0, 59)
        self.seconds.setRange(0, 59)

        layout.addWidget(QLabel("h"))
        layout.addWidget(self.hours)

        layout.addWidget(QLabel("m"))
        layout.addWidget(self.minutes)

        layout.addWidget(QLabel("s"))
        layout.addWidget(self.seconds)

    def setValue(self, td):

        if not td:
            self.hours.setValue(0)
            self.minutes.setValue(0)
            self.seconds.setValue(0)
            return

        total = int(td.total_seconds())

        self.hours.setValue(total // 3600)
        self.minutes.setValue((total % 3600) // 60)
        self.seconds.setValue(total % 60)

    def value(self):

        return timedelta(
            hours=self.hours.value(),
            minutes=self.minutes.value(),
            seconds=self.seconds.value()
        )


# --------------------------------------------------
# Table Model
# --------------------------------------------------

class SampleTableModel(QAbstractTableModel):

    def __init__(self, data, headers):
        super().__init__()

        self._data = data
        self.headers = headers

    def rowCount(self, parent=None):
        return len(self._data)

    def columnCount(self, parent=None):
        return len(self.headers)

    def data(self, index, role):

        if not index.isValid():
            return None

        if role == Qt.DisplayRole:
            return self._data[index.row()][index.column()]

    def headerData(self, section, orientation, role):

        if role == Qt.DisplayRole:

            if orientation == Qt.Horizontal:
                return self.headers[section]

            if orientation == Qt.Vertical:
                return section + 1


# --------------------------------------------------
# Dynamic Metadata Form
# --------------------------------------------------

class MetadataForm(QWidget):

    UNIT_MAPPING = {
        "mass": "g",
        "pressure": "bar",
        "temperature": "K",
        "volume_measurement_cell": "mL",
        "volume": "L",
        "reservoir_volume": "L",
        "enthalpy": "kJ/mol",
        "entropy": "kJ/(mol·K)"
    }

    def __init__(self):
        super().__init__()

        self.layout = QFormLayout(self)
        self.widgets = {}

        for column, attr in MetaData.column_attribute_mapping.items():

            # ------------------------
            # Widget selection
            # ------------------------

            if attr == "average_cycle_duration":
                widget = DurationWidget()

            elif any(x in attr for x in [
                "mass", "pressure", "volume",
                "enthalpy", "entropy", "temperature"
            ]):
                widget = QDoubleSpinBox()
                widget.setMaximum(1e9)
                widget.setDecimals(6)

            else:
                widget = QLineEdit()

            self.widgets[attr] = widget

            # ------------------------
            # Label with units
            # ------------------------

            label = attr.replace("_", " ").title()

            unit = self.get_unit(attr)
            if unit:
                label += f" ({unit})"

            self.layout.addRow(label, widget)

    # ------------------------

    def get_unit(self, attr):

        # direct match first
        if attr in self.UNIT_MAPPING:
            return self.UNIT_MAPPING[attr]

        # fallback: substring match
        for key, unit in self.UNIT_MAPPING.items():
            if key in attr:
                return unit

        return None

    # ------------------------

    def set_metadata(self, meta):

        for attr, widget in self.widgets.items():

            value = getattr(meta, attr, None)

            if isinstance(widget, QLineEdit):
                widget.setText("" if value is None else str(value))

            elif isinstance(widget, QDoubleSpinBox):
                widget.setValue(value if value else 0)

            elif isinstance(widget, DurationWidget):
                widget.setValue(value)

    # ------------------------

    def apply_to_metadata(self, meta):

        for attr, widget in self.widgets.items():

            if isinstance(widget, QLineEdit):
                text = widget.text().strip()
                setattr(meta, attr, text if text else None)

            elif isinstance(widget, QDoubleSpinBox):
                val = widget.value()
                setattr(meta, attr, val if val != 0 else None)

            elif isinstance(widget, DurationWidget):
                setattr(meta, attr, widget.value())


# --------------------------------------------------
# Main GUI
# --------------------------------------------------

class MetaDataGUI(QWidget):

    def __init__(self):

        super().__init__()

        self.db_params = config.db_conn_params
        self.current_meta = None

        self.setWindowTitle("Hydride Experiment Metadata Manager")
        self.resize(1200, 650)

        layout = QHBoxLayout(self)

        # -----------------------
        # Left side
        # -----------------------

        left_layout = QVBoxLayout()

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search sample_id...")

        self.table = QTableView()
        self.table.setSortingEnabled(True)

        left_layout.addWidget(self.search)
        left_layout.addWidget(self.table)

        layout.addLayout(left_layout, 1)

        # -----------------------
        # Right side
        # -----------------------

        right_layout = QVBoxLayout()

        self.form = MetadataForm()

        right_layout.addWidget(self.form)

        btn_layout = QHBoxLayout()

        self.new_btn = QPushButton("New")
        self.save_btn = QPushButton("Save")
        self.delete_btn = QPushButton("Delete")

        btn_layout.addWidget(self.new_btn)
        btn_layout.addWidget(self.save_btn)
        btn_layout.addWidget(self.delete_btn)

        right_layout.addLayout(btn_layout)

        layout.addLayout(right_layout, 1)

        # signals
        self.new_btn.clicked.connect(self.new_sample)
        self.save_btn.clicked.connect(self.save)
        self.delete_btn.clicked.connect(self.delete)
        self.table.clicked.connect(self.load_sample)
        self.search.textChanged.connect(self.filter_samples)

        self.load_samples()

    # --------------------------------------------------

    def load_samples(self):

        table = TableConfig().MetaDataTable

        query = f'SELECT "{table.sample_id}" FROM "{table.table_name}" ORDER BY "{table.sample_id}"'

        with DatabaseConnection(**self.db_params) as conn:

            conn.cursor.execute(query)
            rows = conn.cursor.fetchall()

        data = [[r[0]] for r in rows]

        self.model = SampleTableModel(data, ["Sample ID"])

        self.proxy = QSortFilterProxyModel()
        self.proxy.setSourceModel(self.model)
        self.proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)

        self.table.setModel(self.proxy)

    # --------------------------------------------------

    def filter_samples(self, text):

        self.proxy.setFilterKeyColumn(0)
        self.proxy.setFilterFixedString(text)

    # --------------------------------------------------

    def load_sample(self, index):

        source_index = self.proxy.mapToSource(index)

        sample_id = self.model._data[source_index.row()][0]

        self.current_meta = MetaData(
            sample_id=sample_id,
            db_conn_params=self.db_params
        )

        self.form.set_metadata(self.current_meta)

    # --------------------------------------------------

    def new_sample(self):

        self.current_meta = MetaData(
            sample_id=None,
            db_conn_params=self.db_params
        )

        for w in self.form.widgets.values():

            if isinstance(w, QLineEdit):
                w.clear()

            elif isinstance(w, QDoubleSpinBox):
                w.setValue(0)

            elif isinstance(w, DurationWidget):
                w.setValue(None)

    # --------------------------------------------------

    def save(self):

        if not self.current_meta:

            QMessageBox.warning(self, "Error", "No metadata loaded")
            return

        self.form.apply_to_metadata(self.current_meta)

        if not self.current_meta.sample_id:

            QMessageBox.warning(self, "Error", "Sample ID required")
            return

        try:

            self.current_meta.write()

            QMessageBox.information(self, "Saved", "Metadata saved")

            self.load_samples()

        except Exception as e:

            QMessageBox.critical(self, "Database Error", str(e))

    # --------------------------------------------------

    def delete(self):

        if not self.current_meta or not self.current_meta.sample_id:
            return

        confirm = QMessageBox.question(
            self,
            "Confirm Deletion",
            f"Delete sample '{self.current_meta.sample_id}'?\nThis cannot be undone.",
            QMessageBox.Yes | QMessageBox.No
        )

        if confirm != QMessageBox.Yes:
            return

        try:

            self.current_meta.remove_sample_id()

            self.current_meta = None

            self.load_samples()

            QMessageBox.information(self, "Deleted", "Sample removed")

        except Exception as e:

            QMessageBox.critical(self, "Error", str(e))


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    app = QApplication(sys.argv)

    gui = MetaDataGUI()
    gui.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
