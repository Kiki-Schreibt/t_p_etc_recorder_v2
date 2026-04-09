# file: hydride_gui_v3.py

from __future__ import annotations
import sys
import json
import csv
from typing import Any, Dict
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout,
    QFormLayout, QLineEdit, QComboBox, QPushButton, QLabel, QGroupBox,
    QHBoxLayout, QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox, QTextEdit, QDoubleSpinBox, QFileDialog
)
from PySide6.QtCore import Qt

import src.infrastructure.core.config_reader

# ---- Import backend ----------------------------------------------------------
try:
    from src.infrastructure.handler.hydride_handler import (
        MetalHydrideDatabase, PeriodicTableOfElements, parse_chemical_formula
    )
except ImportError:
    from hydride_handler import MetalHydrideDatabase, PeriodicTableOfElements, parse_chemical_formula


# ---- Utility Functions -------------------------------------------------------
def j_to_kj(value: float | None) -> float | None:
    return None if value is None else value / 1000.0


def show_error(parent, title: str, err: Exception):
    QMessageBox.critical(parent, title, f"{err}")


# ---- Lookup Tab --------------------------------------------------------------
class LookupTab(QWidget):
    def __init__(self, mh_db: MetalHydrideDatabase, parent=None):
        super().__init__(parent)
        self.mh_db = mh_db

        # Inputs
        self.h_edit = QLineEdit()
        self.h_edit.setPlaceholderText("e.g. MgH2 or LaNi0.85Al0.15H6")
        self.state_combo = QComboBox()
        self.state_combo.addItems(["Dehydrogenated", "Hydrogenated"])
        self.btn_lookup = QPushButton("Lookup")
        self.btn_lookup.clicked.connect(self.lookup)

        # Outputs
        self.out_norm = QLabel("-")
        self.out_parse = QLabel("-")
        self.out_enthalpy = QLabel("-")
        self.out_entropy = QLabel("-")
        self.out_density = QLabel("-")
        self.out_cond = QLabel("-")
        self.out_capacity = QLabel("-")
        self.out_molar = QLabel("-")
        self.out_hmass = QLabel("-")

        # Layouts
        form = QFormLayout()
        form.addRow("Hydride:", self.h_edit)
        form.addRow("State:", self.state_combo)
        form.addRow(self.btn_lookup)

        results = QFormLayout()
        results.addRow("Normalized:", self.out_norm)
        results.addRow("Parsed formula:", self.out_parse)
        results.addRow("Enthalpy:", self.out_enthalpy)
        results.addRow("Entropy:", self.out_entropy)
        results.addRow("Density:", self.out_density)
        results.addRow("Bulk conductivity:", self.out_cond)
        results.addRow("Theoretical H capacity:", self.out_capacity)
        results.addRow("Molar mass (total):", self.out_molar)
        results.addRow("Hydrogen mass:", self.out_hmass)

        wrapper = QHBoxLayout()
        left = QGroupBox("Query")
        left.setLayout(form)
        right = QGroupBox("Results")
        right.setLayout(results)
        wrapper.addWidget(left, 1)
        wrapper.addWidget(right, 2)

        self.setLayout(wrapper)

    def lookup(self):
        try:
            hydride = self.h_edit.text().strip()
            if not hydride:
                return
            normalized = self.mh_db._normalize_hydride_string(hydride)
            parsed = parse_chemical_formula(normalized)
            H_j, S_jpk = self.mh_db.get_enthalpy_entropy(hydride)
            enthalpy_str = f"{H_j:.0f} J/mol  ({j_to_kj(H_j):.3f} kJ/mol)" if H_j else "—"
            entropy_str = f"{S_jpk:.3f} J/(mol·K)  ({j_to_kj(S_jpk):.6f} kJ/(mol·K))" if S_jpk else "—"
            state = self.state_combo.currentText()
            density = self.mh_db.get_density(hydride, state=state)
            conductivity = self.mh_db.get_bulk_conductivity(hydride, state=state)
            capacity = self.mh_db.get_capacity(hydride)
            total_m, h_m = self.mh_db.get_molar_mass_hydride(hydride, return_hydrogen_mass=True)

            # Update UI
            self.out_norm.setText(normalized or "—")
            self.out_parse.setText(json.dumps(parsed, ensure_ascii=False))
            self.out_enthalpy.setText(enthalpy_str)
            self.out_entropy.setText(entropy_str)
            self.out_density.setText("—" if density is None else f"{density}")
            self.out_cond.setText("—" if conductivity is None else f"{conductivity}")
            self.out_capacity.setText("—" if capacity is None else f"{capacity:.3f} wt-%")
            self.out_molar.setText("—" if total_m is None else f"{total_m:.6f} u")
            self.out_hmass.setText("—" if h_m is None else f"{h_m:.6f} u")

        except Exception as e:
            show_error(self, "Lookup Failed", e)


# ---- Edit Tab ---------------------------------------------------------------
class EditTab(QWidget):
    def __init__(self, mh_db: MetalHydrideDatabase, parent=None):
        super().__init__(parent)
        self.mh_db = mh_db

        # Fields
        self.h_edit = QLineEdit()
        self.enthalpy_kj = QDoubleSpinBox()
        self.enthalpy_kj.setMaximum(1e9)
        self.entropy_kjpk = QDoubleSpinBox()
        self.entropy_kjpk.setMaximum(1e9)
        self.update_json = QTextEdit()
        self.update_json.setPlaceholderText('Optional JSON for update, e.g. {"Conductivity_Bulk":{"Hydrogenated":1.2}}')

        # Buttons
        self.btn_add = QPushButton("Add")
        self.btn_add.clicked.connect(self.add_hydride)
        self.btn_update = QPushButton("Update")
        self.btn_update.clicked.connect(self.update_hydride)
        self.btn_remove = QPushButton("Remove")
        self.btn_remove.clicked.connect(self.remove_hydride)

        # Layout
        form = QFormLayout()
        form.addRow("Hydride:", self.h_edit)
        form.addRow("Enthalpy (kJ/mol):", self.enthalpy_kj)
        form.addRow("Entropy (kJ/(mol·K)):", self.entropy_kjpk)

        buttons = QHBoxLayout()
        buttons.addWidget(self.btn_add)
        buttons.addWidget(self.btn_update)
        buttons.addWidget(self.btn_remove)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addWidget(self.update_json)
        layout.addLayout(buttons)
        layout.addStretch(1)
        self.setLayout(layout)

    def add_hydride(self):
        try:
            name = self.h_edit.text().strip()
            self.mh_db.hydride_adder(name, self.enthalpy_kj.value(), self.entropy_kjpk.value())
            QMessageBox.information(self, "Added", f"'{name}' added.")
        except Exception as e:
            show_error(self, "Add Failed", e)

    def update_hydride(self):
        try:
            name = self.h_edit.text().strip()
            data = json.loads(self.update_json.toPlainText() or "{}")
            self.mh_db.update_hydride_info(name, data)
            QMessageBox.information(self, "Updated", f"'{name}' updated.")
        except Exception as e:
            show_error(self, "Update Failed", e)

    def remove_hydride(self):
        try:
            name = self.h_edit.text().strip()
            self.mh_db.hydride_remover(name)
            QMessageBox.information(self, "Removed", f"'{name}' removed.")
        except Exception as e:
            show_error(self, "Remove Failed", e)


# ---- Database Table Tab ------------------------------------------------------
class TableTab(QWidget):
    def __init__(self, mh_db: MetalHydrideDatabase, parent=None):
        super().__init__(parent)
        self.mh_db = mh_db

        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            "Hydride", "Enthalpy (J/mol)", "Entropy (J/(mol·K))", "Density", "Capacity (wt-%)", "Bulk Conductivity"
        ])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        self.refresh_btn = QPushButton("Refresh Table")
        self.refresh_btn.clicked.connect(self.load_table)
        self.export_btn = QPushButton("Export CSV")
        self.export_btn.clicked.connect(self.export_csv)

        buttons = QHBoxLayout()
        buttons.addWidget(self.refresh_btn)
        buttons.addWidget(self.export_btn)
        buttons.addStretch(1)

        layout = QVBoxLayout()
        layout.addLayout(buttons)
        layout.addWidget(self.table)
        self.setLayout(layout)

        self.load_table()

    def load_table(self):
        self.table.setRowCount(0)
        try:
            all_hydrides = self.mh_db.get_all_hydrides()
            for row_idx, h in enumerate(all_hydrides):
                self.table.insertRow(row_idx)
                self.table.setItem(row_idx, 0, QTableWidgetItem(str(h)))
                enthalpy, entropy = self.mh_db.get_enthalpy_entropy(h)
                self.table.setItem(row_idx, 1, QTableWidgetItem(str(enthalpy or "—")))
                self.table.setItem(row_idx, 2, QTableWidgetItem(str(entropy or "—")))
                density = self.mh_db.get_density(h)
                self.table.setItem(row_idx, 3, QTableWidgetItem(str(density or "—")))
                capacity = self.mh_db.get_capacity(h)
                self.table.setItem(row_idx, 4, QTableWidgetItem(f"{capacity:.3f}" if capacity else "—"))
                conductivity = self.mh_db.get_bulk_conductivity(h)
                self.table.setItem(row_idx, 5, QTableWidgetItem(str(conductivity or "—")))
        except Exception as e:
            show_error(self, "Load Table Failed", e)

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Save CSV", filter="CSV Files (*.csv)")
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                headers = [self.table.horizontalHeaderItem(i).text() for i in range(self.table.columnCount())]
                writer.writerow(headers)
                for row in range(self.table.rowCount()):
                    row_data = [self.table.item(row, col).text() if self.table.item(row, col) else "" for col in range(self.table.columnCount())]
                    writer.writerow(row_data)
            QMessageBox.information(self, "Exported", f"Table exported to {path}")
        except Exception as e:
            show_error(self, "CSV Export Failed", e)


# ---- Main Window ------------------------------------------------------------
class HydrideHandlerMainWindow(QMainWindow):
    def __init__(self, db_conn_params: MetalHydrideDatabase):
        super().__init__()

        try:
            mh_database = MetalHydrideDatabase(db_conn_params=db_conn_params)  # Provide your DB params if needed
        except Exception as e:
            QMessageBox.critical(None, "Database Error", f"Cannot load MetalHydrideDatabase: {e}")
            sys.exit(1)

        self.mh_db = mh_database
        self.setWindowTitle("Metal Hydride Manager")

        self.tabs = QTabWidget()
        self.lookup_tab = LookupTab(self.mh_db)
        self.edit_tab = EditTab(self.mh_db)
        self.table_tab = TableTab(self.mh_db)
        self.tabs.addTab(self.lookup_tab, "Lookup")
        self.tabs.addTab(self.edit_tab, "Edit")
        self.tabs.addTab(self.table_tab, "Database Table")

        self.setCentralWidget(self.tabs)


# ---- Run Application --------------------------------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)

    window = HydrideHandlerMainWindow(db_conn_params=src.infrastructure.core.config_reader.config.db_conn_params)
    window.resize(1200, 700)
    window.show()
    sys.exit(app.exec())
