# file: hydride_gui.py
from __future__ import annotations

import json
import os
import sys
import traceback
from typing import Any, Dict, List, Tuple

from PySide6.QtCore import Qt
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QTabWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QFormLayout, QLabel, QLineEdit, QComboBox, QPushButton,
    QGroupBox, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox,
    QSplitter, QDoubleSpinBox, QTextEdit, QHeaderView
)

# ---- Import your backend -----------------------------------------------------
# Adjust these imports if your module path differs.
try:
    # If hydride_handler.py is in the working directory or installed module.
    from src.infrastructure.handler.hydride_handler import (
        MetalHydrideDatabase,
        PeriodicTableOfElements,
        parse_chemical_formula,
        create_hydride_data_base,
        create_periodic_table_of_elements,
    )
    from src.infrastructure.utils.standard_paths import (
        standard_hydride_data_base_path,
        standard_periodic_table_path,
        standard_periodic_table_txt,
    )

except Exception as e:
    # Fallback: try relative import if this file sits next to hydride_handler.py
    try:
        sys.path.append(os.path.abspath(os.path.dirname(__file__)))
        from hydride_handler import (
            MetalHydrideDatabase,
            PeriodicTableOfElements,
            parse_chemical_formula,
            create_hydride_data_base,
            create_periodic_table_of_elements,
        )
        from src.infrastructure.utils.standard_paths import (
            standard_hydride_data_base_path,
            standard_periodic_table_path,
            standard_periodic_table_txt,
        )
    except Exception as e2:
        QMessageBox.critical(
            None,
            "Import Error",
            f"Could not import backend modules.\n\n"
            f"{e}\n\n{e2}\n\nMake sure hydride_handler.py and standard_paths are on PYTHONPATH."
        )
        raise

from src.infrastructure.core.global_vars import style as STYLE_SHEET

# ---- Small helpers -----------------------------------------------------------
def j_to_kj(value_j: float | None) -> float | None:
    if value_j is None:
        return None
    return value_j / 1000.0

def safe_float(x: Any, default: float | None = None) -> float | None:
    try:
        return float(x)
    except Exception:
        return default

def show_error(parent, title: str, err: Exception):
    tb = traceback.format_exc()
    QMessageBox.critical(parent, title, f"{err}\n\n{tb}")

def yesno(parent, title: str, text: str) -> bool:
    return QMessageBox.question(parent, title, text, QMessageBox.Yes | QMessageBox.No) == QMessageBox.Yes


# ---- GUI widgets -------------------------------------------------------------
class LookupTab(QWidget):
    def __init__(self, db: MetalHydrideDatabase, parent=None):
        super().__init__(parent)
        self.db = db
        self.pt = db.periodic_table_of_elements  # convenience

        # Inputs
        self.h_edit = QLineEdit()
        self.h_edit.setPlaceholderText("e.g. MgH2 or LaNi0.85Al0.15H6")
        self.state_combo = QComboBox()
        self.state_combo.addItems(["Dehydrogenated", "Hydrogenated"])

        # Buttons
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
                QMessageBox.information(self, "Input needed", "Enter a hydride formula/name.")
                return

            normalized = self.db._normalize_hydride_string(hydride)
            parsed = parse_chemical_formula(normalized)

            # enthalpy / entropy (in DB they are J-based; display both J and kJ for clarity)
            H_j, S_jpk = self.db.get_enthalpy_entropy(hydride)
            enthalpy_str = "—"
            entropy_str = "—"
            if H_j is not None:
                enthalpy_str = f"{H_j:.0f} J/mol  ({j_to_kj(H_j):.3f} kJ/mol)"
            if S_jpk is not None:
                # Also display kJ/(mol*K) for convenience
                entropy_str = f"{S_jpk:.3f} J/(mol·K)  ({j_to_kj(S_jpk):.6f} kJ/(mol·K))"

            state = self.state_combo.currentText()
            density = self.db.get_density(hydride, state=state)
            conductivity = self.db.get_bulk_conductivity(hydride, state="Hydrogenated")  # your API default

            cap = self.db.get_capacity(hydride)
            total_m, h_m = self.db.get_molar_mass_hydride(hydride, return_hydrogen_mass=True)

            # Render
            self.out_norm.setText(normalized or "—")
            self.out_parse.setText(json.dumps(parsed, ensure_ascii=False))
            self.out_enthalpy.setText(enthalpy_str)
            self.out_entropy.setText(entropy_str)
            self.out_density.setText("—" if density is None else f"{density}")
            self.out_cond.setText("—" if conductivity is None else f"{conductivity}")
            self.out_capacity.setText("—" if cap is None else f"{cap:.3f} wt-%")
            self.out_molar.setText("—" if total_m is None else f"{total_m:.6f} u")
            self.out_hmass.setText("—" if h_m is None else f"{h_m:.6f} u")

        except Exception as err:
            show_error(self, "Lookup failed", err)


class EditTab(QWidget):
    def __init__(self, db: MetalHydrideDatabase, parent=None):
        super().__init__(parent)
        self.db = db

        # Fields
        self.h_edit = QLineEdit()
        self.enthalpy_kj = QDoubleSpinBox()
        self.enthalpy_kj.setDecimals(6)
        self.enthalpy_kj.setMaximum(1e9)
        self.entropy_kjpk = QDoubleSpinBox()
        self.entropy_kjpk.setDecimals(6)
        self.entropy_kjpk.setMaximum(1e9)

        self.update_json = QTextEdit()
        self.update_json.setPlaceholderText('Optional: extra fields as JSON, e.g. {"Conductivity_Bulk": {"Hydrogenated": 1.2}}')

        # Buttons
        self.btn_add = QPushButton("Add (expects kJ units)")
        self.btn_add.clicked.connect(self.add_hydride)
        self.btn_update = QPushButton("Update (merge JSON)")
        self.btn_update.clicked.connect(self.update_hydride)
        self.btn_remove = QPushButton("Remove")
        self.btn_remove.clicked.connect(self.remove_hydride)

        # Layout
        form = QFormLayout()
        form.addRow("Hydride name:", self.h_edit)
        form.addRow("Enthalpy (kJ/mol):", self.enthalpy_kj)
        # Note: your hydride_adder expects entropy in *kJ/(mol*K)* (then converts to J) per docstring.
        form.addRow("Entropy (kJ/(mol·K)):", self.entropy_kjpk)

        btns = QHBoxLayout()
        btns.addWidget(self.btn_add)
        btns.addWidget(self.btn_update)
        btns.addWidget(self.btn_remove)

        layout = QVBoxLayout()
        layout.addLayout(form)

        box = QGroupBox("Optional JSON update on 'Update'")
        vb = QVBoxLayout()
        vb.addWidget(self.update_json)
        box.setLayout(vb)

        layout.addWidget(box)
        layout.addLayout(btns)
        layout.addStretch(1)
        self.setLayout(layout)

    def add_hydride(self):
        try:
            name = self.h_edit.text().strip()
            if not name:
                QMessageBox.information(self, "Input needed", "Hydride name is required.")
                return
            H_kj = self.enthalpy_kj.value()
            S_kjpk = self.entropy_kjpk.value()
            self.db.hydride_adder(name, H_kj, S_kjpk)
            QMessageBox.information(self, "Added", f"Hydride '{name}' added.")
        except Exception as err:
            show_error(self, "Add failed", err)

    def update_hydride(self):
        try:
            name = self.h_edit.text().strip()
            if not name:
                QMessageBox.information(self, "Input needed", "Hydride name is required.")
                return
            raw = self.update_json.toPlainText().strip()
            if not raw:
                QMessageBox.information(self, "Nothing to update", "Provide JSON data to merge.")
                return
            data = json.loads(raw)
            self.db.update_hydride_info(name, data)
            QMessageBox.information(self, "Updated", f"Hydride '{name}' updated.")
        except json.JSONDecodeError as je:
            show_error(self, "Invalid JSON", je)
        except Exception as err:
            show_error(self, "Update failed", err)

    def remove_hydride(self):
        try:
            name = self.h_edit.text().strip()
            if not name:
                QMessageBox.information(self, "Input needed", "Hydride name is required.")
                return
            if not yesno(self, "Confirm removal", f"Remove '{name}' from database?"):
                return
            self.db.hydride_remover(name)
            QMessageBox.information(self, "Removed", f"Hydride '{name}' removed (if it existed).")
        except Exception as err:
            show_error(self, "Remove failed", err)


class DatabaseTab(QWidget):
    def __init__(self, db: MetalHydrideDatabase, parent=None):
        super().__init__(parent)
        self.db = db

        self.table = QTableWidget()
        self.table.setColumnCount(3)
        self.table.setHorizontalHeaderLabels(["Hydride", "Enthalpy [kJ/mol]", "Entropy [kJ/(mol·K)]"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.setSortingEnabled(True)

        self.btn_refresh = QPushButton("Refresh")
        self.btn_refresh.clicked.connect(self.populate)
        self.btn_export = QPushButton("Export CSV…")
        self.btn_export.clicked.connect(self.export_csv)
        self.btn_demo = QPushButton("Rebuild Demo DB")
        self.btn_demo.clicked.connect(self.rebuild_demo)

        #path_lbl = QLabel(f"DB file: {self.db.hydride_data_base_path}")
        #path_lbl.setTextInteractionFlags(Qt.TextSelectableByMouse)

        top = QHBoxLayout()
        top.addWidget(self.btn_refresh)
        top.addWidget(self.btn_export)
        top.addStretch(1)
        top.addWidget(self.btn_demo)

        layout = QVBoxLayout()
        layout.addLayout(top)
        layout.addWidget(self.table)
        #layout.addWidget(path_lbl)
        self.setLayout(layout)

        self.populate()

    def populate(self):
        try:
            data: List[Dict[str, Any]] = self.db.hydride_data_base
            self.table.setRowCount(len(data))
            for r, entry in enumerate(data):
                name = str(entry.get("Hydride", ""))
                H_j = safe_float(entry.get("Enthalpy"))
                S_jpk = safe_float(entry.get("Entropy"))

                H_kj = "" if H_j is None else f"{H_j/1000.0:.6f}"
                S_kjpk = "" if S_jpk is None else f"{S_jpk/1000.0:.6f}"

                self.table.setItem(r, 0, QTableWidgetItem(name))
                self.table.setItem(r, 1, QTableWidgetItem(H_kj))
                self.table.setItem(r, 2, QTableWidgetItem(S_kjpk))
        except Exception as err:
            show_error(self, "Load failed", err)

    def export_csv(self):
        try:
            path, _ = QFileDialog.getSaveFileName(self, "Export database to CSV", "hydrides.csv", "CSV (*.csv)")
            if not path:
                return
            data: List[Dict[str, Any]] = self.db.hydride_data_base
            # Collect headers
            headers = set()
            for row in data:
                headers.update(row.keys())
            headers = list(sorted(headers))

            import csv
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for row in data:
                    writer.writerow(row)
            QMessageBox.information(self, "Exported", f"Exported {len(data)} rows to:\n{path}")
        except Exception as err:
            show_error(self, "Export failed", err)

    def rebuild_demo(self):
        try:
            if not yesno(self, "Confirm", "This will overwrite the current DB file with the demo data shipped in code.\nProceed?"):
                return
            create_hydride_data_base()
            # reload DB
            self.db.load_hydride_database()
            self.populate()
            QMessageBox.information(self, "Done", "Demo hydride database written and reloaded.")
        except Exception as err:
            show_error(self, "Rebuild failed", err)


class PeriodicTab(QWidget):
    def __init__(self, db: MetalHydrideDatabase, parent=None):
        super().__init__(parent)
        self.db = db
        self.pt = db.periodic_table_of_elements

        self.symbol_edit = QLineEdit()
        self.symbol_edit.setPlaceholderText("e.g. H, Mg, Ni")
        self.btn_atomic = QPushButton("Get atomic mass")
        self.btn_atomic.clicked.connect(self.get_atomic_mass)
        self.out_mass = QLabel("-")

        self.btn_rebuild_pt = QPushButton("Rebuild Periodic JSON from CSV")
        self.btn_rebuild_pt.clicked.connect(self.rebuild_pt)

        layout = QFormLayout()
        layout.addRow("Element symbol:", self.symbol_edit)
        layout.addRow(self.btn_atomic)
        layout.addRow("Atomic mass (u):", self.out_mass)
        layout.addRow(self.btn_rebuild_pt)

        path_lbl = QLabel(f"Periodic JSON: {self.pt.periodic_table_path}")
        path_lbl.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.path_lbl2 = QLabel(f"CSV source: {standard_periodic_table_txt}")
        self.path_lbl2.setTextInteractionFlags(Qt.TextSelectableByMouse)

        v = QVBoxLayout()
        v.addLayout(layout)
        v.addWidget(path_lbl)
        v.addWidget(self.path_lbl2)
        v.addStretch(1)
        self.setLayout(v)

    def get_atomic_mass(self):
        try:
            sym = self.symbol_edit.text().strip()
            if not sym:
                QMessageBox.information(self, "Input needed", "Enter an element symbol.")
                return
            mass = self.pt.atomic_mass_grabber(sym)
            self.out_mass.setText("—" if mass is None else f"{mass}")
        except Exception as err:
            show_error(self, "Lookup failed", err)

    def rebuild_pt(self):
        try:
            if not yesno(self, "Confirm", "Rebuild periodic table JSON from CSV? Existing JSON will be overwritten."):
                return
            create_periodic_table_of_elements()
            self.pt.load_periodic_table()
            QMessageBox.information(self, "Done", "Periodic table JSON rebuilt and reloaded.")
        except Exception as err:
            show_error(self, "Rebuild failed", err)


# ---- Main Window -------------------------------------------------------------
class HydrideHandlerMainWindow(QMainWindow):
    def __init__(self, db_path: str | None = None):
        super().__init__()
        self.setWindowTitle("Metal Hydride Toolkit")
        self.setStyleSheet(STYLE_SHEET)

        # if a custom DB path is passed, swap it into the db instance
        if db_path:
            # Monkey-patch: instantiate db, then change path and reload
            self.db = MetalHydrideDatabase()
            self.db.hydride_data_base_path = db_path
            self.db.load_hydride_database()
        else:
            self.db = MetalHydrideDatabase()

        self.tabs = QTabWidget()
        self.lookup_tab = LookupTab(self.db, self)
        self.edit_tab = EditTab(self.db, self)
        self.db_tab = DatabaseTab(self.db, self)
        self.pt_tab = PeriodicTab(self.db, self)

        self.tabs.addTab(self.lookup_tab, "Lookup")
        self.tabs.addTab(self.edit_tab, "Edit")
        self.tabs.addTab(self.db_tab, "Database")
        self.tabs.addTab(self.pt_tab, "Periodic Table")

        container = QWidget()
        v = QVBoxLayout(container)
        v.addWidget(self.tabs)
        self.setCentralWidget(container)

        self._make_menu()

        self.resize(1000, 700)

    def _make_menu(self):
        m = self.menuBar()
        file_menu = m.addMenu("&File")

        act_open_db = QAction("Open Hydride DB…", self)
        act_open_db.triggered.connect(self.open_db_file)
        file_menu.addAction(act_open_db)

        act_reload = QAction("Reload DB", self)
        act_reload.triggered.connect(self.reload_db)
        file_menu.addAction(act_reload)

        file_menu.addSeparator()

        act_quit = QAction("Quit", self)
        act_quit.triggered.connect(self.close)
        file_menu.addAction(act_quit)

        help_menu = m.addMenu("&Help")
        act_about = QAction("About", self)
        act_about.triggered.connect(self.show_about)
        help_menu.addAction(act_about)

    def open_db_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select hydride database JSON", "", "JSON (*.json)")
        if not path:
            return
        try:
            # swap path & reload
            self.db.hydride_data_base_path = path
            self.db.load_hydride_database()
            self.db_tab.populate()
            QMessageBox.information(self, "Loaded", f"Loaded DB from:\n{path}")
        except Exception as err:
            show_error(self, "Open failed", err)

    def reload_db(self):
        try:
            self.db.load_hydride_database()
            self.db_tab.populate()
            QMessageBox.information(self, "Reloaded", "Database reloaded from disk.")
        except Exception as err:
            show_error(self, "Reload failed", err)

    def show_about(self):
        QMessageBox.information(
            self,
            "About",
            "Metal Hydride Toolkit\n\n"
            "A PySide6 front-end for managing a metal hydride database, "
            "querying thermodynamic properties, and working with a periodic table backend."
        )


# ---- Entrypoint --------------------------------------------------------------
def main():
    app = QApplication(sys.argv)
    # Optional: allow passing a custom database json path as an argument
    db_path = sys.argv[1] if len(sys.argv) > 1 else None
    w = HydrideHandlerMainWindow(db_path=db_path)
    #w = HydrideHandlerMainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
