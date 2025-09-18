from __future__ import annotations

import os
import sys
import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import numpy as np

# --- Qt / Matplotlib embedding ---
from PySide6.QtCore import Qt, QThread, Signal, Slot, QObject
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QSizePolicy,
    QVBoxLayout,
    QWidget,
)

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 — needed for 3D projection

from src.infrastructure.core.config_reader import config
from src.infrastructure.connections.connections import DatabaseConnection
from src.GUI.kinetics_gui.kinetics_ui import KineticsView, DataAccess, KineticsWorker, Series

# -----------------------------
# CONTROLLER (logic only)
# -----------------------------
class KineticsController(QObject):
    def __init__(self, view: KineticsView, dal: DataAccess):
        super().__init__()
        self.view = view
        self.dal = dal
        self._worker: KineticsWorker | None = None

        # Connect view signals to controller slots
        self.view.loadRequested.connect(self.on_load_curves)
        self.view.runRequested.connect(self.on_run_kinetics)
        self.view.clearRequested.connect(self.view.clear_plot)

    # ---------- helpers ----------
    def _parse_cycles(self, text: str, *, sample_id: str | None = None) -> List[int]:
        """Parse cycles like "1-3,7,10" into [1,2,3,7,10]. If empty, fetch all cycles for sample.
        """
        text = (text or "").strip()
        if not text:
            if not sample_id:
                return []
            try:
                return self.dal.list_cycles(sample_id)
            except Exception as e:  # noqa: BLE001
                self.view.show_error(f"Failed to list cycles: {e}")
                return []
        # tokenize
        out: List[int] = []
        for part in text.split(','):
            part = part.strip()
            if not part:
                continue
            if '-' in part:
                a, b = part.split('-', 1)
                try:
                    ai = int(a)
                    bi = int(b)
                except ValueError:
                    continue
                if ai <= bi:
                    out.extend(range(ai, bi + 1))
                else:
                    out.extend(range(ai, bi - 1, -1))
            else:
                try:
                    out.append(int(part))
                except ValueError:
                    continue
        # de-dup & sort
        return sorted(set(out))

    # ---------- slots (invoked by the view) ----------
    @Slot(str, str, str)
    def on_load_curves(self, sample_id: str, cycles_text: str, y_val_text) -> None:
        #todo: choose which data to load
        if not sample_id:
            self.view.show_error("Please enter a Sample ID.")
            return
        cycles = self._parse_cycles(cycles_text, sample_id=sample_id)
        if not cycles:
            self.view.show_error("No cycles to load (input empty and none found).")
            return
        try:
            series_map = self.dal.fetch_measurements(sample_id, cycles, y_val_text)
            if not series_map:
                self.view.show_error("No measurement data returned for the requested cycles.")
                return
            for cyc in cycles:
                if cyc in series_map:
                    self.view.plot_measurement(cyc, series_map[cyc])
            self.view.set_status(f"Loaded measurement curves for cycles: {cycles}")
        except Exception as e:  # noqa: BLE001
            self.view.show_error(f"DB error while loading curves: {e}")

    @Slot(str, str)
    def on_run_kinetics(self, sample_id: str, cycles_text: str) -> None:
        if not sample_id:
            self.view.show_error("Please enter a Sample ID.")
            return
        cycles = self._parse_cycles(cycles_text, sample_id=sample_id)
        if not cycles:
            self.view.show_error("No cycles to process (input empty and none found).")
            return
        # Ensure measurement curves are visible first
        try:
            series_map = self.dal.fetch_measurements(sample_id, cycles)
            for cyc in cycles:
                if cyc in series_map:
                    self.view.plot_measurement(cyc, series_map[cyc])
        except Exception as e:  # noqa: BLE001
            self.view.show_error(f"DB error while preparing curves: {e}")
            return

        # Kick off worker
        self.view.set_running(True)
        self.view.set_progress(0)
        self.view.set_status("Calculating kinetics…")

        self._worker = KineticsWorker(sample_id, cycles, self.dal)
        self._worker.progress.connect(self.view.set_progress)
        self._worker.error.connect(self._on_worker_error)
        self._worker.result.connect(self._on_worker_result)
        self._worker.finished.connect(self._on_worker_finished)
        self._worker.start()

    # ---------- worker callbacks ----------
    @Slot(str)
    def _on_worker_error(self, msg: str) -> None:
        self.view.show_error(f"Kinetics calculation failed: {msg}")

    @Slot()
    def _on_worker_finished(self) -> None:
        self.view.set_running(False)
        self.view.set_status("Done.")

    @Slot(dict)
    def _on_worker_result(self, out: Dict[int, Series]) -> None:
        replace = self.view.replace_checked()
        for cyc, series in out.items():
            if replace:
                self.view.remove_measurement(cyc)
            self.view.plot_kinetics(cyc, series)


def main() -> None:
    # Services

    dal = DataAccess(config)

    # View + Controller
    app = QApplication(sys.argv)
    view = KineticsView()
    controller = KineticsController(view, dal)

    view.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()
