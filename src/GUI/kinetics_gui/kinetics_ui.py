#kinetics_ui.py
"""
Kinetics 3D Viewer — PySide6 + Matplotlib + psycopg2

Now refactored into clearly separated layers:
- View: KineticsView (UI creation only)
- Controller: KineticsController (logic + data flow)
- Plot management, data access, worker, and DB context remain modular

Features
- Input a Sample ID and one or more cycles (e.g., "1-3,7,10").
- Loads measurement curves per cycle from PostgreSQL using a context-managed DatabaseConnection (psycopg2).
- Embeds a 3D Matplotlib plot into the PySide6 app.
- "Run Kinetics" starts a background calculation (QThread) for the given cycles.
  • First shows measurement curves; after calculation completes, removes them and shows the calculated kinetics curves (toggleable).

Assumptions (tweak as needed)
- DB schema:
    TABLE measurements (
        sample_id TEXT,
        cycle     INTEGER,
        t_sec     DOUBLE PRECISION,
        value_y   DOUBLE PRECISION
    );
- Replace table/column names in DataAccess if yours differ.
- DB credentials from env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.

Install deps
    pip install PySide6 matplotlib psycopg2-binary numpy

Run
    python kinetics_3d_viewer.py
"""
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

from src.GUI.kinetics_gui.kinetics_worker import DataAccess, Series, KineticsWorker
from src.infrastructure.connections.connections import DatabaseConnection



# -----------------------------
# 3D Plot manager
# -----------------------------
class Matplotlib3DCanvas(FigureCanvas):
    def __init__(self) -> None:
        fig = Figure(figsize=(7, 5), tight_layout=True)
        self.ax = fig.add_subplot(111, projection="3d")
        super().__init__(fig)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.updateGeometry()
        self._init_axes()

    def _init_axes(self) -> None:
        ax = self.ax
        ax.clear()
        ax.set_xlabel("Time (min)")
        ax.set_ylabel("Cycle")
        ax.set_zlabel("Value")
        ax.view_init(elev=22, azim=-60)
        ax.grid(True)
        self.draw_idle()


class Plot3DManager:
    """Keeps track of plotted artists so we can remove/replace per cycle."""

    def __init__(self, canvas: Matplotlib3DCanvas) -> None:
        self.canvas = canvas
        self.ax = canvas.ax
        self.measurement_lines: Dict[int, List] = {}
        self.kinetics_lines: Dict[int, List] = {}
        # Track bounds for manual autoscaling in 3D
        self._x_min = math.inf
        self._x_max = -math.inf
        self._y_min = math.inf
        self._y_max = -math.inf
        self._z_min = math.inf
        self._z_max = -math.inf

    # ---- bounds helpers ----
    def _update_bounds(self, series: Series) -> None:
        self._x_min = min(self._x_min, float(np.min(series.x)))
        self._x_max = max(self._x_max, float(np.max(series.x)))
        self._y_min = min(self._y_min, float(np.min(series.y)))
        self._y_max = max(self._y_max, float(np.max(series.y)))
        self._z_min = min(self._z_min, float(np.min(series.z)))
        self._z_max = max(self._z_max, float(np.max(series.z)))

    def _apply_bounds(self) -> None:
        if not all(np.isfinite(v) for v in [self._x_min, self._x_max, self._y_min, self._y_max, self._z_min, self._z_max]):
            return
        # Add small margins
        def _pad(a, b):
            span = b - a
            if span <= 0:
                return a - 0.5, b + 0.5
            pad = 0.03 * span
            return a - pad, b + pad

        ax = self.ax
        x0, x1 = _pad(self._x_min, self._x_max)
        y0, y1 = _pad(self._y_min, self._y_max)
        z0, z1 = _pad(self._z_min, self._z_max)
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_zlim(z0, z1)
        self.canvas.draw_idle()

    # ---- plotting ----
    def plot_measurement(self, cycle: int, series: Series) -> None:
        (line,) = self.ax.plot(series.x, series.y, series.z, linewidth=1.5, alpha=0.9, label=f"meas c{cycle}")
        arts = self.measurement_lines.setdefault(cycle, [])
        arts.append(line)
        self._update_bounds(series)
        self._apply_bounds()

    def plot_kinetics(self, cycle: int, series: Series) -> None:
        # Make kinetics visually distinct (dashed, thicker)
        (line,) = self.ax.plot(series.x, series.y, series.z, linestyle="--", linewidth=2.0, alpha=0.95, label=f"kin c{cycle}")
        arts = self.kinetics_lines.setdefault(cycle, [])
        arts.append(line)
        self._update_bounds(series)
        self._apply_bounds()

    def remove_measurement(self, cycle: int) -> None:
        for art in self.measurement_lines.pop(cycle, []):
            try:
                art.remove()
            except Exception:
                pass
        self.canvas.draw_idle()

    def clear_all(self) -> None:
        self.ax.cla()
        self.canvas._init_axes()
        self.measurement_lines.clear()
        self.kinetics_lines.clear()
        self._x_min = self._y_min = self._z_min = math.inf
        self._x_max = self._y_max = self._z_max = -math.inf
        self.canvas.draw_idle()


# -----------------------------
# VIEW (UI only)
# -----------------------------
class KineticsView(QMainWindow):
    # Signals that the controller can subscribe to
    loadRequested = Signal(str, str)   # sample_id, cycles_text
    runRequested = Signal(str, str)    # sample_id, cycles_text
    clearRequested = Signal()

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Kinetics 3D Viewer")
        self.resize(1200, 720)
        self._build_ui()

    # --------- UI creation ---------
    def _build_ui(self) -> None:
        self.canvas = Matplotlib3DCanvas()
        self.plot_mgr = Plot3DManager(self.canvas)

        self.sample_edit = QLineEdit()
        self.sample_edit.setPlaceholderText("e.g., SAMPLE_001")

        self.cycles_edit = QLineEdit()
        self.cycles_edit.setPlaceholderText("Cycles (e.g., 1-3,7,10)")

        self.btn_load = QPushButton("Load Curves")
        self.btn_run = QPushButton("Run Kinetics")
        self.btn_clear = QPushButton("Clear Plot")

        self.chk_replace = QCheckBox("Replace measurement with kinetics after calc")
        self.chk_replace.setChecked(True)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(True)

        self.status_label = QLabel("Ready")

        left = QGroupBox("Controls")
        form = QFormLayout()
        form.addRow("Sample ID", self.sample_edit)
        form.addRow("Cycles", self.cycles_edit)
        form.addRow(self.btn_load)
        form.addRow(self.btn_run)
        form.addRow(self.btn_clear)
        form.addRow(self.chk_replace)
        form.addRow("Progress", self.progress)
        form.addRow("Status", self.status_label)
        left.setLayout(form)

        root = QWidget()
        hl = QHBoxLayout(root)
        hl.addWidget(left, 0)
        hl.addWidget(self.canvas, 1)
        self.setCentralWidget(root)

        # Wire UI events to outward-facing signals (no logic here)
        self.btn_load.clicked.connect(lambda: self.loadRequested.emit(self.sample_edit.text().strip(), self.cycles_edit.text().strip()))
        self.btn_run.clicked.connect(lambda: self.runRequested.emit(self.sample_edit.text().strip(), self.cycles_edit.text().strip()))
        self.btn_clear.clicked.connect(self.clearRequested)

    # --------- Thin helpers the controller can use ---------
    def set_status(self, text: str) -> None:
        self.status_label.setText(text)

    def show_error(self, message: str) -> None:
        QMessageBox.critical(self, "Error", message)
        self.set_status(message)

    def set_running(self, running: bool) -> None:
        self.btn_run.setEnabled(not running)
        self.btn_load.setEnabled(not running)

    def set_progress(self, value: int) -> None:
        self.progress.setValue(value)

    def replace_checked(self) -> bool:
        return self.chk_replace.isChecked()

    # Delegate plotting to Plot3DManager
    def plot_measurement(self, cycle: int, series: Series) -> None:
        self.plot_mgr.plot_measurement(cycle, series)

    def plot_kinetics(self, cycle: int, series: Series) -> None:
        self.plot_mgr.plot_kinetics(cycle, series)

    def remove_measurement(self, cycle: int) -> None:
        self.plot_mgr.remove_measurement(cycle)

    def clear_plot(self) -> None:
        self.plot_mgr.clear_all()


# -----------------------------
# Entrypoint
# -----------------------------
def main() -> None:
    # View
    app = QApplication(sys.argv)
    view = KineticsView()


    view.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
