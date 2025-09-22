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
    QDoubleSpinBox,
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
    QComboBox,
    QWidget
)

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 — needed for 3D projection
from scipy.interpolate import krogh_interpolate

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

      # ---- limits API (for axis controls) ----
    def get_current_limits(self) -> Tuple[Tuple[float, float], Tuple[float, float], Tuple[float, float]]:
        """Return ((x0,x1), (y0,y1), (z0,z1)) from the Matplotlib axes."""
        ax = self.ax
        return ax.get_xlim(), ax.get_ylim(), ax.get_zlim()

    def get_data_bounds(self) -> Tuple[Tuple[float, float], Tuple[float, float], Tuple[float, float]] | None:
        """Return tracked data bounds or None if we don't have finite bounds yet."""
        vals = [self._x_min, self._x_max, self._y_min, self._y_max, self._z_min, self._z_max]
        if not all(np.isfinite(v) for v in vals):
            return None
        return (self._x_min, self._x_max), (self._y_min, self._y_max), (self._z_min, self._z_max)

    def set_limits(
        self,
        xlim: Tuple[float, float] | None = None,
        ylim: Tuple[float, float] | None = None,
        zlim: Tuple[float, float] | None = None,
        *,
        draw: bool = True,
    ) -> None:
        """Set any subset of axis limits."""
        ax = self.ax
        if xlim is not None:
            ax.set_xlim(*xlim)
        if ylim is not None:
            ax.set_ylim(*ylim)
        if zlim is not None:
            ax.set_zlim(*zlim)
        if draw:
            self.canvas.draw_idle()

    def fit_to_data(self) -> None:
        """Apply the tracked data bounds (with padding) like autoscale."""
        self._apply_bounds()

    def reset_view(self) -> None:
        """Clear limits and camera to the default configured in _init_axes."""
        # keep the plotted artists; just reset view and auto limits from data if present
        elev, azim = 22, -60
        self.ax.view_init(elev=elev, azim=azim)
        if self.get_data_bounds() is not None:
            self._apply_bounds()
        else:
            # no data yet; restore the vanilla axes
            self.canvas._init_axes()
        self.canvas.draw_idle()


    # ---- plotting ----
    def plot_measurement(self, cycle: float, series: Series) -> None:
        (line,) = self.ax.plot(series.x, series.y, series.z,
                               linewidth=1.5, alpha=0.9, label=f"meas c{cycle}")
        line.set_picker(True)          # enable picking
        line.set_pickradius(8)         # easier to click in 3D
        self.measurement_lines.setdefault(float(cycle), []).append(line)
        self._update_bounds(series)
        self._apply_bounds()

    def plot_kinetics(self, cycle: float, series: Series) -> None:
        (line,) = self.ax.plot(series.x, series.y, series.z,
                               linestyle="--", linewidth=2.0, alpha=0.95, label=f"kin c{cycle}")
        line.set_picker(True)
        line.set_pickradius(8)
        self.kinetics_lines.setdefault(float(cycle), []).append(line)
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

    def resolve_artist(self, artist):
        for cyc, arts in self.measurement_lines.items():
            if artist in arts:
                return ("meas", float(cyc))
        for cyc, arts in self.kinetics_lines.items():
            if artist in arts:
                return ("kin", float(cyc))
        return (None, None)

    def mark_selected(self, artist):
        # reset all
        for d in (self.measurement_lines, self.kinetics_lines):
            for arts in d.values():
                for a in arts:
                    a.set_linewidth(1.5); a.set_alpha(0.9); a.set_zorder(1)
        # emphasize selected
        artist.set_linewidth(3.0); artist.set_alpha(1.0); artist.set_zorder(10)
        self.canvas.draw_idle()


# -----------------------------
# VIEW (UI only)
# -----------------------------
class KineticsView(QMainWindow):
    # Signals that the controller can subscribe to
    loadRequested = Signal(str, str, str)   # sample_id, cycles_text
    runRequested = Signal(str, str)    # sample_id, cycles_text
    clearRequested = Signal()
    exportRequested = Signal()
    correctionRequested = Signal()

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Kinetics 3D Viewer")
        self.resize(1200, 720)
        self._build_ui()
        self.selected_cycle = 0

    # --------- UI creation ---------
    def _build_ui(self) -> None:
        self.canvas = Matplotlib3DCanvas()
        self.canvas.setContextMenuPolicy(Qt.CustomContextMenu)
        self.canvas.customContextMenuRequested.connect(self._on_canvas_context_menu)
        self.plot_mgr = Plot3DManager(self.canvas)

        self.sample_edit = QLineEdit()
        self.sample_edit.setPlaceholderText("e.g., WAE-WA-028")

        self.cycles_edit = QLineEdit()
        self.cycles_edit.setPlaceholderText("Cycles (e.g., 1-3,7,10)")

        from src.infrastructure.core.table_config import TableConfig
        kinetics_table = TableConfig().KineticsTable
        kinetics_selectables = [
            kinetics_table.pressure,
            kinetics_table.uptake_wt_p,
            kinetics_table.uptake_kg,
            kinetics_table.rate_kg_min,
            kinetics_table.rate_wt_p_min,
        ]
        self.combo_box_y_select = QComboBox()
        self.combo_box_y_select.addItems([str(kin_select) for kin_select in kinetics_selectables])

        self.btn_load = QPushButton("Load Curves")
        self.btn_run = QPushButton("Run Kinetics")   # ← will be placed in the new group
        self.btn_clear = QPushButton("Clear Plot")

        self.btnSendToOrigin = QPushButton("Send to Origin")
        self.btnSendToOrigin.setToolTip("Create current visible plot in Origin")
        self.btnSendToOrigin.clicked.connect(self.exportRequested)

        self.chk_replace = QCheckBox("Replace measurement with kinetics after calc")
        self.chk_replace.setChecked(True)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(True)

        self.status_label = QLabel("Ready")
        self.status_label.setWordWrap(True)
        self.status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.status_label.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Fixed)
        self.status_label.setMinimumHeight(24)
        self.status_label.setMaximumHeight(48)

        # --- Controls (keep Run button out of here) ---
        left = QGroupBox("Controls")
        form = QFormLayout()
        form.addRow("Sample ID", self.sample_edit)
        form.addRow("Cycles", self.cycles_edit)
        form.addRow(self.combo_box_y_select)
        form.addRow(self.btn_load)
        form.addRow(self.btn_clear)
        form.addRow(self.chk_replace)
        form.addRow(self.btnSendToOrigin)
        form.addRow("Progress", self.progress)
        form.addRow("Status", self.status_label)
        left.setLayout(form)

        # --- NEW: Kinetics options group (Run button moved here) ---
        kin_box = QGroupBox("Kinetics")
        kin_form = QFormLayout()

        self.resample_rule_edit = QLineEdit()
        self.resample_rule_edit.setPlaceholderText("e.g., 60s")
        self.resample_rule_edit.setText("60s")

        self.resample_how_combo = QComboBox()
        self.resample_how_combo.addItems(["mean", "nearest"])
        self.resample_how_combo.setCurrentText("mean")

        self.smooth_seconds_spin = QDoubleSpinBox()
        self.smooth_seconds_spin.setRange(0.0, 1e9)
        self.smooth_seconds_spin.setDecimals(2)
        self.smooth_seconds_spin.setSingleStep(1.0)
        self.smooth_seconds_spin.setSpecialValueText("off")  # 0 = off
        self.smooth_seconds_spin.setValue(0.0)

        self.enforce_monotonic_chk = QCheckBox("Enforce monotonic")
        self.enforce_monotonic_chk.setChecked(True)

        kin_form.addRow("Resample rule", self.resample_rule_edit)
        kin_form.addRow("Resample how", self.resample_how_combo)
        kin_form.addRow("Smooth seconds", self.smooth_seconds_spin)
        kin_form.addRow(self.enforce_monotonic_chk)
        kin_form.addRow(self.btn_run)    # ← Run button placed at the bottom of this group
        kin_box.setLayout(kin_form)

        axes_box = self._build_axes_control()

        root = QWidget()
        hl = QHBoxLayout(root)

        left_col = QVBoxLayout()
        left_col.addWidget(left)
        left_col.addWidget(kin_box)   # ← place the new group “down” from Controls
        left_col.addWidget(axes_box)
        left_col.addStretch(1)
        left_wrap = QWidget(); left_wrap.setLayout(left_col)

        hl.addWidget(left_wrap, 0)
        hl.addWidget(self.canvas, 1)
        self.setCentralWidget(root)

        # wiring (unchanged signature)
        self.btn_load.clicked.connect(lambda: self.loadRequested.emit(
            self.sample_edit.text().strip(),
            self.cycles_edit.text().strip(),
            self.combo_box_y_select.currentText().strip()
        ))
        self.btn_run.clicked.connect(lambda: self.runRequested.emit(
            self.sample_edit.text().strip(),
            self.cycles_edit.text().strip()
        ))
        self.btn_clear.clicked.connect(self.clearRequested)
        self.btn_axes_apply.clicked.connect(self._on_axes_apply_clicked)
        self.btn_axes_sync.clicked.connect(self._on_axes_sync_clicked)
        self.btn_axes_fit.clicked.connect(self._on_axes_fit_clicked)
        self.btn_axes_reset.clicked.connect(self._on_axes_reset_clicked)

        self._on_axes_sync_clicked()

    def _build_axes_control(self):
        # --- Axes controls ---
        axes_box = QGroupBox("Axes")
        axes_form = QFormLayout()

        def _mk_spin():
            s = QDoubleSpinBox()
            s.setDecimals(6)
            s.setRange(-1e12, 1e12)
            s.setSingleStep(0.1)
            s.setButtonSymbols(QDoubleSpinBox.ButtonSymbols.NoButtons)
            s.setMinimumWidth(110)
            return s

        # X axis editors
        self.xmin_spin = _mk_spin()
        self.xmax_spin = _mk_spin()
        xrow = QHBoxLayout()
        xrow.addWidget(QLabel("X min")); xrow.addWidget(self.xmin_spin)
        xrow.addSpacing(8)
        xrow.addWidget(QLabel("X max")); xrow.addWidget(self.xmax_spin)
        xw = QWidget(); xw.setLayout(xrow)
        axes_form.addRow("Time (min):", xw)

        # Y axis editors
        self.ymin_spin = _mk_spin()
        self.ymax_spin = _mk_spin()
        yrow = QHBoxLayout()
        yrow.addWidget(QLabel("Y min")); yrow.addWidget(self.ymin_spin)
        yrow.addSpacing(8)
        yrow.addWidget(QLabel("Y max")); yrow.addWidget(self.ymax_spin)
        yw = QWidget(); yw.setLayout(yrow)
        axes_form.addRow("Cycle:", yw)

        # Z axis editors
        self.zmin_spin = _mk_spin()
        self.zmax_spin = _mk_spin()
        zrow = QHBoxLayout()
        zrow.addWidget(QLabel("Z min")); zrow.addWidget(self.zmin_spin)
        zrow.addSpacing(8)
        zrow.addWidget(QLabel("Z max")); zrow.addWidget(self.zmax_spin)
        zw = QWidget(); zw.setLayout(zrow)
        axes_form.addRow("Value:", zw)

        # Buttons
        btn_row = QHBoxLayout()
        self.btn_axes_apply = QPushButton("Apply")
        self.btn_axes_sync = QPushButton("Sync From Plot")
        self.btn_axes_fit  = QPushButton("Fit Data")
        self.btn_axes_reset = QPushButton("Reset View")
        btn_row.addWidget(self.btn_axes_apply)
        btn_row.addWidget(self.btn_axes_sync)
        btn_row.addWidget(self.btn_axes_fit)
        btn_row.addWidget(self.btn_axes_reset)
        btnw = QWidget(); btnw.setLayout(btn_row)
        axes_form.addRow(btnw)

        axes_box.setLayout(axes_form)
        return axes_box

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

        # --------- Axis helpers ---------

    def _on_axes_sync_clicked(self) -> None:
        """Populate editors from the current plot limits."""
        (x0, x1), (y0, y1), (z0, z1) = self.plot_mgr.get_current_limits()
        self.xmin_spin.setValue(float(x0)); self.xmax_spin.setValue(float(x1))
        self.ymin_spin.setValue(float(y0)); self.ymax_spin.setValue(float(y1))
        self.zmin_spin.setValue(float(z0)); self.zmax_spin.setValue(float(z1))
        self.set_status("Synced axis editors from plot.")

    def _on_axes_apply_clicked(self) -> None:
        """Apply editor values to the plot axes."""
        x0, x1 = self.xmin_spin.value(), self.xmax_spin.value()
        y0, y1 = self.ymin_spin.value(), self.ymax_spin.value()
        z0, z1 = self.zmin_spin.value(), self.zmax_spin.value()

        # Basic guard: swap if user entered reversed bounds
        if x0 > x1: x0, x1 = x1, x0
        if y0 > y1: y0, y1 = y1, y0
        if z0 > z1: z0, z1 = z1, z0

        self.plot_mgr.set_limits((x0, x1), (y0, y1), (z0, z1))
        self.set_status("Applied custom axis limits.")

    def _on_axes_fit_clicked(self) -> None:
        """Autoscale to tracked data bounds (with padding)."""
        bounds = self.plot_mgr.get_data_bounds()
        if bounds is None:
            self.show_error("No data bounds yet — plot some data first.")
            return
        self.plot_mgr.fit_to_data()
        # Also sync editors so they reflect what you see
        self._on_axes_sync_clicked()
        self.set_status("Fitted axes to data.")

    def _on_axes_reset_clicked(self) -> None:
        """Restore default camera and reasonable limits."""
        self.plot_mgr.reset_view()
        self._on_axes_sync_clicked()
        self.set_status("Reset view.")

    def get_kinetics_options(self):
        """Return (resample_rule: str, resample_how: str, smooth_seconds: float|None, enforce_monotonic: bool)."""
        resample_rule = (self.resample_rule_edit.text() or "60s").strip()
        resample_how = self.resample_how_combo.currentText()
        smooth_val = float(self.smooth_seconds_spin.value())
        smooth_seconds = None if smooth_val == 0.0 else smooth_val
        enforce_monotonic = self.enforce_monotonic_chk.isChecked()
        return resample_rule, resample_how, smooth_seconds, enforce_monotonic

    def _on_canvas_context_menu(self, pos):
        from PySide6.QtWidgets import QMenu
        global_pos = self.canvas.mapToGlobal(pos)
        menu = QMenu(self)
        act_reset = menu.addAction("Reset view")
        act_fit   = menu.addAction("Fit to data")
        act_correction   = menu.addAction("Correct Curve")
        chosen = menu.exec(global_pos)
        if chosen == act_reset:
            self.plot_mgr.reset_view()
        elif chosen == act_fit:
            self.plot_mgr.fit_to_data()
        elif chosen == act_correction:
            self.correctionRequested.emit()


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
