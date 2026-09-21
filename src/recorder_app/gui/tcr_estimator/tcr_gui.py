# tcr_gui.py
from __future__ import annotations
import sys
import os
from dataclasses import dataclass
from typing import Optional, List

import numpy as np
import pandas as pd

from PySide6.QtCore import QDate
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QFileDialog, QMessageBox,
    QVBoxLayout, QGridLayout, QLabel, QPushButton,
    QDateEdit, QSpinBox, QDoubleSpinBox, QCheckBox, QGroupBox, QLineEdit, QComboBox
)

# Matplotlib Qt backend
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavToolbar
from matplotlib.figure import Figure
from matplotlib.widgets import LassoSelector
from matplotlib.path import Path

# Optional smoothing (only used if present and user enables it)
try:
    from scipy.signal import savgol_filter  # noqa: F401
    _HAS_SCIPY = True
except Exception:
    _HAS_SCIPY = False

# ---- Project imports (uses your existing modules). We fall back gracefully if not present. ----
t_col_default = "T"
r_col_default = "R"

TableConfig = None
DataLoaderTCR = None
TCRCalculator = None

# Try to import your implementation
try:
    from recorder_app.gui.tcr_estimator.tcr_calculator import TCRCalculator as _TCRCalculator, DataLoaderTCR as _DataLoaderTCR
    from recorder_app.infrastructure.core.table_config import TableConfig as _TableConfig
    TCRCalculator = _TCRCalculator
    DataLoaderTCR = _DataLoaderTCR
    TableConfig = _TableConfig
    t_col_default = TableConfig().ETCDataTable.get_clean("temperature")
    r_col_default = TableConfig().ETCDataTable.get_clean("disk_resistance")
except Exception:
    # Minimal fallback to let the GUI run on ad-hoc CSVs if your project modules aren't available.
    class _FallbackCalc:
        def __init__(self, curie_temp: float = 356.0, t_col: str = "T", r_col: str = "R"):
            self.curie_temp = curie_temp
            self.t_col = t_col
            self.r_col = r_col

        def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
            if self.t_col not in df.columns or self.r_col not in df.columns:
                raise ValueError(f"DataFrame must have '{self.t_col}' and '{self.r_col}'.")
            clean = (
                df[[self.t_col, self.r_col]]
                .dropna()
                .groupby(self.t_col, as_index=False, sort=True)
                .mean(numeric_only=True)
                .sort_values(self.t_col)
                .reset_index(drop=True)
            )
            if clean.shape[0] < 3:
                raise ValueError("Need at least 3 distinct temperature points for differentiation.")
            return clean

        def pointwise_tcr(self, df: pd.DataFrame, sg_window: Optional[int] = None, sg_poly: int = 2) -> pd.DataFrame:
            data = self._prepare(df).copy()
            T = data[self.t_col].to_numpy(dtype=float)
            R = data[self.r_col].to_numpy(dtype=float)

            R_smooth = R
            if sg_window and _HAS_SCIPY and sg_window >= 3 and sg_window % 2 == 1:
                wl = min(sg_window, len(R) if len(R)%2==1 else len(R)-1)
                wl = max(3, wl)
                from scipy.signal import savgol_filter
                R_smooth = savgol_filter(R, window_length=wl, polyorder=sg_poly, mode="interp")

            dR_dT = np.empty_like(R_smooth)
            dR_dT[1:-1] = (R_smooth[2:] - R_smooth[:-2]) / (T[2:] - T[:-2])
            dR_dT[0] = (R_smooth[1] - R_smooth[0]) / (T[1] - T[0])
            dR_dT[-1] = (R_smooth[-1] - R_smooth[-2]) / (T[-1] - T[-2])
            alpha = dR_dT / R_smooth
            return pd.DataFrame({self.t_col: T, self.r_col: R, "dR_dT": dR_dT, "alpha": alpha})

    TCRCalculator = _FallbackCalc

# ----------------------------- Helper dataclasses --------------------------------
@dataclass
class SeriesOverlay:
    label: str
    T: np.ndarray
    alpha: np.ndarray


# ----------------------------- Matplotlib Canvas --------------------------------
class DualPlotCanvas(FigureCanvas):
    """
    Top: R vs T (select points to include/exclude)
    Bottom: TCR vs T (computed)
    """
    def __init__(self, parent=None):
        self.fig = Figure(figsize=(8.5, 7.5), tight_layout=True)
        super().__init__(self.fig)
        self.setParent(parent)

        self.ax_R = self.fig.add_subplot(2, 1, 1)
        self.ax_TCR = self.fig.add_subplot(2, 1, 2)

        self.ax_R.set_title("Resistance vs Temperature (select with lasso)")
        self.ax_R.set_xlabel("Temperature")
        self.ax_R.set_ylabel("Resistance")

        self.ax_TCR.set_title("TCR vs Temperature")
        self.ax_TCR.set_xlabel("Temperature")
        self.ax_TCR.set_ylabel("α (1/°C)")

        self.R_scatter = None
        self.R_scatter_excluded = None
        self.TCR_line = None
        self.overlay_lines: List = []

        self._lasso: Optional[LassoSelector] = None
        self._x = np.array([])
        self._y = np.array([])
        self._included_mask = np.array([], dtype=bool)
        self._last_lasso_indices: List[int] = []

    # ---- plotting helpers ----
    def plot_R(self, T: np.ndarray, R: np.ndarray, included_mask: np.ndarray):
        self.ax_R.cla()
        self.ax_R.set_title("Resistance vs Temperature (select with lasso)")
        self.ax_R.set_xlabel("Temperature")
        self.ax_R.set_ylabel("Resistance")
        # Keep for lasso
        self._x = np.asarray(T, dtype=float)
        self._y = np.asarray(R, dtype=float)
        self._included_mask = np.asarray(included_mask, dtype=bool)

        # Included vs excluded
        inc = self._included_mask
        exc = ~inc
        self.R_scatter = self.ax_R.scatter(self._x[inc], self._y[inc], s=25, label="Included", zorder=3)
        self.R_scatter_excluded = self.ax_R.scatter(self._x[exc], self._y[exc], s=25, marker="x", label="Excluded", zorder=2)

        self.ax_R.grid(True, alpha=0.3)
        self.ax_R.legend(loc="best")
        self.draw_idle()

    def plot_TCR(self, T: np.ndarray, alpha: np.ndarray, curie_temp: Optional[float] = None, style: str = "Line + Dots"):
        self.ax_TCR.cla()
        self.ax_TCR.set_title("TCR vs Temperature")
        self.ax_TCR.set_xlabel("Temperature")
        self.ax_TCR.set_ylabel("α (1/°C)")

        if len(T) > 0:
            if style == "Scatter":
                self.TCR_line = self.ax_TCR.scatter(T, alpha, s=22, label="Computed TCR", zorder=3)
            else:  # "Line + Dots"
                (self.TCR_line,) = self.ax_TCR.plot(T, alpha, linestyle='-', marker='o', markersize=4, linewidth=1.8, label="Computed TCR")

        for line in self.overlay_lines:
            self.ax_TCR.add_line(line)

        if curie_temp is not None and np.isfinite(curie_temp):
            self.ax_TCR.axvline(float(curie_temp), linestyle="--", alpha=0.7, label=f"T_Curie={curie_temp:g}")

        self.ax_TCR.grid(True, alpha=0.3)
        self.ax_TCR.legend(loc="best")
        self.draw_idle()

    def add_overlay(self, overlay: SeriesOverlay):
        # Plot and store a reference (so it persists after redraws)
        line, = self.ax_TCR.plot(overlay.T, overlay.alpha, lw=1.5, label=overlay.label)
        self.overlay_lines.append(line)
        self.ax_TCR.legend(loc="best")
        self.draw_idle()

    def clear_overlays(self):
        for line in self.overlay_lines:
            try:
                line.remove()
            except Exception:
                pass
        self.overlay_lines = []
        self.draw_idle()

    # ---- lasso selection ----
    def enable_lasso(self):
        if self._lasso is not None:
            return
        self._lasso = LassoSelector(ax=self.ax_R, onselect=self._on_lasso_select)

    def disable_lasso(self):
        if self._lasso is not None:
            self._lasso.disconnect_events()
            self._lasso = None

    def _on_lasso_select(self, verts):
        if self._x.size == 0:
            self._last_lasso_indices = []
            return
        path = Path(verts)
        pts = np.column_stack([self._x, self._y])
        inds = np.nonzero(path.contains_points(pts))[0].tolist()
        self._last_lasso_indices = inds

    def get_last_lasso_indices(self) -> List[int]:
        return list(self._last_lasso_indices)

    def set_included_mask(self, new_mask: np.ndarray):
        if new_mask.shape != self._included_mask.shape:
            return
        self._included_mask = new_mask
        self.plot_R(self._x, self._y, self._included_mask)


# ------------------------------------ Main Window ------------------------------------
class TCRWindow(QMainWindow):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("TCR Calculator – PySide6")
        self.resize(1200, 850)

        # State
        self.df_raw: Optional[pd.DataFrame] = None
        self.t_col = t_col_default
        self.r_col = r_col_default
        self.included_mask: Optional[np.ndarray] = None

        # Calculator
        self.calc: Optional[TCRCalculator] = None

        # UI
        self.canvas = DualPlotCanvas(self)
        self.toolbar = NavToolbar(self.canvas, self)

        # Controls
        ctrl = self._build_controls()

        # Layout
        root = QWidget()
        root_layout = QVBoxLayout(root)
        root_layout.addWidget(self.toolbar)
        root_layout.addWidget(self.canvas, 1)
        root_layout.addWidget(ctrl)
        self.setCentralWidget(root)

    # ------------------ Controls ------------------
    def _build_controls(self) -> QWidget:
        box = QGroupBox("Controls")
        grid = QGridLayout(box)

        # Time range pickers
        grid.addWidget(QLabel("From (YYYY-MM-DD):"), 0, 0)
        self.date_from = QDateEdit()
        self.date_from.setDisplayFormat("yyyy-MM-dd")
        self.date_from.setCalendarPopup(True)
        self.date_from.setDate(QDate.fromString("2025-10-09", "yyyy-MM-dd"))
        grid.addWidget(self.date_from, 0, 1)

        grid.addWidget(QLabel("To (YYYY-MM-DD):"), 0, 2)
        self.date_to = QDateEdit()
        self.date_to.setDisplayFormat("yyyy-MM-dd")
        self.date_to.setCalendarPopup(True)
        self.date_to.setDate(QDate.fromString('2025-10-20', "yyyy-MM-dd"))
        grid.addWidget(self.date_to, 0, 3)

        self.btn_load = QPushButton("Load data from DB")
        self.btn_load.clicked.connect(self.on_load_clicked)
        grid.addWidget(self.btn_load, 0, 4)

        # Curie + smoothing
        grid.addWidget(QLabel("Curie T (°C):"), 1, 0)
        self.curie_spin = QDoubleSpinBox()
        self.curie_spin.setDecimals(2)
        self.curie_spin.setRange(-1e6, 1e6)
        self.curie_spin.setValue(356.0)
        grid.addWidget(self.curie_spin, 1, 1)

        self.chk_sg = QCheckBox("Savitzky–Golay")
        self.chk_sg.setChecked(False)
        self.chk_sg.setToolTip("Enable Savitzky–Golay smoothing (requires SciPy)")
        grid.addWidget(self.chk_sg, 1, 2)

        grid.addWidget(QLabel("Window (odd ≥3):"), 1, 3)
        self.sg_window = QSpinBox()
        self.sg_window.setRange(3, 501)
        self.sg_window.setSingleStep(2)
        self.sg_window.setValue(5)
        grid.addWidget(self.sg_window, 1, 4)

        grid.addWidget(QLabel("Poly:"), 1, 5)
        self.sg_poly = QSpinBox()
        self.sg_poly.setRange(2, 7)
        self.sg_poly.setValue(2)
        grid.addWidget(self.sg_poly, 1, 6)

        # Column overrides (in case user loaded CSV / fallback mode)
        grid.addWidget(QLabel("T column:"), 2, 0)
        self.tcol_edit = QLineEdit(t_col_default)
        grid.addWidget(self.tcol_edit, 2, 1)

        grid.addWidget(QLabel("R column:"), 2, 2)
        self.rcol_edit = QLineEdit(r_col_default)
        grid.addWidget(self.rcol_edit, 2, 3)

        # Selection buttons
        self.btn_enable_lasso = QPushButton("Enable Lasso")
        self.btn_enable_lasso.clicked.connect(self.on_enable_lasso)
        grid.addWidget(self.btn_enable_lasso, 3, 0)

        self.btn_disable_lasso = QPushButton("Disable Lasso")
        self.btn_disable_lasso.clicked.connect(self.on_disable_lasso)
        grid.addWidget(self.btn_disable_lasso, 3, 1)

        self.btn_exclude_sel = QPushButton("Exclude Selected")
        self.btn_exclude_sel.clicked.connect(self.on_exclude_selected)
        grid.addWidget(self.btn_exclude_sel, 3, 2)

        self.btn_include_sel = QPushButton("Include Selected")
        self.btn_include_sel.clicked.connect(self.on_include_selected)
        grid.addWidget(self.btn_include_sel, 3, 3)

        self.btn_reset = QPushButton("Reset Selection")
        self.btn_reset.clicked.connect(self.on_reset_selection)
        grid.addWidget(self.btn_reset, 3, 4)

        # Compute + overlays
        self.btn_compute = QPushButton("Compute TCR")
        self.btn_compute.clicked.connect(self.on_compute_clicked)
        self.btn_compute.setDefault(True)
        grid.addWidget(self.btn_compute, 4, 0, 1, 2)

        grid.addWidget(QLabel("TCR Plot Style:"), 4, 5)
        self.tcr_style = QComboBox()
        self.tcr_style.addItems(["Scatter", "Line + Dots"])
        self.tcr_style.setCurrentIndex(1)  # default to Line + Dots
        grid.addWidget(self.tcr_style, 4, 6)
        self.tcr_style.currentIndexChanged.connect(self.on_plot_style_changed)

        self.btn_add_overlay = QPushButton("Add .tcr Overlay")
        self.btn_add_overlay.clicked.connect(self.on_add_overlay_clicked)
        grid.addWidget(self.btn_add_overlay, 4, 2)

        self.btn_clear_overlays = QPushButton("Clear Overlays")
        self.btn_clear_overlays.clicked.connect(lambda: self.canvas.clear_overlays())
        grid.addWidget(self.btn_clear_overlays, 4, 3)

        self.btn_load_csv = QPushButton("Load CSV (fallback)")
        self.btn_load_csv.clicked.connect(self.on_load_csv_clicked)
        self.btn_load_csv.setToolTip("Load CSV with columns T and R (for fallback mode / testing).")
        grid.addWidget(self.btn_load_csv, 4, 4)

        return box

    # ------------------ Event handlers ------------------
    def on_load_clicked(self):
        """Load data from your DB via DataLoaderTCR using chosen time range."""
        if DataLoaderTCR is None:
            QMessageBox.warning(self, "Unavailable", "Project DataLoaderTCR not found. Use 'Load CSV' instead.")
            return
        # Import your config lazily so GUI still opens if project isn't present
        try:
            from recorder_app.infrastructure.core.config_reader import config
        except Exception as e:
            QMessageBox.critical(self, "Config error", f"Could not import project config:\n{e}")
            return

        time_range = [
            self.date_from.date().toString("yyyy-MM-dd"),
            self.date_to.date().toString("yyyy-MM-dd")
        ]
        try:
            loader = DataLoaderTCR(config=config, sensor_type="F1")
            df = loader.load_resistance_values(time_range=time_range)
            if df is None or df.empty:
                QMessageBox.information(self, "No data", f"No rows for {time_range[0]} → {time_range[1]}")
                return
            # Update state
            self.t_col = TableConfig().ETCDataTable.get_clean("temperature")
            self.r_col = TableConfig().ETCDataTable.get_clean("disk_resistance")
            self.tcol_edit.setText(self.t_col)
            self.rcol_edit.setText(self.r_col)
            self.set_dataframe(df)
        except Exception as e:
            QMessageBox.critical(self, "Load error", str(e))

    def on_load_csv_clicked(self):
        path, _ = QFileDialog.getOpenFileName(self, "Load CSV", "", "CSV files (*.csv);;All files (*)")
        if not path:
            return
        try:
            df = pd.read_csv(path)
            # Update columns if present
            if self.tcol_edit.text() in df.columns:
                self.t_col = self.tcol_edit.text()
            else:
                self.t_col = t_col_default
            if self.rcol_edit.text() in df.columns:
                self.r_col = self.rcol_edit.text()
            else:
                self.r_col = r_col_default
            self.set_dataframe(df)
        except Exception as e:
            QMessageBox.critical(self, "CSV error", f"Failed to read CSV:\n{e}")

    def set_dataframe(self, df: pd.DataFrame):
        # Basic cleaning & sorting by T
        if self.t_col not in df.columns or self.r_col not in df.columns:
            QMessageBox.critical(self, "Column error", f"DataFrame must contain '{self.t_col}' and '{self.r_col}'.")
            return
        df2 = (
            df[[self.t_col, self.r_col]]
            .dropna()
            .groupby(self.t_col, as_index=False, sort=True)
            .mean(numeric_only=True)
            .sort_values(self.t_col)
            .reset_index(drop=True)
        )
        self.df_raw = df2
        self.included_mask = np.ones(len(df2), dtype=bool)
        # Initial plot
        self.canvas.plot_R(df2[self.t_col].to_numpy(float), df2[self.r_col].to_numpy(float), self.included_mask)
        self.on_compute_clicked(initial=True)

    def on_enable_lasso(self):
        self.canvas.enable_lasso()

    def on_disable_lasso(self):
        self.canvas.disable_lasso()

    def on_exclude_selected(self):
        if self.included_mask is None:
            return
        inds = self.canvas.get_last_lasso_indices()
        if not inds:
            return
        self.included_mask[inds] = False
        self.canvas.set_included_mask(self.included_mask)

    def on_include_selected(self):
        if self.included_mask is None:
            return
        inds = self.canvas.get_last_lasso_indices()
        if not inds:
            return
        self.included_mask[inds] = True
        self.canvas.set_included_mask(self.included_mask)

    def on_reset_selection(self):
        if self.included_mask is None:
            return
        self.included_mask[:] = True
        self.canvas.set_included_mask(self.included_mask)

    def on_compute_clicked(self, initial: bool = False):
        """Compute TCR on currently included points and update bottom plot."""
        if self.df_raw is None or self.included_mask is None:
            return

        # Column overrides (for fallback mode or custom CSVs)
        self.t_col = self.tcol_edit.text().strip() or self.t_col
        self.r_col = self.rcol_edit.text().strip() or self.r_col
        if self.t_col not in self.df_raw.columns or self.r_col not in self.df_raw.columns:
            QMessageBox.critical(self, "Column error", f"Missing columns '{self.t_col}' / '{self.r_col}' in data.")
            return

        df_inc = self.df_raw.loc[self.included_mask].copy()
        if df_inc.shape[0] < 3:
            QMessageBox.warning(self, "Too few points", "Include at least 3 points to compute TCR.")
            return

        curie = float(self.curie_spin.value())
        self.calc = TCRCalculator(curie_temp=curie, t_col=self.t_col, r_col=self.r_col)

        sg_window = None
        if self.chk_sg.isChecked():
            if not _HAS_SCIPY:
                QMessageBox.information(self, "Smoothing unavailable", "SciPy not found; computing without smoothing.")
            else:
                w = int(self.sg_window.value())
                # Ensure odd >=3 and ≤ number of points
                if w % 2 == 0:
                    w += 1
                w = min(max(3, w), df_inc.shape[0] if df_inc.shape[0] % 2 == 1 else df_inc.shape[0] - 1)
                sg_window = max(3, w)

        try:
            out = self.calc.pointwise_tcr(df_inc, sg_window=sg_window, sg_poly=int(self.sg_poly.value()))
        except Exception as e:
            QMessageBox.critical(self, "Computation error", str(e))
            return

        T = out[self.t_col].to_numpy(float)
        alpha = out["alpha"].to_numpy(float)

        # Update top plot (show current included/excluded)
        self.canvas.plot_R(
            self.df_raw[self.t_col].to_numpy(float),
            self.df_raw[self.r_col].to_numpy(float),
            self.included_mask
        )
        # Update bottom plot
        style = self.tcr_style.currentText()
        self.canvas.plot_TCR(T, alpha, curie_temp=curie, style=style)

        if not initial:
            QMessageBox.information(self, "Done", f"Computed TCR for {len(T)} points.")

    def on_add_overlay_clicked(self):
        """Load a .tcr (xy) file (robust to variable header; tab/space separated)."""
        import re

        def _guess_data_start(file_path: str) -> int:
            """
            Return the 0-based line index where numeric data starts.
            Skips empty lines and lines starting with common comment tokens.
            A data line is the first line where the first two tokens parse as floats.
            """
            comment_starts = ("#", ";", "//")
            float_re = re.compile(r"""^[\+\-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][\+\-]?\d+)?$""")

            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for idx, raw in enumerate(f):
                    line = raw.strip()
                    if not line or line.startswith(comment_starts):
                        continue
                    # split on ANY whitespace (tabs or spaces)
                    parts = re.split(r"\s+", line)
                    if len(parts) < 2:
                        continue
                    # quick numeric check for first two tokens
                    if float_re.match(parts[0]) and float_re.match(parts[1]):
                        return idx
                    # tolerate decimal commas by replacing ',' -> '.'
                    p0 = parts[0].replace(",", ".")
                    p1 = parts[1].replace(",", ".")
                    if float_re.match(p0) and float_re.match(p1):
                        return idx
            # Fallback: assume no header
            return 0

        # Default directory (Windows): use a raw string so backslashes are safe
        default_dir = r"C:\HotDiskTPS_7\data\Config\Tcr"
        if not os.path.isdir(default_dir):
            default_dir = ""  # let the dialog choose the last-used dir

        path, _ = QFileDialog.getOpenFileName(
            self,
            "Open .tcr (xy)",
            default_dir,
            "TCR files (*.tcr *.txt *.csv);;All files (*)"
        )
        if not path:
            return

        try:
            start = _guess_data_start(path)

            # Read two numeric columns (T, alpha) starting at detected row.
            # sep=r"\s+" handles both tabs and spaces; engine='python' supports regex sep.
            df = pd.read_csv(
                path,
                sep=r"\s+",
                engine="python",
                header=None,
                skiprows=start,
                usecols=[0, 1],
                comment="#",
                dtype=str,  # read as str first so we can normalize decimal commas
            )

            if df.shape[1] < 2 or df.empty:
                raise ValueError("No numeric data found (need at least two columns).")

            # Normalize decimal commas then convert to float
            T = pd.to_numeric(df.iloc[:, 0].str.replace(",", ".", regex=False), errors="coerce").to_numpy()
            alpha = pd.to_numeric(df.iloc[:, 1].str.replace(",", ".", regex=False), errors="coerce").to_numpy()

            # Drop any rows that failed to convert
            mask = np.isfinite(T) & np.isfinite(alpha)
            T = T[mask]
            alpha = alpha[mask]

            if T.size == 0:
                raise ValueError("No valid numeric rows after parsing.")

            label = os.path.basename(path)
            self.canvas.add_overlay(SeriesOverlay(label, T, alpha))

        except Exception as e:
            QMessageBox.critical(self, "Overlay error", f"Could not load overlay:\n{e}")

    def on_plot_style_changed(self):
        # re-draw with last computed series if available
        if self.df_raw is None or self.included_mask is None or self.calc is None:
            return
        try:
            # Recompute quickly from included points (fast) or cache last result if you prefer
            df_inc = self.df_raw.loc[self.included_mask].copy()
            sg_window = None
            if self.chk_sg.isChecked() and _HAS_SCIPY:
                w = int(self.sg_window.value())
                if w % 2 == 0:
                    w += 1
                w = min(max(3, w), df_inc.shape[0] if df_inc.shape[0] % 2 == 1 else df_inc.shape[0] - 1)
                sg_window = max(3, w)
            out = self.calc.pointwise_tcr(df_inc, sg_window=sg_window, sg_poly=int(self.sg_poly.value()))
            T = out[self.t_col].to_numpy(float)
            alpha = out["alpha"].to_numpy(float)
            self.canvas.plot_TCR(T, alpha, curie_temp=float(self.curie_spin.value()), style=self.tcr_style.currentText())
        except Exception:
            pass

# ------------------------------------ Main ------------------------------------
def main():
    app = QApplication(sys.argv)
    w = TCRWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
