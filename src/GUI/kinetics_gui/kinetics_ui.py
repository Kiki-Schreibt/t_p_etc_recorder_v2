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
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple, Optional

import numpy as np

import matplotlib.dates as mdates
from matplotlib.collections import LineCollection
from matplotlib.colors import Normalize
from mpl_toolkits.mplot3d.art3d import Line3DCollection
try:
    # Matplotlib ≥ 3.6 (and v4)
    from matplotlib import colormaps as _cmaps
    def _get_cmap(name: str):
        return _cmaps.get_cmap(name)
except Exception:  # Matplotlib ≤ 3.x fallback
    import matplotlib.cm as _cm
    def _get_cmap(name: str):
        return _cm.get_cmap(name)

from matplotlib.cm import ScalarMappable

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
    QWidget,
    QStackedLayout,
    QFileDialog
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

    def _init_axes(self, z_axis_str='Value') -> None:
        ax = self.ax
        ax.clear()
        ax.set_xlabel("Time (min)")
        ax.set_ylabel("Cycle (#)")
        ax.set_zlabel(z_axis_str)
        ax.view_init(elev=22, azim=-60)
        ax.grid(True)
        self.draw_idle()


class Plot3DManager:
    """Keeps track of plotted artists so we can remove/replace per cycle."""

    def __init__(self, canvas: Matplotlib3DCanvas) -> None:
        self.canvas = canvas
        self.ax = canvas.ax
        self.measurement_lines: Dict[float, List] = {}
        self.kinetics_lines: Dict[float, List] = {}
        # data bounds
        self._x_min = math.inf; self._x_max = -math.inf
        self._y_min = math.inf; self._y_max = -math.inf
        self._z_min = math.inf; self._z_max = -math.inf
        # color mapping
        self._cmin = math.inf; self._cmax = -math.inf
        self._cmap = _get_cmap('viridis')
        self._norm: Normalize | None = None
        self._cbar = None
        self._color_label = "Color"
        self._cdata: Dict[float, np.ndarray] = {}  # cycle -> c-array
        self.z_axis_str = ""

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
            self.canvas._init_axes(z_axis_str=self.z_axis_str)
        self.canvas.draw_idle()

     # ---- color helpers ----
    def _update_color_range(self, c: np.ndarray | None) -> bool:
        """Return True if range changed (requiring recolor)."""
        if c is None or c.size == 0 or not np.isfinite(c).any():
            return False
        changed = False
        cmin = float(np.nanmin(c)); cmax = float(np.nanmax(c))
        if cmin < self._cmin: self._cmin = cmin; changed = True
        if cmax > self._cmax: self._cmax = cmax; changed = True
        if changed:
            self._norm = Normalize(vmin=self._cmin, vmax=self._cmax)
        return changed

    def _ensure_colorbar(self):
        if self._norm is None:
            return
        if self._cbar is None:
            sm = ScalarMappable(norm=self._norm, cmap=self._cmap)
            self._cbar = self.canvas.figure.colorbar(sm, ax=self.ax, pad=0.01)
            self._cbar.set_label(self._color_label)
        else:
            self._cbar.mappable.set_norm(self._norm)
            self._cbar.mappable.set_cmap(self._cmap)
            self._cbar.set_label(self._color_label)

    def _recolor_all(self):
        """Recompute segment colors for all collections after range grows."""
        if self._norm is None:
            return
        for cyc, arts in {**self.measurement_lines, **self.kinetics_lines}.items():
            for art in arts:
                if isinstance(art, Line3DCollection) and cyc in self._cdata:
                    cvals = self._cdata[cyc]
                    if cvals is None or cvals.size < 2: continue
                    colors = self._cmap(self._norm(cvals[:-1]))
                    art.set_colors(colors)

    def _color_label_from_param(self, color_label):
        if color_label:
            self._color_label = color_label

    # ---- plotting ----
    def plot_measurement(self, cycle: float, series: Series, z_axis_type="", color_label=None) -> None:
        self._set_z_axis(z_axis_type)
        self._color_label_from_param(color_label)
        artist = self._plot_series(cycle, series)
        self.measurement_lines.setdefault(float(cycle), []).append(artist)
        self._update_bounds(series); self._apply_bounds()

    def plot_kinetics(self, cycle: float, series: Series, color_label=None) -> None:
        self._color_label_from_param(color_label)
        artist = self._plot_series(cycle, series, linestyle="--", linewidth=2.0)
        self.kinetics_lines.setdefault(float(cycle), []).append(artist)
        self._update_bounds(series); self._apply_bounds()

    def _plot_series(self, cycle: float, s: Series, **line_kwargs):
        if s.c is None or s.c.size < 2 or not np.isfinite(s.c).any():
            # Solid line fallback
            (line,) = self.ax.plot(s.x, s.y, s.z, alpha=0.95, **({"linewidth":1.8}|line_kwargs))
            line.set_picker(True); line.set_pickradius(8)
            return line

        # Gradient line: build segments
        n = len(s.x)
        segs = [np.array([[s.x[i], s.y[i], s.z[i]],
                          [s.x[i+1], s.y[i+1], s.z[i+1]]]) for i in range(n-1)]

        lc = Line3DCollection(segs, linewidths=line_kwargs.get("linewidth", 1.8), alpha=0.95)
        lc.set_picker(True)
        self.ax.add_collection3d(lc)

        # update color range & colorbar
        changed = self._update_color_range(s.c)
        if self._norm is not None:
            lc.set_colors(self._cmap(self._norm(s.c[:-1])))
        self._cdata[float(cycle)] = s.c
        if changed:
            self._recolor_all()
        self._ensure_colorbar()
        self.canvas.draw_idle()
        return lc

    def remove_measurement(self, cycle: int) -> None:
        for art in self.measurement_lines.pop(cycle, []):
            try:
                art.remove()
            except Exception:
                pass
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
        # reset
        for d in (self.measurement_lines, self.kinetics_lines):
            for arts in d.values():
                for a in arts:
                    if hasattr(a, "set_linewidth"): a.set_linewidth(1.8)
                    if hasattr(a, "set_linewidths"): a.set_linewidths(1.8)
                    a.set_alpha(0.95); a.set_zorder(1)
        # emphasize
        if hasattr(artist, "set_linewidth"): artist.set_linewidth(3.0)
        if hasattr(artist, "set_linewidths"): artist.set_linewidths(3.0)
        artist.set_alpha(1.0); artist.set_zorder(10)
        self.canvas.draw_idle()

    def _set_z_axis(self, z_axis_type):
        from src.infrastructure.core.table_config import TableConfig
        table = TableConfig().KineticsTable
        if z_axis_type == table.pressure:
            self.z_axis_str = "Pressure (bar)"
        if z_axis_type == table.temperature_res or z_axis_type == table.temperature:
            self.z_axis_str = "Temperature (°C)"
        if z_axis_type == table.rate_kg_min:
            self.z_axis_str = "Rate (kg min^-1)"
        if z_axis_type == table.rate_wt_p_min:
            self.z_axis_str = "Rate (wt-% min^-1)"
        if z_axis_type == table.uptake_kg:
            self.z_axis_str = "Uptake (kg)"
        if z_axis_type == table.uptake_wt_p:
            self.z_axis_str = "Uptake (wt-%)"
        self.canvas.ax.set_zlabel(self.z_axis_str)

    def clear_all(self) -> None:
        self.ax.cla()
        self.canvas._init_axes(z_axis_str=self.z_axis_str)
        self.measurement_lines.clear(); self.kinetics_lines.clear()
        self._x_min = self._y_min = self._z_min = math.inf
        self._x_max = self._y_max = self._z_max = -math.inf
        # reset color mapping
        self._cmin = math.inf; self._cmax = -math.inf
        self._norm = None; self._cdata.clear()
        if self._cbar:
            try: self._cbar.remove()
            except Exception: pass
            self._cbar = None
        self.canvas.draw_idle()


# -----------------------------
# 2D Plot manager
# -----------------------------
class Matplotlib2DCanvas(FigureCanvas):
    def __init__(self) -> None:
        fig = Figure(figsize=(7, 5), tight_layout=True)
        self.ax = fig.add_subplot(111)  # 2D
        super().__init__(fig)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.updateGeometry()
        self._init_axes()

    def _init_axes(self, y_axis_str='Value') -> None:
        ax = self.ax
        ax.clear()
        ax.set_xlabel("Time (min)")
        ax.set_ylabel(y_axis_str)
        ax.grid(True)
        self.draw_idle()


class Plot2DManager:
    """2D manager: plots X=time(min) vs Y=z(value) and colors per cycle."""
    def __init__(self, canvas: Matplotlib2DCanvas) -> None:
        self.canvas = canvas
        self.ax = canvas.ax
        self.measurement_lines: Dict[float, List] = {}
        self.kinetics_lines: Dict[float, List] = {}
        self._x_min = math.inf; self._x_max = -math.inf
        self._y_min = math.inf; self._y_max = -math.inf
        self.y_axis_str = ""

    # --- datetime helpers for Y axis (z series) ---
    def _is_datetime_array(self, a: np.ndarray) -> bool:
        if isinstance(a, np.ndarray):
            if np.issubdtype(a.dtype, np.datetime64): return True
            return a.dtype == object and a.size and isinstance(a[0], datetime)
        return False

    def _minmax_y(self, arr: np.ndarray) -> Tuple[float, float]:
        if self._is_datetime_array(arr):
            nums = mdates.date2num(list(arr))
            return float(np.min(nums)), float(np.max(nums))
        arrf = np.asarray(arr, dtype=float)
        return float(np.min(arrf)), float(np.max(arrf))

    # ---- bounds helpers ----
    def _update_bounds(self, x: np.ndarray, y: np.ndarray) -> None:
        self._x_min = min(self._x_min, float(np.min(x)))
        self._x_max = max(self._x_max, float(np.max(x)))
        ymin, ymax = self._minmax_y(y)
        self._y_min = min(self._y_min, ymin)
        self._y_max = max(self._y_max, ymax)

    def _apply_bounds(self) -> None:
        if not all(np.isfinite(v) for v in [self._x_min, self._x_max, self._y_min, self._y_max]):
            return
        def _pad(a, b):
            span = b - a
            if span <= 0: return a - 0.5, b + 0.5
            pad = 0.03 * span
            return a - pad, b + pad
        x0, x1 = _pad(self._x_min, self._x_max)
        y0, y1 = _pad(self._y_min, self._y_max)
        self.ax.set_xlim(x0, x1); self.ax.set_ylim(y0, y1)
        self.canvas.draw_idle()

    # ---- limits API (mirror 3D signature) ----
    def get_current_limits(self) -> Tuple[Tuple[float,float], Tuple[float,float], Tuple[float,float]]:
        # return (xlim, dummy, ylim) so the existing axis panel keeps working
        xlim = self.ax.get_xlim()
        ylim = self.ax.get_ylim()
        return xlim, (0.0, 1.0), ylim

    def get_data_bounds(self) -> Tuple[Tuple[float,float], Tuple[float,float], Tuple[float,float]] | None:
        vals = [self._x_min, self._x_max, self._y_min, self._y_max]
        if not all(np.isfinite(v) for v in vals):
            return None
        return (self._x_min, self._x_max), (0.0, 1.0), (self._y_min, self._y_max)

    def set_limits(self, xlim=None, ylim=None, zlim=None, *, draw=True) -> None:
        # map zlim -> 2D y-limits
        if xlim is not None: self.ax.set_xlim(*xlim)
        if zlim is not None: self.ax.set_ylim(*zlim)
        if draw: self.canvas.draw_idle()

    def fit_to_data(self) -> None:
        self._apply_bounds()

    def reset_view(self) -> None:
        self.ax.set_xlabel("Time (min)")
        self.ax.set_ylabel(self.y_axis_str or "Value")
        self.ax.grid(True)
        if self.get_data_bounds() is not None:
            self._apply_bounds()
        else:
            self.canvas._init_axes(y_axis_str=self.y_axis_str)
        self.canvas.draw_idle()

    # ---- plotting ----
    def _ensure_y_formatter(self, z: np.ndarray):
        if self._is_datetime_array(z):
            loc = mdates.AutoDateLocator()
            fmt = mdates.ConciseDateFormatter(loc)
            self.ax.yaxis.set_major_locator(loc)
            self.ax.yaxis.set_major_formatter(fmt)

    def plot_measurement(self, cycle: float, series: Series, z_axis_type="", color_label=None) -> None:
        self._set_y_axis(z_axis_type)
        self._ensure_y_formatter(series.z)
        artist = self._plot_series(series, linestyle=None)
        self.measurement_lines.setdefault(float(cycle), []).append(artist)
        self._update_bounds(series.x, series.z); self._apply_bounds()

    def plot_kinetics(self, cycle: float, series: Series, color_label=None) -> None:
        self._ensure_y_formatter(series.z)
        artist = self._plot_series(series, linestyle="--")
        self.kinetics_lines.setdefault(float(cycle), []).append(artist)
        self._update_bounds(series.x, series.z); self._apply_bounds()

    # --- helpers
    _cmin = math.inf; _cmax = -math.inf
    _norm: Normalize | None = None
    _cmap = _get_cmap('viridis')
    _cbar = None
    _color_label = "Color"
    _cdata: Dict[float, np.ndarray] = {}

    def _update_color_range(self, c):
        if c is None or c.size == 0 or not np.isfinite(c).any(): return False
        changed = False
        cmin = float(np.nanmin(c)); cmax = float(np.nanmax(c))
        if cmin < self._cmin: self._cmin = cmin; changed = True
        if cmax > self._cmax: self._cmax = cmax; changed = True
        if changed: self._norm = Normalize(vmin=self._cmin, vmax=self._cmax)
        return changed

    def _ensure_colorbar(self):
        if self._norm is None: return
        if self._cbar is None:
            sm = ScalarMappable(norm=self._norm, cmap=self._cmap)
            self._cbar = self.canvas.figure.colorbar(sm, ax=self.ax, pad=0.01)
            self._cbar.set_label(self._color_label)
        else:
            self._cbar.mappable.set_norm(self._norm)
            self._cbar.mappable.set_cmap(self._cmap)

    def _recolor_all(self):
        if self._norm is None: return
        for arts in {**self.measurement_lines, **self.kinetics_lines}.values():
            for art in arts:
                if isinstance(art, LineCollection):
                    cvals = getattr(art, "_cvals", None)
                    if cvals is None or cvals.size < 2: continue
                    art.set_colors(self._cmap(self._norm(cvals[:-1])))

    def _plot_series(self, s: Series, linestyle=None):
        if s.c is None or s.c.size < 2 or not np.isfinite(s.c).any():
            # Solid line
            (line,) = self.ax.plot(s.x, s.z, linestyle=(linestyle or "-"), linewidth=1.8, alpha=0.95)
            line.set_picker(True); line.set_pickradius(8)
            return line

        n = len(s.x)
        segs = [np.array([[s.x[i], s.z[i]], [s.x[i+1], s.z[i+1]]]) for i in range(n-1)]
        lc = LineCollection(segs, linewidths=1.8, alpha=0.95)
        lc.set_picker(True)
        self.ax.add_collection(lc)

        changed = self._update_color_range(s.c)
        if self._norm is not None:
            lc.set_colors(self._cmap(self._norm(s.c[:-1])))
            lc._cvals = s.c  # stash so we can recolor later
        if changed:
            self._recolor_all()
        self._ensure_colorbar()
        self.canvas.draw_idle()
        return lc

    def remove_measurement(self, cycle: int | float) -> None:
        for art in self.measurement_lines.pop(float(cycle), []):
            try: art.remove()
            except Exception: pass
        self.canvas.draw_idle()

    def resolve_artist(self, artist):
        for cyc, arts in self.measurement_lines.items():
            if artist in arts: return ("meas", float(cyc))
        for cyc, arts in self.kinetics_lines.items():
            if artist in arts: return ("kin", float(cyc))
        return (None, None)

    def mark_selected(self, artist):
        for d in (self.measurement_lines, self.kinetics_lines):
            for arts in d.values():
                for a in arts:
                    a.set_linewidth(1.5); a.set_alpha(0.9); a.set_zorder(1)
        artist.set_linewidth(3.0); artist.set_alpha(1.0); artist.set_zorder(10)
        self.canvas.draw_idle()

    def _set_y_axis(self, z_axis_type):
        # mirror your 3D label logic on Y
        from src.infrastructure.core.table_config import TableConfig
        table = TableConfig().KineticsTable
        if z_axis_type == table.pressure:
            self.y_axis_str = "Pressure (bar)"
        elif z_axis_type == table.rate_kg_min:
            self.y_axis_str = "Rate (kg min^-1)"
        elif z_axis_type == table.rate_wt_p_min:
            self.y_axis_str = "Rate (wt-% min^-1)"
        elif z_axis_type == table.uptake_kg:
            self.y_axis_str = "Uptake (kg)"
        elif z_axis_type == table.uptake_wt_p:
            self.y_axis_str = "Uptake (wt-%)"
        else:
            self.y_axis_str = "Value"
        self.canvas.ax.set_ylabel(self.y_axis_str)

    def clear_all(self) -> None:
        self.ax.cla()
        self.canvas._init_axes(y_axis_str=self.y_axis_str)
        self.measurement_lines.clear(); self.kinetics_lines.clear()
        self._x_min = self._y_min = math.inf; self._x_max = self._y_max = -math.inf
        self._cmin = math.inf; self._cmax = -math.inf; self._norm = None; self._cdata.clear()
        if self._cbar:
            try: self._cbar.remove()
            except Exception: pass
            self._cbar = None
        self.canvas.draw_idle()

# -----------------------------
# 2D Plot manager (Cycle vs Value summary)
# -----------------------------
class CycleValue2DCanvas(FigureCanvas):
    def __init__(self) -> None:
        fig = Figure(figsize=(7, 5), tight_layout=True)
        self.ax = fig.add_subplot(111)
        super().__init__(fig)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.updateGeometry()
        self._init_axes()

    def _init_axes(self, y_axis_str='Value') -> None:
        ax = self.ax
        ax.clear()
        ax.set_xlabel("Cycle (#)")
        ax.set_ylabel(y_axis_str)
        ax.grid(True)
        self.draw_idle()


class CycleValue2DManager:
    """
    2D manager for (X = cycle number, Y = float value) scatter plots.
    Implements the same limits API as the other managers so the existing
    axis panel keeps working.
    """
    def __init__(self, canvas: CycleValue2DCanvas) -> None:
        self.canvas = canvas
        self.ax = canvas.ax
        self.scatters: List = []  # PathCollection objects
        self._x_min = math.inf; self._x_max = -math.inf
        self._y_min = math.inf; self._y_max = -math.inf
        self.y_axis_str = "Value"

    # ---- bounds helpers ----
    def _update_bounds(self, x: np.ndarray, y: np.ndarray) -> None:
        x = np.asarray(x, dtype=float); y = np.asarray(y, dtype=float)
        m = np.isfinite(x) & np.isfinite(y)
        if not m.any():
            return
        self._x_min = min(self._x_min, float(np.min(x[m])))
        self._x_max = max(self._x_max, float(np.max(x[m])))
        self._y_min = min(self._y_min, float(np.min(y[m])))
        self._y_max = max(self._y_max, float(np.max(y[m])))

    def _apply_bounds(self) -> None:
        if not all(np.isfinite(v) for v in [self._x_min, self._x_max, self._y_min, self._y_max]):
            return
        def _pad(a, b):
            span = b - a
            if span <= 0: return a - 0.5, b + 0.5
            pad = 0.03 * span
            return a - pad, b + pad
        x0, x1 = _pad(self._x_min, self._x_max)
        y0, y1 = _pad(self._y_min, self._y_max)
        self.ax.set_xlim(x0, x1); self.ax.set_ylim(y0, y1)
        self.canvas.draw_idle()

    # ---- limits API (same shape as others) ----
    def get_current_limits(self):
        xlim = self.ax.get_xlim()
        ylim = self.ax.get_ylim()
        return xlim, (0.0, 1.0), ylim

    def get_data_bounds(self):
        vals = [self._x_min, self._x_max, self._y_min, self._y_max]
        if not all(np.isfinite(v) for v in vals):
            return None
        return (self._x_min, self._x_max), (0.0, 1.0), (self._y_min, self._y_max)

    def set_limits(self, xlim=None, ylim=None, zlim=None, *, draw=True) -> None:
        if xlim is not None: self.ax.set_xlim(*xlim)
        if zlim is not None: self.ax.set_ylim(*zlim)   # map z->y to match other managers
        if draw: self.canvas.draw_idle()

    def fit_to_data(self) -> None:
        self._apply_bounds()

    def reset_view(self) -> None:
        self.ax.set_xlabel("Cycle (#)")
        self.ax.set_ylabel(self.y_axis_str)
        self.ax.grid(True)
        if self.get_data_bounds() is not None:
            self._apply_bounds()
        else:
            self.canvas._init_axes(y_axis_str=self.y_axis_str)
        self.canvas.draw_idle()

    # ---- plotting (SCATTER) ----
    def plot_xy(self, x: np.ndarray, y: np.ndarray, *, label: str = "") -> None:
        x = np.asarray(x, dtype=float); y = np.asarray(y, dtype=float)
        m = np.isfinite(x) & np.isfinite(y)
        if not m.any():
            return
        sc = self.ax.scatter(x[m], y[m], s=28, alpha=0.9, label=(label or None))
        self.scatters.append(sc)
        if label:
            self.ax.legend(loc="best")
        self._update_bounds(x[m], y[m]); self._apply_bounds()

    def clear_all(self) -> None:
        self.ax.cla()
        self.canvas._init_axes(y_axis_str=self.y_axis_str)
        self.scatters.clear()
        self._x_min = self._y_min = math.inf
        self._x_max = self._y_max = -math.inf
        self.canvas.draw_idle()

    # ---- pick/selection (not used) ----
    def resolve_artist(self, artist):
        return (None, None)

    def mark_selected(self, artist):
        pass


# -----------------------------
# VIEW (UI only)
# -----------------------------
class KineticsView(QMainWindow):
    # Signals that the controller can subscribe to
    loadRequested = Signal(str, str, str)   # sample_id, cycles_text
    runRequested = Signal(str, str)    # sample_id, cycles_text
    deleteRequested = Signal(str, str)    # sample_id, cycles_text
    clearRequested = Signal()
    exportRequested = Signal()
    correctionRequested = Signal()

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Kinetics 3D Viewer")
        self.resize(1400, 720)
        self._build_ui()
        self.selected_cycle = 0

    # --------- UI creation ---------
    def _build_ui(self) -> None:
        # --- canvases + managers ---
        self._build_canvas()

        ##create top controls
        left, form = self._build_controls()

        # Build Kinetics options group
        root = QWidget()
        hl = QHBoxLayout(root)
        left_wrap = self._build_kinetic_controls(left, hl)

        hl.addWidget(left_wrap, 0)
        hl.addWidget(self.canvas_holder, 1)
        self.setCentralWidget(root)

         # wiring
        self.btn_load.clicked.connect(lambda: self.loadRequested.emit(
            self.sample_edit.text().strip(),
            self.cycles_edit.text().strip(),
            self.combo_box_z_select.currentText().strip()
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

        # plot mode selector reacts to user choice
        self.plot_mode_combo.currentIndexChanged.connect(self._on_plot_mode_changed)

        self._on_axes_sync_clicked()

    def _build_canvas(self):
        self.canvas3d = Matplotlib3DCanvas()
        self.canvas2d_time = Matplotlib2DCanvas()
        self.canvas2d_cv = CycleValue2DCanvas()

        self.plot3d_mgr = Plot3DManager(self.canvas3d)
        self.plot2d_mgr = Plot2DManager(self.canvas2d_time)
        self.plotcv_mgr = CycleValue2DManager(self.canvas2d_cv)

        # holder with a stack so we can switch views
        self.canvas_holder = QWidget()
        self.canvas_stack = QStackedLayout(self.canvas_holder)
        self.canvas_stack.addWidget(self.canvas3d)      # index 0
        self.canvas_stack.addWidget(self.canvas2d_time) # index 1
        self.canvas_stack.addWidget(self.canvas2d_cv)   # index 2
        self.canvas_stack.setCurrentIndex(0)

        # keep backward-compat attrs the rest of the code uses
        self.canvas = self.canvas3d
        self.plot_mgr = self.plot3d_mgr

        # context menus for all canvases
        for c in (self.canvas3d, self.canvas2d_time, self.canvas2d_cv):
            c.setContextMenuPolicy(Qt.CustomContextMenu)
            c.customContextMenuRequested.connect(self._on_canvas_context_menu)

    def _build_controls(self):

        self.sample_edit = QLineEdit()
        self.sample_edit.setPlaceholderText("e.g., WAE-WA-028")

        self.cycles_edit = QLineEdit()
        self.cycles_edit.setPlaceholderText("Cycles (e.g., 1-3,7,10)")

        self.plot_mode_combo = QComboBox()
        self.plot_mode_combo.addItems([
            "Auto",             # default: 2D for single cycle, else 3D
            "3D",
            "2D (time-value)",
            "2D (cycle–value)",
        ])
        self.plot_mode_combo.setCurrentText("Auto")

        from src.infrastructure.core.table_config import TableConfig
        kinetics_table = TableConfig().KineticsTable
        kinetics_selectables = [
            kinetics_table.pressure,
            kinetics_table.temperature,
            kinetics_table.temperature_res,
            kinetics_table.uptake_wt_p,
            kinetics_table.uptake_kg,
            kinetics_table.rate_kg_min,
            kinetics_table.rate_wt_p_min,
            kinetics_table.max_rate_kg_min,
            kinetics_table.max_rate_wt_p_min
        ]
        self.combo_box_z_select = QComboBox()
        self.combo_box_z_select.addItems([str(kin_select) for kin_select in kinetics_selectables])

        self.color_by_combo = QComboBox()
        self.color_by_combo.addItems([
            "Solid color",                           # no gradient
            kinetics_table.temperature,              # profile drives the colormap
            kinetics_table.temperature_res,
            kinetics_table.pressure,
        ])

        self.btn_load = QPushButton("Load Curves")
        self.btn_clear = QPushButton("Clear Plot")

        self.btnSendToOrigin = QPushButton("Send to Origin")
        self.btnSendToOrigin.setToolTip("Create current visible plot in Origin")
        self.btnSendToOrigin.clicked.connect(self.exportRequested)

        self.chk_replace = QCheckBox("Replace measurement with kinetics after calc")
        self.chk_replace.setChecked(True)

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
        form.addRow("Plot mode", self.plot_mode_combo)
        form.addRow("Color by", self.color_by_combo)
        form.addRow(self.combo_box_z_select)
        form.addRow(self.btn_load)
        form.addRow(self.btn_clear)
        form.addRow(self.chk_replace)
        form.addRow(self.btnSendToOrigin)

        form.addRow("Status", self.status_label)
        left.setLayout(form)

        return left, form

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

    def _build_kinetic_controls(self, left, hl):
        kin_box = QGroupBox("Kinetics")
        kin_form = QFormLayout()


        self.reaction_duration_edit = QLineEdit()
        self.reaction_duration_edit.setPlaceholderText("e.g., 180min")

        self.resample_rule_edit = QLineEdit()
        self.resample_rule_edit.setPlaceholderText("e.g., 60s")
        self.resample_rule_edit.setText("60s")

        self.resample_how_combo = QComboBox()
        self.resample_how_combo.addItems(["mean", "nearest", 'ffill', 'bfill'])
        self.resample_how_combo.setCurrentText("mean")

        self.smooth_seconds_spin = QDoubleSpinBox()
        self.smooth_seconds_spin.setRange(0.0, 1e9)
        self.smooth_seconds_spin.setDecimals(2)
        self.smooth_seconds_spin.setSingleStep(1.0)
        self.smooth_seconds_spin.setSpecialValueText("off")  # 0 = off
        self.smooth_seconds_spin.setValue(0.0)

        self.enforce_monotonic_chk = QCheckBox("Enforce monotonic")
        self.enforce_monotonic_chk.setChecked(True)

        self.btn_run = QPushButton("Run Kinetics")

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(True)


        kin_form.addRow("Reaction duration", self.reaction_duration_edit)
        kin_form.addRow("Resample rule", self.resample_rule_edit)
        kin_form.addRow("Resample how", self.resample_how_combo)
        kin_form.addRow("Smooth seconds", self.smooth_seconds_spin)
        kin_form.addRow(self.enforce_monotonic_chk)
        kin_form.addRow(self.btn_run)    # ← Run button placed at the bottom of this group
        kin_form.addRow("Progress", self.progress)
        kin_box.setLayout(kin_form)

        axes_box = self._build_axes_control()

        left_col = QVBoxLayout()
        left_col.addWidget(left)
        left_col.addWidget(kin_box)   # ← place the new group “down” from Controls
        left_col.addWidget(axes_box)
        left_col.addStretch(1)
        left_wrap = QWidget(); left_wrap.setLayout(left_col)
        return left_wrap

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

    # Delegate plotting to the active manager
    def plot_measurement(self, cycle: int, series: Series) -> None:
        z_axis_type = self.combo_box_z_select.currentText().strip()
        sample_id = self.sample_edit.text().strip()
        color_label = self.get_color_by_label()
        self.plot_mgr.plot_measurement(cycle, series, z_axis_type, color_label=color_label)

    def plot_kinetics(self, cycle: int, series: Series) -> None:
        color_label = self.get_color_by_label()
        self.plot_mgr.plot_kinetics(cycle, series, color_label=color_label)

    def clear_plot(self) -> None:
        # clear all managers so switching views doesn't show stale curves
        self.plot3d_mgr.clear_all()
        self.plot2d_mgr.clear_all()
        self.plotcv_mgr.clear_all()

    def remove_measurement(self, cycle: int) -> None:
        self.plot_mgr.remove_measurement(cycle)

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
        reaction_duration = self.reaction_duration_edit.text().strip() or None
        return resample_rule, resample_how, smooth_seconds, enforce_monotonic, reaction_duration

    def _on_canvas_context_menu(self, pos):
        from PySide6.QtWidgets import QMenu
        src = self.sender() if hasattr(self, "sender") else self.canvas
        global_pos = src.mapToGlobal(pos)
        menu = QMenu(self)

        act_reset = menu.addAction("Reset view")
        act_fit   = menu.addAction("Fit to data")
        act_correction = menu.addAction("Correct Curve")
        act_delete = menu.addAction("Delete Curve")
        act_export = menu.addAction("Export graph")
        chosen = menu.exec(global_pos)
        if chosen == act_reset:
            self.plot_mgr.reset_view()
        elif chosen == act_fit:
            self.plot_mgr.fit_to_data()
        elif chosen == act_correction:
            self.correctionRequested.emit()
        elif chosen == act_delete:
            self.deleteRequested.emit(self.sample_edit.text().strip(), [self.selected_cycle])
        elif chosen == act_export:
            self._on_plot_export_clicked()

    def _switch_to_3d(self):
        self.canvas_stack.setCurrentIndex(0)
        self.canvas = self.canvas3d
        self.plot_mgr = self.plot3d_mgr
        self._on_axes_sync_clicked()

    def _switch_to_2d_time(self):
        self.canvas_stack.setCurrentIndex(1)
        self.canvas = self.canvas2d_time
        self.plot_mgr = self.plot2d_mgr
        self._on_axes_sync_clicked()

    def _switch_to_2d_cycle_value(self):
        self.canvas_stack.setCurrentIndex(2)
        self.canvas = self.canvas2d_cv
        self.plot_mgr = self.plotcv_mgr
        self._on_axes_sync_clicked()

    def _on_plot_mode_changed(self, *_):
        mode = self.plot_mode_combo.currentText()
        if mode == "3D":
            self._switch_to_3d()
        elif mode.startswith("2D (time"):
            self._switch_to_2d_time()
        elif mode.startswith("2D (cycle"):
            self._switch_to_2d_cycle_value()
        else:
            # Auto: leave as-is; controller will toggle based on #cycles
            pass

    def set_auto_plot_mode(self, single_cycle: bool):
        """If Plot mode is Auto, pick 2D for one cycle, else 3D."""
        if self.plot_mode_combo.currentText() != "Auto":
            return
        if single_cycle:
            self._switch_to_2d_time()
        else:
            self._switch_to_3d()

    def mpl_connect(self, event_name: str, callback):
        # connect to all canvases so picks work regardless of the active view
        for c in (self.canvas3d, self.canvas2d_time, self.canvas2d_cv):
            c.mpl_connect(event_name, callback)

    def plot_cycle_value_pairs(self, pairs: List[Tuple[float, float]], *, label: str = "value", sample_id="") -> None:
        # ensure the correct view is active
        if self.plot_mode_combo.currentText() == "Auto":
            self._switch_to_2d_cycle_value()
        elif self.plot_mode_combo.currentText() != "2D (cycle–value)":
            self._switch_to_2d_cycle_value()
        if not pairs:
            return
        x, y = np.asarray([p[0] for p in pairs], dtype=float), np.asarray([p[1] for p in pairs], dtype=float)
        self.plotcv_mgr.plot_xy(x, y, label=sample_id or None)
        self.plotcv_mgr.ax.set_ylabel(label)

    #-----------
    #plot export
    #------------
    def _on_plot_export_clicked(self):
        """Open a file dialog and save the active canvas as an image."""
        # suggest a name from sample + mode
        sample = (self.sample_edit.text() or "plot").strip().replace(os.sep, "_")
        mode = self.plot_mode_combo.currentText().replace(" ", "_").replace("(", "").replace(")", "").replace("–", "-")
        default_name = f"{sample}_{mode}.png"

        path, selected_filter = QFileDialog.getSaveFileName(
            self,
            "Save Plot Image",
            default_name,
            "PNG (*.png);;JPEG (*.jpg *.jpeg);;SVG (*.svg);;PDF (*.pdf)"
        )
        if not path:
            return

        # infer format from extension; append if user omitted it
        ext_map = {
            ".png": "png",
            ".jpg": "jpg",
            ".jpeg": "jpg",
            ".svg": "svg",
            ".pdf": "pdf",
        }
        root, ext = os.path.splitext(path)
        if ext.lower() not in ext_map:
            # pick from the selected filter if possible, else default to .png
            if "JPEG" in selected_filter:
                ext = ".jpg"
            elif "SVG" in selected_filter:
                ext = ".svg"
            elif "PDF" in selected_filter:
                ext = ".pdf"
            else:
                ext = ".png"
            path = root + ext

        try:
            # you can tweak dpi/transparent as you like
            self.save_current_plot(path, dpi=300, transparent=False, bbox_tight=True)
            self.set_status(f"Saved image: {path}")
        except Exception as e:
            self.show_error(f"Failed to save image: {e}")

    def save_current_plot(self, path: str, *, dpi: int = 300, transparent: bool = False, bbox_tight: bool = True,
                          width_in: float | None = None, height_in: float | None = None) -> None:
        """
        Programmatic export of the active canvas to an image file.
        Supports png/jpg/svg/pdf via the filename extension.
        Optionally override size in inches; restores original size afterwards.
        """
        fig = self.canvas.figure  # active canvas (3D or either 2D)
        orig_size = fig.get_size_inches()
        try:
            if width_in or height_in:
                w = width_in or orig_size[0]
                h = height_in or orig_size[1]
                fig.set_size_inches(w, h, forward=True)
                self.canvas.draw_idle(); self.canvas.flush_events()

            save_kwargs = {
                "dpi": dpi,
                "transparent": transparent,
                "facecolor": fig.get_facecolor(),   # keep current bg
            }
            if bbox_tight:
                save_kwargs["bbox_inches"] = "tight"
                save_kwargs["pad_inches"] = 0.05

            fig.savefig(path, **save_kwargs)
        finally:
            # put the figure back how it was
            if width_in or height_in:
                fig.set_size_inches(orig_size, forward=True)
                self.canvas.draw_idle()

    def get_color_by_column(self) -> Optional[str]:
        val = self.color_by_combo.currentText().strip()
        return None if val == "Solid color" else val

    def get_color_by_label(self) -> str:
        txt = self.color_by_combo.currentText().strip()
        if txt == "Solid color": return "Color"
        # pretty labels (optional)
        from src.infrastructure.core.table_config import TableConfig
        t = TableConfig().KineticsTable
        if txt in (t.temperature, t.temperature_res): return "Temperature (°C)"
        if txt == t.pressure: return "Pressure (bar)"
        return txt or "Color"


def show_manager_canvas_3d():
    app = QApplication(sys.argv)

    canvas = Matplotlib3DCanvas()
    manager = Plot3DManager(canvas)
    manager.canvas.show()
    sys.exit(app.exec())


def show_manager_canvas_2d():
    app = QApplication(sys.argv)

    canvas = Matplotlib2DCanvas()
    manager = Plot2DManager(canvas)
    manager.canvas.show()
    sys.exit(app.exec())

# -----------------------------
# Entrypoint
# -----------------------------

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
    #show_manager_canvas_2d()
    #show_manager_canvas_3d()
    main()
