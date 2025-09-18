#kinetics_worker.py


from __future__ import annotations

import os
import sys
import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import numpy as np

# --- Qt / Matplotlib embedding ---
from PySide6.QtCore import Qt, QThread, Signal, Slot, QObject

from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 — needed for 3D projection

from src.infrastructure.connections.connections import DatabaseConnection
from src.infrastructure.core.table_config import TableConfig


# -----------------------------
# Data Access Layer
# -----------------------------
@dataclass
class Series:
    """Holds a single curve: x (time), y (constant cycle index on Y axis), z (measurement/kinetics value)."""
    x: np.ndarray
    y: np.ndarray
    z: np.ndarray


class DataAccess:
    """Encapsulates SQL for reading measurements/kinetics.

    Adjust table and column names to match your schema.
    """

    def __init__(self, config):
        self.config = config
        self.tp_table = TableConfig().TPDataTable
        self.kinetics_table = TableConfig().KineticsTable
        self.cycle_data_table = TableConfig().CycleDataTable

    def list_cycles(self, sample_id: str) -> List[int]:
        query = (
            f"SELECT DISTINCT {self.cycle_data_table.cycle_number} FROM {self.cycle_data_table.table_name} "
            f"WHERE {self.cycle_data_table.sample_id} = %s ORDER BY {self.cycle_data_table.cycle_number}"
        )
        print(query)
        with DatabaseConnection(**self.config.db_conn_params) as db_conn:
            db_conn.cursor.execute(query, (sample_id,))
            rows = db_conn.cursor.fetchall()
        print([r[0] for r in rows])
        return [r[0] for r in rows]

    def fetch_measurements(
        self, sample_id: str, cycles: Iterable[int]
    ) -> Dict[int, Series]:
        """Return raw measurement curves for each cycle.

        Expected table columns: sample_id, cycle, t_sec, value_y
        """
        cycles = list(cycles)
        if not cycles:
            return {}

        sql = (
            f"SELECT cycle, t_sec, value_y "
            f"FROM measurements "
            f"WHERE sample_id = %s AND cycle = ANY(%s) "
            f"ORDER BY cycle, t_sec"
        )
        data: Dict[int, List[Tuple[float, float]]] = {}
        with self.db as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (sample_id, cycles))
                for cyc, t, y in cur.fetchall():
                    data.setdefault(int(cyc), []).append((float(t), float(y)))

        series: Dict[int, Series] = {}
        for cyc, pts in data.items():
            arr = np.asarray(pts, dtype=float)
            x = arr[:, 0]
            z = arr[:, 1]
            y_axis = np.full_like(x, fill_value=float(cyc))
            series[cyc] = Series(x=x, y=y_axis, z=z)
        return series


# -----------------------------
# Kinetics calculation (placeholder)
# -----------------------------
class KineticsWorker(QThread):
    progress = Signal(int)
    error = Signal(str)
    result = Signal(dict)  # dict[cycle:int] -> Series

    def __init__(self, sample_id: str, cycles: List[int], dal: DataAccess):
        super().__init__()
        self.sample_id = sample_id
        self.cycles = cycles
        self.dal = dal

    def _compute_kinetics(self, x: np.ndarray, z: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """Example kinetics calc (replace with your real backend).

        - Smooth with a moving average
        - Compute normalized derivative as a toy "rate" curve
        Returns (x, kinetics_z)
        """
        if len(z) < 3:
            return x, z.copy()
        # moving average smoothing
        win = max(3, min(31, len(z) // 20 * 2 + 1))  # odd window
        pad = win // 2
        z_pad = np.pad(z, (pad, pad), mode="edge")
        kernel = np.ones(win) / win
        z_smooth = np.convolve(z_pad, kernel, mode="valid")
        # derivative (simple finite diff)
        dx = np.gradient(x)
        dz = np.gradient(z_smooth)
        rate = np.divide(dz, dx, out=np.zeros_like(dz), where=dx != 0)
        # scale for visibility
        rate_norm = (rate - np.min(rate))
        if np.ptp(rate_norm) > 0:
            rate_norm = rate_norm / np.ptp(rate_norm)
        return x, rate_norm

    def run(self) -> None:
        try:
            total = len(self.cycles)
            if total == 0:
                self.result.emit({})
                return
            out: Dict[int, Series] = {}
            # Fetch raw data once for all requested cycles
            raw = self.dal.fetch_measurements(self.sample_id, self.cycles)
            for i, cyc in enumerate(self.cycles, start=1):
                if cyc not in raw:
                    continue
                x, _, z = raw[cyc].x, raw[cyc].y, raw[cyc].z
                xk, zk = self._compute_kinetics(x, z)
                y_axis = np.full_like(xk, fill_value=float(cyc))
                out[cyc] = Series(x=xk, y=y_axis, z=zk)
                self.progress.emit(int(i / max(1, total) * 100))
            self.result.emit(out)
        except Exception as e:  # noqa: BLE001
            self.error.emit(str(e))
