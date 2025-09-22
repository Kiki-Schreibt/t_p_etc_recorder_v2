#kinetics_worker.py


from __future__ import annotations

import os
import sys
import math
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

# --- Qt / Matplotlib embedding ---
from PySide6.QtCore import Qt, QThread, Signal, Slot, QObject

from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 — needed for 3D projection

from src.infrastructure.connections.connections import DatabaseConnection
from src.infrastructure.core.table_config import TableConfig
from src.infrastructure.handler.modbus_handler import KineticCalculator
try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging


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
       # print(query)
        with DatabaseConnection(**self.config.db_conn_params) as db_conn:
            db_conn.cursor.execute(query, (sample_id,))
            rows = db_conn.cursor.fetchall()
       # print([r[0] for r in rows])
        return [r[0] for r in rows]

    def fetch_measurements(
        self, sample_id: str, cycles: Iterable[float], y_col_name: str = ""
    ) -> Dict[float, Series]:
        cycles = list(cycles)
        if not cycles:
            return {}

        query = (
            f"SELECT {self.kinetics_table.cycle_number}, "
            f"       {self.kinetics_table.time_delta_min}, "
            f"       {y_col_name} "
            f"FROM {self.kinetics_table.table_name} "
            f"WHERE {self.kinetics_table.sample_id} = %s "
            f"  AND {self.kinetics_table.cycle_number} = ANY(%s) "
            f"ORDER BY {self.kinetics_table.cycle_number}, {self.kinetics_table.time_delta_min}"
        )

        with DatabaseConnection(**self.config.db_conn_params) as db_conn:
            db_conn.cursor.execute(query, (sample_id, cycles))
            rows = db_conn.cursor.fetchall()

        out: Dict[float, Series] = {}
        for cyc, t_list, y_list in rows:
            if t_list is None or y_list is None:
                continue

            x = np.asarray(list(t_list), dtype=float)         # minutes (numeric)
            z = _as_array_preserve_datetime(y_list)           # keep datetime as datetime

            n = min(len(x), len(z))
            if n == 0:
                continue

            x = x[:n]
            z = z[:n]
            y_axis = np.full(n, float(cyc), dtype=float)
            out[float(cyc)] = Series(x=x, y=y_axis, z=z)

        return out


class DataCreator:

    def __init__(self, config, meta_data):
        self.config = config
        self.meta_data = meta_data
        self.logger = logging.getLogger(__name__)
        self.calculator = KineticCalculator(config=self.config,
                                            meta_data=self.meta_data)

    def calculate_kinetics(self, cycle_number, resample_rule='60s', resample_how='mean',
                            smooth_seconds=None, enforce_monotonic=True):

        self.calculator.run(cycle_number=cycle_number,
                            resample_rule=resample_rule,
                            resample_how=resample_how,
                            smooth_seconds=smooth_seconds,
                            enforce_monotonic=enforce_monotonic)

# -----------------------------
# Kinetics calculation (placeholder)
# -----------------------------
class KineticsWorker(QThread):
    progress = Signal(int)
    error = Signal(str)
    result = Signal(dict)  # dict[cycle:int] -> Series

    def __init__(self, cycles: List[float], config, sample_id):
        super().__init__()
        self.sample_id = sample_id
        self.cycles = cycles
        self.config = config
        from src.infrastructure.handler.metadata_handler import MetaData
        self.meta_data = MetaData(sample_id=self.sample_id,
                                  db_conn_params=self.config.db_conn_params)
        self.logger = logging.getLogger(__name__)
        self.calculator = KineticCalculator(config=self.config,
                                            meta_data=self.meta_data)
        self.resample_rule = '60s'
        self.resample_how = 'mean'
        self.smooth_seconds = None
        self.enforce_monotonic = True

    def _compute_kinetics(self, cycle_number):

        self.calculator.run(cycle_number=cycle_number,
                            resample_rule=self.resample_rule,
                            resample_how=self.resample_how,
                            smooth_seconds=self.smooth_seconds,
                            enforce_monotonic=self.enforce_monotonic)

    def run(self) -> None:
        total = len(self.cycles)
        if total == 0:
            self.result.emit({})
            return
        out: Dict[int, Series] = {}
        for cyc in self.cycles:
            try:
                self._compute_kinetics(cyc)
                self.progress.emit(int(cyc / max(1, total) * 100))
            except Exception as e:  # noqa: BLE001
                self.logger.error(str(e))
                self.progress.emit(int(cyc / max(1, total) * 100))
                #self.error.emit(str(e))
        self.progress.emit(100)
        #self.result.emit(out)


###helper methods
def _as_array_preserve_datetime(values) -> np.ndarray:

    vals = list(values or [])
    if not vals:
        return np.array([], dtype=float)

    first = vals[0]
    if isinstance(first, datetime):
        # keep tz-aware datetimes as Python datetime objects
        return np.array(vals, dtype=object)
    if isinstance(first, np.datetime64):
        # keep NumPy datetime64 (tz-naive) as datetime64
        return np.array(vals, dtype='datetime64[ns]')
    # numeric
    return np.asarray(vals, dtype=float)

###testing methods
def test_grabbing_time_ranges_for_cycle():
    from src.infrastructure.core.config_reader import config

    cycle_list = [17]
    sample_id = "WAE-WA-028"
    dal = DataAccess(config=config)

    dates = dal.fetch_measurements(sample_id=sample_id,
                                cycles=cycle_list,
                                y_col_name=TableConfig().KineticsTable.time)

    time_range = {}
    for cyc in cycle_list:
        start_reading = min(dates[cyc].z)
        end_reading = max(dates[cyc].z)
        time_range[cyc] = [start_reading, end_reading]
    print(time_range[17])


if __name__ == '__main__':
    test_grabbing_time_ranges_for_cycle()


