# tp_etc_gui.py
# PySide6 GUI for controlling CSV/ETC imports with a clean separation of UI and business logic.
# Requirements: PySide6, your project modules on PYTHONPATH

from __future__ import annotations
import os
import traceback
from dataclasses import dataclass
from typing import Callable, Optional, List, Tuple

from PySide6.QtCore import QObject, Signal, Slot, QRunnable, QThreadPool, QSize, Qt
from PySide6.QtWidgets import (
    QApplication, QWidget, QFileDialog, QLineEdit, QPushButton, QVBoxLayout, QHBoxLayout,
    QLabel, QGroupBox, QGridLayout, QTextEdit, QCheckBox, QComboBox, QDateTimeEdit,
    QMessageBox, QSpinBox, QDoubleSpinBox
)
from PySide6.QtGui import QAction

# ---- bring in your backend ---------------------------------------------
try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging

from datetime import datetime
from zoneinfo import ZoneInfo

from src.infrastructure.handler.excel_data_handler import write_ETC_folder
from src.infrastructure.handler.metadata_handler import MetaData  # the MetaData you posted
from src.infrastructure.core.table_config import TableConfig
from src.infrastructure.core import global_vars
from src.infrastructure.core.config_reader import config

from src.config_connection_reading_management.database_reading_writing import DataRetriever
from src.infrastructure.utils.eq_p_calculation import VantHoffCalcEq as EqCalculator
from src.infrastructure.handler.modbus_handler import CycleCounter
from src.infrastructure.connections.connections import DatabaseConnection

# CSV pipeline
# (Assumes your CSVProcessor and helpers are importable where this module lives)
from src.infrastructure.handler.csv_handler import CSVProcessor, CSVCounter  # if running inside one file with your post
# If this file is separate in your project, replace the line above with:
# from src.your_module.csv_pipeline import CSVProcessor, CSVCounter

LOCAL_TZ = global_vars.local_tz_qt
STATE_HYD = global_vars.state_hyd
STATE_DEHYD = global_vars.state_dehyd
CYCLE_COUNTER_MODE = global_vars.cycle_counter_mode_CSV_recorder

logger = logging.getLogger("tp_etc_gui")


# ---------------------- Worker infra (threaded tasks) --------------------
class Worker(QRunnable):
    """Generic QRunnable that executes a callable and streams text logs back to UI."""
    class Signals(QObject):
        progress = Signal(str)                # append log line
        finished = Signal(bool, str)          # success?, message

    def __init__(self, fn: Callable, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = Worker.Signals()

    @staticmethod
    def _fmt_exc() -> str:
        import traceback
        return "".join(traceback.format_exc())

    def run(self):
        try:
            # Make a callable that emits on the progress signal
            def progress(msg: str):
                self.signals.progress.emit(msg)

            # Pass the callable to your function
            self.fn(progress, *self.args, **self.kwargs)

            self.signals.finished.emit(True, "Done")
        except Exception as e:
            err = f"{e}\n{self._fmt_exc()}"
            self.signals.progress.emit(err)
            self.signals.finished.emit(False, str(e))

# ---------------------- Business logic (no UI code) ----------------------
@dataclass
class AppConfig:
    db_conn_params: dict
    compress_data: bool = False
    init_state: str = STATE_DEHYD


class BackendController:
    """Business logic façade. No UI code; designed for threaded execution."""

    def __init__(self, app_cfg: AppConfig):
        self.app_cfg = app_cfg
        self.tp_folder: Optional[str] = None
        self.tp_files: List[str] = []
        self.etc_folder: Optional[str] = None
        self.etc_files: List[str] = []  # (write_ETC_folder expects folder; files provided for completeness)

    # --------- metadata ----------
    def load_metadata(self, sample_id: str, log: Callable[[str], None]) -> MetaData:
        md = MetaData(db_conn_params=config.db_conn_params, sample_id=sample_id)
        log(f"Loaded metadata for {sample_id}.")
        return md

    def update_metadata(self, md: MetaData, updates: dict, log: Callable[[str], None]) -> None:
        for attr, val in updates.items():
            if hasattr(md, attr) and val is not None:
                setattr(md, attr, val)
        md.write()
        log(f"Metadata saved for {md.sample_id}.")

    # --------- selections ----------
    def set_tp_folder(self, folder: str):
        self.tp_folder = folder
        self.tp_files = []  # clear single-file selection

    def set_tp_files(self, files: List[str]):
        self.tp_files = files
        self.tp_folder = None  # clear folder selection to avoid ambiguity

    def set_etc_folder(self, folder: str):
        self.etc_folder = folder

    def set_etc_files(self, files: List[str]):
        self.etc_files = files  # not used by write_ETC_folder, but kept for parity

    # --------- operations (to be called inside Worker) ----------
    def process_tp(self, log: Callable[[str], None], sample_id: str):
        """Process T/P from folder or file(s)."""
        if not sample_id:
            raise ValueError("Sample ID required before importing T/P.")
        if not (self.tp_folder or self.tp_files):
            raise ValueError("Select a T/P folder or at least one T/P file first.")

        # Folder path mode (fast path)
        if self.tp_folder:
            csvp = CSVProcessor(
                sample_id=sample_id,
                folder_path=self.tp_folder,
                mode="",
                compress_data=self.app_cfg.compress_data,
                config=config,
            )
            log(f"Processing T/P folder: {self.tp_folder}")
            csvp.process(init_state=self.app_cfg.init_state)
            log("T/P folder processed and cycles counted.")
            return

        # File(s) mode
        for i, file_path in enumerate(self.tp_files, start=1):
            log(f"[{i}/{len(self.tp_files)}] Processing file: {file_path}")
            csvp = CSVProcessor(
                sample_id=sample_id,
                full_file_path=file_path,
                mode="",
                compress_data=self.app_cfg.compress_data,
                config=config,
            )
            csvp.process(init_state=self.app_cfg.init_state)
        log("All selected T/P files processed and cycles counted.")

    def write_etc(self, log: Callable[[str], None], sample_id: str):
        """Write ETC data from a chosen folder."""
        if not sample_id:
            raise ValueError("Sample ID required before writing ETC.")
        if not self.etc_folder:
            raise ValueError("Select an ETC folder first.")

        log(f"Writing ETC from: {self.etc_folder}")
        write_ETC_folder(
            dir_etc_folder=self.etc_folder,
            sample_id=sample_id,
            logger_inst=logger,
            config=config
        )
        log("ETC write completed.")

    def count_cycles_only(self, log: Callable[[str], None], sample_id: str):
        """Run only the cycle counting/uptake for an already-imported sample."""
        if not sample_id:
            raise ValueError("Sample ID required.")
        log("Counting cycles precisely based on DB data…")
        counter = CSVCounter(config=config)
        counter.count(sample_id=sample_id, init_state=self.app_cfg.init_state)
        log("Cycle counting finished.")


# ---------------------- GUI (UI only; calls BackendController) ----------------------
class TPETCImporterWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("T/P & ETC Importer")
        self.setMinimumSize(QSize(900, 650))
        self.thread_pool = QThreadPool.globalInstance()

        # Backend
        self.backend = BackendController(
            AppConfig(
                db_conn_params=config.db_conn_params,
                compress_data=False,
                init_state=STATE_DEHYD,
            )
        )
        self._build_ui()

    # ---- UI construction
    def _build_ui(self):
        layout = QVBoxLayout(self)

        # Meta group
        meta_group = QGroupBox("Meta Data (set before importing)")
        meta_layout = QGridLayout(meta_group)
        self.le_sample = QLineEdit()
        self.btn_load_meta = QPushButton("Load from DB")
        self.btn_save_meta = QPushButton("Save/Update to DB")

        self.le_material = QLineEdit()
        self.dsb_mass = QDoubleSpinBox(); self.dsb_mass.setRange(0, 1e6); self.dsb_mass.setSuffix(" g")
        self.dsb_reservoir = QDoubleSpinBox(); self.dsb_reservoir.setRange(0, 1e6); self.dsb_reservoir.setSuffix(" L")
        self.le_cell = QLineEdit()
        self.dte_first_hyd = QDateTimeEdit(); self.dte_first_hyd.setDisplayFormat("yyyy-MM-dd HH:mm:ss"); self.dte_first_hyd.setCalendarPopup(True)
        self.dsb_pmax = QDoubleSpinBox(); self.dsb_pmax.setRange(0, 1e6)
        self.dsb_tmin = QDoubleSpinBox(); self.dsb_tmin.setRange(-273, 2_000); self.dsb_tmin.setSuffix(" °C")

        row = 0
        meta_layout.addWidget(QLabel("Sample ID*"), row, 0); meta_layout.addWidget(self.le_sample, row, 1)
        meta_layout.addWidget(self.btn_load_meta, row, 2); meta_layout.addWidget(self.btn_save_meta, row, 3); row += 1
        meta_layout.addWidget(QLabel("Material"), row, 0); meta_layout.addWidget(self.le_material, row, 1); row += 1
        meta_layout.addWidget(QLabel("Sample Mass"), row, 0); meta_layout.addWidget(self.dsb_mass, row, 1)
        meta_layout.addWidget(QLabel("Reservoir Volume"), row, 2); meta_layout.addWidget(self.dsb_reservoir, row, 3); row += 1
        meta_layout.addWidget(QLabel("Measurement Cell"), row, 0); meta_layout.addWidget(self.le_cell, row, 1)
        meta_layout.addWidget(QLabel("First Hydrogenation"), row, 2); meta_layout.addWidget(self.dte_first_hyd, row, 3); row += 1
        meta_layout.addWidget(QLabel("Max Pressure Cycling"), row, 0); meta_layout.addWidget(self.dsb_pmax, row, 1)
        meta_layout.addWidget(QLabel("Min Temperature Cycling"), row, 2); meta_layout.addWidget(self.dsb_tmin, row, 3); row += 1

        layout.addWidget(meta_group)

        # Options group
        opt_group = QGroupBox("Options")
        opt_layout = QHBoxLayout(opt_group)
        self.cb_compress = QCheckBox("Compress T/P data")
        self.cb_compress.stateChanged.connect(self._on_compress_changed)
        self.cb_init_state = QComboBox(); self.cb_init_state.addItems([STATE_DEHYD, STATE_HYD])
        opt_layout.addWidget(self.cb_compress)
        opt_layout.addWidget(QLabel("Initial state for cycle counting:"))
        opt_layout.addWidget(self.cb_init_state)
        opt_layout.addStretch(1)
        layout.addWidget(opt_group)

        # T/P group
        tp_group = QGroupBox("Temperature/Pressure CSVs")
        tp_layout = QGridLayout(tp_group)
        self.le_tp_folder = QLineEdit(); self.le_tp_folder.setPlaceholderText("Select a folder OR one or more files …")
        self.btn_tp_browse_folder = QPushButton("Browse Folder")
        self.btn_tp_browse_files = QPushButton("Browse Files")
        self.btn_tp_process = QPushButton("Process T/P")
        tp_layout.addWidget(QLabel("Folder / File(s)"), 0, 0, 1, 1)
        tp_layout.addWidget(self.le_tp_folder, 0, 1, 1, 3)
        tp_layout.addWidget(self.btn_tp_browse_folder, 1, 1)
        tp_layout.addWidget(self.btn_tp_browse_files, 1, 2)
        tp_layout.addWidget(self.btn_tp_process, 1, 3)
        layout.addWidget(tp_group)

        # ETC group
        etc_group = QGroupBox("ETC data (Excel)")
        etc_layout = QGridLayout(etc_group)
        self.le_etc_folder = QLineEdit()
        self.btn_etc_browse_folder = QPushButton("Browse Folder")
        self.btn_etc_write = QPushButton("Write ETC")
        etc_layout.addWidget(QLabel("Folder"), 0, 0)
        etc_layout.addWidget(self.le_etc_folder, 0, 1, 1, 2)
        etc_layout.addWidget(self.btn_etc_browse_folder, 1, 1)
        etc_layout.addWidget(self.btn_etc_write, 1, 2)
        layout.addWidget(etc_group)

        # Actions group
        actions_box = QGroupBox("Actions")
        actions_layout = QHBoxLayout(actions_box)
        self.btn_count_cycles = QPushButton("Count Cycles Only")
        actions_layout.addWidget(self.btn_count_cycles)
        actions_layout.addStretch(1)
        layout.addWidget(actions_box)

        # Log
        self.log = QTextEdit(); self.log.setReadOnly(True)
        layout.addWidget(self.log, stretch=1)

        # Wire signals
        self.btn_tp_browse_folder.clicked.connect(self._pick_tp_folder)
        self.btn_tp_browse_files.clicked.connect(self._pick_tp_files)
        self.btn_tp_process.clicked.connect(self._start_process_tp)

        self.btn_etc_browse_folder.clicked.connect(self._pick_etc_folder)
        self.btn_etc_write.clicked.connect(self._start_write_etc)

        self.btn_load_meta.clicked.connect(self._load_meta)
        self.btn_save_meta.clicked.connect(self._save_meta)

        self.btn_count_cycles.clicked.connect(self._start_count_cycles)

    # ---- Helpers
    def _append_log(self, text: str):
        self.log.append(text)
        self.log.ensureCursorVisible()

    def _with_worker(self, func: Callable, *args, **kwargs):
        worker = Worker(func, *args, **kwargs)
        worker.signals.progress.connect(self._append_log)
        worker.signals.finished.connect(self._on_worker_finished)
        self.thread_pool.start(worker)

    @Slot(bool, str)
    def _on_worker_finished(self, ok: bool, msg: str):
        if not ok:
            QMessageBox.critical(self, "Error", msg)

    # ---- Option handlers
    def _on_compress_changed(self):
        self.backend.app_cfg.compress_data = self.cb_compress.isChecked()

    # ---- Meta
    def _load_meta(self):
        sid = self.le_sample.text().strip()
        if not sid:
            QMessageBox.warning(self, "Missing Sample ID", "Please enter a sample ID first.")
            return

        def _load(progress: Callable[[str], None]):
            md = self.backend.load_metadata(sid, progress)
            # push to fields
            self.le_material.setText(md.sample_material or "")
            self.dsb_mass.setValue(md.sample_mass or 0.0)
            self.dsb_reservoir.setValue(md.reservoir_volume or 0.0)
            self.le_cell.setText(md.measurement_cell or "")
            if md.first_hydrogenation:
                dt = md.first_hydrogenation.to_pydatetime() if hasattr(md.first_hydrogenation, "to_pydatetime") else md.first_hydrogenation
                self.dte_first_hyd.setDateTime(dt)
            self.dsb_pmax.setValue(md.max_pressure_cycling or 0.0)
            self.dsb_tmin.setValue(md.min_temperature_cycling or 0.0)
            progress("Meta fields populated.")

        self._with_worker(_load)

    def _save_meta(self):
        sid = self.le_sample.text().strip()
        if not sid:
            QMessageBox.warning(self, "Missing Sample ID", "Please enter a sample ID first.")
            return

        init_state = self.cb_init_state.currentText()
        self.backend.app_cfg.init_state = init_state

        def _save(progress: Callable[[str], None]):
            md = self.backend.load_metadata(sid, progress)
            updates = {
                "sample_material": self.le_material.text().strip() or None,
                "sample_mass": float(self.dsb_mass.value()) or None,
                "reservoir_volume": float(self.dsb_reservoir.value()) or None,
                "measurement_cell": self.le_cell.text().strip() or None,
                "first_hydrogenation": self.dte_first_hyd.dateTime().toPython() if self.dte_first_hyd.dateTime().isValid() else None,
                "max_pressure_cycling": float(self.dsb_pmax.value()) or None,
                "min_temperature_cycling": float(self.dsb_tmin.value()) or None,
            }
            self.backend.update_metadata(md, updates, progress)

        self._with_worker(_save)

    # ---- Pickers
    def _pick_tp_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select T/P CSV Folder")
        if folder:
            self.le_tp_folder.setText(folder)
            self.backend.set_tp_folder(folder)

    def _pick_tp_files(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select T/P CSV File(s)", filter="CSV Files (*.csv);;All Files (*)")
        if files:
            self.le_tp_folder.setText("; ".join(files))
            self.backend.set_tp_files(files)

    def _pick_etc_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Select ETC Folder")
        if folder:
            self.le_etc_folder.setText(folder)
            self.backend.set_etc_folder(folder)

    # ---- Actions
    def _start_process_tp(self):
        sid = self.le_sample.text().strip()
        if not sid:
            QMessageBox.warning(self, "Missing Sample ID", "Please set metadata and sample ID first.")
            return
        self.backend.app_cfg.init_state = self.cb_init_state.currentText()
        self._with_worker(self.backend.process_tp, sid)

    def _start_write_etc(self):
        sid = self.le_sample.text().strip()
        if not sid:
            QMessageBox.warning(self, "Missing Sample ID", "Please set metadata and sample ID first.")
            return
        self._with_worker(self.backend.write_etc, sid)

    def _start_count_cycles(self):
        sid = self.le_sample.text().strip()
        if not sid:
            QMessageBox.warning(self, "Missing Sample ID", "Please enter a sample ID first.")
            return
        self.backend.app_cfg.init_state = self.cb_init_state.currentText()
        self._with_worker(self.backend.count_cycles_only, sid)


# ---------------------- Entrypoint ----------------------
def main():
    import sys
    app = QApplication(sys.argv)
    w = TPETCImporterWindow()
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
