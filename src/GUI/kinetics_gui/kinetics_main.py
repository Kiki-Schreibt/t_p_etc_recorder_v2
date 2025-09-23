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
        # Track what's currently shown (cycle -> Series)
        self._visible_series: Dict[float, Series] = {}
        self.selected_cycle = None
        # Connect view signals to controller slots
        self.view.loadRequested.connect(self.on_load_curves)
        self.view.runRequested.connect(self.on_run_kinetics)
        self.view.clearRequested.connect(self._on_clear_all)
        self.view.exportRequested.connect(self._on_export_to_origin)
        self.view.deleteRequested.connect(self._on_delete_requested)
        self.view.mpl_connect('pick_event', self._on_pick)
        self.view.correctionRequested.connect(self._on_correction_requested)

    # ---------- helpers ----------
    def _parse_cycles(self, text: str, *, sample_id: str | None = None) -> List[float]:
        """Parse cycles like "1-3,7,10" into floats with 0.5 increments.
           Example: "1-3" → [1.0, 1.5, 2.0, 2.5, 3.0].
           If empty, fetch all cycles for sample.
        """
        text = (text or "").strip()
        if not text:
            if not sample_id:
                return []
            try:
                return [float(c) for c in self.dal.list_cycles(sample_id)]
            except Exception as e:  # noqa: BLE001
                self.view.show_error(f"Failed to list cycles: {e}")
                return []

        out: List[float] = []
        for part in text.split(','):
            part = part.strip()
            if not part:
                continue
            if '-' in part:
                a, b = part.split('-', 1)
                try:
                    ai = float(a)
                    bi = float(b)
                except ValueError:
                    continue
                step = 0.5 if ai <= bi else -0.5
                # + step ensures the endpoint is included
                n_steps = int(round((bi - ai) / step)) + 1
                seq = [ai + i * step for i in range(n_steps)]
                out.extend(seq)
            else:
                try:
                    out.append(float(part))
                except ValueError:
                    continue

        # de-dup & sort
        return sorted(set(out))

    # ---------- slots (invoked by the view) ----------
    @Slot(str, str, str)
    def on_load_curves(self, sample_id: str, cycles_text: str, y_val_text) -> None:
        if not sample_id:
            self.view.show_error("Please enter a Sample ID.")
            return
        cycles = self._parse_cycles(cycles_text, sample_id=sample_id)
        if not cycles:
            self.view.show_error("No cycles to load (input empty and none found).")
            return
        self.view.set_auto_plot_mode(single_cycle=(len(cycles) == 1))
        try:
            series_map = self.dal.fetch_measurements(sample_id, cycles, y_val_text)
            if not series_map:
                self.view.show_error("No measurement data returned for the requested cycles.")
                return
            for cyc in cycles:
                if cyc in series_map:
                    s = series_map[cyc]
                    self.view.plot_measurement(cyc, s)
                    self._visible_series[float(cyc)] = s              # track it
            self.view.set_status(f"Loaded measurement curves for cycles: {cycles}")
        except Exception as e:
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
        try:
            pass
            #series_map = self.dal.fetch_measurements(sample_id, cycles)
            #for cyc in cycles:
            #    if cyc in series_map:
            #        s = series_map[cyc]
            #        self.view.plot_measurement(cyc, s)
            #        self._visible_series[float(cyc)] = s              # track it
        except Exception as e:
            self.view.show_error(f"DB error while preparing curves: {e}")
            return

        self.view.set_running(True)
        self.view.set_progress(0)
        self.view.set_status("Calculating kinetics…")

        self._worker = KineticsWorker(cycles=cycles,
                                      sample_id=sample_id,
                                      config=self.dal.config)
        rr, rh, ss, em, rd = self.view.get_kinetics_options()
        self._worker.resample_rule = rr
        self._worker.resample_how = rh
        self._worker.smooth_seconds = ss
        self._worker.enforce_monotonic = em
        self._worker.reaction_duration = rd

        self._worker.progress.connect(self.view.set_progress)
        self._worker.error.connect(self._on_worker_error)
        self._worker.result.connect(self._on_worker_result)
        self._worker.finished.connect(self._on_worker_finished)
        self._worker.start()

    def _on_delete_requested(self) -> None:
        sample_id = (self.view.sample_edit.text() or "").strip()
        if not sample_id:
            self.view.show_error("Please enter a Sample ID before deleting.")
            return

        # Prefer currently selected cycle(s); otherwise use what's typed in the Cycles field
        cycles = self.selected_cycle
        if not cycles:
            self.view.show_error("No cycles selected or entered to delete.")
            return

        # Confirm with user
        cycs_str = ", ".join(str(c) for c in sorted(set(cycles)))
        if QMessageBox.question(
            self.view,
            "Delete kinetics",
            f"Delete kinetics rows for sample '{sample_id}'\nCycles: {cycs_str} ?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        ) != QMessageBox.Yes:
            self.view.set_status("Deletion cancelled.")
            return

        try:
            # Delegate DB deletion to the DAL
            deleted = self.dal.delete_kinetics_from_db(sample_id=sample_id, cycles=cycles)

            # Remove visuals & local cache for those cycles
            for cyc in cycles:
                # measurement lines (public API)
                self.view.plot_mgr.remove_measurement(cyc)

                # kinetics lines (access manager storage)
                arts = self.view.plot_mgr.kinetics_lines.pop(cyc, [])
                for a in arts:
                    try:
                        a.remove()
                    except Exception:
                        pass

                # drop from our cache
                self._visible_series.pop(float(cyc), None)

            self.view.canvas.draw_idle()
            self.selected_cycle = None
            self.view.set_status(f"Deleted {deleted} row(s) from kinetics for cycles [{cycs_str}].")

        except Exception as e:
            self.view.show_error(f"Failed to delete kinetics: {e}")

    # ---------- worker callbacks ----------
    @Slot()
    def _on_clear_all(self) -> None:
        """Clear plot AND our local registry."""
        self._visible_series.clear()
        self.view.clear_plot()

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
        # Auto-select view based on number of output cycles
        self.view.set_auto_plot_mode(single_cycle=(len(out) == 1))
        for cyc, series in out.items():
            if replace:
                self.view.remove_measurement(cyc)
            self.view.plot_kinetics(cyc, series)
            # prefer showing kinetics if replace is on; either way, track what the user now sees
            self._visible_series[float(cyc)] = series

    # ---------- Origin export ----------
    @Slot()
    def _on_export_to_origin(self) -> None:
        if not self._visible_series:
            self.view.show_error("There are no visible curves to export.")
            return

        try:
            import originpro as op
        except Exception:
            self.view.show_error(
                "The 'originpro' package is not available. "
                "Install with `pip install originpro` and ensure Origin/OriginPro is installed."
            )
            return

        try:
            op.set_show()

            # One worksheet for all cycles; use triplets (X,Y,Z) per cycle
            wks = op.new_sheet('w')
            wks.name = "Kinetics_Export"

            # Use a 3D graph template / layer
            # If your Origin has a template name for 3D scatter/line, use it:
            # gp = op.new_graph(template='3D Scatter')
            gp = op.new_graph()     # fallback: create default graph
            gl = gp[0]
            # If needed, switch the layer to 3D via LabTalk (uncomment if your default layer is 2D)
            # op.lt_exec('layer -d 3;')  # set active layer to 3D

            made = 0
            for idx, cyc in enumerate(sorted(self._visible_series.keys())):
                s = self._visible_series[cyc]

                # allocate consecutive triplets: [X,Y,Z] per cycle
                base = 3 * idx
                xcol = base
                ycol = base + 1
                zcol = base + 2

                # X (time minutes)
                wks.from_list(
                    xcol, s.x.tolist(),
                    lname=f"Time_Cyc_{str(cyc).replace('.', '_')}",
                    units="min",
                    axis="X",
                )
                # Y (cycle index, constant vector)
                wks.from_list(
                    ycol, s.y.tolist(),
                    lname=f"Cycle_{str(cyc).replace('.', '_')}",
                    axis="Y",
                )
                # Z (value)  ← write into zcol (not ycol)
                wks.from_list(
                    zcol, s.z.tolist(),
                    lname=f"Value_Cyc_{str(cyc).replace('.', '_')}",
                    axis="Z",
                )

                # Add an XYZ plot for this triplet.
                # IMPORTANT: use a 3D plot type. The exact code depends on Origin/OriginPro:
                # - 3D Scatter is commonly used for XYZ sets.
                # Replace `PLOT_TYPE_3D_SCATTER` with your actual code/enum for 3D Scatter in originpro.
              #  p = gl.add_plot(wks, colx=xcol, coly=ycol, colz=zcol, type=op.PLOT_TYPE_3D_SCATTER)
              #  p.legend = f"Cycle {cyc}"
              #  made += 1

            gl.rescale()
            self.view.set_status(f"Exported {made} XYZ curve(s) to Origin.")
            # Don't close Origin here; let the user inspect the graph
            # op.exit()

        except Exception as e:
            self.view.show_error(f"Failed to export to Origin: {e}")
            # op.exit()

    def _on_pick(self, event):
        artist = event.artist
        kind, cycle = self.view.plot_mgr.resolve_artist(artist)
        if cycle is None:
            return
        self.view.plot_mgr.mark_selected(artist)
        self.view.set_status(f"Selected {kind} line — cycle {cycle}")
        self.selected_cycle = [cycle]
        #print(cycle)
        # If you want the value programmatically:
        # do something with `cycle` (store it, open a detail pane, etc.)

    def _on_correction_requested(self):
        if not self.selected_cycle:
            return
        from src.infrastructure.core.table_config import TableConfig

        dates = self.dal.fetch_measurements(sample_id=self.view.sample_edit.text(),
                                            cycles=self.selected_cycle,
                                            y_col_name=TableConfig().KineticsTable.time)

        time_range = {}
        for cyc in self.selected_cycle:
            start_reading = min(dates[cyc].z)
            end_reading = max(dates[cyc].z)
            time_range= [start_reading, end_reading]
        self._open_correction_gui(time_range)

    def _open_correction_gui(self, time_range):
        from src.GUI.side_operations.h2_uptake_correction_gui import UptakeCorrectionWindow
        from src.infrastructure.handler.metadata_handler import MetaData
        from src.infrastructure.core.config_reader import config
        meta_data = MetaData(sample_id=self.view.sample_edit.text(),
                             db_conn_params=config.db_conn_params)


        self.correction_win = UptakeCorrectionWindow(
            meta_data=meta_data,
            config=config,
            time_range_to_read=time_range.copy()
        )
        self.correction_win.top_plot.reader.time_range_to_read = time_range
        self.correction_win.top_plot.reader.reading_mode = "by_time"
        self.correction_win.top_plot.reader.start()

        self.correction_win.show()

    def _stop_all(self):
        pass

    def closeEvent(self, event):
        self._stop_all()
        self.view.closeEvent(event)

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
