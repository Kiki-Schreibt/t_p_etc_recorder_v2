import sys
import datetime

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget,
    QVBoxLayout, QPushButton, QTextEdit, QProgressBar
)
from PySide6.QtCore import QThread, Signal, QTimer

from src.infrastructure.connections.connections import DatabaseConnection


class MaintenanceThread(QThread):
    log = Signal(str)
    finished = Signal()

    def __init__(self, db_conn_params):
        super().__init__()
        self.db_params = db_conn_params

    def run(self):
        commands = [
            ("VACUUM VERBOSE",       "VACUUM VERBOSE;"),
            ("ANALYZE",           "ANALYZE;"),
            ("REINDEX DATABASE",  f"REINDEX DATABASE {self.db_params['DB_DATABASE']};"),
        ]

        # open a connection in autocommit mode
        db = DatabaseConnection(**self.db_params)
        try:
            db.open_connection(auto_close=False)
            db.conn.autocommit = True

            for name, sql in commands:
                ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.log.emit(f"[{ts}] Starting {name}…")
                db.cursor.execute(sql)
                ts2 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.log.emit(f"[{ts2}] {name} completed.")
            self.log.emit("✅ Maintenance finished successfully.")
        except Exception as e:
            self.log.emit(f"❌ Error during maintenance: {e}")
        finally:
            try:
                db.close_connection()
            except:
                pass
            self.finished.emit()


class MaintenanceWindow(QMainWindow):
    def __init__(self, db_conn_params):
        super().__init__()
        self.setWindowTitle("Database Maintenance")
        self.db_params = db_conn_params

        # Log viewer
        self.log_view = QTextEdit()
        self.log_view.setReadOnly(True)

        # Run button
        self.run_btn = QPushButton("Run VACUUM/ANALYZE/REINDEX")
        self.run_btn.clicked.connect(self.start_maintenance)

        # Progress bar (indeterminate)
        self.progress = QProgressBar()
        self.progress.setVisible(False)

        # Layout
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.addWidget(self.log_view)
        layout.addWidget(self.progress)
        layout.addWidget(self.run_btn)
        self.setCentralWidget(container)

        self.thread = None

    def start_maintenance(self):
        # disable the button, clear logs
        self.run_btn.setEnabled(False)
        self.log_view.clear()

        # show busy indicator
        self.progress.setVisible(True)
        self.progress.setRange(0, 0)   # makes it indeterminate

        # start background thread
        self.thread = MaintenanceThread(self.db_params)
        self.thread.log.connect(self.append_log)
        self.thread.finished.connect(self.on_finished)
        self.thread.start()

    def append_log(self, text):
        self.log_view.append(text)

    def on_finished(self):
        # switch progress bar to 100%
        self.progress.setRange(0, 100)
        self.progress.setValue(100)

        # re-enable button
        self.run_btn.setEnabled(True)

        # after a short pause, hide the bar again
        QTimer.singleShot(2000, lambda: self.progress.setVisible(False))

    def closeEvent(self, event):
        super().closeEvent(event)


if __name__ == "__main__":
    from src.infrastructure.core.config_reader import GetConfig

    app = QApplication(sys.argv)
    cfg = GetConfig()

    win = MaintenanceWindow(cfg.db_conn_params)
    win.resize(600, 400)
    win.show()
    sys.exit(app.exec())
