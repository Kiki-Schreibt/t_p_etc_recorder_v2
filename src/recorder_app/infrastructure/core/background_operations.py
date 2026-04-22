#!/usr/bin/env python3
import sys
import datetime
from dateutil.relativedelta import relativedelta

from PySide6.QtWidgets import QApplication, QMainWindow, QLabel, QVBoxLayout, QWidget
from PySide6.QtCore    import QTimer

from table_config import TableConfig
from connections import DatabaseConnection


tp_table = TableConfig().TPDataTable

def create_monthly_partition(db_conn_params, months_ahead=1):

    today     = datetime.date.today().replace(day=1)
    target    = today + relativedelta(months=months_ahead)
    start_ts  = target.strftime("'%Y-%m-01 00:00:00'")
    end_ts    = (target + relativedelta(months=1)).strftime("'%Y-%m-01 00:00:00'")
    part_name = f"t_p_data_{target:%Y_%m}"
    sql = f"""
      CREATE TABLE IF NOT EXISTS {part_name}
        PARTITION OF {tp_table.table_name}
        FOR VALUES FROM ({start_ts}) TO ({end_ts});
    """
    try:
        with DatabaseConnection(**db_conn_params) as conn:
            conn.cursor.execute(sql)
            conn.commit()
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] ensured {part_name}")
    except Exception as e:
        print(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] ERROR {e}")

class MainWindow(QMainWindow):
    def __init__(self, db_conn_params):
        super().__init__()
        self.db_conn_params = db_conn_params

        # On startup, create partitions for this + next 2 months
        for i in range(3):
            create_monthly_partition(self.db_conn_params, months_ahead=i)

        # QTimer that fires once every 24h
        self._last_run_month = None
        timer = QTimer(self)
        timer.setInterval(24 * 3600 * 1000)      # 24 hours
        timer.timeout.connect(self._check_partition)
        timer.start()
        # also run immediately
        self._check_partition()

        # simple UI so app stays alive
        lbl = QLabel("Partition autopilot running.\nCheck console for logs.")
        w = QWidget()
        w.setLayout(QVBoxLayout())
        w.layout().addWidget(lbl)
        self.setCentralWidget(w)
        self.setWindowTitle("Partition Manager")

    def _check_partition(self):
        """Runs daily: if we haven’t yet created this month’s partitions, do it."""
        today = datetime.date.today()
        current_month = (today.year, today.month)
        if current_month == self._last_run_month:
            return
        # create “this month+1” partition
        create_monthly_partition(self.db_conn_params, months_ahead=1)
        self._last_run_month = current_month

if __name__ == "__main__":
    from config_reader import config

    app = QApplication(sys.argv)
    win = MainWindow(db_conn_params=config.db_conn_params)
    win.resize(400,200)
    win.show()
    sys.exit(app.exec())
