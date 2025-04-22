from PySide6.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTextEdit, QPushButton, QMessageBox, QApplication
)
from PySide6.QtGui import QFont
from PySide6.QtCore import Qt
from pyqtgraph import LinearRegionItem
from datetime import datetime


class UptakeCorrectionUi(QMainWindow):
    """
    A window to display a plot, information text, and buttons
    for calculating uptake and updating data.
    """
    def __init__(self, meta_data, top_plot, bottom_plot):
        super().__init__()
        self.setWindowTitle("Uptake Correction")
        self.meta_data = meta_data
        self.top_plot = top_plot
        self.bottom_plot = bottom_plot
        self.bottom_plot.setXLink(self.top_plot)
        self._setup_ui()
        t0_dt, t1_dt = self.top_plot.reader.time_range_to_read
        t0 = t0_dt.timestamp()
        t1 = t1_dt.timestamp()

        # 2) make the region selector with those floats
        self.region = LinearRegionItem(
            [t0, t1],
            orientation=LinearRegionItem.Vertical,
            brush=(0, 0, 255, 50)
        )
        # make sure it sits behind your data
        self.region.setZValue(-10)
        # add it to the top plot’s ViewBox
        self.top_plot.addItem(self.region)

        # 2) connect to the region‐changed signal
        self.region.sigRegionChanged.connect(self._on_region_changed)

    def _setup_ui(self):
        """
        Set up the main UI components: plot area, text edit, and buttons.
        """
        # Central widget and main layout
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)



        main_layout.addWidget(self.top_plot)
        main_layout.addWidget(self.bottom_plot)
        self.top_plot.reader.start()
        self.bottom_plot.reader.start()

        # Text edit for information display
        self.info_text_edit = QTextEdit()
        self.info_text_edit.setReadOnly(True)
        self.info_text_edit.setFont(QFont("Arial", 10))
        self.info_text_edit.setPlaceholderText("Information will appear here...")
        self.info_text_edit.setFixedHeight(150)
        main_layout.addWidget(self.info_text_edit)

        # Buttons layout
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(20)
        btn_layout.addStretch()

        self.calc_button = QPushButton("Calculate Uptake")
        self.calc_button.setFixedWidth(150)
        btn_layout.addWidget(self.calc_button)

        self.update_button = QPushButton("Update Data")
        self.update_button.setFixedWidth(150)
        btn_layout.addWidget(self.update_button)

        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)

    def _on_region_changed(self):
        # getRegion() returns (minX, maxX) in your plot’s x‐units
        t0, t1 = self.region.getRegion()
        # convert from your float‐timestamps to datetime objects
        dt0 = datetime.fromtimestamp(t0)
        dt1 = datetime.fromtimestamp(t1)
        # display to the user
        self.info_text_edit.append(
            f"Selected time range: {dt0:%Y-%m-%d %H:%M:%S} → {dt1:%Y-%m-%d %H:%M:%S}"
        )


class UptakeCorrectionWindow(UptakeCorrectionUi):
    def __init__(self, meta_data, top_plot, bottom_plot):
        super().__init__(meta_data, top_plot, bottom_plot)
        # Connect signals
        self.calc_button.clicked.connect(self._on_calculate_uptake)
        self.update_button.clicked.connect(self._on_update_data)

    def _on_calculate_uptake(self):
        """
        Handler for the 'Calculate Uptake' button.
        Replace with your calculation logic.
        """
        try:
            # Placeholder computation
            result = "Calculated uptake: 1.23 wt-%"
            self.info_text_edit.append(result)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Calculation failed: {e}")

    def _on_update_data(self):
        """
        Handler for the 'Update Data' button.
        Replace with your update logic.
        """
        try:
            # Placeholder update
            self.info_text_edit.append("Data updated successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Update failed: {e}")


class UptakeCorrectionBackend:
    def __init__(self):
        pass

def main():
    from src.GUI.recording_gui.recording_business_v2 import StaticPlotWindow
    from src.config_connection_reading_management.config_reader import GetConfig
    from src.meta_data.meta_data_handler import MetaData
    app = QApplication([])
    config = GetConfig()
    meta_data = MetaData(sample_id="WAE-WA-028", db_conn_params=config.db_conn_params)
    top_plot = StaticPlotWindow(meta_data=meta_data, db_conn_params=config.db_conn_params, y_axis="temperature", read_on_init=False)
    bottom_plot = StaticPlotWindow(meta_data=meta_data, db_conn_params=config.db_conn_params, y_axis="pressure", read_on_init=False)
    from datetime import datetime
    time_range = [datetime(2021, 9, 18), datetime(2021, 9, 21)]
    top_plot.reader.time_range_to_read = time_range
    win = UptakeCorrectionWindow(meta_data=meta_data, top_plot=top_plot, bottom_plot=bottom_plot)
    win.show()
    app.exec()

# Example usage:
if __name__ == '__main__':
    main()
