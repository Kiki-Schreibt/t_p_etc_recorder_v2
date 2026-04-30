# gui.py

import sys
from PySide6.QtWidgets import (
    QApplication, QWidget, QMainWindow, QLabel, QLineEdit, QPushButton,
    QGridLayout, QMessageBox
)
import pyqtgraph as pg

from recorder_app.gui.data_fitter.data_fitter_ui_business import DataLoader
from recorder_app.gui.data_fitter.data_fitter_ui_business import main as fitting_main


class DataFitterGuiWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Thermal Conductivity Fitting Tool")

        # Central widget
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)

        # Input fields
        self.sample_id_input = QLineEdit()
        self.cycle_number_input = QLineEdit()
        self.temperature_input = QLineEdit()

        # Plot widget
        self.plot_widget = pg.PlotWidget()

        self._create_widgets()

    def _create_widgets(self):
        layout = QGridLayout()

        # Sample ID
        layout.addWidget(QLabel("Sample ID:"), 0, 0)
        layout.addWidget(self.sample_id_input, 0, 1)

        # Cycle Number
        layout.addWidget(QLabel("Cycle Number:"), 1, 0)
        layout.addWidget(self.cycle_number_input, 1, 1)

        # Temperature
        layout.addWidget(QLabel("Temperature (°C):"), 2, 0)
        layout.addWidget(self.temperature_input, 2, 1)

        # Buttons
        self.load_button = QPushButton("Load Data")
        layout.addWidget(self.load_button, 3, 0)
        self.fit_button = QPushButton("Start Fitting")
        layout.addWidget(self.fit_button, 3, 2)

        # Add the plot widget
        layout.addWidget(self.plot_widget, 4, 0, 1, 3)

        self.central_widget.setLayout(layout)


class DataFitterMain(DataFitterGuiWindow):

    def __init__(self):
        super().__init__()
        self.data_loader = None
        self.isotherm = None
        self.mean_temperature = None
        self.de_hyd_state = None
        self._init_connections()

    def _init_connections(self):
        self.fit_button.clicked.connect(self.start_fitting)
        self.load_button.clicked.connect(self.load_data)

    def load_data(self):
        # Create DataLoader instance
        try:
            sample_id = self.sample_id_input.text()
            cycle_number = float(self.cycle_number_input.text())
            temperature = float(self.temperature_input.text())
            from recorder_app.infrastructure.core.config_reader import config
            self.data_loader = DataLoader(
                sample_id=sample_id,
                cycle_number=cycle_number,
                temperature=temperature,
                db_conn_params=config.db_conn_params
            )
            self.isotherm, self.mean_temperature, self.de_hyd_state = self.data_loader.get_isotherm()
            if self.isotherm is not None and not self.isotherm.empty:
                self.plot_data()
            else:
                QMessageBox.warning(self, "No Data", "No data found for the given parameters.")
        except ValueError as e:
            QMessageBox.warning(self, "Input Error", "Please enter valid numeric values for cycle number and temperature.")

    def preview_data(self):
        if self.isotherm is not None:
            # Show data in a message box or implement a table if needed
            QMessageBox.information(self, "Data Preview", str(self.isotherm.head()))
        else:
            QMessageBox.warning(self, "No Data", "Please load data first.")

    def plot_data(self):
        if self.isotherm is not None:
            self.plot_widget.clear()
            pressure = self.isotherm["pressure"]
            conductivity = self.isotherm["ThConductivity"]
            self.plot_widget.plot(pressure, conductivity, pen=None, symbol='o')
            self.plot_widget.setLogMode(x=True, y=False)
            self.plot_widget.setLabel('left', 'Thermal Conductivity', units='W/mK')
            self.plot_widget.setLabel('bottom', 'Pressure', units='bar')
            self.plot_widget.setTitle('Thermal Conductivity vs Pressure')
        else:
            QMessageBox.warning(self, "No Data", "Please load data first.")

    def start_fitting(self):
        if self.isotherm is not None:
            # Proceed with fitting
            # Optionally, you can keep the GUI open or close it
            # self.close()  # Close the GUI if desired
            fitting_main(self.data_loader)
        else:
            QMessageBox.warning(self, "No Data", "Please load data first.")


def main():
    app = QApplication(sys.argv)
    window = DataFitterMain()
    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
