import sys
import os
import concurrent.futures

from PySide6.QtWidgets import (QApplication, QWidget, QVBoxLayout,
                               QPushButton, QLineEdit, QFileDialog,
                               QLabel, QMessageBox)
from PySide6.QtCore import Signal

# Import your HotDiskController and other necessary classes/modules
from recorder_app.infrastructure.handler.hot_disk_handler import HotDiskController  # Assuming this is the correct import path


class StartETCMeasurementGUI(QWidget):
    file_exported_sig = Signal(str)
    new_etc_data_written = Signal(tuple)

    def __init__(self, config, standard_etc_folder_path,
                 response_delay=None, meta_data=None):
        super().__init__()
        self.setWindowTitle("Start ETC Measurement")
        self.setGeometry(300, 300, 400, 250)

        # Set up the GUI elements
        self.config = config
        self.response_delay = response_delay  # You can change this if needed
        self.standard_etc_folder_path = standard_etc_folder_path  # Default folder path
        self.meta_data = meta_data
        self.init_ui()
        self.file_exported_sig.connect(self._on_data_exported)

    def init_ui(self):
        # Layout for the window
        layout = QVBoxLayout()

        # Label for file name input
        self.file_name_label = QLabel("Enter File Name:")
        layout.addWidget(self.file_name_label)

        # Input for file name
        self.file_name_input = QLineEdit(self)
        if self.meta_data:
            self.file_name_input.setText(self.meta_data.sample_id)
        layout.addWidget(self.file_name_input)

        # Label for folder selection
        self.folder_label = QLabel("Select Export Folder:")
        layout.addWidget(self.folder_label)

        # Button for folder selection
        self.folder_button = QPushButton("Select Folder", self)
        self.folder_button.clicked.connect(self.select_folder)
        layout.addWidget(self.folder_button)

        # Label to display the selected folder
        self.selected_folder_label = QLabel(f"Selected Folder: {self.standard_etc_folder_path}")
        layout.addWidget(self.selected_folder_label)

        # Start Button to trigger the HotDiskController run
        self.start_button = QPushButton("Start Measurement", self)
        self.start_button.clicked.connect(self.start_measurement)
        layout.addWidget(self.start_button)

        # Export Button to trigger the export functionality
        self.export_button = QPushButton("Export Results", self)
        self.export_button.clicked.connect(self.export_results)
        layout.addWidget(self.export_button)

        # Set the layout for the window
        self.setLayout(layout)

    def select_folder(self):
        """Open file dialog to select export folder."""
        folder = QFileDialog.getExistingDirectory(self, "Select Folder", self.standard_etc_folder_path)
        if folder:
            self.selected_folder_label.setText(f"Selected Folder: {folder}")
            self.export_folder = folder

    def start_measurement(self):
        """Start the measurement process using the HotDiskController."""
        file_name = self.file_name_input.text().strip()
        if not file_name or not hasattr(self, 'export_folder'):
            self.show_error_message("Please enter a file name and select a folder.")
            return

        # Initialize the HotDiskController
        hd_controller = HotDiskController(hd_conn_params=self.config.hd_conn_params, response_delay=self.response_delay)

        # Run the HotDiskController in a separate thread
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(hd_controller.run)
            result = future.result()

        self.export_results()

    def export_results(self):
        """Export the results if measurements were performed."""
        file_name = self.file_name_input.text().strip()
        if not file_name or not hasattr(self, 'export_folder'):
            self.show_error_message("Please enter a file name and select a folder.")
            return


        # Initialize the HotDiskController
        hd_controller = HotDiskController(hd_conn_params=self.config.hd_conn_params, response_delay=self.response_delay)
        full_path = os.path.join(self.export_folder, file_name+'xlsx')
        # Export results
        try:
            hd_controller.export_results(self.export_folder, file_name)
            self.show_message(f"Results successfully exported to {full_path}")
        except Exception as e:
            self.show_error_message(f"Error during export: {e}")

    def _on_data_exported(self, file_path):
        pass
        #from src.infrastructure.handler.excel_data_handler import ExcelDataProcessor
        #processor = ExcelDataProcessor(file_path=file_path,
        #                               meta_data=self.meta_data,
        #                               db_conn_params=self.config.db_conn_params)
        #time_range = processor.execute()
        #self.new_etc_data_written.emit(time_range)

    def show_message(self, message):
        """Show a message to the user."""
        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Information)
        dialog.setText(message)
        dialog.setWindowTitle("Success")
        dialog.exec()

    def show_error_message(self, message):
        """Show an error message to the user."""
        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Critical)
        dialog.setText(message)
        dialog.setWindowTitle("Error")
        dialog.exec()


def main():
    # Assuming you have a way to get the `hd_conn_params`
    from recorder_app.infrastructure.core.config_reader import config
    from recorder_app.infrastructure.core.global_vars import standard_etc_folder_path
    from recorder_app.infrastructure.handler.metadata_handler import MetaData
    meta_data = MetaData(db_conn_params=config.db_conn_params, sample_id="WAE-WA-060")

    # Standard folder path for the export


    app = QApplication(sys.argv)
    window = StartETCMeasurementGUI(config=config,
                                    standard_etc_folder_path=standard_etc_folder_path,
                                    meta_data=meta_data)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
