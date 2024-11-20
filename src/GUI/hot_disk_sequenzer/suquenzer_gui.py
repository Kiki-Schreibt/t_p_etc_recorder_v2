from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QLineEdit, QLabel, QTableWidget, QTableWidgetItem, QFileDialog

class ScheduleGenerator(QWidget):
    def __init__(self):
        super().__init__()

        self.init_ui()

    def init_ui(self):
        # Create UI elements
        self.layout = QVBoxLayout()

        # Input fields for measurement parameters
        self.temp_label = QLabel('Temperature:')
        self.temp_input = QLineEdit()
        self.layout.addWidget(self.temp_label)
        self.layout.addWidget(self.temp_input)

        # Add more input fields as needed

        # Buttons
        self.add_measurement_button = QPushButton('Add Measurement')
        self.add_measurement_button.clicked.connect(self.add_measurement)
        self.layout.addWidget(self.add_measurement_button)

        self.generate_schedule_button = QPushButton('Generate Schedule')
        self.generate_schedule_button.clicked.connect(self.generate_schedule)
        self.layout.addWidget(self.generate_schedule_button)

        # Measurement table
        self.measurement_table = QTableWidget()
        self.measurement_table.setColumnCount(3)  # Adjust column count
        self.measurement_table.setHorizontalHeaderLabels(['Time', 'Temperature', 'Settings'])
        self.layout.addWidget(self.measurement_table)

        self.setLayout(self.layout)

    def add_measurement(self):
        # Collect data from input fields
        temp = self.temp_input.text()
        # ... collect other parameters

        # Add to measurement table
        row_position = self.measurement_table.rowCount()
        self.measurement_table.insertRow(row_position)
        self.measurement_table.setItem(row_position, 0, QTableWidgetItem('Time Placeholder'))
        self.measurement_table.setItem(row_position, 1, QTableWidgetItem(temp))
        self.measurement_table.setItem(row_position, 2, QTableWidgetItem('Settings Placeholder'))

        # Store data for schedule generation
        # ...

    def generate_schedule(self):
        # Collect all measurements from the table
        # ...

        # Open file dialog to save the .hseq file
        options = QFileDialog.Options()
        file_name, _ = QFileDialog.getSaveFileName(self, "Save Schedule", "", "HotDisk Schedule Files (*.hseq)", options=options)
        if file_name:
            # Call create_schedule function with collected data
            # create_schedule(measurement_times, measurement_params, file_name)
            pass

if __name__ == '__main__':
    app = QApplication([])
    window = ScheduleGenerator()
    window.show()
    app.exec()
