import sys

from PySide6.QtWidgets import QApplication, QMainWindow, QWidget

from src.infrastructure.utils.standard_paths import exporter_ui_file_path


class ExporterWindow(QMainWindow):
    def __init__(self, ui_file_path, parent=None):
        super().__init__(parent=parent)
        self.ui = self._load_ui(ui_file_path)
        self.setCentralWidget(self.ui)
        self.setMinimumSize(800, 600)

    def _load_ui(self, ui_path):
        """
        Load the UI file using QUiLoader.
        """
        try:
            from PySide6.QtUiTools import QUiLoader
            from PySide6.QtCore import QFile
            loader = QUiLoader()
            ui_file = QFile(ui_path)
            if not ui_file.exists():
                self.logger.error(f"UI file {ui_path} does not exist.")
                return None
            if not ui_file.open(QFile.ReadOnly):
                self.logger.error(f"Unable to open UI file: {ui_path}")
                return None
            ui = loader.load(ui_file)
            ui_file.close()
            if ui is None:
                self.logger.error("Failed to load UI file.")
            return ui
        except Exception as e:
            self.logger.exception("Exception occurred while loading UI:")
            return None



def main():
    app = QApplication(sys.argv)
    win = ExporterWindow(exporter_ui_file_path)
    win.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
