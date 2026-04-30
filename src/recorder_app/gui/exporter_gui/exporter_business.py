import sys

from recorder_app.gui.recording_gui.recording_business_v2 import StaticPlotWindow

try:
    import recorder_app.infrastructure.core.logger as logging
except ImportError:
    import logging


class ExporterBackend:

    def __init__(self, config, meta_data=None):
        self.config = config
        self.meta_data = meta_data
        self.init_static_plots() if self.meta_data else None






    def init_static_plots(self):
        self.top_plot = StaticPlotWindow(y_axis='temperature',
                                         meta_data=self.meta_data,
                                         db_conn_params=self.config.db_conn_params)

        self.bottom_plot = StaticPlotWindow(y_axis="pressure",
                                            meta_data=self.meta_data,
                                            db_conn_params=self.config.db_conn_params,
                                            passive_window=True)



def main():
    try:
        from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout
        from recorder_app.infrastructure.core.config_reader import config
        from recorder_app.infrastructure.handler.metadata_handler import MetaData

        app = QApplication(sys.argv)
        widget = QWidget()
        #layout = QVBoxLayout(widget)
        widget.resize(800,600)
        # load metadata
        #meta_data = MetaData(sample_id='WAE-WA-028',
        #                     db_conn_params=config.db_conn_params)

        # this is the one that was failing
        #backend = ExporterBackend(config=config, meta_data=meta_data)

        # now add your plots
        #layout.addWidget(backend.top_plot)
        #layout.addWidget(backend.bottom_plot)  # if you actually want the second one
        widget.show()

        sys.exit(app.exec())
    except Exception as e:
        logging.getLogger(__name__).exception("Error in main block of recording_busines_v2.py:")

