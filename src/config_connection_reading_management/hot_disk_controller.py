from src.config_connection_reading_management.connections import HotDiskConnection
from src.config_connection_reading_management.logger import AppLogger


class HotDiskController:

    def __init__(self):
        self.logger = AppLogger().get_logger(__name__)
        pass

    def start_schedule_file(self, full_file_path):
        try:
            with HotDiskConnection() as client:
                client.send_command(f"SCHED:INIT {full_file_path}")
        except Exception as e:
            self.logger.error(f"Could not start schedule file: {e}")

    def calculate_results(self, index_start, index_end):
        """

        :param index_start: transient values start
        :param index_end: transient values end
        :return:
        """

        with HotDiskConnection() as client:
            client.send_command(f"CALC:START {index_start}")
            client.send_command(f"CALC:END {index_end}")

    def receive_results(self):
        pass





with HotDiskConnection() as client:
    client.send_command("CALC:TCOND?")
    response = client.receive_response()
