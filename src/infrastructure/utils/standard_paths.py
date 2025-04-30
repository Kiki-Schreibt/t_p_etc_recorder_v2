import os
import sys
from pathlib import Path

def get_base_dir():
    # 1) PyInstaller bundle extraction dir, if set
    if getattr(sys, "_MEIPASS", None):
        return Path(sys._MEIPASS)
    # 2) otherwise, climb up from this file into project root
    #    utils → infrastructure → src → project
    return Path(__file__).resolve().parents[3]


def get_config_path(filename):
    base_dir = get_base_dir()
    config_dir = os.path.join(base_dir, "config")
    return os.path.join(config_dir, filename)


def get_ui_file_path(folder_name, ui_file_name):
    base_dir = get_base_dir()
    config_dir = os.path.join(base_dir, "src", "GUI", folder_name, "ui")
    return os.path.join(config_dir, ui_file_name)


def get_user_home_path(subpath):
    user_home = os.path.expanduser("~")
    return os.path.join(user_home, subpath)

#todo: try to import paths from config file and use standard paths if not possible. then delete from config class

planner_ui_file_path = get_ui_file_path('planner_gui', 'planner_ui.ui')
recording_ui_file_path = get_ui_file_path('recording_gui','recording_ui_design.ui')
main_ui_file_path = get_ui_file_path("recording_gui", "recording_ui_design.ui")

# Paths relative to the script location
standard_periodic_table_txt = get_config_path('Periodic_Table_of_Elements.txt')
standard_config_file_path = get_config_path('config_logging_modbus_database.json')
standard_hydride_data_base_path = get_config_path('hydride_data_base.json')
standard_periodic_table_path = get_config_path('periodic_table_of_elements.json')

standard_t_p_test_data_folder_path = os.path.join(get_base_dir(),  "test_data", "full_test_028")
standard_log_dir = os.path.join(get_base_dir(), '..', 'Log')

standard_export_path = get_user_home_path(os.path.join("T_p_ETC_recorder", "Exports"))

standard_hot_disk_file_path = r"C:\HotDiskTPS_7\data\Log"

# Ensure the export path exists
os.makedirs(standard_export_path, exist_ok=True)


if __name__ == "__main__":
    print(get_base_dir())
    print(main_ui_file_path)
    print("Config File Path:", standard_config_file_path)
    print("Hydride Database Path:", standard_hydride_data_base_path)
    print("Periodic Table Path:", standard_periodic_table_path)
