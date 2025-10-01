#global_vars.py


from zoneinfo import ZoneInfo
local_tz = ZoneInfo("Europe/Berlin")
time_format_str = "yyyy-MM-dd HH:mm:ss"

STANDARD_CONSTRAINTS = {
    "min_TotalCharTime": 0.28,
    "max_TotalCharTime": 1.1,
    "min_TotalTempIncr": 1.5,
    "max_TotalTempIncr": 5.2
}

####for logger
log_file_name = 'Application_Log.log'


####for reader / should equal sleep interval in config file
sleep_interval = 0.5

###for calculators
R_H2: float = 4124.49         # [J/(kg·K)] Specific gas constant for hydrogen
R_universal: float = 8.31447    # [J/(mol·K)] Universal gas constant
V_pipes: float = 1e-7         # [m³] Pipe volume

###for readers
reading_mode_full_test = 'full_test'
reading_mode_by_time = 'by_time'

###for handler
realistic_max_temp = 1000
realistic_min_pressure = 1.0
supported_file_extensions = ['.csv', '.txt']
state_hyd = 'Hydrogenated'
state_dehyd = 'Dehydrogenated'
cycle_counter_mode_CSV_recorder = 'CSV_Recorder'
mode_modbus_recording = 'recording'
compression_factor = 60
data_point_reading_limit = 500
standard_etc_folder_path = r"C:\Daten\Kiki"
###pyside related
try:
    from src.GUI.qt_styles import gpt_light as style
    from PySide6.QtGui import QFont
    FONT = QFont("Arial", 8)
    from PySide6.QtCore import QTimeZone
    local_tz_qt = QTimeZone(b'Europe/Berlin')
except:
    pass
###for plots
colors = [
    "#FF0000",  # Red
    "#00FF00",  # Lime
    "#0000FF",  # Blue
    "#FFFF00",  # Yellow
    "#00FFFF",  # Cyan
    "#FF00FF",  # Magenta
    "#800000",  # Maroon
    "#808000",  # Olive
    "#008000",  # Green
    "#800080",  # Purple
    "#008080",  # Teal
    "#000080",  # Navy
    "#FFA500",  # Orange
    "#A52A2A",  # Brown
    "#20B2AA",  # Light Sea Green
    "#778899",  # Light Slate Gray
    "#D2691E",  # Chocolate
    "#DC143C",  # Crimson
    "#7FFF00",  # Chartreuse
    "#6495ED"   # Cornflower Blue
]

STANDARD_TCR_FOLDER_PATH = r"C:\HotDiskTPS_7\Data\Config\Tcr"





