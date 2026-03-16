diff --git a/README.md b/README.md
index d12e60990745e2b48005c2e5b3cdfe8893374651..42048965c2d936abb71ec3b32b7972ea505d2d50 100644
--- a/README.md
+++ b/README.md
@@ -1,37 +1,41 @@
  #T/P ETC Recorder v2
 
 ## Overview
 T/P ETC Recorder v2 is a desktop application for planning, executing, and analyzing thermal-program (T/P) and effective thermal conductivity (ETC) experiments. The project couples a PySide6-based GUI with infrastructure modules for database access, Modbus-connected hardware control, schedule planning, and data post-processing. It is designed to streamline experimental setup, automate data collection, and provide tooling for exporting and correcting measurements.
 
 The repository bundles several companion utilities:
 
 - **Recording GUI** for live acquisition, constraint management, and quick exports.
 - **Test planner & schedule sequencer** to organize Hot Disk measurement sequences.
 - **Simulation tooling** (DICON Modbus simulator) to test setups without live hardware.
 - **Side operations** such as hydrogen uptake correction, ETC measurement launching, and plot individualization.
 
+
+## User Manual
+A full end-user manual is available in [`USER_MANUAL.md`](USER_MANUAL.md).
+
 ## Project Structure
 ```
 .
 ├── Scripts/main.py           # Primary GUI entry point
 ├── src/
 │   ├── GUI/                  # PySide6 widgets and windows (recording, planning, simulation, etc.)
 │   ├── infrastructure/       # Hardware handlers, utilities, logging, and data abstractions
 │   ├── config/               # Default configuration templates
 │   └── ...
 ├── config/                   # Deployment-specific configuration files
 ├── requirements.txt          # Python dependencies
 └── README.md
 ```
 
 Additional assets such as schedule templates, log files, and exported data are created at runtime (see [Standard Paths](#standard-paths)).
 
 ## Prerequisites
 - **Python 3.10+** (PySide6 6.7 requires modern Python builds).
 - **Operating system:** Developed for Windows (default paths target `C:\...`), but most functionality runs cross-platform when compatible drivers are available.
 - **PostgreSQL database** reachable with credentials defined in the configuration file, when using the live recording workflow.
 - **Modbus-capable equipment** or the bundled simulator when testing without hardware.
 
 ## Setup
 1. **Clone the repository**


diff --git a/USER_MANUAL.md b/USER_MANUAL.md
new file mode 100644
index 0000000000000000000000000000000000000000..bbc097e97bdf642d2b82527c4c2179b17cf8a70d
--- /dev/null
+++ b/USER_MANUAL.md
@@ -0,0 +1,273 @@
+# T/P ETC Recorder v2 — User Manual
+
+## 1. Purpose
+T/P ETC Recorder v2 is a desktop application for running, monitoring, and post-processing thermal-program (Temperature/Pressure) and ETC-related experiments. 
It combines live data recording, live analysis, plotting, export tooling, planning/scheduling utilities, and helper tools for lab workflows.
+
+This manual is written for operators who need to use the application end-to-end.
+
+---
+
+## 2. What the application includes
+From the main application window, you can access:
+
+- **Live recording** (temperature/pressure and experiment metadata)
+- **Test Planner**
+- **Configuration Settings**
+- **Quick Export**
+- **DICON Simulator** (Modbus simulation)
+- **Schedule Creator** (To schedule measurements on the Thermal Constants Analyzer from HotDisk AB over the Automation Master)
+- **Uptake Correction** (To correct flags after wrong operation or falsely detected pressures)
+- **Database Maintenance**
+- **ETC Measurement Starter** (To perform thermal conductivity measurements without using the Thermal Constants Analyzer GUI)
+- **Plot Individualizer** (To plot individual columns from the database tables freely)
+- **Hydride Handler** (Allows adding/updating/removing metal hydrides to the database. This data is necessary to correctly detect the de-/hydrogenation state of the measured hydride)
+
+The main integration point is `Scripts/main.py`, which launches the recording window and exposes these tools through actions (Drop down menu or buttons) in the GUI.
+
+---
+
+## 3. System requirements
+
+### 3.1 Software
+- Python **3.10+**
+- Dependencies from `requirements.txt`
+- PostgreSQL 15.4 (for live DB-backed workflows)
+
+### 3.2 Hardware / interfaces
+- A Modbus TCP endpoint (real hardware) **or** the bundled simulator
+- Optional: Hot Disk-related software/log paths depending on your lab workflow
+
+### 3.3 Platform notes
+The project is developed with Windows-style defaults, but much of the stack is cross-platform if drivers and file paths are adapted.
+
+---
+
+## 4. Installation and setup
Install and set up Postgres before using the application. Necessary database tables will be created on first start up of the app. PgAdmin might make your live easier in case something behaves unexpected

+1. Clone repository
+   ```bash
+   git clone <repo-url>
+   cd t_p_etc_recorder_v2
+   ```
+2. Create/activate venv
+   ```bash
+   python -m venv .venv
+   # Windows
+   .venv\Scripts\activate
+   # Linux/macOS
+   source .venv/bin/activate
+   ```
+3. Install dependencies
+   ```bash
+   pip install --upgrade pip
+   pip install -r requirements.txt
+   ```
+4. Start app
+   ```bash
+   python Scripts/main.py
+   ```
+
+On first launch (if no config file exists), the **Configuration Input** window opens first.
+
+---
+
+## 5. First launch workflow (Configuration Input)
+
+The configuration window is used to define connection and runtime settings before recording.
+
+### 5.1 Sections and fields
+- **Database Settings (PostgreSQL)**
+  - `DB_SERVER`, `DB_DATABASE`, `DB_USERNAME`, `DB_PASSWORD`, `DB_PORT`
+- **Modbus Settings (Tested with Jumo Dicon touch)**
+  - `MODBUS_HOST`, `MODBUS_PORT`, `REGS_OF_INTEREST`, `START_REG`, `END_REG`, `SLEEP_INTERVAL`
- **START_REG and END_REG define which registers to read. REGS_OF_INTEREST define the registers where the sample temperature, heater temperature, pressure and the setpoint temperatures can be found
+- **Log Settings**
+  - `LOG_DIRECTORY`, `LOG_FILE`
+- **File Paths**
+  - `HOT_DISK_LOG_FILE_PATH`
- ** Is used to monitor the Thermal Constants Analyzer for new exported thermal conductivity data. When new exports are found they will be imported in the database in the background and then visualized in the live plot
+- **Other Settings**
+  - `MINIMUM_TEMPERATURE_INCREASE`
+  - `MAXIMUM_TEMPERATURE_INCREASE`
+  - `MINIMUM_TOTAL_TO_CHARACTERISTIC_TIME`
+  - `MAXIMUM_TOTAL_TO_CHARACTERISTIC_TIME`
- ** Defined by the Hot Disk user manual
+
+### 5.2 Recommended first-start process
+1. Enter DB and Modbus values.
+2. Click **Test Connection**.
+3. Resolve any errors (DB/network, Modbus endpoint, invalid ports).
+4. Click **Save Configuration**.
+
+When saved successfully, the app writes configuration JSON and initializes database tables.
+
+---
+
+## 6. Main recording workflow
+
+After configuration is available, the main window opens.
+
+### 6.1 Metadata panel
+The UI allows entering/updating core sample metadata such as:
+- Sample ID
+- Sample mass
+- Material composition
+- Measurement cell and volume
+- First hydrogenation time (The date and time when your sample is first activated. The program will assume all data before this point are in dehydrogenated state)
+- Test start/end times (Will be updated automatically during the recording)
+- Cycling pressure/temperature limits (Must be chosen by experience. Pressures higher then the input will not be considered for hydrogen uptake calculation. At temperatures lower the input cycles will not be counted as kinetic hindrance is assumed. )
+- Cycle duration (Approximately the duration your material will take to de-/hydrogenate. Transitions between de-/hydrogenation state that occur faster then this time will not be counted as a cycle and ignored)
+
+Use the metadata update controls to persist values for the active sample.
+
+### 6.2 Plot areas
+The recording UI supports multiple visualizations:
+- **Top/Bottom plots on the left side** for temperature pressure and thermal conductivity over time
+- **Right-side XY plot** for selectable cross-plots
+- **Uptake-related plotting** for analysis windows
+
+### 6.3 Recording operation
+Typical sequence:
+1. Confirm metadata and constraints. (Update metadata button to store the data in the database)
+2. Confirm Modbus connection target.
+3. Start T/P recording. (After all metadata is stored. This starts the recording of your test)
+4. Monitor plots and live values. (Can be explored using the mouse wheel. When zooming data will be loaded from the database. So you can explore all data during the recording)
+5. Stop recording when complete.
+6. Export/analyze using side tools. (Currently only quick export (drop down menu) is working. Exports all necessary data cycle dependent, isothermal, as well as all data combined.)
+
+---
+
+## 7. Tools menu modules (how and when to use)
+
+### 7.1 Test Planner
+Use this to define or review measurement/test planning logic before acquisition runs. 
+
+### 7.2 Configuration Settings
+Re-open the configuration editor at any time to adjust DB/Modbus/logging/path settings.
+
+### 7.3 Quick Export
+Runs a background export using current constraints and metadata.
+
+### 7.4 DICON Simulator
+Starts a local Modbus simulation server for development/testing:
+- Useful when no physical hardware is connected.
+- While active, recorder Modbus host/port is temporarily switched.
+- On close/stop, previous host/port is restored.
+
+### 7.5 Schedule Creator
+Opens the Hot Disk sequence/schedule builder for planning conductivity measurements based on a temperature program.
+
+### 7.6 Uptake Correction
+Loads current sample and selected plot time range into an correction workflow.
+
+### 7.7 Database Maintenance
+Opens DB maintenance utilities. The recording controller can be paused/stopped while maintenance starts.
+
+### 7.8 ETC Measurement Starter
+Launches tooling to initiate conductivity measurements from the GUI.
+
+### 7.9 Plot Individualizer
+Open advanced plotting/individualization utilities for analysis.
+
+### 7.10 Hydride Handler
+Dedicated tool window for hydride-specific data handling tasks.
+
+---
+
+## 8. Files and paths you should know
+
+- Main launcher: `Scripts/main.py`
+- Core code: `src/`
+- Config templates and runtime configs: `config/` and `src/config/`
+- Logs: default `Log/`
+
+Common defaults (may need adaptation for your machine):
+- User exports/schedules under user profile folders
+- Hot Disk folders under `C:\HotDiskTPS_7\...`
+
+If your environment differs, update configuration and path defaults accordingly. All necessary updates can be made in `global_vars.py`
+
+---
+
+## 9. Typical operating procedures
+
+### 9.1 New experiment run
+1. Open app.
+2. Verify configuration and connectivity.
+3. Enter sample metadata.
+4. Start recording.
+5. Monitor plots.
+6. Stop recording.
+7. Export and archive outputs.
+
+### 9.2 Offline/UI testing without instruments
+1. Open app.
+2. Start **DICON Simulator**.
+3. Run recording flow against simulator.
+4. Validate logging, DB writes, plotting, and export.
+
+### 9.3 Post-processing focus session
+1. Load/select sample metadata.
+2. Use **Uptake Correction**, **Plot Individualizer**, and export tools.
+3. Save outputs for reporting.
+
+---
+
+## 10. Troubleshooting guide
+
+### 10.1 App does not start
+- Ensure your virtualenv is active.
+- Verify all dependencies installed from `requirements.txt`.
+- Check terminal output for missing package errors.
+
+### 10.2 Configuration test fails (DB)
+- Validate host/port/db/user/password.
+- Ensure PostgreSQL service is running and reachable.
+- Confirm network/firewall rules.
+
+### 10.3 Configuration test fails (Modbus)
+- Validate Modbus host/port.
+- Confirm hardware is online and supports Modbus TCP.
+- Try DICON Simulator to isolate network vs hardware issues.
+
+### 10.4 No live data appears
+- Verify recording started.
+- Check register settings (`REGS_OF_INTEREST`, range).
+- Confirm no stale simulator host override remains.
+
+### 10.5 Export issues
+- Check output directories and write permissions.
+- Verify sample metadata exists and constraints are valid.
+- Review logs in `Log/`.
+
+---
+
+## 11. Testing and validation for users
+
+
+
+---
+
+## 12. Safety and data integrity recommendations
+
+- Keep a separate configuration per lab environment (dev/test/prod).
+- Avoid running maintenance operations during critical live acquisition unless planned.
+- Back up PostgreSQL data regularly.
+- Version-control key config templates (excluding secrets).
+- Keep operator notes for each sample ID to preserve traceability.
+
+---
+
+## 13. Quick reference cheat sheet
+
+- **Start app:** `python Scripts/main.py`
+- **First-run required action:** fill config, test, save
+- **No hardware available:** use DICON Simulator (Start the simulator before the recording and end it after the recording stop)
+- **Need fast output dump:** Quick Export
+- **Need sequence setup:** Schedule Creator
+- **Need correction/advanced plots:** Uptake Correction + Plot Individualizer
+
