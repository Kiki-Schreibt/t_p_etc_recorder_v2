 #T/P ETC Recorder v2

## Overview
T/P ETC Recorder v2 is a desktop application for planning, executing, and analyzing thermal-program (T/P) and effective thermal conductivity (ETC) experiments. The project couples a PySide6-based GUI with infrastructure modules for database access, Modbus-connected hardware control, schedule planning, and data post-processing. It is designed to streamline experimental setup, automate data collection, and provide tooling for exporting and correcting measurements.

The repository bundles several companion utilities:

- **Recording GUI** for live acquisition, constraint management, and quick exports.
- **Test planner & schedule sequencer** to organize Hot Disk measurement sequences.
- **Simulation tooling** (DICON Modbus simulator) to test setups without live hardware.
- **Side operations** such as hydrogen uptake correction, ETC measurement launching, and plot individualization.

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
├── tests/                    # Pytest-based unit tests and utilities
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
   ```bash
   git clone <repo-url>
   cd t_p_etc_recorder_v2
   ```
2. **Create and activate a virtual environment**
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   # Linux/macOS
   source .venv/bin/activate
   ```
3. **Install dependencies**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
4. **Prepare configuration**
   - Run main.py (enter configuration requirements for postgres, modbus devices and log file folder of Constants Analyzer Software)
   - Update database credentials, Modbus host/port, logging options, and hardware-specific settings as needed.
   - Review Windows-specific paths (e.g., Hot Disk templates, export directories) and adapt them for your environment.
   - Click test then save if everything is fine. Upon save database tables will be created and partitioned after sample_id

## Running the Application
Launch the GUI from the `Scripts` entry point:
```bash
python Scripts/main.py
```
This opens the main recording window with access to planner, configuration editor, quick export, DICON simulator, schedule creator, uptake correction, ETC measurement starter, and plot individualizer modules. Several tools spawn in separate windows so you can operate planners and recorders simultaneously.

### Bundled Simulator
To validate workflows without hardware, open the **DICON Simulator** from the "Tools" menu in the main window. When started, it exposes a Modbus server and updates the live recording host/port settings automatically.

## Standard Paths
Many infrastructure utilities rely on conventional locations that are created or assumed at runtime:

- Exports are saved under `%USERPROFILE%\T_p_ETC_recorder\Exports` by default.
- Schedule templates default to `%USERPROFILE%\T_p_ETC_recorder\Schedules`.
- Hot Disk logs and TCR templates default to `C:\HotDiskTPS_7\...` (edit in `src/infrastructure/core/global_vars.py` and `src/infrastructure/utils/standard_paths.py` if your environment differs).
- Logs are written to `Log/Application_Log.log` relative to the project root unless configured otherwise.

Ensure these locations exist or adjust them via configuration files to avoid runtime errors.

## Testing
Unit tests use `pytest` and primarily cover scheduling utilities. To run the suite:
```bash
pytest
```
Create additional fixtures for hardware interactions by mocking Modbus connections and database layers to expand coverage.

## Packaging
The codebase includes helper functions for PyInstaller bundles (see `src/infrastructure/utils/standard_paths.py`). When packaging:
- Ensure configuration files and UI assets are collected in the bundle data directory.
- Verify environment variables or command-line overrides for machine-specific paths.

## Troubleshooting
- **Missing Qt platform plugins:** Ensure `PySide6` is installed in the active environment and that you are not mixing Anaconda with system Python.
- **Cannot connect to equipment:** Confirm Modbus TCP/IP parameters in `config/config_logging_modbus_database.json` or use the simulator for diagnostics.
- **Database errors:** Verify PostgreSQL credentials and network access. Use a connection string tester before launching the GUI.

## Contributing
1. Fork the repository and create a feature branch.
2. Ensure changes are covered with tests where practical.
3. Run `pytest` locally before submitting a pull request.
4. Submit a PR describing the change, testing performed, and any environment considerations.

## License
This project is licensed under the terms described in `LICENSE`.
