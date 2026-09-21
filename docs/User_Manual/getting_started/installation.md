site_name: Installation

## Prerequisites

-   **Python 3.12:** (PySide6 6.7 requires modern Python builds)
-   **Operating system:** Developed for Windows (default paths target
    `C:\...`), but most functionality runs cross-platform when
    compatible drivers are available
-   **PostgreSQL 15 database** reachable with credentials defined in the
    configuration file, when using the live recording workflow
-   **Modbus-capable equipment** or the bundled simulator when testing
    without hardware
-   **TPS from HotDisk + Thermal Constants Analyzer 7.6** (bundled
    simulator with basic functionality might be delivered later)

------------------------------------------------------------------------

## Setup

### Database Setup / PostgreSQL 15

1.  **Download and install** PostgreSQL 15\
    https://www.postgresql.org/download/

2.  *(Optional)* **Download and install** PgAdmin\
    https://www.pgadmin.org/download/pgadmin-4-windows/

------------------------------------------------------------------------

### Temperature, Pressure, ETC Recorder

1.  **Clone the repository**

``` bash
git clone https://github.com/Kiki-Schreibt/t_p_etc_recorder_v2
cd t_p_etc_recorder_v2
```

2.  **Create and activate a virtual environment**

``` bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/macOS
source .venv/bin/activate
```

3.  **Install dependencies**

``` bash
pip install --upgrade pip
pip install -r requirements.txt
```

------------------------------------------------------------------------

## Running the Application

Launch the GUI from the `Scripts` entry point:

``` bash
python src/main.py
```

This opens the main recording window with access to planner,
configuration editor, quick export, DICON simulator, schedule creator,
uptake correction, ETC measurement starter, and plot individualizer
modules. Several tools spawn in separate windows so you can operate
planners and recorders simultaneously.

On first start, this will open the Configuration Creator. Here you enter
IDs and ports for database and device connections.

For a detailed explanation see:
**[Configuration Creation](config_creation.md)**
