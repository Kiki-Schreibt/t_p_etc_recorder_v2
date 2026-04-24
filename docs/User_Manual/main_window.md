site_name: Main Window Usage

## The Main Window
This window opens when you run:
``` bash 
python Scripts/main.py
```

It serves as the main user interface from which all functionalities of the software can be accessed. Tests can be selected by entering their sample id in the *Sample ID* edit field.  
Once done the sample's meta data will be loaded from the database and 
filled into the text edit fields. 
    - 
![Das ist ein Bild](../../assets/pics/main_window/main_overview.PNG)

The user now has the following options:

1. **Start Tp Recording / ETC Data Recording**
    - When clicking on *Start Tp Recording* connection to the configured modbus device will be established using the host and port defined in the 
      [configuration](getting_started/config_creation.md). Data will be read from the device, the equilibrium pressure at the current temperature and the setpoint temperature calculated and all data written into the temperature and pressure data table in the database. 
      The plot windows on the left side will display this data live
    - When clicking on *Start Log File Tracker* the log file of the **Thermal Constants Analyzer** 
      defined in [configuration](getting_started/config_creation.md) will be tracked for changes. 
      Whenever a .xlsx file is exported from the **Thermal Constants Analyzer** the software will notice it and import all content from the .xlsx file into the 
      [Thermal Conductivity Tables](database/database_tables.md) in the database. Once done the thermal conductivity values will be included in the live plots on the left side.
    - 
