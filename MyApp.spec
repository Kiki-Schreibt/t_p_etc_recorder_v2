# -*- mode: python ; coding: utf-8 -*-

# MyApp.spec

# -*- mode: python -*-

import os
import sys
from pathlib import Path

block_cipher = None

base_dir = os.getcwd()

# Add additional source folders to the path
additional_paths = [
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "calculations"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "config_connection_reading_management"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "GUI"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "GUI", "main_gui"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "GUI", "planner_gui"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "GUI", "general_stuff_for_plots"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "GUI", "recording_gui"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "manual_data_import"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "manual_data_import"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "src", "meta_data"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "Scripts"),
    os.path.join(base_dir, "t_p_etc_recorder_v2", "utils")
]


# Convert paths to strings for the Analysis object
additional_paths = [str(path) for path in additional_paths]

a = Analysis(
    ['t_p_etc_recorder_v2/Scripts/main.py'],
    pathex=[str(base_dir), *additional_paths],
    binaries=[],
    datas=[('t_p_etc_recorder_v2/config/config_logging_modbus_database.json', 'config'),
           ('t_p_etc_recorder_v2/config/hydride_data_base.json', 'config'),
           ('t_p_etc_recorder_v2/config/periodic_table_of_elements.json', 'config')],
    hiddenimports=[],
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='MyApp',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    icon=None
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='MyApp',
)