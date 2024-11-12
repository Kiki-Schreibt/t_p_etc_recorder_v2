import sys
import numpy as np

from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtCore import Qt
from manual_csv_xlsx_inserter import ManualTpCSVInserter
import manual_csv_xlsx_inserter
import pyqtgraph as pg

class PlotWindow(QMainWindow):
    def __init__(self, title):
        super().__init__()
        self.graphWidget = pg.GraphicsLayoutWidget()
        self.setCentralWidget(self.graphWidget)
        self.setWindowTitle(title)
        self.plot = self.graphWidget.addPlot()
        self.plot.showGrid(x=True, y=True)


def plot_temperature(df):
    win = PlotWindow('Temperature and Conditions')
    time = df['time'].values  # Assuming 'Time' is already in the appropriate format
    temperature = df['temperature_sample'].values

    # Create the main temperature plot
    win.plot.plot(time, temperature, pen='b', name='Temperature Sample')

    # Check and mark regions for Hydrogenated and Dehydrogenated
    for state, sub_df in df.groupby((df['de_hyd_state'] != df['de_hyd_state'].shift()).cumsum()):
        time_floats = df['time'].view('int64')  # Convert pandas Timestamps to seconds since epoch

        start = min(time_floats)
        end = max(time_floats)

        #if sub_df['de_hyd_state'].iloc[0] == 'Hydrogenated':
        #    region = pg.LinearRegionItem(values=[start, end], brush='g', movable=False)
        #    win.plot.addItem(region)
        #elif sub_df['de_hyd_state'].iloc[0] == 'Dehydrogenated':
        #    region = pg.LinearRegionItem(values=[start, end], brush='r', movable=False)
        #    win.plot.addItem(region)

    win.show()
    return win


def plot_pressure(df):
    win = PlotWindow('Pressure over Time')
    time = df['time']
    pressure = df['pressure']
    eq_pressure = df['eq_pressure']

    win.plot.plot(time, pressure, pen='r', name='Pressure')
    win.plot.plot(time, eq_pressure, pen=pg.mkPen('g', style=Qt.PenStyle.DashLine), name='Eq Pressure')
    win.show()
    return win

def plot_h2_uptake(df):
    win = PlotWindow('H2 Uptake over Time')
    time = df['time']
    h2_uptake = df['h2_uptake']

    win.plot.plot(time, h2_uptake, symbol='o', pen=None, name='H2 Uptake')
    win.show()
    return win


sample_id = 'WAE-WA-030'
reservoir_volume = 3.75
inserter_wizard = ManualTpCSVInserter(sample_id=sample_id, reservoir_volume=reservoir_volume)
df = manual_csv_xlsx_inserter.read_and_plot_tp(inserter_wizard=inserter_wizard, sample_id=sample_id)  # Load or prepare your dataframe
condition_plot = (df["temperature_sample"] < 1000) & (df["pressure"]<1000)
df = df[condition_plot]
app = QApplication(sys.argv)
#     # Load or prepare your dataframe
win1 = plot_temperature(df=df)
win2 = plot_pressure(df)
win3 = plot_h2_uptake(df)
sys.exit(app.exec())




#    sample_id = 'WAE-WA-030'
#    reservoir_volume = 3.75
    #inserter_wizard = ManualTpCSVInserter(sample_id=sample_id, reservoir_volume=reservoir_volume)

#    app = QApplication(sys.argv)
    #df = manual_csv_xlsx_inserter.read_and_plot_tp(inserter_wizard=inserter_wizard,sample_id=sample_id)  # Load or prepare your dataframe
#    win1 = plot_temperature(df="")
    #win2 = plot_pressure(df)
    #win3 = plot_h2_uptake(df)

 #   sys.exit(app.exec())
