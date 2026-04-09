import sys
import numpy as np

from PySide6.QtWidgets import QApplication
import pyqtgraph as pg
from PySide6.QtCore import Signal

from src.infrastructure.utils.eq_p_calculation import VantHoffCalcEq
from src.infrastructure.handler.hydride_handler import MetalHydrideDatabase


class VantHoffPlot(pg.PlotWidget):
    p_calc_sig = Signal(float, float) #p_hyd, p_dehyd
    wt_p_sig = Signal(float)

    def __init__(self,db_conn_params, parent=None):
        super().__init__(parent)
        self.db_conn_params = db_conn_params
        self.hydride = None
        self.mass = None
        self.temp_hyd = None
        self.temp_dehyd = None
        self.p_hyd = None
        self.p_dehyd = None
        self.V_cell = None
        self.V_res = None
        self.V_pipes = 1e-7
        self.color_index = 0
        self.colors = ['r', 'g', 'b', 'c', 'm', 'y', 'k']
        self.init_ui()
        if parent:
            self.adjust_plot_size()
        #self.plotItem.sigRangeChanged.connect(self.plot_new_range)

    def init_ui(self):
        self.setWindowTitle("Van't Hoff Plot")
        self.setLabel('left', 'Equilibrium Pressure', units='bar')
        self.setLabel('bottom', 'Temperature', units='°C')
        self.plotItem.addLegend(offset=(0,1))

        #self.show_grid()

    def show_grid(self):
        self.showGrid(x=True, y=True, alpha=0.5)

    def plot_vant_hoff(self, temperature_range=range(0, 700, 2), hydride=None, keep_color=False):
        self.hydride = hydride
        if not hydride:
            return
        if hydride.lower() == "clear":
            self.clear_plot()
            return
        mh_database = MetalHydrideDatabase(db_conn_params=self.db_conn_params)
        self.wt_p_sig.emit(mh_database.get_capacity(hydride_name=hydride))
        vant_hoff_calculator = VantHoffCalcEq(hydride=hydride)
        y_data = vant_hoff_calculator.calc_vant_hoff_lin(temperature_range)

        if not y_data.any():
            return

        x_data = np.array(temperature_range)
        self.plotItem.plot(x_data, y_data, pen=pg.mkPen(color=self.colors[self.color_index],
                                                        width=2), name=hydride)
        if not keep_color:
            if self.color_index < len(self.colors):
                self.color_index += 1
            else:
                self.color_index = 0

    def plot_new_range(self):
        new_axis = self.plotItem.viewRange()[0]
        new_range = range(int(new_axis[0]), int(new_axis[1]), 2)
        self.plot_vant_hoff(temperature_range=new_range, hydride=self.hydride, keep_color=True)

    def plot_delta_p(self, p_hyd, p_dehyd, temp_hyd, temp_dehyd, V_res, mass, wt_p=None, cell_type=None, cell_volume=None, hydride=None):

        def _calculate_pressure_change(p_hyd, p_dehyd, temp_hyd, temp_dehyd, V_res, mass, wt_p=None, cell_type=None, cell_volume=None, hydride=None):
            if cell_type:
                if '2nd' in cell_type:
                    cell_volume = 30.24
                if '3rd' in cell_type:
                    cell_volume = 44.37
            if not cell_volume:
                return None, None
            if hydride and not wt_p:
                mh_database = MetalHydrideDatabase(db_conn_params=self.db_conn_params)
                wt_p = mh_database.get_capacity(hydride_name=hydride)
                self.wt_p_sig.emit(wt_p)
            elif not wt_p:
                return None, None

            vant_hoff_calculator = VantHoffCalcEq()

            if p_hyd or p_dehyd:
                p_hyd, p_dehyd = vant_hoff_calculator.calc_delta_p(wt_p=wt_p,
                                                            m_sample=mass,
                                                            p_hyd=p_hyd,
                                                            p_dehyd=p_dehyd,
                                                            T_hyd=temp_hyd,
                                                            T_dehyd=temp_dehyd,
                                                            V_res=V_res,
                                                            V_cell=cell_volume)
            return p_hyd, p_dehyd

        def _calculate_capacity(p_hyd, p_dehyd, temp_hyd, temp_dehyd, V_res, mass, cell_type=None, cell_volume=None):

            wt_p=None
            if cell_type:
                if '2nd' in cell_type:
                    cell_volume = 30.24
                if '3rd' in cell_type:
                    cell_volume = 44.37
            if not cell_volume:
                return None
            vant_hoff_calculator = VantHoffCalcEq()


            if p_hyd and p_dehyd:
                wt_p = vant_hoff_calculator.calc_h2_uptake( m_sample=mass,
                                                            p_hyd=p_hyd,
                                                            p_dehyd=p_dehyd,
                                                            T_hyd=temp_hyd,
                                                            T_dehyd=temp_dehyd,
                                                            V_res=V_res,
                                                            V_cell=cell_volume)
            return wt_p


        if (p_hyd and not p_dehyd) or (p_dehyd and not p_hyd):
            p_hyd, p_dehyd = _calculate_pressure_change(p_hyd=p_hyd,
                                                        p_dehyd=p_dehyd,
                                                        temp_hyd=temp_hyd,
                                                        temp_dehyd=temp_dehyd,
                                                        mass=mass,
                                                        V_res=V_res,
                                                        wt_p=wt_p,
                                                        cell_type=cell_type,
                                                        cell_volume=cell_volume,
                                                        hydride=hydride)

        elif p_hyd and p_dehyd:
            wt_p = _calculate_capacity(p_hyd=p_hyd,
                                       p_dehyd=p_dehyd,
                                       temp_hyd=temp_hyd,
                                       temp_dehyd=temp_dehyd,
                                       V_res=V_res,
                                       mass=mass,
                                       cell_type=cell_type,
                                       cell_volume=cell_volume,
                                       )
            self.wt_p_sig.emit(wt_p)

        if not p_hyd or not p_dehyd:
            return

        self.plotItem.plot(x=[temp_hyd, temp_dehyd],
                           y=[p_hyd, p_dehyd],
                           pen=pg.mkPen(color=self.colors[self.color_index],
                            width=2), name=f"Route {self.color_index}")

        if self.color_index < len(self.colors):
            self.color_index += 1
        else:
            self.color_index = 0
        self.p_calc_sig.emit(p_hyd, p_dehyd)

    def clear_plot(self):
        self.plotItem.clear()
        self.color_index = 0

    def adjust_plot_size(self):
        # Adjust the plot size as needed when the parent widget is resized
        self.setGeometry(self.parent().rect())


def main():
    app = QApplication([])

    # Create an instance of VantHoffCalcEq
    # Create an instance of VantHoffPlot and plot the data
    from src.infrastructure.core.config_reader import config
    vant_hoff_plot = VantHoffPlot(db_conn_params=config.db_conn_params)
    vant_hoff_plot.plot_vant_hoff()
    vant_hoff_plot.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
