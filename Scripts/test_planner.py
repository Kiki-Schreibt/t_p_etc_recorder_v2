from PySide6.QtWidgets import QApplication, QVBoxLayout
from PySide6.QtUiTools import QUiLoader
from PySide6.QtCore import QFile
from PySide6.QtWidgets import QMainWindow

from src.GUI.planner_gui.planner_business import VantHoffPlot
from src.standard_paths import planner_ui_file_path

class TestPlannerMain(QMainWindow):


    def __init__(self):
        super().__init__()
        ui_file_path = planner_ui_file_path
        self.setWindowTitle("Test planner")
        self.ui = load_ui_file(ui_file_path)
        self.setCentralWidget(self.ui)
        self.setMinimumSize(800, 650)
        self.plot = VantHoffPlot(self.ui.test_plot_window)
        self.plot.p_calc_sig.connect(lambda p_hyd, p_dehyd: self._update_pressure_edit_fields(p_hyd, p_dehyd))
        self.plot.wt_p_sig.connect(lambda wt: self.ui.capacity_edit_field.setText(str(round(wt, 3))))
        self._init_text_box_connections()
        self.ui.material_edit_field.editingFinished.connect(self.on_material_changed)
        self.ui.plot_route_button.clicked.connect(self._on_plot_route_button_clicked)
        self.ui.cell_combo_box.textActivated.connect(self._on_cell_chosen)
    def _init_text_box_connections(self):
        self.ui.pressure_hyd_edit_field.editingFinished.connect(lambda: self.update_vars(self.ui.pressure_hyd_edit_field.text(),"pressure_hyd"))
        self.ui.pressure_dehyd_edit_field.editingFinished.connect(lambda: self.update_vars(self.ui.pressure_dehyd_edit_field.text(),"pressure_dehyd"))
        self.ui.temperature_hyd_edit_field.editingFinished.connect(lambda: self.update_vars(self.ui.temperature_hyd_edit_field.text(),"temperature_hyd"))
        self.ui.temperature_dehyd_edit_field.editingFinished.connect(lambda: self.update_vars(self.ui.temperature_dehyd_edit_field.text(),"temperature_dehyd"))

    def _on_cell_chosen(self, value):
        if "2nd" in value:
            self.ui.v_cell_edit_field.setText("30.24")
        if "3rd" in value:
            self.ui.v_cell_edit_field.setText("44.37")
        self.ui.v_res_edit_field.setText("1")

    def _update_pressure_edit_fields(self, p_hyd=None, p_dehyd=None):
        self.ui.pressure_hyd_edit_field.setText(str(round(p_hyd, 2)))
        self.ui.pressure_dehyd_edit_field.setText(str(round(p_dehyd, 2)))

    def on_material_changed(self):
        self.plot.plot_vant_hoff(hydride=self.ui.material_edit_field.text())

    def update_vars(self, value, name):

        value = _is_num_str(value)
        if "hyd" in name and "pressure" in name:
            self.p_hyd = value
        if "dehyd" in name and "pressure" in name:
            self.p_dehyd = value
        if "hyd" in name and "temp" in name:
            self.temp_hyd = value
        if "dehyd" in name and "temp" in name:
            self.temp_dehyd = value

    def _on_plot_route_button_clicked(self):

        p_hyd = _is_num_str(self.ui.pressure_hyd_edit_field.text())
        p_dehyd =_is_num_str(self.ui.pressure_dehyd_edit_field.text())
        T_hyd = _is_num_str(self.ui.temperature_hyd_edit_field.text())
        T_dehyd =_is_num_str(self.ui.temperature_dehyd_edit_field.text())
        cell_volume = _is_num_str(self.ui.v_cell_edit_field.text())
        reservoir_volume = _is_num_str(self.ui.v_res_edit_field.text())
        mass = _is_num_str(self.ui.mass_edit_field.text())
        wt_p = _is_num_str(self.ui.capacity_edit_field.text())
        hydride = _is_num_str(self.ui.material_edit_field.text(), "str")


        self.plot.plot_delta_p(p_hyd=p_hyd, p_dehyd=p_dehyd,
                               temp_hyd=T_hyd, temp_dehyd=T_dehyd,
                               cell_volume=cell_volume,
                               V_res=reservoir_volume,
                               mass=mass,
                               hydride=hydride,
                               wt_p=wt_p)

    def resizeEvent(self, event):
        super(QMainWindow, self).resizeEvent(event)


@staticmethod
def _is_num_str(value, mode="Num"):
    if not value:
        return None
    if "num" in mode.lower():
        try:
            if value == 0 or value == "0":
                return 0.000000000001
            else:
                return float(value)
        except ValueError:
            return False
    if "date" in mode.lower():
        try:
            if isinstance(value, str):
                from datetime import datetime
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return False
    if "str" in mode.lower():
        try:
            return str(value)
        except ValueError:
            return False
    if 'duration' in mode.lower():
        try:
            from datetime import timedelta
            hours, minutes, seconds = map(int, value.split(':'))
            duration = timedelta(hours=hours, minutes=minutes, seconds=seconds)
            return duration
        except ValueError:
            return False

def load_ui_file(ui_file_path):
        try:
            loader = QUiLoader()
            ui_file = QFile(ui_file_path)

            ui_file.open(QFile.ReadOnly)
            ui = loader.load(ui_file)
            ui_file.close()
            if not ui:
                print("Failed to load UI file")
                return
            return ui
        except Exception as e:
            print("Unable to load ui file %s", e)


def main():
    # Create the Qt Application
    app = QApplication([])
    # Create and show the main application window
    main_window = TestPlannerMain()
    main_window.show()
    # Run the event loop
    app.exec()

if __name__ == "__main__":
    main()
