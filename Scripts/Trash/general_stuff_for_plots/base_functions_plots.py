import pyqtgraph as pg
from datetime import datetime



class PlotBaseController:

    def __init__(self, plot_window, colors):
        self.colors = colors
        plot_item = plot_window.plotItem
        self.rightViewBox = plot_window.rightViewBox
        self.plot_item = plot_item
        self.original_mousePressEvent = plot_item.getViewBox().mousePressEvent
        self.original_mouseReleaseEvent = plot_item.getViewBox().mouseReleaseEvent
        self.original_mouseMoveEvent = plot_item.getViewBox().mouseMoveEvent
        self.original_wheelEvent = plot_item.getViewBox().wheelEvent
        self.init_constrains_params()
        self.current_color = []
        self.color_index = 0
        self.color_index_scatter = 0
        self.color_scatter = 0

    def init_constrains_params(self):
        self.previous_x_range = self.plot_item.viewRange()[0]
        self.previous_y_range = self.plot_item.viewRange()[1]
        self.min_x_axis_timestamp = datetime(2000, 1, 1).timestamp()  # Unix epoch start
        self.max_x_axis_timestamp = datetime.now().timestamp()  # Current time
        self.ignore_initial_range_changes = 10

    def enable_zooming(self):
        self.plot_item.getViewBox().mousePressEvent = self.original_mousePressEvent
        self.plot_item.getViewBox().mouseReleaseEvent = self.original_mouseReleaseEvent
        self.plot_item.getViewBox().mouseMoveEvent = self.original_mouseMoveEvent
        self.plot_item.getViewBox().wheelEvent = self.original_wheelEvent

    def disable_zooming(self):
        self.plot_item.getViewBox().mousePressEvent = lambda event: None
        self.plot_item.getViewBox().mouseReleaseEvent = lambda event: None
        self.plot_item.getViewBox().mouseMoveEvent = lambda event: None
        self.plot_item.getViewBox().wheelEvent = lambda event, **kwargs: None

    def constrain_x_axis_range(self):
        view_box = self.plot_item.getViewBox()
        x_range = view_box.viewRange()[0]

        # Constrain the x-axis range
        if x_range[0] < self.min_x_axis_timestamp:
            x_range[0] = self.min_x_axis_timestamp
        if x_range[1] > self.max_x_axis_timestamp:
            x_range[1] = self.max_x_axis_timestamp

        # Set the adjusted range back to the view box
        view_box.setXRange(x_range[0], x_range[1], padding=0)

    @staticmethod
    def is_significant_change(current_range, previous_range, threshold=0.3, y_changed=False):
        #if y_changed:

        #    return False
        current_range_change = abs(current_range[1] - current_range[0])
        previous_range_change = abs(previous_range[1] - previous_range[0])

        if current_range_change < previous_range_change:
            relative_change = previous_range_change / current_range_change
            print(f"Relative change current > previous: {relative_change}")
        else:
            relative_change = current_range_change / previous_range_change
            print(f"Relative change current < previous: {relative_change}")

        return relative_change > threshold

    def set_axis_titles(self, column, axis_position="left"):
        """
        Set axis titles based on the column name.

        Args:
            column (str): The column name to derive the title from.
            axis_position (str): The position of the axis ("left" or "right").
        """
        axis_label = AxisLabel.create_axis_label(column)
        self.plot_item.getAxis(axis_position).setLabel(axis_label)

    def get_next_color(self):
        # Get the next color from the list and increment the index
        self.current_color = self.colors[self.color_index % len(self.colors)]
        self.color_index += 1
        return self.current_color

    def reset_color(self):
        self.current_color = []
        self.color_index = 0


    def get_next_color_scatter(self):
        # Get the next color from the list and increment the index
        self.current_color_scatter = self.colors[self.color_index_scatter % len(self.colors)]
        self.color_index_scatter += 1
        return self.current_color_scatter

    def reset_color_scatter(self):
        self.current_color_scatter = []
        self.color_index_scatter = 0



class AxisLabel:

    @staticmethod
    def create_axis_label(column_name):
        """
        :param column_name: (str) name of the parameter that is plotted
        :return: axis title str
        """
        if "temperature" in column_name.lower():
            unit_str = "°C"
            variable_name = "Temperature"
        elif "pressure" in column_name.lower():
            unit_str = "bar"
            variable_name = "Pressure"
        elif "conductivity" in column_name.lower():
            unit_str = "Wm^-1K^-1"
            variable_name = "ETC"
        elif "time" in column_name.lower():
            unit_str = "Y-M-D H:M:s"
            variable_name = "Time"
        else:
            unit_str = ""  # Default unit if neither temperature nor pressure

        return variable_name + " (" + unit_str + ")"
