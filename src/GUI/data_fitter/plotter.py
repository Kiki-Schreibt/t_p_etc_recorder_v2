#plotter.py
import matplotlib.pyplot as plt
import numpy as np

class Plotter:
    @staticmethod
    def plot_results(x, y, fitted_values_dict, x_plot):
        plt.figure()
        plt.plot(x, y, '-x', label='Measured Data')
        for beta_name, fitted in fitted_values_dict.items():
            plt.plot(x_plot, fitted, label=f'Fitted Data {beta_name}')
        plt.xscale('log')
        plt.xlabel('Pressure (bar)')
        plt.ylabel('Thermal Conductivity (W/m/K)')
        plt.legend()
        plt.title('ETC Fit for Different Beta Values')
        plt.show()
