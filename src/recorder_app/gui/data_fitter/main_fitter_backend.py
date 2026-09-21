import numpy as np
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt

from recorder_app.gui.data_fitter.data_loader import DataLoader
from recorder_app.gui.data_fitter.material_properties import MaterialProperties
from recorder_app.gui.data_fitter.models import KaganerModel, ZehnerBauerSchluenderModel  # Use appropriate model here
from recorder_app.gui.data_fitter.model_fitter import ModelFitter
from recorder_app.gui.data_fitter.plotter import Plotter
from recorder_app.infrastructure.core.config_reader import config

try:
    import recorder_app.infrastructure.core.logger as logging
except ImportError:
    import logging

class ZBSFitter:
    """
    Class for fitting the Zehner-Bauer-Schlunder (ZBS) model to experimental data using
    non-linear least squares.

    The fitted parameters are:
      - particle_diameter: The characteristic particle diameter [m]
      - lambda_null_gas: The thermal conductivity of the gas at the reference state [W/m/K]
      - porosity: The sample porosity (must be between 0 and 1)
      - lambda_base: The baseline (bulk) thermal conductivity of the solid material [W/m/K]

    Default logical bounds are provided:
      particle_diameter: [1e-6, 1e-3] m
      lambda_null_gas: [0.005, 0.05] W/m/K
      porosity: [0.01, 0.99] (unitless)
      lambda_base: [0.01, 10.0] W/m/K
    """
    def __init__(self, material_properties, Temperature, beta):
        # Create an instance of the ZBS model
        self.model = ZehnerBauerSchluenderModel(material_properties, Temperature, beta)

    def model_function(self, p, particle_diameter, lambda_null_gas, porosity, lambda_base):
        """
        Wraps the ZBS model's ETC_fun method so that it can be used with curve_fit.

        Parameters:
            p : array-like
                Pressure data.
            particle_diameter, lambda_null_gas, porosity, lambda_base : float
                Fitting parameters.

        Returns:
            Calculated effective thermal conductivity for each pressure value.
        """
        params = (particle_diameter, lambda_null_gas, porosity, lambda_base)
        return self.model.ETC_fun(p, params)

    def fit(self, p_data, ETC_data, initial_guess=None, bounds=None):
        """
        Fit the ZBS model to the experimental data.

        Parameters:
            p_data : array-like
                Experimental pressure data.
            ETC_data : array-like
                Experimental effective thermal conductivity data.
            initial_guess : list or tuple, optional
                Initial guess for the parameters [particle_diameter, lambda_null_gas, porosity, lambda_base].
                If not provided, defaults from material properties will be used.
            bounds : 2-tuple of lists, optional
                Lower and upper bounds for the parameters. Defaults to:
                lower_bounds = [1e-6, 0.005, 0.01, 0.01]
                upper_bounds = [1e-3, 0.05, 0.99, 10.0]

        Returns:
            popt : array
                Optimal values for the parameters.
            pcov : 2D array
                The estimated covariance of popt.
        """
        # Define a default initial guess if not provided.
        if initial_guess is None:
            # Use the particle diameter from material properties
            particle_diameter_guess = self.model.mp.particle_diameter
            # Use the lambda_null_gas from material properties or default value
            lambda_null_gas_guess = getattr(self.model.mp, 'lambda_null_gas', 0.018)
            # Use the porosity computed in material_properties; if not available, use 0.4 as an example.
            porosity_guess = getattr(self.model.mp, 'porosity', 0.4)
            # The baseline thermal conductivity is taken from the bulk value.
            lambda_base_guess = self.model.lambda_bulk
            initial_guess = [particle_diameter_guess, lambda_null_gas_guess, porosity_guess, lambda_base_guess]

        # Set default bounds if not provided.
        if bounds is None:
            lower_bounds = [1e-6, 0.005, 0.01, 0.01]  # Avoid zero or extreme values
            upper_bounds = [1e-3, 0.05, 0.99, 100.0]
            bounds = (lower_bounds, upper_bounds)

        # Perform the curve fit.
        popt, pcov = curve_fit(self.model_function, p_data, ETC_data, p0=initial_guess, bounds=bounds)
        return popt, pcov

    def predict(self, p, params):
        """
        Calculate the model prediction given pressure data and a set of parameters.

        Parameters:
            p : array-like
                Pressure data.
            params : list or tuple
                Model parameters [particle_diameter, lambda_null_gas, porosity, lambda_base].

        Returns:
            Model prediction for effective thermal conductivity.
        """
        return self.model.ETC_fun(p, params)


def main(fit_method='curve_fit'):
    # Retrieve configuration for database connection

    data_loader = DataLoader(
        sample_id="WAE-WA-040",
        cycle_number=0.5,
        temperature=200,
        db_conn_params=config.db_conn_params
    )
    isotherm, mean_temp, de_hyd_state = data_loader.get_isotherm()
    x = isotherm["pressure"]
    y = isotherm["ThConductivity"]

    # Initialize material properties
    material_props = MaterialProperties(material="MgH2", de_hyd_state=de_hyd_state)
    T = mean_temp

    # Prepare dictionaries for storing fitting results
    fitted_values_dict = {}
    popt_dict = {}
    pcov_dict = {}
    deviation_dict = {}
    x_plot = np.logspace(-5, 4, 100)

    # Loop over different beta values
    for alpha_name, beta_dict in material_props.beta_values.items():
        for beta_name, beta_value in beta_dict.items():
            model = KaganerModel(material_properties=material_props, Temperature=T, beta=beta_value)

            fitter = ModelFitter(model)
            try:
                popt, pcov, metrics = fitter.fit_ETC(x, y, method=fit_method)
                fitted_values = model.ETC_fun(x_plot, popt)
                popt_dict[beta_name] = popt
                pcov_dict[beta_name] = pcov
                deviation_dict[beta_name] = np.sqrt(np.diag(pcov))
                fitted_values_dict[beta_name] = fitted_values
                print(f"Fitted {beta_name}")
                for key, value in metrics.items():
                    print(f"{key}: {value}")
            except Exception as e:
                print(f"Fitting failed for {beta_name}: {e}")
                continue

    print("Optimized parameters for different beta values:")
    for beta_name, popt in popt_dict.items():
        deviation = deviation_dict.get(beta_name, None)
        if deviation is not None:
            print(f"{beta_name}:")
            print(f"  Diameter: {round(popt[0], 4)} ± {np.round(deviation[0], 4)} [m]")
            print(f"  Lambda Gas: {round(popt[1], 4)} ± {np.round(deviation[1], 4)} [W/m/K]")
            print(f"  Porosity: {round(popt[2], 4)} ± {np.round(deviation[2], 4)}")
            print(f"  Lambda Base: {round(popt[3], 4)} ± {np.round(deviation[3], 4)} [W/m/K]")

    Plotter.plot_results(x, y, fitted_values_dict, x_plot)


def try_fitting():
    # Create a dummy material properties class for testing
    class DummyMaterialProperties:
        def __init__(self):
            # These dummy values should roughly mimic your real material properties
            self.lambda_solid = 0.5         # Bulk thermal conductivity in W/m/K
            self.particle_diameter = 45e-6  # Particle diameter in meters
            self.lambda_null_gas = 0.018    # Thermal conductivity of the gas [W/m/K]
            self.porosity = 0.4             # Porosity (dimensionless)
            self.kB = 1.3806e-23            # Boltzmann constant in J/K
            self.sigma0 = 0.27e-18          # Collision cross-section in m²

    # Instantiate the dummy material properties
    material_properties = MaterialProperties(material="MgH2", de_hyd_state="Hydrogenated")
    print(material_properties.porosity)
    dummy_mp = DummyMaterialProperties()

    # Define test conditions
    Temperature = 30  # in Celsius
    beta = 0.5        # Example beta value (adjust as needed)
    for alpha_name, beta_dict in material_properties.beta_values.items():
        for beta_name, beta_value in beta_dict.items():
            try:
                # Instantiate the ZBS fitter using our dummy properties
                fitter = ZBSFitter(material_properties, Temperature, beta_value)
                #fitter = ZBSFitter(dummy_mp, Temperature, beta_value)
                # Example experimental data (pressure and ETC)
                p_data = np.array([0.9702389,
                                   2.045748,
                                   3.091432,
                                   5.108416,
                                   10.15543,
                                   15.03001,
                                   20.02524,
                                   30.46603])
                ETC_data = np.array([1.645590531,
                                     1.778177876,
                                     1.851483134,
                                     1.931695899,
                                     2.034875209,
                                     2.084308118,
                                     2.115438222,
                                     2.159283779])

                popt=None
                for i in range(1):
                    # Fit the model to the experimental data
                    popt, pcov = fitter.fit(p_data, ETC_data, initial_guess=popt)

                # Compute uncertainties from the covariance matrix
                uncertainties = np.sqrt(np.diag(pcov))

                print("Optimal parameters found:")
                print(f"Particle Diameter: {popt[0]:.2e} ± {uncertainties[0]:.2e} m")
                print(f"Lambda Null Gas: {popt[1]:.3f} ± {uncertainties[1]:.3f} W/m/K")
                print(f"Porosity: {popt[2]:.3f} ± {uncertainties[2]:.3f}")
                print(f"Lambda Base: {popt[3]:.3f} ± {uncertainties[3]:.3f} W/m/K")
                print("Covariance matrix:")
                print(pcov)

                # Plot the experimental data and the fitted model
                ETC_fit = fitter.predict(p_data, popt)
                plt.figure(figsize=(8, 6))
                plt.plot(p_data, ETC_data, 'bo', label='Experimental Data')
                plt.plot(p_data, ETC_fit, 'r-', label='Fitted Model')
                plt.xlabel('Pressure')
                plt.ylabel('Effective Thermal Conductivity')
                plt.title('ZBS Model Fit')
                plt.legend()
                plt.show()
                return
            except Exception as e:
                print(e)


if __name__ == '__main__':
    # Use try_fitting() for testing with dummy data
    try_fitting()

    # Alternatively, use main() for fitting with real data from the database
    # main('curve_fit')
    # main('curve_fit_log')
