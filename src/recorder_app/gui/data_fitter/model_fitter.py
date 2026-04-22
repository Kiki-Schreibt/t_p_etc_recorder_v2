#model_fitter.py
import numpy as np
from scipy.optimize import curve_fit, differential_evolution, brute

class ModelFitter:
    """
    Fits model parameters to experimental data using different methods.
    """
    def __init__(self, model):
        self.model = model
        self.x0 = self.create_starting_values()
        self.fit_methods = {
            'curve_fit': self._fit_curve_fit,
            'differential_evolution': self._fit_differential_evolution,
            'brute': self._fit_brute,
            'curve_fit_log': self._fit_curve_fit_log
        }

    def create_starting_values(self):
        # [particle_diameter, lambda_null_gas, porosity, lambda_base]
        return [self.model.mp.particle_diameter,
                self.model.mp.lambda_solid,
                self.model.mp.porosity,
                1e-5]

    def fit_ETC(self, x, y, method='curve_fit', **kwargs):
        if method not in self.fit_methods:
            raise ValueError(f"Unknown fitting method: {method}")
        params, pcov = self.fit_methods[method](x, y, **kwargs)
        metrics = self.calculate_metrics(x, y, params, log_space=('log' in method))
        return params, pcov, metrics

    def _fit_curve_fit(self, x, y, bounds=None):
        def fit_function(x, d, lambda_null_gas, porosity, lambda_base):
            return self.model.ETC_fun(x, (d, lambda_null_gas, porosity, lambda_base))
        if bounds:
            popt, pcov = curve_fit(fit_function, x, y, p0=self.x0, bounds=bounds)
        else:
            popt, pcov = curve_fit(fit_function, x, y, p0=self.x0)
        return popt, pcov

    def _fit_differential_evolution(self, x, y, bounds):
        def fit_function(params, x, y):
            return np.sum((y - self.model.ETC_fun(x, params)) ** 2)
        result = differential_evolution(fit_function, bounds, args=(x, y))
        return result.x, result.fun

    def _fit_brute(self, x, y, ranges):
        def fit_function(params, x, y):
            return np.sum((y - self.model.ETC_fun(x, params)) ** 2)
        result = brute(fit_function, ranges=ranges, args=(x, y), full_output=True, finish=None)
        return result[0], result[1]

    def _fit_curve_fit_log(self, x, y, bounds=None):
        x_log = np.log10(x)
        y_log = np.log10(y)
        def fit_function(x_log, d, lambda_null_gas, porosity, lambda_base):
            params = (d, lambda_null_gas, porosity, lambda_base)
            return np.log10(self.model.ETC_fun(10**x_log, params))
        if bounds:
            popt, pcov = curve_fit(fit_function, x_log, y_log, p0=self.x0, bounds=bounds)
        else:
            popt, pcov = curve_fit(fit_function, x_log, y_log, p0=self.x0)
        return popt, pcov

    def calculate_metrics(self, x, y, params, log_space=False):
        y_pred = self.model.ETC_fun(x, params)
        if log_space:
            residuals = np.log10(y) - np.log10(y_pred)
            ss_tot = np.sum((np.log10(y) - np.mean(np.log10(y))) ** 2)
        else:
            residuals = y - y_pred
            ss_tot = np.sum((y - np.mean(y)) ** 2)
        ss_res = np.sum(residuals ** 2)
        r_squared = 1 - (ss_res / ss_tot)
        rmse = np.sqrt(np.mean(residuals ** 2))
        mae = np.mean(np.abs(residuals))
        return {'R_squared': r_squared, 'RMSE': rmse, 'MAE': mae}
