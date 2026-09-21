"""Parameter fitting for powder-bed effective thermal-conductivity models."""
from __future__ import annotations

import numpy as np
from scipy.optimize import curve_fit, differential_evolution, least_squares


class ModelFitter:
    """Fit an ETC model while keeping physical bounds explicit.

    The old implementation fitted four strongly correlated quantities at once.
    This fitter supports arbitrary model parameter counts, robust least-squares,
    log-space residuals, and a global-then-local optimizer.
    """

    DEFAULT_BOUNDS = {
        "particle_diameter": (1e-7, 5e-3),
        "lambda_null_gas": (1e-3, 1.0),
        "gas_conductivity_scale": (0.2, 5.0),
        "porosity": (0.05, 0.90),
        "lambda_base": (1e-4, 100.0),
        "solid_conductivity": (1e-4, 100.0),
        "contact_fraction": (0.0, 0.20),
        "emissivity": (0.01, 1.0),
    }

    def __init__(self, model, x0=None, bounds=None):
        self.model = model
        self.parameter_names = tuple(
            getattr(model, "parameter_names", self._infer_parameter_names())
        )
        self.x0 = np.asarray(
            x0 if x0 is not None else self.create_starting_values(), dtype=float
        )
        self.bounds = bounds if bounds is not None else self.create_bounds()

        if len(self.x0) != len(self.parameter_names):
            raise ValueError("Initial guess length does not match model parameters.")

    def _infer_parameter_names(self):
        if hasattr(self.model, "default_parameters"):
            return (
                "particle_diameter", "gas_conductivity_scale", "porosity",
                "solid_conductivity", "contact_fraction", "emissivity"
            )
        return ("particle_diameter", "lambda_null_gas", "porosity", "lambda_base")

    def create_starting_values(self):
        if hasattr(self.model, "default_parameters"):
            return self.model.default_parameters()

        return (
            float(self.model.mp.particle_diameter),
            float(getattr(self.model.mp, "lambda_null_gas", 0.018)),
            float(self.model.mp.porosity),
            float(self.model.lambda_bulk),
        )

    def create_bounds(self):
        lower, upper = [], []
        for name, x0 in zip(self.parameter_names, self.x0):
            lo, hi = self.DEFAULT_BOUNDS[name]
            lower.append(lo)
            upper.append(hi)
            if not lo < x0 < hi:
                self.x0[list(self.parameter_names).index(name)] = np.clip(x0, lo * 1.001, hi * 0.999)
        return np.asarray(lower), np.asarray(upper)

    def _clean_data(self, x, y):
        x = np.asarray(x, dtype=float)
        y = np.asarray(y, dtype=float)
        mask = np.isfinite(x) & np.isfinite(y) & (x > 0) & (y > 0)
        if mask.sum() < len(self.x0) + 1:
            raise ValueError("Not enough valid positive ETC data points for fitting.")
        return x[mask], y[mask]

    def _residuals(self, params, x, y, log_space=False):
        pred = np.asarray(self.model.ETC_fun(x, params), dtype=float)
        if np.any(~np.isfinite(pred)) or np.any(pred <= 0):
            return np.full_like(y, 1e12, dtype=float)
        if log_space:
            return np.log(pred) - np.log(y)
        return pred - y

    def fit_ETC(
        self,
        x,
        y,
        method="least_squares",
        bounds=None,
        x0=None,
        log_space=False,
        loss="linear",
        f_scale=1.0,
        max_nfev=10000,
        **kwargs,
    ):
        x, y = self._clean_data(x, y)
        if bounds is None:
            bounds = self.bounds
        if x0 is None:
            x0 = self.x0

        if method in {"least_squares", "robust_least_squares"}:
            if method == "robust_least_squares":
                loss = "soft_l1"
            result = least_squares(
                self._residuals,
                x0=np.asarray(x0, dtype=float),
                bounds=bounds,
                args=(x, y, log_space),
                loss=loss,
                f_scale=f_scale,
                x_scale="jac",
                max_nfev=max_nfev,
                **kwargs,
            )
            popt = result.x
            pcov = self._covariance(result.jac, result.cost, len(y), len(popt))
        elif method in {"curve_fit", "curve_fit_log"}:
            use_log = log_space or method.endswith("_log")

            def f(xdata, *params):
                pred = self.model.ETC_fun(xdata, params)
                return np.log(pred) if use_log else pred

            popt, pcov = curve_fit(
                f,
                x,
                np.log(y) if use_log else y,
                p0=np.asarray(x0, dtype=float),
                bounds=bounds,
                maxfev=max_nfev,
            )
            log_space = use_log
        elif method in {"differential_evolution", "global"}:
            def objective(params):
                r = self._residuals(params, x, y, log_space)
                return float(np.sum(r * r))

            result = differential_evolution(
                objective,
                bounds=list(zip(*bounds)),
                seed=kwargs.pop("seed", 42),
                polish=False,
                **kwargs,
            )
            local = least_squares(
                self._residuals,
                result.x,
                bounds=bounds,
                args=(x, y, log_space),
                loss=loss,
                f_scale=f_scale,
                x_scale="jac",
                max_nfev=max_nfev,
            )
            popt = local.x
            pcov = self._covariance(local.jac, local.cost, len(y), len(popt))
        else:
            raise ValueError(f"Unknown fitting method: {method}")

        metrics = self.calculate_metrics(x, y, popt, log_space=log_space)
        return popt, pcov, metrics

    @staticmethod
    def _covariance(jac, cost, n_obs, n_params):
        if jac is None or jac.size == 0 or n_obs <= n_params:
            return np.full((n_params, n_params), np.nan)
        try:
            jtj_inv = np.linalg.pinv(jac.T @ jac)
            variance = 2.0 * cost / max(n_obs - n_params, 1)
            return jtj_inv * variance
        except np.linalg.LinAlgError:
            return np.full((n_params, n_params), np.nan)

    def calculate_metrics(self, x, y, params, log_space=False):
        pred = np.asarray(self.model.ETC_fun(x, params), dtype=float)
        residuals = np.log(y) - np.log(pred) if log_space else y - pred
        observed = np.log(y) if log_space else y
        ss_res = float(np.sum(residuals**2))
        ss_tot = float(np.sum((observed - observed.mean())**2))
        return {
            "R_squared": 1.0 - ss_res / ss_tot if ss_tot else np.nan,
            "RMSE": float(np.sqrt(np.mean(residuals**2))),
            "MAE": float(np.mean(np.abs(residuals))),
            "MAPE_percent": float(np.mean(np.abs((y - pred) / y)) * 100.0),
            "N_points": int(len(y)),
        }

    def predict(self, x, params):
        return self.model.ETC_fun(np.asarray(x, dtype=float), params)

    def parameter_report(self, params, pcov=None):
        params = np.asarray(params)
        if pcov is None:
            sigma = np.full(len(params), np.nan)
        else:
            sigma = np.sqrt(np.maximum(np.diag(pcov), 0.0))
        return {
            name: {"value": float(value), "std": float(std)}
            for name, value, std in zip(self.parameter_names, params, sigma)
        }
