"""Backend entry point for ETC model comparison."""
from __future__ import annotations

import numpy as np

from recorder_app.gui.data_fitter.data_loader import DataLoader
from recorder_app.gui.data_fitter.material_properties import MaterialProperties
from recorder_app.gui.data_fitter.model_fitter import ModelFitter
from recorder_app.gui.data_fitter.models import (
    KaganerModel,
    ZehnerSchluenderModel,
    ModifiedSZBSModel,
    ZehnerSchluenderDamkohlerModel,
    HeatTransferConcentratingModel,
)
from recorder_app.infrastructure.core.config_reader import config


MODERN_H2_BETA = 9.87


def fit_isotherm(data_loader, mode="robust_least_squares"):
    """Fit legacy and modern powder-bed models to one measured isotherm.

    The modern models deliberately use temperature-dependent H2 conductivity
    and a fixed accommodation/Smoluchowski coefficient for H2. A single
    isotherm should not be allowed to independently identify particle size,
    porosity, solid conductivity and contact fraction: those parameters are
    strongly correlated. Therefore the fitter's bounds/defaults are explicit
    and should be tightened further when independent characterization exists.
    """
    isotherm, mean_temperature, de_hyd_state = data_loader.get_isotherm()
    if isotherm is None or isotherm.empty:
        raise ValueError("No ETC data found for the requested isotherm.")

    x = np.asarray(isotherm["pressure"], dtype=float)
    y = np.asarray(isotherm["ThConductivity"], dtype=float)

    mp = MaterialProperties(material="MgH2", de_hyd_state=de_hyd_state)
    models = {
        "Kaganer (legacy)": KaganerModel(mp, mean_temperature, beta=MODERN_H2_BETA),
        "ZS": ZehnerSchluenderModel(mp, mean_temperature, beta=MODERN_H2_BETA),
        "modified S-ZBS": ModifiedSZBSModel(mp, mean_temperature, beta=MODERN_H2_BETA),
        "modified ZSD": ZehnerSchluenderDamkohlerModel(
            mp, mean_temperature, beta=MODERN_H2_BETA
        ),
        "HTC": HeatTransferConcentratingModel(
            mp, mean_temperature, beta=MODERN_H2_BETA
        ),
    }

    results = {}
    for name, model in models.items():
        try:
            fitter = ModelFitter(model)
            popt, pcov, metrics = fitter.fit_ETC(
                x, y, method=mode, log_space=True, f_scale=0.02
            )
            results[name] = {
                "model": model,
                "fitter": fitter,
                "params": popt,
                "covariance": pcov,
                "metrics": metrics,
                "prediction": fitter.predict(x, popt),
                "parameter_report": fitter.parameter_report(popt, pcov),
            }
        except Exception as exc:
            results[name] = {"error": str(exc)}

    return {
        "isotherm": isotherm,
        "temperature": mean_temperature,
        "de_hyd_state": de_hyd_state,
        "results": results,
    }


def main(data_loader=None, mode="robust_least_squares"):
    if data_loader is None:
        data_loader = DataLoader(
            sample_id="WAE-WA-040",
            cycle_number=0.5,
            temperature=200,
            db_conn_params=config.db_conn_params,
        )

    result = fit_isotherm(data_loader, mode=mode)
    for name, fit in result["results"].items():
        if "error" in fit:
            print(f"{name}: fitting failed: {fit['error']}")
            continue
        print(f"\n{name}")
        for key, value in fit["metrics"].items():
            print(f"  {key}: {value}")
        for parameter, values in fit["parameter_report"].items():
            print(f"  {parameter}: {values['value']:.6g} +/- {values['std']:.3g}")
    return result


if __name__ == "__main__":
    main()
