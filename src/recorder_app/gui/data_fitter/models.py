"""Effective thermal-conductivity models for gas-solid powder beds.

Keeps the original Kaganer/ZBS implementations and adds ZS, ZSD and
Smoluchowski-corrected modified ZSD (MZSD) models.

Pressure is accepted in bar by default because that is the unit used by the
recorder database. Internally gas-transport calculations use Pa.
"""
from __future__ import annotations

import numpy as np


_NIST_H2_T_K = np.array(
    [200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000],
    dtype=float,
)
_NIST_H2_K_MW = np.array(
    [132.76, 161.36, 186.64, 209.53, 230.87, 251.23,
     270.93, 309.10, 346.47, 383.80, 421.60, 460.15],
    dtype=float,
)


class BaseModel:
    """Common transport and geometry functions."""

    def __init__(self, material_properties, Temperature, beta=0.5, pressure_unit="bar"):
        self.mp = material_properties
        self.lambda_bulk = float(self.mp.lambda_solid)
        self.T = float(Temperature) + 273.15
        self.beta = float(beta)
        self.pressure_unit = pressure_unit

    def pressure_pa(self, p):
        p = np.asarray(p, dtype=float)
        unit = self.pressure_unit.lower()
        if unit in {"bar", "bara"}:
            return p * 1.0e5
        if unit == "pa":
            return p
        if unit == "kpa":
            return p * 1.0e3
        if unit == "mpa":
            return p * 1.0e6
        raise ValueError(f"Unsupported pressure unit: {self.pressure_unit}")

    def char_length(self, porosity, particle_diameter):
        porosity = np.asarray(porosity, dtype=float)
        if np.any((porosity <= 0) | (porosity >= 1)):
            raise ValueError("Porosity must be strictly between 0 and 1.")
        return (2.0 / 3.0) * (porosity / (1.0 - porosity)) * particle_diameter

    def mean_free_path(self, p):
        p_pa = np.maximum(self.pressure_pa(p), np.finfo(float).tiny)
        return self.mp.kB * self.T / (np.sqrt(2.0) * self.mp.sigma0 * p_pa)

    def nist_h2_conductivity(self):
        """Low-density normal-H2 conductivity, interpolated from NIST values."""
        if not 200.0 <= self.T <= 1000.0:
            raise ValueError(
                f"NIST H2 conductivity table is implemented for 200--1000 K; "
                f"got {self.T:.1f} K."
            )
        return float(np.interp(self.T, _NIST_H2_T_K, _NIST_H2_K_MW)) * 1.0e-3

    def gas_conductivity(
        self,
        p,
        porosity,
        particle_diameter,
        beta=None,
        lambda_null_gas=None,
        use_nist=True,
        conductivity_scale=1.0,
    ):
        """Gas conductivity including the Smoluchowski correction."""
        beta = self.beta if beta is None else float(beta)
        if lambda_null_gas is None and use_nist:
            lambda_inf = self.nist_h2_conductivity()
        elif lambda_null_gas is None:
            lambda_inf = float(self.mp.lambda_null_gas)
        else:
            lambda_inf = float(lambda_null_gas)

        kn = self.mean_free_path(p) / self.char_length(porosity, particle_diameter)
        return conductivity_scale * lambda_inf / (1.0 + 2.0 * beta * kn)

    def lambda_gas(self, p, porosity, particle_diameter, lambda_null_gas, beta):
        return self.gas_conductivity(
            p, porosity, particle_diameter, beta=beta,
            lambda_null_gas=lambda_null_gas, use_nist=False
        )

    def kapa_kaganer(self, p, porosity, particle_diameter, lambda_null_gas, beta, lambda_particle):
        return 1.0 - self.lambda_gas(
            p, porosity, particle_diameter, lambda_null_gas, beta
        ) / lambda_particle

    @staticmethod
    def deformation_parameter(porosity):
        return 1.25 * ((1.0 - porosity) / porosity) ** (10.0 / 9.0)

    @staticmethod
    def radiation_conductivity(temperature_k, particle_diameter, emissivity=0.8):
        """Damköhler-equivalent radiative conductivity."""
        sigma = 5.670374419e-8
        view_factor = emissivity / (1.0 - 0.132 * emissivity)
        return 4.0 * view_factor * sigma * temperature_k**3 * particle_diameter


class KaganerModel(BaseModel):
    """Original Kaganer implementation retained for comparison."""

    def ETC_fun(self, p, params):
        particle_diameter, lambda_null_gas, porosity, lambda_base = params
        lambda_gas_val = self.lambda_gas(
            p, porosity, particle_diameter, lambda_null_gas, self.beta
        )
        k_g = self.kapa_kaganer(
            p, porosity, particle_diameter, lambda_null_gas, self.beta, self.lambda_bulk
        )
        return lambda_base + (
            lambda_gas_val * (5.81 * ((1.0 - porosity) ** 2) / k_g)
            * (1.0 / k_g * np.log(self.lambda_bulk / lambda_gas_val) - 1.0 - k_g / 2.0)
            + 1.0
        )


class ZehnerSchluenderModel(BaseModel):
    """Classical Zehner-Schlünder model."""

    def ETC_fun(self, p, params):
        particle_diameter, lambda_null_gas, porosity, lambda_base = params
        kg = self.lambda_gas(p, porosity, particle_diameter, lambda_null_gas, self.beta)
        ks = float(lambda_base)
        B = self.deformation_parameter(porosity)
        kappa = ks / np.maximum(kg, 1e-300)
        delta = 1.0 - B / kappa
        delta = np.where(np.abs(delta) < 1e-12, 1e-12, delta)
        bracket = (
            (kappa - 1.0) / delta**2 * (B / kappa) * np.log(np.maximum(kappa / B, 1e-300))
            - (B - 1.0) / delta - (B + 1.0) / 2.0
        )
        gamma = 2.0 / delta * bracket
        return kg * (1.0 - np.sqrt(1.0 - porosity) + gamma * np.sqrt(1.0 - porosity))


class ZehnerSchluenderDamkohlerModel(BaseModel):
    """ZSD model with Damköhler radiation and flattened-contact conduction."""

    def ETC_fun(self, p, params):
        particle_diameter, lambda_null_gas, porosity, lambda_base, contact_fraction, emissivity = params
        kg = self.lambda_gas(p, porosity, particle_diameter, lambda_null_gas, self.beta)
        ks = float(lambda_base)
        B = self.deformation_parameter(porosity)
        kappa = ks / np.maximum(kg, 1e-300)
        delta = 1.0 - B / kappa
        delta = np.where(np.abs(delta) < 1e-12, 1e-12, delta)
        bracket = (
            (kappa - 1.0) / delta**2 * (B / kappa) * np.log(np.maximum(kappa / B, 1e-300))
            - (B - 1.0) / delta - (B + 1.0) / 2.0
        )
        gamma = 2.0 / delta * bracket
        k_cond = kg * (1.0 - np.sqrt(1.0 - porosity) + gamma * np.sqrt(1.0 - porosity))
        k_r = self.radiation_conductivity(self.T, particle_diameter, emissivity)
        k_contact = ks * contact_fraction * np.sqrt(1.0 - porosity)
        return k_cond + k_r * np.sqrt(1.0 - porosity) + k_contact


class ModifiedZSDModel(BaseModel):
    """Smoluchowski-corrected modified ZSD model.

    Parameters:
        particle_diameter [m]
        gas_conductivity_scale [-]
        porosity [-]
        solid_conductivity [W/m/K]
        contact_fraction [-]
        emissivity [-]

    The gas conductivity uses temperature-dependent normal-H2 data and the
    Smoluchowski pressure correction. The model explicitly contains gas,
    gas-solid and particle-contact heat-transfer paths plus radiation.
    """

    parameter_names = (
        "particle_diameter", "gas_conductivity_scale", "porosity",
        "solid_conductivity", "contact_fraction", "emissivity"
    )

    def default_parameters(self):
        return (
            float(self.mp.particle_diameter), 1.0, float(self.mp.porosity),
            float(self.mp.lambda_solid),
            float(getattr(self.mp, "contact_fraction", 3.0e-4)),
            float(getattr(self.mp, "emissivity", 0.8)),
        )

    def ETC_fun(self, p, params):
        (
            particle_diameter, gas_conductivity_scale, porosity,
            solid_conductivity, contact_fraction, emissivity
        ) = params

        kg = self.gas_conductivity(
            p, porosity, particle_diameter, beta=self.beta,
            use_nist=True, conductivity_scale=gas_conductivity_scale
        )
        ks = max(float(solid_conductivity), 1e-12)
        B = self.deformation_parameter(porosity)
        kappa = ks / np.maximum(kg, 1e-300)
        delta = 1.0 - B / kappa
        delta = np.where(np.abs(delta) < 1e-12, 1e-12, delta)
        bracket = (
            (kappa - 1.0) / delta**2 * (B / kappa) * np.log(np.maximum(kappa / B, 1e-300))
            - (B - 1.0) / delta - (B + 1.0) / 2.0
        )
        gamma = 2.0 / delta * bracket
        k_cond = kg * (1.0 - np.sqrt(1.0 - porosity) + gamma * np.sqrt(1.0 - porosity))
        k_r = self.radiation_conductivity(self.T, particle_diameter, emissivity)
        k_contact = ks * contact_fraction * np.sqrt(1.0 - porosity)
        return k_cond + k_r * np.sqrt(1.0 - porosity) + k_contact


MZSDModel = ModifiedZSDModel


class ZehnerBauerSchluenderModel(BaseModel):
    """Original recorder ZBS implementation, retained for old analyses."""

    def ETC_fun(self, p, params):
        particle_diameter, lambda_null_gas, porosity, lambda_base = params
        B = self.deformation_parameter(porosity)
        lg = self.lambda_gas(p, porosity, particle_diameter, lambda_null_gas, self.beta)
        kg_ratio = 1.0 - lg / self.lambda_bulk
        term1 = 1.0 - np.sqrt(1.0 - porosity)
        term2 = 2.0 * np.sqrt(1.0 - porosity) / (kg_ratio * B)
        term3 = (
            kg_ratio * B / (1.0 - lg / self.lambda_bulk) ** 2
            * np.log(np.maximum(self.lambda_bulk / (lg * B), 1e-300))
        )
        term4 = (B + 1.0) / 2.0
        term5 = (B - 1.0) / (kg_ratio * B)
        return lambda_base + lg * (term1 + term2 * (term3 - term4 - term5))


ZBSModel = ZehnerBauerSchluenderModel
