"""Effective thermal-conductivity models for gas-solid powder beds.

Keeps the recorder's historical Kaganer/ZBS models and adds:
- classical Zehner-Schlünder (ZS)
- modified S-ZBS with the Smoluchowski effect
- Zehner-Schlünder-Damköhler (ZSD)
- modified ZSD with temperature-dependent H2 transport
- heat-transfer-concentrating (HTC) model for metal-hydride beds
"""
from __future__ import annotations
import numpy as np

_NIST_H2_T_K = np.array([200,250,300,350,400,450,500,550,600], dtype=float)
_NIST_H2_K_MW = np.array(
    [132.27,160.44,185.63,210.20,233.94,256.84,280.40,304.11,327.99],
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
        factors = {"bar": 1e5, "bara": 1e5, "pa": 1.0, "kpa": 1e3, "mpa": 1e6}
        if unit not in factors:
            raise ValueError(f"Unsupported pressure unit: {self.pressure_unit}")
        return p * factors[unit]

    def char_length(self, porosity, particle_diameter):
        if np.any((np.asarray(porosity) <= 0) | (np.asarray(porosity) >= 1)):
            raise ValueError("Porosity must be strictly between 0 and 1.")
        return (2.0 / 3.0) * porosity / (1.0 - porosity) * particle_diameter

    def mean_free_path(self, p):
        p_pa = np.maximum(self.pressure_pa(p), np.finfo(float).tiny)
        return self.mp.kB * self.T / (np.sqrt(2.0) * self.mp.sigma0 * p_pa)

    def nist_h2_conductivity(self):
        """Zero-density normal-H2 conductivity, NIST SRD23 values."""
        if not 200.0 <= self.T <= 600.0:
            raise ValueError(f"NIST H2 conductivity table covers 200--600 K; got {self.T:.1f} K.")
        return float(np.interp(self.T, _NIST_H2_T_K, _NIST_H2_K_MW)) * 1e-3

    def gas_conductivity(self, p, porosity, particle_diameter, beta=None,
                         lambda_null_gas=None, use_nist=True, conductivity_scale=1.0):
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
        return self.gas_conductivity(p, porosity, particle_diameter, beta=beta,
                                     lambda_null_gas=lambda_null_gas, use_nist=False)

    def kapa_kaganer(self, p, porosity, particle_diameter, lambda_null_gas, beta, lambda_particle):
        return 1.0 - self.lambda_gas(
            p, porosity, particle_diameter, lambda_null_gas, beta
        ) / lambda_particle

    @staticmethod
    def deformation_parameter(porosity):
        return 1.25 * ((1.0 - porosity) / porosity) ** (10.0 / 9.0)

    @staticmethod
    def radiation_conductivity(temperature_k, particle_diameter, emissivity=0.8):
        sigma = 5.670374419e-8
        view_factor = emissivity / (1.0 - 0.132 * emissivity)
        return 4.0 * view_factor * sigma * temperature_k**3 * particle_diameter

    def _zs_conduction(self, kg, porosity, ks):
        B = self.deformation_parameter(porosity)
        ratio = kg / max(float(ks), 1e-300)
        denominator = 1.0 - ratio * B
        denominator = np.where(np.abs(denominator) < 1e-12, 1e-12, denominator)
        bracket = (
            ((1.0 - ratio) * B / denominator**2)
            * np.log(np.maximum(1.0 / (ratio * B), 1e-300))
            - (B + 1.0) / 2.0
            - (B - 1.0) / denominator
        )
        return kg * (
            1.0 - np.sqrt(1.0 - porosity)
            + 2.0 * np.sqrt(1.0 - porosity) / denominator * bracket
        )


class KaganerModel(BaseModel):
    """Historical Kaganer implementation."""

    def ETC_fun(self, p, params):
        d, lambda_null_gas, porosity, lambda_base = params
        lg = self.lambda_gas(p, porosity, d, lambda_null_gas, self.beta)
        kg = self.kapa_kaganer(p, porosity, d, lambda_null_gas, self.beta, self.lambda_bulk)
        return lambda_base + lg * (
            5.81 * (1.0 - porosity) ** 2 / kg
            * (np.log(self.lambda_bulk / lg) / kg - 1.0 - kg / 2.0) + 1.0
        )


class ZehnerSchluenderModel(BaseModel):
    """Classical Zehner-Schlünder model."""

    def ETC_fun(self, p, params):
        d, lambda_null_gas, porosity, lambda_base = params
        kg = self.lambda_gas(p, porosity, d, lambda_null_gas, self.beta)
        return self._zs_conduction(kg, porosity, lambda_base)


class ModifiedSZBSModel(BaseModel):
    """2025-style simplified ZBS with Smoluchowski-corrected gas conductivity.

    The model uses the ZS particle geometry but replaces the gas conductivity
    with kg = kg,inf(T)/(1 + 2 beta Kn), as recommended for stagnant powder beds.
    """

    parameter_names = ("particle_diameter", "gas_conductivity_scale",
                       "porosity", "solid_conductivity")

    def default_parameters(self):
        return (float(self.mp.particle_diameter), 1.0,
                float(self.mp.porosity), float(self.mp.lambda_solid))

    def ETC_fun(self, p, params):
        d, gas_scale, porosity, ks = params
        kg = self.gas_conductivity(
            p, porosity, d, beta=self.beta, use_nist=True,
            conductivity_scale=gas_scale
        )
        return self._zs_conduction(kg, porosity, ks)


class ZehnerSchluenderDamkohlerModel(BaseModel):
    """ZSD model with radiation and particle-contact conduction."""

    parameter_names = ("particle_diameter", "gas_conductivity_scale",
                       "porosity", "solid_conductivity",
                       "contact_fraction", "emissivity")

    def default_parameters(self):
        return (float(self.mp.particle_diameter), 1.0, float(self.mp.porosity),
                float(self.mp.lambda_solid),
                float(getattr(self.mp, "contact_fraction", 3e-4)),
                float(getattr(self.mp, "emissivity", 0.8)))

    def ETC_fun(self, p, params):
        d, gas_scale, porosity, ks, contact_fraction, emissivity = params
        kg = self.gas_conductivity(
            p, porosity, d, beta=self.beta, use_nist=True,
            conductivity_scale=gas_scale
        )
        k_cond = self._zs_conduction(kg, porosity, ks)
        k_rad = self.radiation_conductivity(self.T, d, emissivity)
        k_contact = ks * contact_fraction * np.sqrt(1.0 - porosity)
        return k_cond + k_rad * np.sqrt(1.0 - porosity) + k_contact


ModifiedZSDModel = ZehnerSchluenderDamkohlerModel
MZSDModel = ZehnerSchluenderDamkohlerModel


class HeatTransferConcentratingModel(BaseModel):
    """Bai et al. heat-transfer-concentrating model (HTC).

    The 2023 model explicitly represents the dominant particle-gas-film-
    particle pathway. G is the fitted gas-volume concentration factor.
    """

    parameter_names = ("gas_conductivity_scale", "porosity",
                       "solid_conductivity", "gas_volume_factor")

    def default_parameters(self):
        return (1.0, float(self.mp.porosity), float(self.mp.lambda_solid), 3.8)

    def ETC_fun(self, p, params):
        gas_scale, porosity, ks, gas_volume_factor = params
        kg = self.gas_conductivity(
            p, porosity, float(self.mp.particle_diameter),
            beta=self.beta, use_nist=True, conductivity_scale=gas_scale
        )
        G = max(float(gas_volume_factor), 1e-12)
        return (
            ks * kg * (porosity / G + 1.0 - porosity)
            / ((porosity / G) * ks + kg * (1.0 - porosity))
        )


class ZehnerBauerSchluenderModel(BaseModel):
    """Historical recorder ZBS implementation, retained for old analyses."""

    def ETC_fun(self, p, params):
        d, lambda_null_gas, porosity, lambda_base = params
        B = self.deformation_parameter(porosity)
        lg = self.lambda_gas(p, porosity, d, lambda_null_gas, self.beta)
        kg = 1.0 - lg / self.lambda_bulk
        term1 = 1.0 - np.sqrt(1.0 - porosity)
        term2 = 2.0 * np.sqrt(1.0 - porosity) / (kg * B)
        term3 = kg * B / (1.0 - lg / self.lambda_bulk) ** 2 * np.log(
            np.maximum(self.lambda_bulk / (lg * B), 1e-300)
        )
        term4 = (B + 1.0) / 2.0
        term5 = (B - 1.0) / (kg * B)
        return lambda_base + lg * (term1 + term2 * (term3 - term4 - term5))


ZBSModel = ZehnerBauerSchluenderModel
