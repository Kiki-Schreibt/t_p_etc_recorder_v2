#models.py
import numpy as np


class BaseModel:
    """
    Base model containing common functions for calculating thermal conductivity.
    """
    def __init__(self, material_properties, Temperature, beta):
        self.mp = material_properties
        self.lambda_bulk = self.mp.lambda_solid
        self.T = Temperature + 273.15  # Convert Celsius to Kelvin
        self.beta = beta

    def char_length(self, porosity, particle_diameter):
        return (2/3) * (porosity / (1 - porosity)) * particle_diameter

    def mean_free_path(self, p):
        return self.mp.kB * self.T / (np.sqrt(2) * self.mp.sigma0 * p)

    def lambda_gas(self, p, porosity, particle_diameter, lambda_null_gas, beta):
        kn = self.mean_free_path(p) / self.char_length(porosity, particle_diameter)
        return lambda_null_gas / (1 + 2 * beta * kn)

    def kapa_kaganer(self, p, porosity, particle_diameter, lambda_null_gas, beta, lambda_particle):
        return 1 - self.lambda_gas(p, porosity, particle_diameter, lambda_null_gas, beta) / lambda_particle


class KaganerModel(BaseModel):
    """
    Implements the Kaganer model for effective thermal conductivity.
    """
    def __init__(self, material_properties, Temperature, beta):
        super().__init__(material_properties, Temperature, beta)

    def ETC_fun(self, p, params):
        particle_diameter, lambda_null_gas, porosity, lambda_base = params
        lambda_gas_val = self.lambda_gas(p, porosity, particle_diameter, lambda_null_gas, self.beta)
        k_g = self.kapa_kaganer(p, porosity, particle_diameter, lambda_null_gas, self.beta, self.lambda_bulk)
        return lambda_base + (lambda_gas_val * (5.81 * ((1 - porosity) ** 2) / k_g) *
                              (1 / k_g * np.log(self.lambda_bulk / lambda_gas_val) - 1 - (k_g / 2)) + 1)


class ZehnerBauerSchluenderModel(BaseModel):
    """
    Implements the Zehner-Bauer-Schlunder model.
    """
    def __init__(self, material_properties, Temperature, beta):
        super().__init__(material_properties, Temperature, beta)

    def ETC_fun(self, p, params):
        particle_diameter, lambda_null_gas, porosity, lambda_base = params

        def b(porosity):
            return 1.25 * ((1 - porosity) / porosity) ** (10 / 9)

        l_g = self.lambda_gas(p, porosity, particle_diameter, lambda_null_gas, self.beta)
        k_g = self.kapa_kaganer(p, porosity, particle_diameter, lambda_null_gas, self.beta, self.lambda_bulk)
        term1 = (1 - np.sqrt(1 - porosity))
        term2 = (2 * np.sqrt(1 - porosity) / (k_g * b(porosity)))
        term3 = (k_g * b(porosity) / (1 - l_g / self.lambda_bulk) ** 2 * np.log(self.lambda_bulk / (l_g * b(porosity))))
        term4 = ((b(porosity) + 1) / 2)
        term5 = ((b(porosity) - 1) / (k_g * b(porosity)))
        return lambda_base + l_g * (term1 + term2 * (term3 - term4 - term5))
