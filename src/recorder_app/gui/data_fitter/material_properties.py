#material_properties.py
import numpy as np


from recorder_app.infrastructure.handler.hydride_handler import MetalHydrideDatabase
from recorder_app.infrastructure.core.config_reader import config

try:
    import recorder_app.infrastructure.core.logger as logging
except ImportError:
    import logging

class MaterialProperties:
    """
    Handles material-specific properties and calculates auxiliary parameters.
    """
    def __init__(self, sample_mass=18.0741e-3, cell_type='3rd', cell_volume=None,
                 material=None, de_hyd_state="Dehydrogenated", particle_diameter=45e-6):
        if not material:
            return  # No material specified; nothing to do.
        self.logger = logging.getLogger(__name__)
        self.hydride_worker = MetalHydrideDatabase(db_conn_params=config.db_conn_params)
        self.material = material  # e.g., "MgH2"
        self.particle_diameter = particle_diameter

        # Constants
        self.gas = 'H2'
        self.lambda_null_gas = 0.018  # W/m/K
        self.kB = 1.3806e-23  # Boltzmann constant
        self.sigma0 = 0.27e-18  # Collision cross-section
        Cp = 14.199  # J/kg/K
        Cv = 10.074  # J/kg/K
        self.kappa = Cp / Cv
        self.Pr = 4 * self.kappa / (9 * self.kappa - 5)  # Prandtl number

        self.sample_mass = self.get_mass(material, sample_mass)
        self.volume_cell = self.get_cell_volume(cell_volume, cell_type)

        self.molar_mass_gas = self.get_molar_mass(self.gas)
        self.molar_mass_material = self.get_molar_mass(self.material)
        self.density_material = self.get_density(self.material)
        self.porosity = 1 - ((self.sample_mass) / (self.volume_cell * self.density_material))
        self.lambda_solid = self.get_initial_conductivity(self.material, de_hyd_state)

        self.alpha_values = self.calculate_alpha_values(self.molar_mass_gas, self.molar_mass_material)
        self.beta_values = {name: self.calculate_beta(alpha, name)
                            for name, alpha in self.alpha_values.items()}

    def get_cell_volume(self, cell_volume=None, cell_type=None):
        if cell_volume:
            return cell_volume
        if cell_type == '3rd':
            return 40.37e-6  # m^3
        if cell_type == '2nd':
            return 32e-6  # m^3
        self.logger.info("No cell volume found")
        return None

    def get_molar_mass(self, material):
        return self.hydride_worker.get_molar_mass_hydride(hydride_name=material)

    def get_density(self, material):
        return self.hydride_worker.get_density(hydride_name=material) * 1e3  # kg/m³

    def get_mass(self, material, sample_mass):
        capacity = self.hydride_worker.get_capacity(hydride_name=material)
        return sample_mass + sample_mass * capacity / 100 if capacity else sample_mass

    def get_initial_conductivity(self, material, de_hyd_state):
        return self.hydride_worker.get_bulk_conductivity(hydride_name=material, state=de_hyd_state)

    def calculate_alpha_values(self, M_gas, M_solid):
        alpha_baule = 2 * M_gas * M_solid / (M_gas + M_solid) ** 2
        alpha_goodman = 2.4 * (M_gas / M_solid) / (1 + (M_gas / M_solid)) ** 2
        alpha_kaganer = 1 - ((M_solid - M_gas) / (M_solid + M_gas)) ** 2
        alpha_bauer = 1 - 144.6 / ((M_gas + 12) ** 2)
        return {
            "alpha_baule": alpha_baule,
            "alpha_goodman": alpha_goodman,
            "alpha_kaganer": alpha_kaganer,
            "alpha_bauer": alpha_bauer
        }

    def calculate_beta(self, alpha, name):
        if alpha > 0:
            beta1 = ((2 - alpha) / alpha * 2 * self.kappa / (self.kappa + 1)) / self.Pr
            beta2 = 0.5 * ((2 - alpha) / alpha * 2 * self.kappa / (self.kappa + 1)) / self.Pr
            beta3 = (2 - alpha) / alpha
            beta4 = (5 * np.pi / 32) * (9 * self.kappa - 5) / (self.kappa + 1) * ((2 - alpha) / alpha)
            return {"beta1_" + name: beta1,
                    "beta2_" + name: beta2,
                    "beta3_" + name: beta3,
                    "beta4_" + name: beta4}
        return None
