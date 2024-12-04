import numpy as np

from src.calculations.hydride_worker import MetalHydrideDatabase
try:
    import src.config_connection_reading_management.logger as logging
except ImportError:
    import logging
from src.meta_data.meta_data_handler import MetaData

# Constants
R_H2 = 4124.49         # [J/kg K] Specific gas constant for hydrogen
R_universal = 8.31447  # [J/(mol K)] Universal gas constant
V_pipes = 1e-7         # [m^3] Pipe volume


class VantHoffCalcEq:
    def __init__(self, enthalpy=None, entropy=None, meta_data=None, sample_id=None, hydride=None):
        """
        Initialize the VantHoffCalcEq with default values for entropy and enthalpy.

        Parameters:
            enthalpy (float): enthalpy in J mol^-1
            entropy (float): entropy in J mol^-1 K^-1
            meta_data (meta_data): meta_data object that contains enthalpy and entropy

        """

        self.logger = logging.getLogger(__name__)
        self.enthalpy = None
        self.entropy = None

        if meta_data:
            self.meta_data = meta_data
        else:
            self.meta_data = MetaData()

        if self.meta_data.enthalpy and self.meta_data.entropy:
            self.enthalpy = self.meta_data.enthalpy
            self.entropy = self.meta_data.entropy

        elif self.meta_data.sample_material:
            mh_database = MetalHydrideDatabase()
            self.enthalpy, self.entropy = mh_database.get_enthalpy_entropy(hydride_to_grab=self.meta_data.sample_material)

        elif sample_id:
            self.meta_data = MetaData(sample_id=sample_id)
            self.enthalpy = self.meta_data.enthalpy
            self.entropy = self.meta_data.entropy

        elif hydride:
            mh_database = MetalHydrideDatabase()
            self.enthalpy, self.entropy = mh_database.get_enthalpy_entropy(hydride_to_grab=hydride)

        elif enthalpy and entropy:
            self.enthalpy = enthalpy
            self.entropy = entropy
        elif not self.enthalpy and not self.entropy:
            self.enthalpy = 7.4701e+04
            self.entropy = 134.6944
            self.logger.info("No sample material entered or test does not exist. Standard values of MgH2 will be taken for enthalpy and entropy")



    def calc_eq(self, temperature, enthalpy=None, entropy=None):

        """
        Calculate the Van't Hoff equation value for a given temperature.

        Parameters:
        temperature (float): Temperature in degrees Celsius.

        mode:  'calc', 'inf', 'none'

        Returns:
        float: Calculated value or None if an error occurs.
        """
        if enthalpy and entropy:
            self.enthalpy = enthalpy
            self.entropy = entropy
            self.logger.info("Enthalpy and Entropy entered by user are considered in eq pressure calculation")

        try:
            if self.enthalpy and self.entropy:
                #temperature = float(temperature)
                term1 = -self.enthalpy / (R_universal * (temperature + 273.15))
                term2 = self.entropy / R_universal

                return np.exp(term1 + term2)
            else:
                self.logger.error("An error occured in calc_eq. No enthalpy, temperature or pressure found ")
                return np.array([])
        except Exception as e:
            # Log the exception if needed
            #print("nononononone")
            self.logger.error(f"An error occurred in calc_eq: {e}")
            return np.array([])

    def calc_vant_hoff_lin(self, temperature_range=range(30, 500, 2), enthalpy=None, entropy=None):
        if enthalpy and entropy and temperature_range:
            temperature_array = np.array(temperature_range)  # Convert range to numpy array
            term1 = -enthalpy / (R_universal * (temperature_array + 273.15))
            term2 = entropy / R_universal
            return np.exp(term1 + term2)
        elif self.enthalpy and self.entropy and temperature_range:
            temperature_array = np.array(temperature_range)  # Convert range to numpy array
            term1 = -self.enthalpy / (R_universal * (temperature_array + 273.15))
            term2 = self.entropy / R_universal
            return np.exp(term1 + term2)
        else:
            self.logger.error("No enthalpy or entropy for eq curve calculation")
            return np.array([])

    def calc_h2_uptake(self, p_hyd, p_dehyd, T_hyd, T_dehyd, V_res, V_cell, m_sample, T_reservoir=30):
        m_sample = np.round(m_sample * 1e-3, 9)  # This works with both single values and Series
        V_cell = V_cell*1e-6
        V_res = V_res*1e-3


        # Assuming _H2_mass_fun can handle both Series and single values
        m_hyd = self._H2_mass_fun(p_list=p_hyd, T_list=T_hyd, V_res_list=V_res, V_cell=V_cell, T_res_list=T_reservoir)
        m_dehyd = self._H2_mass_fun(p_list=p_dehyd, T_list=T_dehyd, V_res_list=V_res, V_cell=V_cell, T_res_list=T_reservoir)

        # Calculate hydrogen uptake
        m_uptake = m_dehyd - m_hyd
        wt_p = m_uptake * 100 / (m_uptake + m_sample)

        # Handling theoretical uptake comparison
        if hasattr(self.meta_data, 'theoretical_uptake') and self.meta_data.theoretical_uptake is not None:
            # Vectorized comparison using numpy where
            condition_is_uptake = (wt_p <= self.meta_data.theoretical_uptake + 0.2) & (wt_p >= 0)

            result = np.where(condition_is_uptake, wt_p, None)

            if isinstance(result, np.ndarray) and result.size == 1:
                return result.item()
            return result
        else:
            result = wt_p
            if isinstance(result, np.ndarray) and result.size == 1:
                return result.item()
            return wt_p

    def _H2_mass_fun(self, p_list, T_list, V_res_list, V_cell, T_res_list):
        """
        Calculate the mass of hydrogen gas using the given pressure and temperature lists.

        Parameters:
        - p_list (list of floats): List of pressures in bar.
        - T_list (list of floats): List of temperatures in Celsius.
        - volume_res (float): Volume of the reservoir.
        - volume_pipes (float): Volume of the pipes.
        - volume_auto (float): Volume of the automatic system.
        - R_H2 (float): Gas constant for hydrogen.

        Returns:
        - mass_list (list of floats): List of masses of hydrogen gas in kg.
        """
        if isinstance(T_res_list, list):
            T_res_list = [273.15 + T for T in T_res_list]
        else:
            T_res_list = T_res_list + 273.15 # T ambient in K
        # Convert temperatures from Celsius to Kelvin


        if isinstance(T_list, list):
            T_K_list = [273.15 + T for T in T_list]
        else:
            T_K_list = 273.15 + T_list

        # Calculate the mass of hydrogen gas for each pressure and temperature
        mass_list = []
        if isinstance(p_list, list) and isinstance(T_res_list, list):
            for p, T_K, V_res, T_res in zip(p_list, T_K_list, V_res_list, T_res_list):
                mass = ((p * 1e5 * (V_res + V_pipes)) / (R_H2 * T_res)) \
                       + ((p * 1e5 * V_cell) / (R_H2 * T_K))
                mass_list.append(mass)

        elif isinstance(p_list, list):
            for p, T_K, V_res in zip(p_list, T_K_list, V_res_list):
                mass = ((p * 1e5 * (V_res + V_pipes)) / (R_H2 * T_res_list)) \
                       + ((p * 1e5 * V_cell) / (R_H2 * T_K))
                mass_list.append(mass)
        else:
                mass_list = ((p_list * 1e5 * (V_res_list + V_pipes)) / (R_H2 * T_res_list)) \
                            + ((p_list * 1e5 * V_cell) / (R_H2 * T_K_list))

        return mass_list

    def calc_delta_p(self, wt_p, m_sample, p_hyd, p_dehyd, T_hyd, T_dehyd, V_res, V_cell, T_reservoir=30):
        """
        Reverse the calculation to find the pressure change from weight percentage.

        Parameters:
        - wt_p: Weight percentage
        - m_sample: Sample mass
        - T_hyd: Temperature during hydride
        - T_dehyd: Temperature during dehydride
        - V_res: Reservoir volume
        - V_cell: Cell volume
        - T_reservoir: Reservoir temperature

        Returns:
        - p_hyd: Pressure during hydride
        - p_dehyd: Pressure during dehydride
        """
        m_sample = m_sample * 1e-3

        # Calculate the uptake mass from the weight percentage
        wt_p /= 100  # Convert to fraction
        m_H2 = wt_p * m_sample / (1 - wt_p)
        V_res = V_res * 1e-3
        V_cell = V_cell * 1e-6
        # Convert temperatures from Celsius to Kelvin
        T_hyd = 273.15 + T_hyd
        T_dehyd = 273.15 + T_dehyd
        T_res = 273.15 + T_reservoir

        # Calculate the pressure for hydride and dehydride
        if p_hyd and not p_dehyd:
            p_dehyd = p_hyd + self._reverse_H2_mass_fun(m=m_H2, T_cell=T_hyd,
                                                      V_res=V_res, V_cell=V_cell,
                                                      T_res=T_res)
        elif p_dehyd and not p_hyd:
            p_hyd = p_dehyd - self._reverse_H2_mass_fun(m=m_H2, T_cell=T_dehyd,
                                                      V_res=V_res, V_cell=V_cell,
                                                      T_res=T_res)
        return p_hyd, p_dehyd

    def _reverse_H2_mass_fun(self, m, T_cell, V_res, V_cell, T_res):
        """
        Reverse the mass function to calculate pressure.

        Parameters:
        - m: Mass of hydrogen
        - T_K: Temperature in Kelvin
        - V_res: Reservoir volume
        - V_cell: Cell volume
        - T_res_K: Reservoir temperature in Kelvin

        Returns:
        - p: Pressure in bar
        """

        p = m * R_H2 * 1e-5 / (((V_res+V_pipes)/T_res) + (V_cell/T_cell))

       # p = (m * R_H2 * T_res) / (1e5 * (V_res + V_pipes)) + (m * R_H2 * T_cell) / (1e5 * V_cell)
        return p


def test_calc_delta_p():
    m = 10
    p_hyd = None
    p_dehyd = 20.126531419458807
    T_hyd = 350
    T_dehyd = 400
    wt_p = 7.6
    V_res = 1
    V_cell = 32
    calculator = VantHoffCalcEq()
    p_hyd = calculator.calc_delta_p(wt_p=wt_p,
                                      p_hyd=p_hyd,
                                      p_dehyd=p_dehyd,
                                      T_hyd=T_hyd,
                                      T_dehyd=T_dehyd,
                                      V_res=V_res,
                                      V_cell=V_cell,
                                      m_sample=m)
    print(p_hyd)


if __name__ == "__main__":

    test_calc_delta_p()
    #sample_id = "WAE-WA-040"
    #calculator = VantHoffCalcEq(sample_id=sample_id)

    #print(calculator.meta_data.print())
