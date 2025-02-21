import numpy as np
import logging
from src.calculations.hydride_worker import MetalHydrideDatabase
from src.meta_data.meta_data_handler import MetaData

# Constants
R_H2: float = 4124.49         # [J/(kg·K)] Specific gas constant for hydrogen
R_universal: float = 8.31447  # [J/(mol·K)] Universal gas constant
V_pipes: float = 1e-7         # [m³] Pipe volume


class VantHoffCalcEq:
    """
    Class to calculate hydrogen equilibrium pressure and uptake values using the Van't Hoff equation.
    """

    def __init__(self,
                 enthalpy: float = None,
                 entropy: float = None,
                 meta_data: MetaData = None,
                 sample_id: str = None,
                 hydride: str = None,
                 db_conn_params: dict = None) -> None:
        """
        Initialize the VantHoffCalcEq with enthalpy and entropy values.

        The values are determined in the following order:
          1. Use the provided meta_data if it contains enthalpy and entropy.
          2. If meta_data has a sample_material, retrieve values from MetalHydrideDatabase.
          3. If hydride is provided, retrieve its values.
          4. If enthalpy and entropy are provided directly, use them.
          5. Otherwise, fall back to default values (e.g., for MgH2).

        Parameters:
            enthalpy (float, optional): Enthalpy in J/mol.
            entropy (float, optional): Entropy in J/(mol·K).
            meta_data (MetaData, optional): Metadata instance with pre-stored values.
            sample_id (str, optional): Identifier for the sample.
            hydride (str, optional): Hydride material name.
            db_conn_params (dict, optional): Database connection parameters.
        """
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params or {}
        self.meta_data = meta_data or MetaData(sample_id=sample_id, db_conn_params=self.db_conn_params)
        self.enthalpy, self.entropy = self._get_enthalpy_entropy(enthalpy, entropy, hydride)

    def _get_enthalpy_entropy(self, enthalpy: float, entropy: float, hydride: str) -> tuple:
        """
        Determine enthalpy and entropy based on provided inputs and metadata.

        Returns:
            tuple: (enthalpy, entropy)
        """
        if self.meta_data.enthalpy is not None and self.meta_data.entropy is not None:
            return self.meta_data.enthalpy, self.meta_data.entropy
        elif getattr(self.meta_data, 'sample_material', None):
            return MetalHydrideDatabase().get_enthalpy_entropy(self.meta_data.sample_material)
        elif hydride:
            return MetalHydrideDatabase().get_enthalpy_entropy(hydride)
        elif enthalpy is not None and entropy is not None:
            return enthalpy, entropy
        else:
            self.logger.info("No sample material or values provided. Using default values for MgH2.")
            return 7.4701e+04, 134.6944

    def _compute_vant_hoff(self, temperature_array: np.ndarray, enthalpy: float, entropy: float) -> np.ndarray:
        """
        Compute the Van't Hoff equation term for an array of temperatures.

        Parameters:
            temperature_array (np.ndarray): Temperatures in Celsius.
            enthalpy (float): Enthalpy value.
            entropy (float): Entropy value.

        Returns:
            np.ndarray: Equilibrium pressures computed via the Van't Hoff equation.
        """
        T_kelvin = temperature_array + 273.15
        exponent = -enthalpy / (R_universal * T_kelvin) + entropy / R_universal
        return np.exp(exponent)

    def calc_eq(self, temperature: float, enthalpy: float = None, entropy: float = None) -> float:
        """
        Calculate the equilibrium pressure using the Van't Hoff equation at a given temperature.

        Parameters:
            temperature (float): Temperature in Celsius.
            enthalpy (float, optional): Enthalpy in J/mol.
            entropy (float, optional): Entropy in J/(mol·K).

        Returns:
            float: Equilibrium pressure.
        """
        if enthalpy is not None and entropy is not None:
            self.enthalpy, self.entropy = enthalpy, entropy
            self.logger.info("Using user-provided enthalpy and entropy for eq calculation.")
        result = self._compute_vant_hoff(np.array([temperature]), self.enthalpy, self.entropy)
        return float(result[0]) if result.size > 0 else 0.0

    def calc_vant_hoff_lin(self, temperature_range: range = range(30, 500, 2),
                            enthalpy: float = None, entropy: float = None) -> np.ndarray:
        """
        Calculate the Van't Hoff curve over a range of temperatures.

        Parameters:
            temperature_range (range, optional): Temperature range in Celsius.
            enthalpy (float, optional): Enthalpy in J/mol.
            entropy (float, optional): Entropy in J/(mol·K).

        Returns:
            np.ndarray: Array of equilibrium pressures.
        """
        used_enthalpy, used_entropy = (enthalpy, entropy) if (enthalpy is not None and entropy is not None) \
                                      else (self.enthalpy, self.entropy)
        temperature_array = np.array(list(temperature_range))
        return self._compute_vant_hoff(temperature_array, used_enthalpy, used_entropy)

    def calc_h2_uptake(self, p_hyd, p_dehyd, T_hyd, T_dehyd, V_res, V_cell, m_sample, T_reservoir: float = 30):
        """
        Calculate the hydrogen uptake based on hydride and dehydride measurements.

        Parameters:
            p_hyd: Pressure during hydride (bar).
            p_dehyd: Pressure during dehydride (bar).
            T_hyd: Temperature during hydride (°C).
            T_dehyd: Temperature during dehydride (°C).
            V_res: Reservoir volume (mL).
            V_cell: Cell volume (µL).
            m_sample: Sample mass (g).
            T_reservoir (float, optional): Reservoir temperature (°C).

        Returns:
            The weight percentage of hydrogen uptake.
        """
        m_sample = np.round(m_sample * 1e-3, 9)  # Convert sample mass from g to kg
        V_cell = V_cell * 1e-6  # Convert cell volume from µL to m³
        V_res = V_res * 1e-3    # Convert reservoir volume from mL to m³

        m_hyd = self._H2_mass_fun(p_list=p_hyd, T_list=T_hyd, V_res_list=V_res, V_cell=V_cell, T_res_list=T_reservoir)
        m_dehyd = self._H2_mass_fun(p_list=p_dehyd, T_list=T_dehyd, V_res_list=V_res, V_cell=V_cell, T_res_list=T_reservoir)

        # Calculate hydrogen uptake and weight percentage
        m_uptake = m_dehyd - m_hyd
        wt_p = m_uptake * 100 / (m_uptake + m_sample)

        if hasattr(self.meta_data, 'theoretical_uptake') and self.meta_data.theoretical_uptake is not None:
            condition_is_uptake = (wt_p <= self.meta_data.theoretical_uptake + 0.2) & (wt_p >= 0)
            result = np.where(condition_is_uptake, wt_p, None)
            return result.item() if isinstance(result, np.ndarray) and result.size == 1 else result
        else:
            return wt_p.item() if isinstance(wt_p, np.ndarray) and wt_p.size == 1 else wt_p

    def _H2_mass_fun(self, p_list, T_list, V_res_list, V_cell, T_res_list):
        """
        Calculate the mass of hydrogen gas using the provided pressure and temperature values.

        Parameters:
            p_list: Pressure(s) in bar.
            T_list: Temperature(s) in °C.
            V_res_list: Reservoir volume(s) in m³.
            V_cell: Cell volume in m³.
            T_res_list: Reservoir temperature(s) in °C or Kelvin.

        Returns:
            Mass (or list of masses) of hydrogen gas in kg.
        """
        # Convert reservoir temperature(s) to Kelvin
        if isinstance(T_res_list, list):
            T_res_list = [273.15 + T for T in T_res_list]
        else:
            T_res_list = T_res_list + 273.15

        # Convert T_list from Celsius to Kelvin
        if isinstance(T_list, list):
            T_K_list = [273.15 + T for T in T_list]
        else:
            T_K_list = T_list + 273.15

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

    def calc_delta_p(self, wt_p, m_sample, p_hyd, p_dehyd, T_hyd, T_dehyd, V_res, V_cell, T_reservoir: float = 30):
        """
        Reverse the calculation to find the pressure change from a given weight percentage.

        Parameters:
            wt_p: Weight percentage (e.g., 7.6 for 7.6%).
            m_sample: Sample mass in g.
            p_hyd: Pressure during hydride (bar), can be None.
            p_dehyd: Pressure during dehydride (bar), can be None.
            T_hyd: Temperature during hydride in °C.
            T_dehyd: Temperature during dehydride in °C.
            V_res: Reservoir volume in mL.
            V_cell: Cell volume in µL.
            T_reservoir (float, optional): Reservoir temperature in °C.

        Returns:
            tuple: (p_hyd, p_dehyd) calculated pressures.
        """
        m_sample = m_sample * 1e-3  # Convert g to kg
        wt_p_fraction = wt_p / 100  # Convert percentage to fraction
        m_H2 = wt_p_fraction * m_sample / (1 - wt_p_fraction)
        V_res = V_res * 1e-3       # Convert mL to m³
        V_cell = V_cell * 1e-6     # Convert µL to m³

        T_hyd_K = T_hyd + 273.15
        T_dehyd_K = T_dehyd + 273.15
        T_res_K = T_reservoir + 273.15

        if p_hyd is not None and p_dehyd is None:
            p_dehyd = p_hyd + self._reverse_H2_mass_fun(m=m_H2, T_cell=T_hyd_K,
                                                         V_res=V_res, V_cell=V_cell, T_res=T_res_K)
        elif p_dehyd is not None and p_hyd is None:
            p_hyd = p_dehyd - self._reverse_H2_mass_fun(m=m_H2, T_cell=T_dehyd_K,
                                                         V_res=V_res, V_cell=V_cell, T_res=T_res_K)
        return p_hyd, p_dehyd

    def _reverse_H2_mass_fun(self, m: float, T_cell: float, V_res: float, V_cell: float, T_res: float) -> float:
        """
        Reverse the hydrogen mass function to calculate pressure from hydrogen mass.

        Parameters:
            m (float): Mass of hydrogen in kg.
            T_cell (float): Cell temperature in Kelvin.
            V_res (float): Reservoir volume in m³.
            V_cell (float): Cell volume in m³.
            T_res (float): Reservoir temperature in Kelvin.

        Returns:
            float: Pressure in bar.
        """
        p = m * R_H2 * 1e-5 / (((V_res + V_pipes) / T_res) + (V_cell / T_cell))
        return p


def test_calc_delta_p() -> None:
    """
    Test the calc_delta_p method.
    """
    m_sample = 10  # grams
    p_hyd = None
    p_dehyd = 20.126531419458807  # bar
    T_hyd = 350   # °C
    T_dehyd = 400  # °C
    wt_p = 7.6
    V_res = 1     # mL
    V_cell = 32   # µL
    calculator = VantHoffCalcEq()
    p_hyd, p_dehyd = calculator.calc_delta_p(wt_p=wt_p,
                                               m_sample=m_sample,
                                               p_hyd=p_hyd,
                                               p_dehyd=p_dehyd,
                                               T_hyd=T_hyd,
                                               T_dehyd=T_dehyd,
                                               V_res=V_res,
                                               V_cell=V_cell)
    print("Calculated p_hyd:", p_hyd)
    print("Calculated p_dehyd:", p_dehyd)


if __name__ == "__main__":
    from src.config_connection_reading_management.config_reader import GetConfig
    config = GetConfig()
    # Instantiate VantHoffCalcEq with dependency injection for db_conn_params
    calculator = VantHoffCalcEq(sample_id="WAE-WA-040", db_conn_params=config.db_conn_params)
    # Example usage: calculate equilibrium pressure at 100°C
    eq_pressure = calculator.calc_eq(temperature=350)
    print("Equilibrium Pressure at 100°C:", eq_pressure)

    # Run test for calc_delta_p
    test_calc_delta_p()
