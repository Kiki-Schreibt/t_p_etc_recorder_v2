import numpy as np
import pandas as pd
import logging
from src.calculations.hydride_worker import MetalHydrideDatabase
from src.meta_data.meta_data_handler import MetaData

# Constants
R_H2: float = 4124.49         # [J/(kg·K)] Specific gas constant for hydrogen
R_universal: float = 8.31447    # [J/(mol·K)] Universal gas constant
V_pipes: float = 1e-7         # [m³] Pipe volume


class VantHoffCalcEq:
    """
    Class to calculate hydrogen equilibrium pressure and uptake values using the Van't Hoff equation.

    This version has been adapted to allow inputs as scalars, NumPy arrays, or pandas Series.
    """
    def __init__(self,
                 enthalpy: float = None,
                 entropy: float = None,
                 meta_data: MetaData = None,
                 sample_id: str = None,
                 hydride: str = None,
                 db_conn_params: dict = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params or {}
        self.meta_data = meta_data or MetaData(sample_id=sample_id, db_conn_params=self.db_conn_params)
        self.enthalpy, self.entropy = self._get_enthalpy_entropy(enthalpy, entropy, hydride)

    def _get_enthalpy_entropy(self, enthalpy: float, entropy: float, hydride: str) -> tuple:
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
        The temperatures are assumed to be in Celsius.
        """
        T_kelvin = temperature_array + 273.15
        exponent = -enthalpy / (R_universal * T_kelvin) + entropy / R_universal
        return np.exp(exponent)

    def calc_eq(self, temperature, enthalpy: float = None, entropy: float = None):
        """
        Calculate the equilibrium pressure using the Van't Hoff equation at a given temperature.
        The input temperature can be a scalar, NumPy array, or pandas Series (in °C).

        Returns:
            A scalar (if input is scalar) or an array/Series of equilibrium pressures.
        """
        if enthalpy is not None and entropy is not None:
            self.enthalpy, self.entropy = enthalpy, entropy

        # Convert temperature input to at least a 1D NumPy array
        temperature_array = np.atleast_1d(temperature)
        result = self._compute_vant_hoff(temperature_array, self.enthalpy, self.entropy)

        # If the input was a pandas Series, convert the result back to a Series with the same index.
        if isinstance(temperature, pd.Series):
            return pd.Series(result, index=temperature.index)
        # Return a scalar if only one value was given.
        if result.size == 1:
            return float(result[0])
        return result

    def calc_vant_hoff_lin(self, temperature_range: range = range(30, 500, 2),
                            enthalpy: float = None, entropy: float = None) -> np.ndarray:
        """
        Calculate the Van't Hoff curve over a range of temperatures (°C).
        """
        used_enthalpy, used_entropy = (enthalpy, entropy) if (enthalpy is not None and entropy is not None) \
                                      else (self.enthalpy, self.entropy)
        temperature_array = np.array(list(temperature_range))
        return self._compute_vant_hoff(temperature_array, used_enthalpy, used_entropy)

    def _H2_mass_fun(self, p_list, T_list, V_res_list, V_cell, T_res_list):
        """
        Calculate the mass of hydrogen gas using vectorized operations.
        Accepts scalar, list, NumPy array, or pandas Series inputs.
        Temperatures (T_list and T_res_list) are assumed to be in °C.
        """
        # Convert all inputs to at least 1D NumPy arrays
        p_arr = np.atleast_1d(p_list)
        T_arr = np.atleast_1d(T_list) + 273.15  # Convert to Kelvin
        V_res_arr = np.atleast_1d(V_res_list)
        T_res_arr = np.atleast_1d(T_res_list) + 273.15  # Convert to Kelvin

        mass_arr = (p_arr * 1e5 * (V_res_arr + V_pipes)) / (R_H2 * T_res_arr) \
                   + (p_arr * 1e5 * V_cell) / (R_H2 * T_arr)
        return mass_arr if mass_arr.size > 1 else mass_arr.item()

    def calc_h2_uptake(self, p_hyd, p_dehyd, T_hyd, T_dehyd, V_res, V_cell, m_sample, T_reservoir: float = 30):
        """
        Calculate the hydrogen uptake based on hydride and dehydride measurements.
        Input pressures and temperatures can be scalars, lists, NumPy arrays, or pandas Series.

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
            The weight percentage of hydrogen uptake as a scalar, NumPy array, or pandas Series.
        """
        # Convert m_sample to kg, and volumes to m³
        m_sample = np.round(m_sample * 1e-3, 9)  # g -> kg
        V_cell = V_cell * 1e-6                    # µL -> m³
        V_res = V_res * 1e-3                      # mL -> m³

        if (not p_hyd or not p_dehyd or not T_hyd
            or not T_dehyd or not V_res or not V_cell
            or not m_sample or not T_reservoir):
            self.logger.error("No sufficient data for uptake calculation")
            return None



        m_hyd = self._H2_mass_fun(p_list=p_hyd, T_list=T_hyd, V_res_list=V_res, V_cell=V_cell, T_res_list=T_reservoir)
        m_dehyd = self._H2_mass_fun(p_list=p_dehyd, T_list=T_dehyd, V_res_list=V_res, V_cell=V_cell, T_res_list=T_reservoir)

        # Calculate uptake and weight percentage
        m_uptake = m_dehyd - m_hyd
        wt_p = m_uptake * 100 / (m_uptake + m_sample)

        # If meta_data.theoretical_uptake is defined, use it as a condition
        if hasattr(self.meta_data, 'theoretical_uptake') and self.meta_data.theoretical_uptake is not None:
            condition_is_uptake = (wt_p <= self.meta_data.theoretical_uptake + 0.2) & (wt_p >= 0)
            result = np.where(condition_is_uptake, wt_p, None)
        else:
            result = wt_p

        # If the original input (e.g., T_hyd) was a pandas Series, return a Series with the same index.
        if isinstance(T_hyd, pd.Series):
            return pd.Series(result, index=T_hyd.index)
        # If the result is a single value, return a scalar.
        if np.isscalar(result) or (hasattr(result, 'size') and result.size == 1):
            return float(np.atleast_1d(result)[0])
        return result

    def calc_delta_p(self, wt_p, m_sample, p_hyd, p_dehyd, T_hyd, T_dehyd, V_res, V_cell, T_reservoir: float = 30):
        """
        Reverse the calculation to find the pressure change from a given weight percentage.
        This method currently assumes scalar inputs.
        """
        m_sample = m_sample * 1e-3  # Convert g to kg
        wt_p_fraction = wt_p / 100  # Percentage to fraction
        m_H2 = wt_p_fraction * m_sample / (1 - wt_p_fraction)
        V_res = V_res * 1e-3       # mL -> m³
        V_cell = V_cell * 1e-6     # µL -> m³

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
        This version assumes scalar values.
        """
        p = m * R_H2 * 1e-5 / (((V_res + V_pipes) / T_res) + (V_cell / T_cell))
        return p

# Example test functions to check behavior:


def test_calc_eq():
    calculator = VantHoffCalcEq(sample_id="WAE-WA-040")
    # Scalar input
    eq_pressure_scalar = calculator.calc_eq(temperature=350)
    print("Equilibrium Pressure (scalar):", eq_pressure_scalar)

    # Using a pandas Series
    temp_series = pd.Series([350, 360, 370], index=[0, 1, 2])
    eq_pressure_series = calculator.calc_eq(temperature=temp_series)
    print("Equilibrium Pressure (Series):")
    print(eq_pressure_series)


def test_calc_h2_uptake():
    calculator = VantHoffCalcEq(sample_id="WAE-WA-040")
    # Example scalar inputs
    wt_p_scalar = calculator.calc_h2_uptake(
        p_hyd=10, p_dehyd=20, T_hyd=350, T_dehyd=400,
        V_res=1, V_cell=32, m_sample=10, T_reservoir=30)
    print("H2 Uptake (scalar):", wt_p_scalar)

    # Example with pandas Series (using same index for demonstration)
    T_hyd_series = pd.Series([350, 350, 350], index=[0, 1, 2])
    T_dehyd_series = pd.Series([400, 400, 400], index=[0, 1, 2])
    p_hyd_series = pd.Series([10, 10, 10], index=[0, 1, 2])
    p_dehyd_series = pd.Series([20, 20, 20], index=[0, 1, 2])
    uptake_series = calculator.calc_h2_uptake(
        p_hyd=p_hyd_series, p_dehyd=p_dehyd_series,
        T_hyd=T_hyd_series, T_dehyd=T_dehyd_series,
        V_res=1, V_cell=32, m_sample=10, T_reservoir=30)
    print("H2 Uptake (Series):")
    print(uptake_series)


if __name__ == "__main__":
    test_calc_eq()
    test_calc_h2_uptake()
