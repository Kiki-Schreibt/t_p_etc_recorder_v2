import numpy as np
import pandas as pd


from recorder_app.infrastructure.handler.hydride_handler import MetalHydrideDatabase
from recorder_app.infrastructure.core import global_vars

from pandas.api.types import (
    is_integer_dtype, is_float_dtype, is_string_dtype
)
from typing import Optional, Sequence, Tuple, Union
from zoneinfo import ZoneInfo

try:
    import recorder_app.infrastructure.core.logger as logging
except ImportError:
    import logging


# Constants
R_H2 = global_vars.R_H2      # [J/(kg·K)] Specific gas constant for hydrogen
R_universal = global_vars.R_universal    # [J/(mol·K)] Universal gas constant
V_pipes = global_vars.V_pipes     # [m³] Pipe volume


class VantHoffCalcEq:
    """
    Thermodynamic calculator for hydrogen equilibrium pressure and uptake
    based on the Van't Hoff relation and ideal gas mass balance.

    This class provides a complete toolkit for evaluating hydrogen storage
    systems, including:

    - Equilibrium pressure calculation via the Van't Hoff equation
    - Hydrogen uptake (wt.%) computation from pressure–temperature data
    - Inverse reconstruction of pressures from known uptake values
    - Full and simplified ideal gas mass-balance models

    The implementation supports scalar, NumPy array, and pandas Series inputs,
    enabling both single-point evaluation and vectorized dataset processing.

    Core Thermodynamic Models
    -------------------------
    1. Van't Hoff relation:
        P_eq ∝ exp( -ΔH / (R * T) + ΔS / R )

    2. Ideal gas hydrogen mass balance:
        m = (p * (V_res + V_pipes)) / (R_H2 * T_res)
          + (p * V_cell) / (R_H2 * T_cell)

    3. Inverse pressure reconstruction from mass:
        p = m * R_H2 / (V/T terms)

    Key Features
    ------------
    - Flexible input handling (scalar, vector, Series)
    - Database-driven thermodynamic parameter retrieval (enthalpy/entropy)
    - Multiple pressure reconstruction strategies:
        * direct inversion
        * full mass-balance solution
        * simplified reverse model
    - Optional metadata integration for material-specific properties
    - Built-in validation against theoretical uptake limits

    Parameters
    ----------
    enthalpy : float, optional
        Reaction enthalpy ΔH in J/mol.
    entropy : float, optional
        Reaction entropy ΔS in J/(mol·K).
    meta_data : object, optional
        Metadata container expected to provide:
        - enthalpy, entropy
        - sample_material
        - theoretical_uptake (optional constraint)
    hydride : str, optional
        Hydride identifier used for database lookup if metadata is incomplete.
    db_conn_params : dict, optional
        Database connection parameters used for retrieving thermodynamic data.

    Attributes
    ----------
    enthalpy : float
        Active enthalpy value used in calculations.
    entropy : float
        Active entropy value used in calculations.
    meta_data : object
        Stored metadata object, if provided.
    db_conn_params : dict
        Database connection configuration.
    logger : logging.Logger
        Logger instance for runtime diagnostics.

    Notes
    -----
    - All temperature inputs are assumed to be in °C unless explicitly stated.
    - Pressures are handled in bar externally but converted to Pa internally.
    - Volumes must be provided in consistent units per method documentation.
    - Ideal gas behavior is assumed throughout; no real gas corrections are applied.
    - Constants such as `R_H2` and `V_pipes` are assumed to be defined at module level.
    - The class is designed for research-grade hydrogen storage modeling rather
      than high-pressure engineering certification.
    """

    def __init__(self,
                 enthalpy: float = None,
                 entropy: float = None,
                 meta_data=None,
                 hydride: str = None,
                 db_conn_params: dict = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.db_conn_params = db_conn_params or {}
        self.meta_data = meta_data
        #if self.meta_data:
        #    if self.meta_data.sample_id:
        self.enthalpy, self.entropy = self._get_enthalpy_entropy(enthalpy, entropy, hydride)

    def _get_enthalpy_entropy(self, enthalpy: float, entropy: float, hydride: str) -> tuple:
        """
        Resolve enthalpy and entropy values for a metal hydride system using a prioritized lookup strategy.

        The method determines the appropriate enthalpy (ΔH) and entropy (ΔS) values based on the following order:

        1. If both `enthalpy` and `entropy` are defined in `self.meta_data`, those values are returned.
        2. If `self.meta_data.sample_material` is defined, values are retrieved from the database
           using the sample material identifier.
        3. If a `hydride` name is provided, values are retrieved from the database using that identifier.
        4. If both `enthalpy` and `entropy` arguments are explicitly provided, those values are returned.
        5. If none of the above sources are available, default values for MgH2 are used.

        Parameters
        ----------
        enthalpy : float
            User-provided enthalpy value (ΔH) in J/mol. Used as a fallback if no metadata or database values are available.
        entropy : float
            User-provided entropy value (ΔS) in J/(mol·K). Used as a fallback if no metadata or database values are available.
        hydride : str
            Name of the hydride material used to query the database if metadata is incomplete.

        Returns
        -------
        tuple
            A tuple `(enthalpy, entropy)` where:
            - enthalpy is in J/mol
            - entropy is in J/(mol·K)

        Notes
        -----
        - Database lookups are performed via `MetalHydrideDatabase.get_enthalpy_entropy`.
        - Default fallback values correspond to MgH2:
            enthalpy = 7.4701e+04 J/mol
            entropy = 134.6944 J/(mol·K)
        - A log message is emitted when default values are used.
        """



        if hasattr(self.meta_data, 'enthalpy') and self.meta_data.enthalpy is not None \
           and hasattr(self.meta_data, 'entropy') and self.meta_data.entropy is not None:
            return self.meta_data.enthalpy, self.meta_data.entropy
        elif getattr(self.meta_data, 'sample_material', None):
            return MetalHydrideDatabase(self.db_conn_params).get_enthalpy_entropy(self.meta_data.sample_material)
        elif hydride:
            return MetalHydrideDatabase(self.db_conn_params).get_enthalpy_entropy(hydride)
        elif enthalpy is not None and entropy is not None:
            return enthalpy, entropy
        else:
            self.logger.info("No sample material or values provided. Using default values for MgH2.")
            return 7.4701e+04, 134.6944

    def _compute_vant_hoff(self, temperature_array: np.ndarray, enthalpy: float, entropy: float) -> np.ndarray:
        """
        Compute the equilibrium pressure term using the Van't Hoff equation.

        This method evaluates the exponential form of the Van't Hoff relation:

            P_eq ∝ exp( -ΔH / (R * T) + ΔS / R )

        where:
            - ΔH is the reaction enthalpy (J/mol)
            - ΔS is the reaction entropy (J/(mol·K))
            - R is the universal gas constant
            - T is the absolute temperature (K)

        The input temperature array is assumed to be in degrees Celsius and is internally
        converted to Kelvin.

        Parameters
        ----------
        temperature_array : np.ndarray
            Array of temperatures in degrees Celsius.
        enthalpy : float
            Reaction enthalpy ΔH in J/mol.
        entropy : float
            Reaction entropy ΔS in J/(mol·K).

        Returns
        -------
        np.ndarray
            Array of exponential Van't Hoff terms (dimensionless), proportional to equilibrium pressure.

        Notes
        -----
        - The returned values are not scaled by a reference pressure unless handled elsewhere.
        - Numerical stability may be affected for very large |ΔH| or very low temperatures.
        """
        T_kelvin = temperature_array + 273.15
        exponent = -enthalpy / (R_universal * T_kelvin) + entropy / R_universal
        return np.exp(exponent)

    def calc_eq(self, temperature, enthalpy: float = None, entropy: float = None):
        """
        Calculate the equilibrium pressure using the Van't Hoff relation.

        This method evaluates the equilibrium pressure (or proportional term) as a function
        of temperature based on the Van't Hoff equation. The computation is delegated to
        `_compute_vant_hoff`, with internal handling for different input types.

        Parameters
        ----------
        temperature : float, np.ndarray, or pandas.Series
            Temperature input in degrees Celsius. Can be a scalar, NumPy array,
            or pandas Series.
        enthalpy : float, optional
            Reaction enthalpy ΔH in J/mol. If provided יחד with `entropy`, overrides
            the instance attributes `self.enthalpy` and `self.entropy`.
        entropy : float, optional
            Reaction entropy ΔS in J/(mol·K). Must be provided יחד with `enthalpy`
            to override instance attributes.

        Returns
        -------
        float, np.ndarray, or pandas.Series
            Equilibrium pressure (or proportional Van't Hoff term), with output type
            matching the input:
            - scalar float if `temperature` is scalar
            - np.ndarray if input is array-like
            - pandas.Series if input is a Series (index preserved)

        Notes
        -----
        - Temperatures are internally converted from Celsius to Kelvin.
        - If both `enthalpy` and `entropy` are provided, they overwrite the instance
          attributes for subsequent calculations.
        - The returned value is the exponential Van't Hoff term and may require
          additional scaling (e.g., reference pressure) depending on the application.
        - Uses `np.atleast_1d` to normalize input for vectorized computation.
        """
        if enthalpy is not None and entropy is not None:
            self.enthalpy, self.entropy = enthalpy, entropy

        # Convert temperature input to at least a 1D NumPy array.
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
        Compute the Van't Hoff equilibrium curve over a specified temperature range.

        This method evaluates the exponential Van't Hoff relation across a discrete
        set of temperatures:

            P_eq ∝ exp( -ΔH / (R * T) + ΔS / R )

        where temperatures are provided in degrees Celsius and internally converted
        to Kelvin.

        Parameters
        ----------
        temperature_range : range, optional
            Range of temperatures in degrees Celsius. Default is `range(30, 500, 2)`.
            The range is converted to a NumPy array for vectorized computation.
        enthalpy : float, optional
            Reaction enthalpy ΔH in J/mol. If provided יחד with `entropy`, these values
            override the instance attributes for this calculation.
        entropy : float, optional
            Reaction entropy ΔS in J/(mol·K). Must be provided יחד with `enthalpy`
            to override instance attributes.

        Returns
        -------
        np.ndarray
            Array of Van't Hoff exponential terms (dimensionless), proportional to
            equilibrium pressure at each temperature in `temperature_range`.

        Notes
        -----
        - If `enthalpy` and `entropy` are not provided, `self.enthalpy` and
          `self.entropy` are used.
        - The output corresponds to the unscaled exponential term; any reference
          pressure factor must be applied externally if required.
        - The temperature range is explicitly materialized into a NumPy array,
          which may have memory implications for very large ranges.
        """

        used_enthalpy, used_entropy = (enthalpy, entropy) if (enthalpy is not None and entropy is not None) \
                                      else (self.enthalpy, self.entropy)
        temperature_array = np.array(list(temperature_range))
        return self._compute_vant_hoff(temperature_array, used_enthalpy, used_entropy)

    def _H2_mass_fun(self, p_list, T_list, V_res_list, V_cell, T_res_list):
        """
        Compute the mass of hydrogen gas in a coupled reservoir–cell system.

        This method evaluates the total hydrogen mass assuming ideal gas behavior
        in two volumes:
            1. Reservoir and connected piping volume
            2. Measurement cell volume

        The total mass is calculated as:

            m = (p * (V_res + V_pipes)) / (R_H2 * T_res)
              + (p * V_cell) / (R_H2 * T_cell)

        where pressure is converted from bar to Pa and temperatures are converted
        from Celsius to Kelvin.

        Parameters
        ----------
        p_list : float, list, np.ndarray, or pandas.Series
            Hydrogen pressure(s) in bar.
        T_list : float, list, np.ndarray, or pandas.Series
            Cell temperature(s) in degrees Celsius.
        V_res_list : float, list, np.ndarray, or pandas.Series
            Reservoir volume(s) in m³.
        V_cell : float
            Cell volume in m³.
        T_res_list : float, list, np.ndarray, or pandas.Series
            Reservoir temperature(s) in degrees Celsius.

        Returns
        -------
        float or np.ndarray
            Total hydrogen mass in kilograms:
            - scalar if all inputs resolve to a single value
            - NumPy array for vectorized inputs

        Notes
        -----
        - All inputs are internally converted to at least 1D NumPy arrays using
          `np.atleast_1d` to enable vectorized computation.
        - Pressure is assumed to be provided in bar and is converted to Pa via
          multiplication by 1e5.
        - `V_pipes` and `R_H2` are assumed to be defined in the enclosing scope
          or as class/module-level constants.
        - The calculation assumes ideal gas behavior and does not account for
          real gas effects (e.g., compressibility factor).
        - Shape compatibility between inputs must allow NumPy broadcasting.
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
        Calculate hydrogen uptake (wt.%) from hydride and dehydride measurements.

        This method determines the hydrogen mass difference between two states
        (hydrogenation and dehydrogenation) using the ideal gas law, and converts
        it into gravimetric uptake (weight percent):

            m_uptake = |m_dehyd - m_hyd|
            wt.% = 100 * m_uptake / (m_uptake + m_sample)

        The hydrogen masses are computed via `_H2_mass_fun`, which accounts for
        both reservoir and cell gas contributions.

        Parameters
        ----------
        p_hyd : float, list, np.ndarray, or pandas.Series
            Pressure during hydrogenation in bar.
        p_dehyd : float, list, np.ndarray, or pandas.Series
            Pressure during dehydrogenation in bar.
        T_hyd : float, list, np.ndarray, or pandas.Series
            Temperature during hydrogenation in °C.
        T_dehyd : float, list, np.ndarray, or pandas.Series
            Temperature during dehydrogenation in °C.
        V_res : float
            Reservoir volume in mL.
        V_cell : float
            Cell volume in µL.
        m_sample : float
            Sample mass in grams.
        T_reservoir : float, optional
            Reservoir temperature in °C. Default is 30 °C.

        Returns
        -------
        float, np.ndarray, or pandas.Series, or None
            Hydrogen uptake in weight percent:
            - scalar float if inputs are scalar
            - NumPy array for vectorized inputs
            - pandas.Series if input temperatures are Series (index preserved)
            - None if required inputs are missing

        Notes
        -----
        - Unit conversions applied internally:
            * Pressure: bar → Pa (×1e5)
            * Temperature: °C → K (+273.15)
            * Reservoir volume: mL → m³ (×1e-3)
            * Cell volume: µL → m³ (×1e-6)
            * Sample mass: g → kg (×1e-3)
        - The calculation assumes ideal gas behavior (no compressibility correction).
        - Absolute difference between hydride and dehydride states is used to ensure
          non-negative uptake.
        - If `self.meta_data.theoretical_uptake` is defined, results are filtered:
            values outside [0, theoretical_uptake + 0.2] are replaced with None.
        - All inputs are broadcast using NumPy semantics via `_H2_mass_fun`.
        - A log error is emitted and `None` is returned if any required input is missing.
        """

        # Convert m_sample to kg, and volumes to m³
        m_sample = np.round(m_sample * 1e-3, 9)  # g -> kg
        V_cell = V_cell * 1e-6                    # µL -> m³
        V_res = V_res * 1e-3                      # mL -> m³

        # Check for sufficient data; here we assume that zero-like values (0 or None) mean missing data.
        if (p_hyd is None or p_dehyd is None or T_hyd is None
            or T_dehyd is None or V_res is None or V_cell is None
            or m_sample is None or T_reservoir is None):
            self.logger.error("No sufficient data for uptake calculation")
            return None

        m_hyd = self._H2_mass_fun(p_list=p_hyd, T_list=T_hyd, V_res_list=V_res, V_cell=V_cell, T_res_list=T_reservoir)
        m_dehyd = self._H2_mass_fun(p_list=p_dehyd, T_list=T_dehyd, V_res_list=V_res, V_cell=V_cell, T_res_list=T_reservoir)
        # Calculate uptake and weight percentage
        m_uptake = abs(m_dehyd - m_hyd)
        wt_p = m_uptake * 100 / (m_uptake + m_sample)

        # If meta_data.theoretical_uptake is defined, enforce condition elementwise.
        if hasattr(self.meta_data, 'theoretical_uptake') and self.meta_data.theoretical_uptake is not None:
            condition_is_uptake = (wt_p <= self.meta_data.theoretical_uptake + 0.2) & (wt_p >= 0)
            result = np.where(condition_is_uptake, wt_p, None)
        else:
            result = wt_p
        # Return in the same type as the input temperature if it was a pandas Series.
        if isinstance(T_hyd, pd.Series):
            return pd.Series(result, index=T_hyd.index)
        if np.isscalar(result) or (hasattr(result, 'size') and result.size == 1):
            return float(np.atleast_1d(result)[0])
        return result

    def calc_delta_p(self, wt_p, m_sample,
                             p_hyd, p_dehyd, T_hyd,
                             T_dehyd, V_res, V_cell,
                             T_reservoir: float = 30):
        """
        Compute the pressure change corresponding to a given hydrogen uptake (wt.%).

        This method performs the inverse of the uptake calculation by reconstructing
        the pressure difference between hydrogenation and dehydrogenation states
        from a specified gravimetric hydrogen content. The hydrogen mass is first
        derived from the weight percentage:

            m_H2 = (wt.% / 100) * m_sample / (1 - wt.% / 100)

        The corresponding pressure change is then obtained using an ideal gas-based
        formulation consistent with `_H2_mass_fun`, accounting for both reservoir
        and cell volumes.

        Depending on which pressure is provided (`p_hyd` or `p_dehyd`), the method
        computes the missing counterpart:
            - If `p_hyd` is given → compute `p_dehyd`
            - If `p_dehyd` is given → compute `p_hyd`

        Parameters
        ----------
        wt_p : float, np.ndarray, or pandas.Series
            Hydrogen uptake in weight percent (wt.%).
        m_sample : float
            Sample mass in grams.
        p_hyd : float, np.ndarray, or pandas.Series, or None
            Hydrogenation pressure in bar. If None, it will be computed.
        p_dehyd : float, np.ndarray, or pandas.Series, or None
            Dehydrogenation pressure in bar. If None, it will be computed.
        T_hyd : float, np.ndarray, or pandas.Series
            Hydrogenation temperature in °C.
        T_dehyd : float, np.ndarray, or pandas.Series
            Dehydrogenation temperature in °C.
        V_res : float
            Reservoir volume in mL.
        V_cell : float
            Cell volume in µL.
        T_reservoir : float, optional
            Reservoir temperature in °C. Default is 30 °C.

        Returns
        -------
        tuple
            (p_hyd, p_dehyd), where each element is:
            - scalar float if inputs resolve to a single value
            - np.ndarray for vectorized inputs
            - pandas.Series if input pressures were Series (index preserved)

        Notes
        -----
        - Unit conversions applied internally:
            * Sample mass: g → kg (×1e-3)
            * Volume: mL → m³ (×1e-3), µL → m³ (×1e-6)
            * Temperature: °C → K (+273.15)
            * Pressure: returned in bar
        - Uses ideal gas assumptions; no real gas corrections are applied.
        - Requires exactly one of `p_hyd` or `p_dehyd` to be provided; if both
          are given, no adjustment is performed.
        - Computation is vectorized using NumPy broadcasting.
        - If inputs are pandas Series, the output preserves the original index.
        - Relies on constants `R_H2` and `V_pipes` defined in the enclosing scope.
        """

        # Convert sample mass and volumes
        m_sample = m_sample * 1e-3  # g -> kg
        wt_p_fraction = wt_p / 100  # Percentage to fraction
        m_H2 = wt_p_fraction * m_sample / (1 - wt_p_fraction)
        V_res = V_res * 1e-3       # mL -> m³
        V_cell = V_cell * 1e-6     # µL -> m³

        # Convert temperatures to Kelvin; support scalars or arrays
        T_hyd_K = np.atleast_1d(T_hyd + 273.15)
        T_dehyd_K = np.atleast_1d(T_dehyd + 273.15)
        T_res_K = np.atleast_1d(T_reservoir + 273.15)

        # Determine whether inputs are pandas Series so we can restore the index later.
        series_index = None
        if isinstance(p_hyd, pd.Series):
            series_index = p_hyd.index
        elif isinstance(p_dehyd, pd.Series):
            series_index = p_dehyd.index

        # Convert pressures to arrays if they are provided; if not, leave as None.
        p_hyd_arr = np.atleast_1d(p_hyd) if p_hyd is not None else None
        p_dehyd_arr = np.atleast_1d(p_dehyd) if p_dehyd is not None else None

        # Use vectorized operations for the reverse mass calculation.
        # The expression below is directly vectorized.
        def reverse_H2_mass_fun(T_cell):
            return m_H2 * R_H2 * 1e-5 / (((V_res + V_pipes) / T_res_K) + (V_cell / T_cell))

        # Compute the missing pressure if needed.
        if p_hyd_arr is not None and p_dehyd_arr is None:
            # Calculate p_dehyd elementwise from p_hyd.
            p_dehyd_arr = p_hyd_arr + reverse_H2_mass_fun(T_dehyd_K)
        elif p_dehyd_arr is not None and p_hyd_arr is None:
            # Calculate p_hyd elementwise from p_dehyd.
            p_hyd_arr = p_dehyd_arr - reverse_H2_mass_fun(T_hyd_K)

        # Prepare outputs. If the resulting arrays have one element, return a scalar.
        def maybe_return(x):
            return x[0] if x.size == 1 else x

        p_hyd_final = maybe_return(p_hyd_arr)
        p_dehyd_final = maybe_return(p_dehyd_arr)

        # If original input was a Series, return Series with the same index.
        if series_index is not None:
            p_hyd_final = pd.Series(p_hyd_final, index=series_index) if isinstance(p_hyd_final, np.ndarray) else pd.Series([p_hyd_final], index=series_index)
            p_dehyd_final = pd.Series(p_dehyd_final, index=series_index) if isinstance(p_dehyd_final, np.ndarray) else pd.Series([p_dehyd_final], index=series_index)
        return p_hyd_final, p_dehyd_final

    def calc_delta_p_full(self, wt_p, m_sample,
                            p_hyd, p_dehyd,
                            T_hyd, T_dehyd, V_res,
                            V_cell, T_reservoir=30):

        """
        Compute hydrogenation/dehydrogenation pressures from a given uptake (wt.%)
        using a full mass-balance formulation.

        This method reconstructs the missing pressure (`p_hyd` or `p_dehyd`) by
        explicitly solving the ideal gas mass balance between two thermodynamic
        states. The formulation accounts for temperature-dependent gas volumes
        in both the reservoir (including piping) and the measurement cell.

        The gas-phase hydrogen mass is expressed as:

            m_gas = (p * 1e5 * C(T)) / R_H2

        where:
            C(T) = (V_res + V_pipes) / T_res + V_cell / T_cell

        The hydrogen uptake (wt.%) is converted to absolute hydrogen mass:

            m_H2 = (wt.% / 100) * m_sample / (1 - wt.% / 100)

        The missing pressure is then obtained by enforcing:

            m_gas,dehyd = m_gas,hyd ± m_H2

        depending on the direction of the process.

        Parameters
        ----------
        wt_p : float or None
            Hydrogen uptake in weight percent (wt.%). If None, no mass change is applied.
        m_sample : float
            Sample mass in grams.
        p_hyd : float or None
            Hydrogenation pressure in bar. If None, it will be computed.
        p_dehyd : float or None
            Dehydrogenation pressure in bar. If None, it will be computed.
        T_hyd : float
            Hydrogenation temperature in °C.
        T_dehyd : float
            Dehydrogenation temperature in °C.
        V_res : float
            Reservoir volume in mL.
        V_cell : float
            Cell volume in µL.
        T_reservoir : float, optional
            Reservoir temperature in °C. Default is 30 °C.

        Returns
        -------
        tuple
            (p_hyd, p_dehyd) in bar.

        Notes
        -----
        - Unit conversions applied internally:
            * Sample mass: g → kg (×1e-3)
            * Volume: mL → m³ (×1e-3), µL → m³ (×1e-6)
            * Temperature: °C → K (+273.15)
            * Pressure: internally converted between bar and Pa
        - Uses ideal gas assumptions; no compressibility corrections are included.
        - Requires exactly one of `p_hyd` or `p_dehyd` to be provided.
        - `V_pipes` and `R_H2` are assumed to be defined in the enclosing scope.
        - Unlike `calc_delta_p`, this implementation explicitly computes intermediate
          gas masses, improving transparency and numerical traceability.
        """

        # --- Unit conversions ---
        m_sample = m_sample * 1e-3
        V_res = V_res * 1e-3
        V_cell = V_cell * 1e-6

        T_hyd_K = T_hyd + 273.15
        T_dehyd_K = T_dehyd + 273.15
        T_res_K = T_reservoir + 273.15

        # --- Gas coefficient ---
        def gas_coeff(T_cell):
            return ((V_res + V_pipes) / T_res_K) + (V_cell / T_cell)

        C_hyd = gas_coeff(T_hyd_K)
        C_dehyd = gas_coeff(T_dehyd_K)

        # --- Hydrogen mass from wt% ---
        if wt_p is not None:
            wt_frac = wt_p / 100
            m_H2 = wt_frac * m_sample / (1 - wt_frac)
        else:
            m_H2 = 0.0

        # --- Solve pressure relation ---
        # m_gas = p * C / R
        # total mass change = m_H2

        if p_hyd is not None and p_dehyd is None:
            m_gas_hyd = p_hyd * 1e5 * C_hyd / R_H2
            m_gas_dehyd = m_gas_hyd + m_H2
            p_dehyd = (m_gas_dehyd * R_H2) / (1e5 * C_dehyd)

        elif p_dehyd is not None and p_hyd is None:
            m_gas_dehyd = p_dehyd * 1e5 * C_dehyd / R_H2
            m_gas_hyd = m_gas_dehyd - m_H2
            p_hyd = (m_gas_hyd * R_H2) / (1e5 * C_hyd)

        return p_hyd, p_dehyd

    def _reverse_H2_mass_fun(self, m: float, T_cell: float, V_res: float, V_cell: float, T_res: float) -> float:
        """
        Compute pressure from hydrogen mass using the inverse ideal gas relation.

        This method inverts the hydrogen mass calculation used in `_H2_mass_fun`,
        solving for pressure given a known hydrogen mass distributed between the
        reservoir (including piping) and the measurement cell.

        The pressure is calculated as:

            p = m * R_H2 * 1e-5 / [ (V_res + V_pipes) / T_res + V_cell / T_cell ]

        where:
            - m is the hydrogen mass (kg)
            - R_H2 is the specific gas constant for hydrogen (J/(kg·K))
            - T_res and T_cell are absolute temperatures (K)
            - V_res, V_cell, and V_pipes are volumes in m³
            - The factor 1e-5 converts pressure from Pa to bar

        Parameters
        ----------
        m : float
            Hydrogen mass in kilograms.
        T_cell : float
            Cell temperature in Kelvin.
        V_res : float
            Reservoir volume in m³.
        V_cell : float
            Cell volume in m³.
        T_res : float
            Reservoir temperature in Kelvin.

        Returns
        -------
        float
            Pressure in bar.

        Notes
        -----
        - This function assumes scalar inputs and does not support vectorized operations.
        - Ideal gas behavior is assumed (no compressibility correction).
        - `V_pipes` and `R_H2` are expected to be defined in the enclosing scope.
        - Primarily used as a fallback or low-level helper for pressure reconstruction.
        """

        p = m * R_H2 * 1e-5 / (((V_res + V_pipes) / T_res) + (V_cell / T_cell))
        return p


Number = Union[int, float, np.number]
TimeLike = Union[pd.Timestamp, str]


class KineticCalcEquations:
    """
    Compute hydrogen uptake kinetics from pressure/temperature time series.

    Inputs:
        - DataFrame or (pressure, temperature) Series with a timezone-aware DatetimeIndex.
        - Geometry and sample info (V_cell [µL], V_res [mL], m_sample [g]).
        - Reservoir temperature can be scalar (°C) or a time series aligned to pressure.

    Features:
        - Uptake(t) from gas mass balance: uptake(t) = m_gas(t0) - m_gas(t)
          (for absorption; flips sign automatically for desorption if you prefer).
        - Kinetic slopes (dm/dt) in kg/s and d(wt%)/dt in %/min.
        - Optional resampling (e.g., '2S') and/or custom time windows ([(t0,t1), ...]).
        - Works with gaps/duplicates; enforces monotonic time within each window.

    Outputs:
        A pandas.DataFrame with:
            'p_bar', 'T_cell_C', 'T_res_C', 'm_gas_kg',
            'uptake_kg', 'uptake_wt_pct',
            'rate_kg_min', 'rate_pct_min'
    """

    def __init__(
        self,
        V_cell_mL: Number,
        V_res_L: Number,
        m_sample_g: Number,
        *,
        T_reservoir_C: Union[Number, pd.Series] = 30,
        absorption_sign: int = +1,
    ) -> None:
        """
        Args:
            V_cell_mL: Cell volume in µL.
            V_res_L: Reservoir volume in mL.
            m_sample_g: Sample mass in g.
            T_reservoir_C: Reservoir temperature (°C), scalar or Series aligned to pressure.
            columns: (pressure_col, temperature_col) in the input DataFrame.
            absorption_sign: +1 for uptake = m(t0)-m(t) (absorption),
                             -1 for uptake = m(t)-m(t0) (desorption).
        """
        from recorder_app.infrastructure.core.table_config import TableConfig
        self.V_cell_m3 = float(V_cell_mL) * 1e-6
        self.V_res_m3 = float(V_res_L) * 1e-3
        self.m_sample_kg = float(m_sample_g) * 1e-3
        self.tp_table = TableConfig().TPDataTable
        self.kinetics_table = TableConfig().KineticsTable
        self.p_col, self.T_cell_col = self.tp_table.pressure, self.tp_table.temperature_sample

        if absorption_sign not in (+1, -1):
            raise ValueError("absorption_sign must be +1 (absorption) or -1 (desorption).")
        self.sign = absorption_sign
        self.T_reservoir_C = T_reservoir_C

    # ---- public API ---------------------------------------------------------

    def compute(
        self,
        df: pd.DataFrame,
        *,
        intervals: Optional[Sequence[Tuple[TimeLike, TimeLike]]] = None,
        resample_rule: Optional[str] = None,
        resample_how: str = "nearest",  # 'ffill'|'bfill'|'nearest'|'mean'
        smooth_seconds: Optional[Number] = None,  # optional rolling mean for noise (seconds)
        enforce_monotonic: bool = True,
        reaction_duration=None
    ) -> pd.DataFrame:
        """
        Main entry: returns a DataFrame with uptake & kinetics columns.

        Args:
            df: DataFrame containing at least pressure and temperature columns with
                a tz-aware DatetimeIndex. Pressure in bar; temperature in °C.
            intervals: Optional list of (start, end) times to analyze. Outside
                these windows is ignored. Results from windows are concatenated.
            resample_rule: Optional pandas offset alias (e.g., '1S', '200L').
            resample_how: Aggregation/alignment strategy when resampling.
            smooth_seconds: Optional centered rolling average window (seconds) to
                reduce noise before differentiating.
            enforce_monotonic: If True, drops duplicate/descending timestamps per window.

        Returns:
            DataFrame indexed by time with computed columns.
        """
        df_in = df.copy()
        time_col = self.tp_table.time
        if time_col not in df_in.columns:
            raise KeyError(f"time_col '{time_col}' not in DataFrame.")
        df_in.index = pd.to_datetime(df_in[time_col], errors="raise")
        df_in.drop(columns=[time_col], inplace=True)
        df_in = self._ensure_dt_index(df_in)

        if reaction_duration is not None:
            df_in = self._ensure_df_in_reaction_time(df_in=df_in,
                                                     reaction_duration=reaction_duration)
        if df_in.empty:
            return df_in

        # Basic column checks
        for col in (self.p_col, self.T_cell_col):
            if col not in df_in.columns:
                raise KeyError(f"Missing required column '{col}' in DataFrame.")

        # T_reservoir handling (scalar or Series)
        T_res_series = self._align_T_reservoir(df_in.index)

        # Handle intervals → list of sliced DataFrames
        windows = self._slice_into_windows(df_in, intervals)

        # Process each window independently (keeps baseline m_gas(t0) per window)
        out_chunks = []
        for win_df in windows:
            if win_df.empty:
                continue
            # optional resample
            if resample_rule:
                win_df, T_res_win = self._resample_window(
                    win_df, T_res_series, resample_rule, resample_how
                )
            else:
                T_res_win = T_res_series.reindex(win_df.index, method=None)

            # clean/monotonic
            if enforce_monotonic:
                win_df = self._ensure_monotonic(win_df)

            # optional smoothing
            if smooth_seconds and smooth_seconds > 0:
                win_df = self._smooth(win_df, smooth_seconds)
                if isinstance(T_res_win, pd.Series):
                    T_res_win = self._smooth_series(T_res_win, smooth_seconds)

            # compute gas mass and kinetics
            out_chunks.append(self._compute_window(win_df, T_res_win))

        if not out_chunks:
            return pd.DataFrame(columns=[
                self.p_col, self.T_cell_col, self.kinetics_table.temperature_res,
                self.kinetics_table.m_gas_kg, self.kinetics_table.uptake_kg,
                self.kinetics_table.uptake_wt_p,
                self.kinetics_table.rate_kg_min, self.kinetics_table.rate_wt_p_min
            ])

        out = pd.concat(out_chunks).sort_index()
        out[self.kinetics_table.time_delta_min] = (out.index - out.index[0]) / pd.Timedelta(seconds=1)/60
        if self.sign < 0:
            out[self.kinetics_table.uptake_kg] *= self.sign
            out[self.kinetics_table.uptake_wt_p] *= self.sign
            out[self.kinetics_table.rate_kg_min] *= self.sign
            out[self.kinetics_table.rate_wt_p_min] *= self.sign

        return out

    # ---- internals ----------------------------------------------------------

    def _align_T_reservoir(self, index: pd.DatetimeIndex) -> pd.Series:
        """Return a Series of reservoir temperature (°C) aligned to index."""
        if isinstance(self.T_reservoir_C, pd.Series):
            # Ensure timezone/align
            s = self.T_reservoir_C.copy()
            if s.index.tz is None and index.tz is not None:
                raise ValueError("T_reservoir series must have a tz-aware index to match df.")
            return s.reindex(index, method="nearest")  # rough alignment
        else:
            return pd.Series(self.T_reservoir_C, index=index, dtype=float)

    def _slice_into_windows(
        self,
        df: pd.DataFrame,
        intervals: Optional[Sequence[Tuple[TimeLike, TimeLike]]],
    ) -> list:
        if intervals is None:
            return [df]
        chunks = []
        for start, end in intervals:
            start_ts = pd.to_datetime(start)
            end_ts = pd.to_datetime(end)
            # keep tz — if naive, interpret in df's tz
            if start_ts.tz is None and df.index.tz is not None:
                start_ts = start_ts.tz_localize(df.index.tz)
            if end_ts.tz is None and df.index.tz is not None:
                end_ts = end_ts.tz_localize(df.index.tz)
            chunks.append(df.loc[start_ts:end_ts])
        return chunks

    def _resample_window(
        self,
        win_df: pd.DataFrame,
        T_res_series: pd.Series,
        rule: str,
        how: str,
    ) -> Tuple[pd.DataFrame, pd.Series]:
        if how == "ffill":
            df_r = win_df.resample(rule).ffill()
            Tres_r = T_res_series.reindex(df_r.index).ffill()
        elif how == "bfill":
            df_r = win_df.resample(rule).bfill()
            Tres_r = T_res_series.reindex(df_r.index).bfill()
        elif how == "nearest":
            # use asfreq + nearest reindex to avoid averaging
            idx = pd.date_range(win_df.index.min(), win_df.index.max(), freq=rule, tz=win_df.index.tz)
            df_r = win_df.reindex(idx, method="nearest")
            Tres_r = T_res_series.reindex(idx, method="nearest")
        elif how == "mean":
            df_r = win_df.resample(rule).mean(numeric_only=True)
            Tres_r = T_res_series.reindex(df_r.index, method="nearest")
        else:
            raise ValueError("resample_how must be one of: 'ffill', 'bfill', 'nearest', 'mean'.")
        return df_r, Tres_r

    def _ensure_monotonic(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[~df.index.duplicated(keep="first")].sort_index()
        # drop negative time deltas
        return df

    def _smooth(self, df: pd.DataFrame, seconds: Number) -> pd.DataFrame:
        window = f"{int(seconds)}s"
        out = df.copy()
        out[self.p_col] = df[self.p_col].rolling(window, center=True, min_periods=1).mean()
        out[self.T_cell_col] = df[self.T_cell_col].rolling(window, center=True, min_periods=1).mean()
        return out

    def _smooth_series(self, s: pd.Series, seconds: Number) -> pd.Series:
        window = f"{int(seconds)}s"
        return s.rolling(window, center=True, min_periods=1).mean()

    def _gas_mass_kg(
        self,
        p_bar: pd.Series,
        T_cell_C: pd.Series,
        T_res_C: pd.Series,
    ) -> pd.Series:
        """
        m_gas = p * (V_res+V_pipes)/R/T_res + p * V_cell/R/T_cell
        with unit conversions to SI.
        """
        p_Pa = p_bar.astype(float) * 1e5
        T_cell_K = T_cell_C.astype(float) + 273.15
        T_res_K = T_res_C.astype(float) + 273.15

        term_res = p_Pa * (self.V_res_m3 + V_pipes) / (R_H2 * T_res_K)
        term_cell = p_Pa * self.V_cell_m3 / (R_H2 * T_cell_K)
        return term_res + term_cell  # [kg]

    def _compute_window(
        self,
        win_df: pd.DataFrame,
        T_res_win: pd.Series,
    ) -> pd.DataFrame:
        dfw = self._ensure_dt_index(win_df[[self.p_col, self.T_cell_col]].copy())
        dfw[self.kinetics_table.temperature_res] = T_res_win.astype(float)

        # Gas mass in the rig
        dfw[self.kinetics_table.m_gas_kg] = self._gas_mass_kg(dfw[self.kinetics_table.pressure],
                                                              dfw[self.kinetics_table.temperature],
                                                              dfw[self.kinetics_table.temperature_res])

        # Baseline at window start
        m0 = dfw[self.kinetics_table.m_gas_kg].iloc[0]

        # Uptake definition: absorption (+) = m0 - m(t); desorption (+) = m(t) - m0
        dfw[self.kinetics_table.uptake_kg] = self.sign * (m0 - dfw[self.kinetics_table.m_gas_kg])
        # Keep uptake non-negative for the chosen direction
        dfw[self.kinetics_table.uptake_kg] = dfw[self.kinetics_table.uptake_kg].clip(lower=0)

        # wt% over time
        dfw[self.kinetics_table.uptake_wt_p] = 100.0 * dfw[self.kinetics_table.uptake_kg] / (dfw[self.kinetics_table.uptake_kg] + self.m_sample_kg)

        # Time delta in seconds for derivatives
        dt_s = (dfw.index.to_series().diff().dt.total_seconds()).astype(float)
        dt_s = pd.Series([np.nan, *dt_s.iloc[1:]], index=dfw.index)

        # Rates (simple backward diff). For less noise, you can smooth before.
        dfw[self.kinetics_table.rate_kg_min] = dfw[self.kinetics_table.uptake_kg].diff() / (dt_s / 60.0)
        dfw[self.kinetics_table.rate_wt_p_min] = dfw[self.kinetics_table.uptake_wt_p].diff() / (dt_s / 60.0)

        return dfw

    def _ensure_dt_index(self, df: pd.DataFrame) -> pd.DataFrame:
        idx = df.index

        # Case 1: already datetime
        if isinstance(idx, pd.DatetimeIndex):
            if idx.tz is None:
                df = df.copy()
                df.index = idx.tz_localize(ZoneInfo("Europe/Berlin"))
            return df

        # Case 2: numeric → refuse (this is what led to 1970 microseconds)
        if is_integer_dtype(idx) or is_float_dtype(idx):
            raise TypeError(
                "DataFrame index is numeric. Please pass a tz-aware DatetimeIndex "
                "or specify time_col=... so I can build it."
            )

        # Case 3: strings/objects → parse
        if is_string_dtype(idx) or idx.dtype == "object":
            new_idx = pd.to_datetime(idx, errors="raise")
            if new_idx.tz is None:
                new_idx = new_idx.tz_localize(ZoneInfo("Europe/Berlin"))
            df = df.copy()
            df.index = new_idx
            return df

        raise TypeError("Unsupported index type for time axis.")

    def _ensure_df_in_reaction_time(self, df_in, reaction_duration):
        dur = _to_timedelta(reaction_duration)
        if dur <= pd.Timedelta(0):
            return pd.DataFrame()  # or raise ValueError
        t0 = df_in.index.min()
        t_end = t0 + dur
        # inclusive on both ends; adjust if you prefer half-open [t0, t_end)
        df_in = df_in.loc[(df_in.index >= t0) & (df_in.index <= t_end)]
        if df_in.empty:
            return pd.DataFrame(columns=[
                self.p_col, self.T_cell_col, self.kinetics_table.temperature_res,
                self.kinetics_table.m_gas_kg, self.kinetics_table.uptake_kg,
                self.kinetics_table.uptake_wt_p, self.kinetics_table.rate_kg_min,
                self.kinetics_table.rate_wt_p_min
            ])
        return df_in

###helper methods
def _to_timedelta(value) -> pd.Timedelta:
    """Accepts pd.Timedelta, numpy timedelta, strings like '45s','10min','1H',
       or a number (interpreted as seconds)."""
    import pandas as pd
    if isinstance(value, pd.Timedelta):
        return value
    try:
        # numeric -> seconds
        if isinstance(value, (int, float)):
            return pd.to_timedelta(value, unit="s")
        # strings / numpy timedeltas
        return pd.to_timedelta(value)
    except Exception as e:
        raise ValueError(f"Invalid reaction_duration {value!r}: {e}")

# Example test functions to check behavior:


def test_calc_eq(meta_data, db_conn_params):
    calculator = VantHoffCalcEq(meta_data=meta_data)
    # Scalar input
    eq_pressure_scalar = calculator.calc_eq(temperature=350)
    print("Equilibrium Pressure (scalar):", eq_pressure_scalar)

    # Using a pandas Series
    temp_series = pd.Series([350, 360, 370], index=[0, 1, 2])
    eq_pressure_series = calculator.calc_eq(temperature=temp_series)
    print("Equilibrium Pressure (Series):")
    print(eq_pressure_series)


def test_calc_h2_uptake(meta_data, db_conn_params):
    calculator = VantHoffCalcEq(meta_data=meta_data)
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
    from recorder_app.infrastructure.handler.metadata_handler import MetaData
    from recorder_app.infrastructure.core.config_reader import config
    meta_data = MetaData(sample_id="WAE-WA-040", db_conn_params=config.db_conn_params)
    test_calc_eq(meta_data, db_conn_params=config.db_conn_params)
    test_calc_h2_uptake(meta_data,db_conn_params=config.db_conn_params)
