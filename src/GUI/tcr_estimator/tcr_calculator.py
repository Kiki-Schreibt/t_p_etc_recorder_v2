#tcr_calculator.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Optional, Tuple, Dict
import numpy as np
import pandas as pd

try:
    # Savitzky–Golay is optional; used only if user asks for smoothing
    from scipy.signal import savgol_filter  # type: ignore
    _HAS_SCIPY = True
except Exception:
    _HAS_SCIPY = False


from src.infrastructure.core.global_vars import STANDARD_TCR_FOLDER_PATH
from src.config_connection_reading_management.database_reading_writing import DataRetriever
from src.infrastructure.core.table_config import TableConfig
try:
    import src.infrastructure.core.logger as logging
except ImportError:
    import logging
"""tcr_calculator.py

A small utility for computing Temperature Coefficient of Resistance (TCR)
from pandas DataFrames, with helpers to analyze regions below/above a Curie
temperature.

Expected input
--------------
A pandas DataFrame with at least two columns:
  - temperature column (°C or K; units cancel in derivative)
  - resistance column (Ω)

By default the class expects columns named 'T' and 'R'. You can override
via the 't_col' and 'r_col' parameters.

Definitions
-----------
- *Pointwise TCR*: \alpha(T) = (1/R) * dR/dT, numerically estimated by
  a central difference (first-order) or Savitzky–Golay smoothing if desired.
- *Interval TCR*: perform a linear fit R = a*T + b over an interval
  [Tmin, Tmax], and report \bar\alpha = a / R_ref, where R_ref can be chosen as:
    * 'Tin'   : R at Tmin (interpolated),
    * 'Tmid'  : R at the midpoint of the interval (interpolated) [default],
    * 'custom': a user-supplied temperature T_ref (interpolated),
    * 'meanR' : the mean R in the interval (units: 1/°C).

Around a Curie temperature, behavior is often non-linear. You can either:
  - compute pointwise TCR in a narrow window around T_Curie, or
  - compute interval TCR for the regions below and above T_Curie.

Example
-------
>>> import pandas as pd
>>> from tcr_calculator import TCRCalculator
>>> df = pd.DataFrame({'T':[300, 320, 340, 360, 380], 'R':[100, 104, 109, 130, 170]})
>>> calc = TCRCalculator(curie_temp=356)
>>> tcr_series = calc.pointwise_tcr(df)
>>> below, above = calc.split_by_curie(df, delta=10)  # 346–356 and 356–366
>>> interval = calc.interval_tcr(df, Tmin=340, Tmax=355, ref='Tmid')

Notes
-----
- Input is sorted by temperature and duplicate T rows are averaged.
- For stability near sharp transitions, consider using a small Savitzky–Golay
  smoothing window in pointwise TCR (set sg_window to an odd integer).
- Celsius vs Kelvin does not change alpha because dR/dT uses the same scale.
"""


class DataLoaderTCR:

    def __init__(self, config, sensor_type):
        self.config = config
        self.sensor_type = sensor_type  # "_, F1, F2"
        self.db_retriever = DataRetriever(db_conn_params=self.config.db_conn_params)
        self.logger = logging.getLogger(__name__)

    def load_resistance_values(self, time_range):
        table = TableConfig().ETCDataTable
        df = self.db_retriever.fetch_data_by_time_no_limit(table=table, time_range=time_range)
        df = df.sort_values(by=[table.get_clean('temperature')])
        if df.empty:
            self.logger.info(f"No data found in time range: {" -> ".join(time_range)}")
            return
        return df


@dataclass
class TCRCalculator:
    curie_temp: float = 356.0
    t_col: str = TableConfig().ETCDataTable.get_clean("temperature")
    r_col: str = TableConfig().ETCDataTable.get_clean("disk_resistance")

    def _prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.t_col not in df.columns or self.r_col not in df.columns:
            raise ValueError(f"DataFrame must contain columns '{self.t_col}' and '{self.r_col}'.")
        # Drop NaNs, group duplicate temperatures by mean, sort by temperature
        clean = (
            df[[self.t_col, self.r_col]]
            .dropna()
            .groupby(self.t_col, as_index=False, sort=True)
            .mean(numeric_only=True)
            .sort_values(self.t_col)
            .reset_index(drop=True)
        )
        if clean.shape[0] < 3:
            raise ValueError("Need at least 3 distinct temperature points for differentiation.")
        return clean

    def _interp_R(self, T_query: float, T: np.ndarray, R: np.ndarray) -> float:
        if T_query < T.min() or T_query > T.max():
            raise ValueError("Reference temperature outside the data range.")
        return float(np.interp(T_query, T, R))

    def pointwise_tcr(
        self,
        df: pd.DataFrame,
        sg_window: Optional[int] = None,
        sg_poly: int = 2
    ) -> pd.DataFrame:
        """Return a DataFrame with columns [T, R, dR_dT, alpha].

        alpha(T) = (1/R) * dR/dT  [units: 1/°C]

        Parameters
        ----------
        sg_window : Optional[int]
            If provided and odd (>=3), applies a Savitzky–Golay filter to R(T)
            before differentiating. Requires SciPy. If SciPy is unavailable
            or window is invalid, falls back to raw central differences.
        sg_poly : int
            Polynomial order for Savitzky–Golay smoothing (default 2).
        """
        data = self._prepare(df).copy()
        T = data[self.t_col].to_numpy(dtype=float)
        R = data[self.r_col].to_numpy(dtype=float)

        if sg_window is not None and sg_window >= 3 and sg_window % 2 == 1 and _HAS_SCIPY:
            if sg_window > len(R):
                sg_window = len(R) if len(R) % 2 == 1 else len(R) - 1
                sg_window = max(sg_window, 3)
            R_smooth = savgol_filter(R, window_length=sg_window, polyorder=sg_poly, mode="interp")
        else:
            R_smooth = R

        # Central differences for interior points; forward/backward for edges
        dT = np.diff(T)
        if np.any(dT == 0):
            raise ValueError("Duplicate temperatures remained after cleaning.")
        dR = np.diff(R_smooth)
        dR_dT = np.empty_like(R_smooth)
        dR_dT[1:-1] = (R_smooth[2:] - R_smooth[:-2]) / (T[2:] - T[:-2])
        dR_dT[0] = dR[0] / dT[0]
        dR_dT[-1] = dR[-1] / dT[-1]

        alpha = dR_dT / R_smooth
        out = pd.DataFrame({self.t_col: T, self.r_col: R, "dR_dT": dR_dT, "alpha": alpha})
        return out

    def interval_tcr(
        self,
        df: pd.DataFrame,
        Tmin: float,
        Tmax: float,
        ref: Literal["Tin", "Tmid", "custom", "meanR"] = "Tmid",
        T_ref: Optional[float] = None
    ) -> Dict[str, float]:
        """Compute a single TCR value over [Tmin, Tmax] via linear regression.

        Returns a dict with keys: {'Tmin','Tmax','slope','intercept','R_ref','alpha'}.

        alpha = slope / R_ref, where slope is dR/dT from a least squares fit.
        """
        if Tmin >= Tmax:
            raise ValueError("Tmin must be < Tmax.")
        data = self._prepare(df)
        mask = (data[self.t_col] >= Tmin) & (data[self.t_col] <= Tmax)
        window = data.loc[mask]
        if window.shape[0] < 2:
            raise ValueError("Not enough points in [Tmin, Tmax] for regression.")

        T = window[self.t_col].to_numpy(dtype=float)
        R = window[self.r_col].to_numpy(dtype=float)
        # Linear least squares fit
        A = np.vstack([T, np.ones_like(T)]).T
        slope, intercept = np.linalg.lstsq(A, R, rcond=None)[0]

        # Choose reference resistance
        Tmid = 0.5 * (Tmin + Tmax)
        if ref == "Tin":
            R_ref = self._interp_R(Tmin, data[self.t_col].to_numpy(float), data[self.r_col].to_numpy(float))
        elif ref == "Tmid":
            R_ref = self._interp_R(Tmid, data[self.t_col].to_numpy(float), data[self.r_col].to_numpy(float))
        elif ref == "custom":
            if T_ref is None:
                raise ValueError("Provide T_ref when ref='custom'.")
            R_ref = self._interp_R(T_ref, data[self.t_col].to_numpy(float), data[self.r_col].to_numpy(float))
        elif ref == "meanR":
            R_ref = float(R.mean())
        else:
            raise ValueError("Invalid ref. Use 'Tin', 'Tmid', 'custom', or 'meanR'.")

        alpha = slope / R_ref
        return {
            "Tmin": float(Tmin),
            "Tmax": float(Tmax),
            "slope": float(slope),
            "intercept": float(intercept),
            "R_ref": float(R_ref),
            "alpha": float(alpha),
        }

    def split_by_curie(
        self,
        df: pd.DataFrame,
        delta: float = 5.0,
        ref: Literal["Tin", "Tmid", "custom", "meanR"] = "Tmid",
        T_ref_below: Optional[float] = None,
        T_ref_above: Optional[float] = None
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        """Compute interval TCR just below and just above the Curie temperature.

        The intervals are:
          - Below: [T_Curie - delta, T_Curie]
          - Above: [T_Curie, T_Curie + delta]

        Returns two dicts (below, above) in the same format as `interval_tcr`.
        """
        Tc = float(self.curie_temp)
        below = self.interval_tcr(
            df, Tmin=Tc - delta, Tmax=Tc, ref=ref, T_ref=T_ref_below
        )
        above = self.interval_tcr(
            df, Tmin=Tc, Tmax=Tc + delta, ref=ref, T_ref=T_ref_above
        )
        return below, above

    def tcr_at(
        self,
        df: pd.DataFrame,
        T_query: float,
        sg_window: Optional[int] = None,
        sg_poly: int = 2
    ) -> float:
        """Return pointwise alpha(T_query) via interpolation on the pointwise series."""
        series = self.pointwise_tcr(df, sg_window=sg_window, sg_poly=sg_poly)
        T = series[self.t_col].to_numpy(float)
        alpha = series["alpha"].to_numpy(float)
        if T_query < T.min() or T_query > T.max():
            raise ValueError("T_query is outside the data range.")
        return float(np.interp(T_query, T, alpha))

import matplotlib.pyplot as plt

def plot_tcr_from_df(
    df: pd.DataFrame,
    t_col: str = "Temperature",
    r_col: str = "R",
    curie_temp: float | None = 356.0,
    sg_window: int | None = None,   # odd integer >=3 (requires SciPy)
    sg_poly: int = 2,
    save_path: str | None = None,
):
    """
    Compute pointwise TCR and plot alpha(T).

    alpha(T) = (1/R) * dR/dT   [units: 1/°C]

    Parameters
    ----------
    df : DataFrame with temperature and resistance columns.
    t_col, r_col : names of the temperature and resistance columns.
    curie_temp : draw a vertical reference line at this temperature (set None to skip).
    sg_window : optional Savitzky–Golay window length (odd >=3) for smoothing R(T).
                Requires SciPy; if unavailable, falls back to raw differences.
    sg_poly : polynomial order for Savitzky–Golay smoothing.
    save_path : if provided, saves the plot as a PNG to this path.

    Returns
    -------
    out_df : DataFrame with columns [T, R, dR_dT, alpha].
    save_path or None
    """

    # --- plot ---
    plt.figure(figsize=(7, 4.5))
    plt.scatter(df[t_col].to_numpy(), df["alpha"].to_numpy(), marker=None)
    plt.xlabel("Temperature (°C)")
    plt.ylabel("TCR α (1/°C)")
    plt.title("TCR vs Temperature")
    plt.grid(True, alpha=0.3)
    if curie_temp is not None:
        plt.axvline(float(curie_temp), linestyle="--", alpha=0.8)
    plt.tight_layout()
    if save_path:
       plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.show()





__all__ = ["TCRCalculator"]


if __name__ == '__main__':
    from src.infrastructure.core.config_reader import config

    time_range = ["2023-05-10", '2024-01-28']
    table = TableConfig().ETCDataTable
    data_loader = DataLoaderTCR(config=config, sensor_type="F1")
    df = data_loader.load_resistance_values(time_range=time_range)

    # df needs columns 'T' (temperature) and 'R' (resistance)
    # You can rename via TCRCalculator(t_col='temp', r_col='res') if needed.
   # df = pd.DataFrame({'T':[320, 340, 350, 356, 362, 380],
   #                    'R':[105.2, 109.8, 115.0, 140.0, 168.0, 210.5]})

    calc = TCRCalculator(curie_temp=356)

    # 1) Pointwise TCR (optionally smoothed with Savitzky–Golay if SciPy available)
    tcr_series = calc.pointwise_tcr(df, sg_window=5)   # returns DataFrame with T, R, dR_dT, alpha

    # 2) Interval TCR below/above Curie (default ±5 °C windows)
    below, above = calc.split_by_curie(df, delta=10, ref='Tmid')

    print(below['alpha'], above['alpha'])

    # 3) Interval TCR on a custom range, using midpoint as R_ref
    stats = calc.interval_tcr(df, Tmin=100, Tmax=400, ref='Tmid')
    print(stats['alpha'])

    # 4) Pointwise alpha at an exact temperature (interpolated)
    alpha_355 = calc.tcr_at(df, T_query=355)
    plot_tcr_from_df(df=tcr_series)


