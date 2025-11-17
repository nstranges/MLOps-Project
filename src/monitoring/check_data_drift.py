import pandas as pd
import numpy as np
from src.ds import LakeFSDataStore, S3DataStore
from src.data.utils import get_data_from_main

# Testing action
def get_reference_dataframe() -> pd.DataFrame:
    """
    Fetches the reference dataset from LakeFS and returns it as a pandas DataFrame.
    """
    lakefs_ds = LakeFSDataStore(
        repo_name = "weather-data",
        endpoint = "http://18.222.212.217:8000"
    )
    end_date = pd.Timestamp(lakefs_ds.load_json(key = "data/processed/manifest.json")["last_updated_date"])
    start_date = end_date - pd.Timedelta(years = 2)
    df = get_data_from_main(
        lakefs_ds = lakefs_ds,
        type = "processed",
        start_date = start_date,
        end_date = end_date
    )
    return df

def get_current_dataframe(n_rows = 14) -> pd.DataFrame:
    s3_ds = S3DataStore(bucket_name = "weather-model-478492276227")
    df = s3_ds.load_df(key = "logs/daily_predictions.csv")
    return df.tail(n_rows)

def _ks_2samp_statistic(x: np.ndarray, y: np.ndarray) -> float:
    """Compute KS D statistic (no SciPy)."""
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    x = np.sort(x)
    y = np.sort(y)
    data_all = np.concatenate([x, y])
    cdf_x = np.searchsorted(x, data_all, side="right") / x.size
    cdf_y = np.searchsorted(y, data_all, side="right") / y.size
    return float(np.max(np.abs(cdf_x - cdf_y)))

def _ks_2samp_pvalue(d: float, n: int, m: int) -> float:
    """
    Asymptotic two-sample KS p-value using the Massey (1951) correction.
    Accurate for moderate/large n, m.
    """
    if n == 0 or m == 0:
        return np.nan
    en = n * m / (n + m)  # effective n
    lam = (np.sqrt(en) + 0.12 + 0.11 / np.sqrt(en)) * d
    # Kolmogorov Q-function: p = 2 * sum_{k=1..∞} (-1)^{k-1} e^{-2 k^2 lam^2}
    # Truncate when terms get tiny.
    p = 0.0
    for k in range(1, 200):
        term = np.exp(-2.0 * (k * k) * (lam * lam))
        add = (2.0 * (-1) ** (k - 1)) * term
        p += add
        if term < 1e-10:
            break
    return float(min(max(p, 0.0), 1.0))

def detect_data_drift(
    reference_df: pd.DataFrame,
    current_df: pd.DataFrame,
    alpha: float = 0.01,
    missing_threshold: float = 0.10,
    require_frac: float = 0.0
) -> tuple[bool, dict]:
    """
    Per-feature two-sample KS test with Missingness drift check (Δ missing rate > missing_threshold)
    Returns: (overall_drift: bool, details: dict)
    """
    assert list(reference_df.columns) == list(current_df.columns), "Column order/names must match."

    details = {}
    drifted = 0
    total = len(reference_df.columns)

    for col in reference_df.columns:
        ref_col = reference_df[col]
        cur_col = current_df[col]

        # Missingness drift
        miss_ref = ref_col.isna().mean()
        miss_cur = cur_col.isna().mean()
        miss_delta = abs(miss_ref - miss_cur)
        miss_flag = miss_delta > missing_threshold
        
        # KS test for numeric drift
        r_num = ref_col.astype(float)
        c_num = cur_col.astype(float)
        r = r_num.dropna().astype(float).values
        c = c_num.dropna().astype(float).values
        if len(r) == 0 or len(c) == 0:
            D = np.nan
            p = np.nan
            ks_flag = False
        else:
            D = _ks_2samp_statistic(r, c)
            p = _ks_2samp_pvalue(D, len(r), len(c))
            ks_flag = (p < alpha)

        details[col] = {
            "type": "numeric",
            "D": None if not np.isfinite(D) else float(D),
            "p_value": None if not np.isfinite(p) else float(p),
            "missing_delta": float(miss_delta),
            "missing_flag": miss_flag,
            "ks_flag": ks_flag,
            "drift": bool(ks_flag or miss_flag),
        }

        if details[col]["drift"]:
            drifted += 1

    frac = drifted / max(1, total)
    overall = (frac >= require_frac) if require_frac > 0 else (drifted > 0)
    return overall, details

def craft_drift_alert(details: dict) -> str:
    drifted_features = [col for col, res in details.items() if res["drift"]]
    messages = []
    for col in drifted_features:
        res = details[col]
        msg_parts = [f"Feature '{col}':"]
        if res["missing_flag"]:
            msg_parts.append(f"Missingness drift detected (Δ missing rate = {res['missing_delta']:.2%}).")
        if res["ks_flag"]:
            msg_parts.append(f"KS test indicates drift (D = {res['D']:.4f}, p-value = {res['p_value']:.4f}).")
        messages.append(" ".join(msg_parts))
    alert_message = "Data Drift Detected in the following features:\n" + "\n".join(messages)
    return alert_message
    

def lambda_handler(event, context):
    reference_df = get_reference_dataframe()
    curr_df = get_current_dataframe()
    columns_to_use = [
        'temperature_2m_max', 'temperature_2m_min',
        'apparent_temperature_max', 'apparent_temperature_min',
        'daylight_duration', 'sunshine_duration',
        'rain_sum', 'showers_sum', 'snowfall_sum',
        'precipitation_sum', 'precipitation_hours', 'wind_speed_10m_max',
        'wind_gusts_10m_max', 'wind_direction_10m_dominant',
        'shortwave_radiation_sum', 'et0_fao_evapotranspiration',
        'apparent_temperature_mean', 'temperature_2m_mean', 'cape_mean',
        'cape_max', 'cape_min', 'cloud_cover_mean', 'cloud_cover_max',
        'cloud_cover_min', 'dew_point_2m_mean', 'dew_point_2m_max',
        'dew_point_2m_min', 'et0_fao_evapotranspiration_sum',
        'relative_humidity_2m_mean', 'relative_humidity_2m_max',
        'relative_humidity_2m_min', 'snowfall_water_equivalent_sum',
        'pressure_msl_mean', 'pressure_msl_max', 'pressure_msl_min',
        'surface_pressure_mean', 'surface_pressure_max', 'surface_pressure_min',
        'visibility_mean', 'visibility_min', 'visibility_max',
        'winddirection_10m_dominant', 'wind_gusts_10m_mean',
        'wind_speed_10m_mean', 'wind_gusts_10m_min', 'wind_speed_10m_min',
        'wet_bulb_temperature_2m_mean', 'wet_bulb_temperature_2m_max',
        'wet_bulb_temperature_2m_min', 'vapour_pressure_deficit_max',
        'soil_moisture_0_to_10cm_mean'
    ]
    reference_df = reference_df[columns_to_use]
    curr_df = curr_df[columns_to_use]
    overall_drift, details = detect_data_drift(
        reference_df=reference_df,
        current_df=curr_df,
        alpha=0.01,
        missing_threshold=0.10,
        require_frac=0.35
    )
    if overall_drift:
        drift_message = craft_drift_alert(details)
        return {
            "statusCode": 200,
            "drift_detected": True,
            "message": drift_message
        }
    else:
        return {
            "statusCode": 200,
            "drift_detected": False,
            "message": "No data drift detected."
        }
    
