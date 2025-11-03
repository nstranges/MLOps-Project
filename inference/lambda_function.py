import sys
sys.path.append("/opt")

import json
import boto3
import pandas as pd
import numpy as np
import openmeteo_requests
import requests_cache
from retry_requests import retry

runtime = boto3.client("sagemaker-runtime", region_name="us-east-2")

FEATURES = [
    "weather_code", "temperature_2m_max", "temperature_2m_min",
    "apparent_temperature_max", "apparent_temperature_min",
    "sunrise", "sunset", "daylight_duration", "sunshine_duration",
    "uv_index_max", "uv_index_clear_sky_max",
    "rain_sum", "showers_sum", "snowfall_sum", "precipitation_sum",
    "precipitation_hours", "precipitation_probability_max",
    "wind_speed_10m_max", "wind_gusts_10m_max",
    "wind_direction_10m_dominant", "shortwave_radiation_sum",
    "et0_fao_evapotranspiration", "apparent_temperature_mean",
    "temperature_2m_mean", "cape_mean", "cape_max", "cape_min",
    "cloud_cover_mean", "cloud_cover_max", "cloud_cover_min",
    "dew_point_2m_mean", "dew_point_2m_max", "dew_point_2m_min",
    "et0_fao_evapotranspiration_sum",
    "growing_degree_days_base_0_limit_50",
    "leaf_wetness_probability_mean", "precipitation_probability_mean",
    "precipitation_probability_min", "relative_humidity_2m_mean",
    "relative_humidity_2m_max", "relative_humidity_2m_min",
    "snowfall_water_equivalent_sum", "pressure_msl_mean",
    "pressure_msl_max", "pressure_msl_min",
    "surface_pressure_mean", "surface_pressure_max",
    "surface_pressure_min", "updraft_max", "visibility_mean",
    "visibility_min", "visibility_max", "winddirection_10m_dominant",
    "wind_gusts_10m_mean", "wind_speed_10m_mean",
    "wind_gusts_10m_min", "wind_speed_10m_min",
    "wet_bulb_temperature_2m_mean", "wet_bulb_temperature_2m_max",
    "wet_bulb_temperature_2m_min", "vapour_pressure_deficit_max",
    "soil_moisture_0_to_100cm_mean", "soil_moisture_0_to_10cm_mean",
    "soil_moisture_0_to_7cm_mean", "soil_moisture_28_to_100cm_mean",
    "soil_moisture_7_to_28cm_mean", "soil_temperature_0_to_100cm_mean",
    "soil_temperature_0_to_7cm_mean", "soil_temperature_28_to_100cm_mean",
    "soil_temperature_7_to_28cm_mean"
]

# columns with lot of nan, removed after EDA
REMOVE =[
    
    'sunrise', 'sunset',
    'precipitation_probability_max', 'growing_degree_days_base_0_limit_50',
    'leaf_wetness_probability_mean', 'precipitation_probability_mean',
    'precipitation_probability_min', 'updraft_max', 'soil_moisture_0_to_100cm_mean', 'soil_moisture_0_to_7cm_mean', 'soil_moisture_28_to_100cm_mean',
    'soil_moisture_7_to_28cm_mean', 'soil_temperature_0_to_100cm_mean', 'soil_temperature_0_to_7cm_mean',
    'soil_temperature_28_to_100cm_mean', 'soil_temperature_7_to_28cm_mean' 
]

def get_current_weather() -> pd.DataFrame:
    cache_session = requests_cache.CachedSession(cache_name=':memory:', backend='sqlite', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    # get today's date
    end = pd.Timestamp.now().strftime("%Y-%m-%d")
    start = end
    params = {
        "latitude": 43.7064,
        "longitude": -79.3986,
        "start_date": start,
        "end_date": end,
        "daily": FEATURES,
        "timezone": "America/New_York"
    }
    response = openmeteo.weather_api(url, params=params)[0]
    daily = response.Daily()
    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}
    for i, var_name in enumerate(FEATURES):
        daily_data[var_name] = daily.Variables(i).ValuesAsNumpy()

    df = pd.DataFrame(daily_data)
    return df

def process_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    # Drop unwanted columns
    df = df.drop(columns=[c for c in REMOVE if c in df.columns])

    # Filter 2018+ only
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'].dt.year >= 2018]

    # Fill remaining NaNs with median
    for col in df.columns:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].median())

    # Add cyclic features
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day_of_month"] = df["date"].dt.day
    df["day_of_week"] = df["date"].dt.dayofweek
    df["day_of_year"] = df["date"].dt.dayofyear

    # Monthly cyclic
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)

    # Yearly cyclic
    df["year_sin"] = np.sin(2 * np.pi * df["day_of_year"] / 365.25)
    df["year_cos"] = np.cos(2 * np.pi * df["day_of_year"] / 365.25)
    return df

def make_prediction(input_X: pd.DataFrame) -> float:
    if len(input_X) == 1:
        input_X = pd.concat([input_X, input_X], ignore_index=True)
    body_bytes = input_X.to_csv(index=False, header=False).encode("utf-8")
    resp = runtime.invoke_endpoint(
        EndpointName="sklearn-serverless-endpoint",
        ContentType="text/csv",
        Accept="application/json",
        Body=body_bytes
    )
    pred = json.loads(resp["Body"].read().decode("utf-8"))
    return pred[0]

def get_weather_code():
    weather_df = get_current_weather()
    weather_df = process_weather_data(weather_df)
    drop_cols = [c for c in ["date", "weather_code"] if c in weather_df.columns]
    X = weather_df.drop(columns=drop_cols)
    weather_code = make_prediction(X)
    return weather_code

def lambda_handler(event, context):
    weather_code = get_weather_code()
    return {
        'statusCode': 200,
        'weather': f"The weather code for today is : {weather_code}"
    }
