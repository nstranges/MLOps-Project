import time
import openmeteo_requests
import requests_cache
from retry_requests import retry

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

class OpenMeteoAPI:

    def __init__(self):
        cache_session = requests_cache.CachedSession(
            cache_name=':memory:',
            backend='sqlite',
            expire_after=3600
        )
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)
        self.url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
        self.features = FEATURES
    
    def get_weather(
            self,
            lat: float,
            long: float,
            start_date: str,
            end_date: str,
            timezone: str
    ):
        params = {
            "latitude": lat,
            "longitude": long,
            "start_date": start_date,
            "end_date": end_date,
            "daily": FEATURES,
            "timezone": timezone
        }
        
        try:
            response = self.openmeteo.weather_api(self.url, params=params)
            return response[0]
        except Exception as e:
            err_str = str(e)
            if "request limit" in err_str.lower():
                print("Rate limit hit, sleeping for 60 seconds...")
                time.sleep(60)
                return self.get_weather(lat, long, start_date, end_date, timezone)
            else:
                raise RuntimeError(f"Weather API failed: {err_str}")
