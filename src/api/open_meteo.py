import time
import openmeteo_requests
import requests_cache
from retry_requests import retry
from src.shared.columns import FEATURES

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
