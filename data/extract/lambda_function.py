import sys
sys.path.append("/opt")
sys.path.append(".")

import json
import pandas as pd
from datetime import datetime
from extract.open_meteo import OpenMeteoAPI
from shared.data_store import LakeFSDataStore
from extract.utils import get_valid_date_ranges

def get_weather_data(
        lakefs_ds: LakeFSDataStore,
        default_start_date: str
    ):
    
    manifest = lakefs_ds.load_json(key = "data/raw/manifest.json")
    if not manifest:
        manifest = {"last_updated_date": default_start_date}
    start_date = pd.to_datetime(manifest['last_updated_date']) + pd.Timedelta(days=1)
    end_date = pd.Timestamp(datetime.now().date())

    date_ranges = get_valid_date_ranges(
        start_date = start_date.strftime("%Y-%m-%d"),
        end_date = end_date.strftime("%Y-%m-%d")
    )

    api = OpenMeteoAPI()
    for start, end in date_ranges:
        print(f"\nFetching data from {start} to {end}...")
        response = api.get_weather(
            lat = 43.7064,
            long = -79.3986,
            start_date = start,
            end_date = end,
            timezone = "America/New_York"
        )
        daily = response.Daily()
        daily_data = {"date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )}

        for i, var_name in enumerate(api.features):
            daily_data[var_name] = daily.Variables(i).ValuesAsNumpy()

        df = pd.DataFrame(daily_data)
        year = datetime.strptime(start, "%Y-%m-%d").year
        for month in range(1, 13):
            df_month = df[df['date'].dt.month == month]
            if not df_month.empty:
                lakefs_ds.save_df(
                    df = df_month,
                    key = f"data/raw/year={year}/month={month}/data.csv"
                )
    lakefs_ds.save_json(
        key = "data/raw/manifest.json",
        data = {
            "last_updated_date": date_ranges[-1][1]
        }
    )
    return date_ranges[-1][1]

def lambda_handler(event, _):

    lakefs_ds = LakeFSDataStore(
        repo_name = event["repo_name"],
        endpoint = event["lakefs_endpoint"]
    )
    current_date = pd.Timestamp(datetime.now().date()).strftime("%Y-%m-%d")
    lakefs_ds.create_branch(
        name = f"{current_date}-data-extract",
        checkout = True
    )
    last_updated_date = get_weather_data(lakefs_ds, event["default_start_date"])
    
    lakefs_ds.commit(message = f"Extracted data till {last_updated_date}")
    merge_commit = lakefs_ds.merge_branch(
        dest = "main",
        delete_after_merge = True
    )
    return {
        "statusCode": 200,
        "body": json.dumps({"merge_commit": merge_commit})
    }