import sys
sys.path.append("/opt")

import pandas as pd
import numpy as np
from datetime import datetime
from shared.data_store import S3DataStore
from transform.utils import get_raw_data

def process_weather_data(s3_ds: S3DataStore):
    manifest = s3_ds.load_json(key = "data/processed/manifest.json")
    if not manifest:
        manifest = {"last_updated_date": "2018-01-01"}
    start_date = pd.to_datetime(manifest['last_updated_date']) + pd.Timedelta(days=1)
    end_date = pd.Timestamp(datetime.now().date())

    df = get_raw_data(s3_ds, start_date, end_date)
    # Drop unwanted columns
    df = df.drop(columns=[c for c in ["sunrise", "sunset"] if c in df.columns])

    # Filter 2018+ only
    df['date'] = pd.to_datetime(df['date'])
    df = df[df['date'].dt.year >= 2018]

    # Drop columns with >50% NaN
    threshold = len(df) * 0.5
    df = df.dropna(axis=1, thresh=threshold)

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

    # Save per month
    for year in df['year'].unique():
        df_year = df[df['year'] == year]
        for month in df_year['month'].unique():
            df_month = df_year[df_year['month'] == month]
            if not df_month.empty:
                s3_ds.save_df(
                    df = df_month,
                    key = f"data/processed/year={year}/month={month}/data.csv"
                )
    s3_ds.save_json(
        key = "data/processed/manifest.json",
        data = {
            "last_updated_date": s3_ds.load_json(
                key = "data/raw/manifest.json"
            )["last_updated_date"]
        }
    )

def lambda_handler(event, _):
    s3_ds = S3DataStore(bucket_name = "weather-data-bucket-mlops")
    process_weather_data(s3_ds)