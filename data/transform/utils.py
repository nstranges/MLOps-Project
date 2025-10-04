
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_valid_date_prefixes(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_prefixes = []
    current = start
    while current <= end:
        year = current.year
        month = f"{current.month:02d}"
        date_prefixes.append(f"year={year}/month={month}")
        current += relativedelta(months=1)
    return date_prefixes

def get_raw_data(s3_ds, start_date, end_date):
    date_prefixes = get_valid_date_prefixes(
        start_date = start_date.strftime("%Y-%m-%d"),
        end_date = end_date.strftime("%Y-%m-%d")
    )
    dfs = []
    for date_prefix in date_prefixes:
        key = f"data/raw/{date_prefix}/data.csv"
        try:
            dfs.append(s3_ds.load_df(key))
        except Exception as e:
            print(f"Error loading {key}: {e}")
    df = pd.concat(dfs, ignore_index=True)
    print(f"Combined raw dataset shape: {df.shape}")
    return df