
import pandas as pd
from dateutil.relativedelta import relativedelta

def get_valid_date_prefixes(start_date: pd.Timestamp, end_date: pd.Timestamp):
    date_prefixes = []
    current = start_date
    while current <= end_date:
        year = current.year
        month = f"{current.month:02d}"
        date_prefixes.append(f"year={year}/month={month}")
        current += relativedelta(months=1)
    return date_prefixes

def get_raw_data(s3_ds, start_date: pd.Timestamp, end_date: pd.Timestamp):
    date_prefixes = get_valid_date_prefixes(
        start_date = start_date,
        end_date = end_date
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