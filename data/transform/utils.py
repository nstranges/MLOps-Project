
import pandas as pd
from dateutil.relativedelta import relativedelta
from shared.data_store import LakeFSDataStore

def get_valid_date_prefixes(start_date: pd.Timestamp, end_date: pd.Timestamp):
    date_prefixes = []
    current = start_date
    while current <= end_date:
        year = current.year
        month = current.month
        date_prefixes.append(f"year={year}/month={month}")
        current += relativedelta(months=1)
    return date_prefixes

def get_raw_data_from_main(lakefs_ds: LakeFSDataStore, start_date: pd.Timestamp, end_date: pd.Timestamp):
    current_branch = lakefs_ds.branch
    print(f"Switching from {current_branch} to main to fetch raw data")
    lakefs_ds.checkout("main")
    date_prefixes = get_valid_date_prefixes(
        start_date = start_date,
        end_date = end_date
    )
    dfs = []
    for date_prefix in date_prefixes:
        key = f"data/raw/{date_prefix}/data.csv"
        try:
            dfs.append(lakefs_ds.load_df(key))
        except Exception as e:
            print(f"Error loading {key}: {e}")
    df = pd.concat(dfs, ignore_index=True)
    print(f"Combined raw dataset shape: {df.shape}")
    lakefs_ds.checkout(current_branch)
    return df