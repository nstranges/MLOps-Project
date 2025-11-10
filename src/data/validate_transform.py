import sys
sys.path.append("/opt")
sys.path.append(".")

import json
import pandas as pd
import numpy as np
from datetime import datetime
from src.ds.lakefs_ds import LakeFSDataStore
from src.shared.columns import FEATURES, REMOVE

def validate_processed_data(lakefs_ds: LakeFSDataStore, default_start_date: str) -> list[pd.DataFrame, list]:
    """
    Loads and validates the newly transformed data from the current branch.
    
    1. Defines the full list of expected columns after transformation.
    2. Finds the date range of new data by comparing processed manifests.
    3. Loads all CSV files from data/processed/ for that date range.
    4. Performs validation checks for columns, NaNs, and missing dates.
    """
    
    # 1. Define the exact set of columns expected after transformation
    base_features = [col for col in FEATURES if col not in REMOVE]
    cyclic_features = [
        "year", "month", "day_of_month", "day_of_week", "day_of_year",
        "month_sin", "month_cos", "year_sin", "year_cos"
    ]
    # The 'date' column is also expected
    final_expected_columns = set(["date"] + base_features + cyclic_features)

    # 2. Get the date range of the new processed data
    current_branch = lakefs_ds.branch
    new_manifest = lakefs_ds.load_json(key="data/processed/manifest.json")
    if not new_manifest or "last_updated_date" not in new_manifest:
        raise ValueError(f"Could not load processed manifest.json from branch '{current_branch}'")
    
    # The end date is the 'last_updated_date' from the *raw* manifest
    # (as set by the transform function)
    end_date = pd.to_datetime(new_manifest["last_updated_date"])
    
    # Get the old processed manifest from 'main' to find the start date
    lakefs_ds.checkout("main")
    old_manifest = lakefs_ds.load_json(key="data/processed/manifest.json")
    
    start_date_str = default_start_date
    if old_manifest and "last_updated_date" in old_manifest:
        start_date_str = old_manifest["last_updated_date"]
        
    start_date = pd.to_datetime(start_date_str) + pd.Timedelta(days=1)
    
    # *** IMPORTANT ***
    # The transform logic filters for year >= 2018. 
    # We must adjust our validation start date to respect this.
    filter_start_date = pd.to_datetime("2018-01-01")
    validation_start_date = max(start_date, filter_start_date)

    # Switch back to the data branch to load the files
    lakefs_ds.checkout(current_branch)
    
    print(f"Validation data from {validation_start_date.date()} to {end_date.date()}")
    if validation_start_date > end_date:
        print("No new data to validate (start date is after end date). Skipping.")
        return None, [] # Nothing to validate

    # 3. Load all new data files from the data/processed branch
    periods = pd.date_range(validation_start_date, end_date).to_period('M').unique()
    all_dfs = []
    
    for period in periods:
        key = f"data/processed/year={period.year}/month={period.month}/data.csv"
        try:
            print(f"Loading data from {key}...")
            df = lakefs_ds.load_df(key)
            all_dfs.append(df)
        except Exception as e:
            # This file should exist if data was extracted for this month
            raise FileNotFoundError(f"Failed to load required data file: {key}. Error: {e}")

    if not all_dfs:
        raise ValueError("No processed data files found for the new date range.")

    # 4. Combine and filter for the exact date range
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df["date"] = pd.to_datetime(combined_df["date"])
    
    data_to_validate = combined_df[
        (combined_df["date"] >= validation_start_date) & 
        (combined_df["date"] <= end_date)
    ].reset_index(drop=True)

    if data_to_validate.empty:
        raise ValueError(f"No data found between {validation_start_date.date()} and {end_date.date()} after loading files.")
        
    print(f"Successfully loaded {len(data_to_validate)} rows for validation.")

    # 5. Perform validation checks
    validation_errors = []
    actual_columns = set(data_to_validate.columns)

    # Check 1: Missing columns
    missing_columns = final_expected_columns - actual_columns
    if missing_columns:
        validation_errors.append(f"Missing expected columns: {sorted(list(missing_columns))}")

    # Check 2: Unknown/Extra columns
    extra_columns = actual_columns - final_expected_columns
    if extra_columns:
        validation_errors.append(f"Found unexpected columns: {sorted(list(extra_columns))}")

    # Check 3: No null values (NaNs)
    # Your transform step should have filled all NaNs.
    columns_to_check = list(final_expected_columns.intersection(actual_columns))
    null_counts = data_to_validate[columns_to_check].isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0].index.tolist()
    
    if cols_with_nulls:
        validation_errors.append(f"Validation FAILED: Null values found in columns: {cols_with_nulls}")
        
    # Check 4: All dates are present
    expected_dates = set(pd.date_range(validation_start_date, end_date, freq='D').normalize())
    actual_dates = set(pd.to_datetime(data_to_validate['date']).dt.normalize())
    
    missing_dates = expected_dates - actual_dates
    
    if missing_dates:
        sorted_missing = sorted(list(missing_dates))
        validation_errors.append(f"Missing data for {len(sorted_missing)} dates. "
                                 f"First 3 missing: {[d.strftime('%Y-%m-%d') for d in sorted_missing[:3]]}")

    return data_to_validate, validation_errors

def lambda_handler(event, _):

    try:
        # Get parameters from the Step Function event
        repo_name = event["repo_name"]
        lakefs_endpoint = event["lakefs_endpoint"]
        default_start_date = event["default_start_date"]

        # Manually determine the branch name based on the current date
        current_date = pd.Timestamp(datetime.now().date()).strftime("%Y-%m-%d")
        branch_to_validate = f"{current_date}-data-transform"

        print(f"Starting validation for transform branch: {branch_to_validate}")

        lakefs_ds = LakeFSDataStore(
            repo_name=repo_name,
            endpoint=lakefs_endpoint
        )
        
        # Check out the branch created by the transform function
        lakefs_ds.checkout(branch_to_validate)
        
        # Run the validation
        data_df, validation_errors = validate_processed_data(lakefs_ds, default_start_date)
        
        if validation_errors:
            print("Validation FAILED.")
            for error in validation_errors:
                print(f"- {error}")
            
            # Return an error to stop the Step Function
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Data transform validation failed.",
                    "branch": branch_to_validate,
                    "errors": validation_errors
                })
            }
        
        # Validation successful, merge to main
        print("Validation SUCCEEDED. Merging to main...")
        
        # We are already on branch_to_validate
        merge_commit = lakefs_ds.merge_branch(
            dest="main",
            delete_after_merge=True
        )
        
        print(f"Merge successful. Commit ID: {merge_commit}")
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data transform validation and merge successful.",
                "branch": branch_to_validate,
                "merge_commit_id": merge_commit
            })
        }

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal server error during validation.",
                "error": str(e)
            })
        }