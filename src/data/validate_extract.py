import sys
sys.path.append("/opt")
sys.path.append(".")

import json
import pandas as pd
from datetime import datetime
from src.ds.lakefs_ds import LakeFSDataStore
from src.shared.columns import FEATURES

def validate_data(lakefs_ds: LakeFSDataStore, default_start_date: str) -> list[pd.DataFrame, list]:
    """
    Loads and validates the newly extracted data from the current branch.
    
    1. Finds the date range of new data by comparing main and current branch manifests.
    2. Loads all CSV files for that date range.
    3. Filters for the exact date range.
    4. Performs validation checks.
    """
    
    # 1. Get the date range of the new data
    
    # Get the new manifest from the current data branch
    current_branch = lakefs_ds.branch
    new_manifest = lakefs_ds.load_json(key="data/raw/manifest.json")
    if not new_manifest or "last_updated_date" not in new_manifest:
        raise ValueError(f"Could not load manifest.json from branch '{current_branch}'")
    
    end_date = pd.to_datetime(new_manifest["last_updated_date"])
    
    # Get the old manifest from the 'main' branch to find the start date
    lakefs_ds.checkout("main")
    old_manifest = lakefs_ds.load_json(key="data/raw/manifest.json")
    
    start_date_str = default_start_date
    if old_manifest and "last_updated_date" in old_manifest:
        start_date_str = old_manifest["last_updated_date"]
        
    start_date = pd.to_datetime(start_date_str) + pd.Timedelta(days=1)
    
    # Switch back to the data branch to load the files
    lakefs_ds.checkout(current_branch)
    
    print(f"Validation data from {start_date.date()} to {end_date.date()}")

    # 2. Load all new data files from the data branch
    periods = pd.date_range(start_date, end_date).to_period('M').unique()
    all_dfs = []
    
    for period in periods:
        key = f"data/raw/year={period.year}/month={period.month}/data.csv"
        try:
            print(f"Loading data from {key}...")
            df = lakefs_ds.load_df(key)
            all_dfs.append(df)
        except Exception as e:
            # This file should exist if data was extracted for this month
            raise FileNotFoundError(f"Failed to load required data file: {key}. Error: {e}")

    if not all_dfs:
        raise ValueError("No data files found for the new date range.")

    # 3. Combine and filter for the exact date range
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df["date"] = pd.to_datetime(combined_df["date"])
    
    # Filter to the exact date range we're validating
    data_to_validate = combined_df[
        (combined_df["date"] >= start_date) & 
        (combined_df["date"] <= end_date)
    ].reset_index(drop=True)

    if data_to_validate.empty:
        raise ValueError(f"No data found between {start_date.date()} and {end_date.date()} after loading files.")
        
    print(f"Successfully loaded {len(data_to_validate)} rows for validation.")

    # 4. Perform validation checks
    validation_errors = []
    
    # Check 1: All columns are present
    expected_columns = ["date"] + FEATURES
    actual_columns = set(data_to_validate.columns)
    missing_columns = set(expected_columns) - actual_columns
    
    if missing_columns:
        validation_errors.append(f"Missing expected columns: {sorted(list(missing_columns))}")
        
    # Check 2: All dates are present
    # Create a full set of expected dates (normalized to remove time part)
    expected_dates = set(pd.date_range(start_date, end_date, freq='D').normalize())
    
    # Get the set of actual dates present in the data
    actual_dates = set(pd.to_datetime(data_to_validate['date']).dt.normalize())
    
    missing_dates = expected_dates - actual_dates
    
    if missing_dates:
        # Sort the dates to make the error message readable
        sorted_missing = sorted(list(missing_dates))
        validation_errors.append(f"Missing data for {len(sorted_missing)} dates. "
                                 f"First 3 missing: {[d.strftime('%Y-%m-%d') for d in sorted_missing[:3]]}")

    return data_to_validate, validation_errors

def lambda_handler(event, _):

    try:
        # Get parameters from the Step Function event
        # These are passed from the previous step's output or the original execution input
        repo_name = event["repo_name"]
        lakefs_endpoint = event["lakefs_endpoint"]
        default_start_date = event["default_start_date"] # Needed to find the start range

        # --- MODIFICATION ---
        # Manually determine the branch name based on the current date (since we are running on 1st of every month should be fine)
        current_date = pd.Timestamp(datetime.now().date()).strftime("%Y-%m-%d")
        
        branch_to_validate = f"{current_date}-data-extract"
        # --------------------

        print(f"Starting validation for manually determined branch: {branch_to_validate}")

        lakefs_ds = LakeFSDataStore(
            repo_name=repo_name,
            endpoint=lakefs_endpoint
        )
        
        # Check out the branch created by the extract function
        lakefs_ds.checkout(branch_to_validate)
        
        # Run the validation
        data_df, validation_errors = validate_data(lakefs_ds, default_start_date)
        
        if validation_errors:
            print("Validation FAILED.")
            for error in validation_errors:
                print(f"- {error}")
            
            # Return an error to stop the Step Function
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Data validation failed.",
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
                "message": "Data validation and merge successful.",
                "branch": branch_to_validate,
                "merge_commit_id": merge_commit
            })
        }

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        # If the branch doesn't exist (e.g., date mismatch), checkout will fail
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal server error during validation.",
                "error": str(e)
            })
        }