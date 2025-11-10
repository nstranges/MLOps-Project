import sys
sys.path.append("/opt")
sys.path.append(".")

import json
import boto3
import pandas as pd
from src.ds import S3DataStore
from src.data.extract import fetch_data_from_api
from src.data.transform import process_dataframe
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

runtime = boto3.client("sagemaker-runtime", region_name="us-east-2")

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
    
    # get today's date
    end = pd.Timestamp.now().strftime("%Y-%m-%d")
    start = end
    
    # fetch data from api and process
    weather_df = fetch_data_from_api(start, end)
    weather_df = process_dataframe(weather_df)
    drop_cols = [c for c in ["date", "weather_code"] if c in weather_df.columns]
    X = weather_df.drop(columns=drop_cols)
    
    # make prediction
    weather_code = make_prediction(X)
    # add prediction to weather_df
    weather_df["prediction"] = weather_code

    # log prediction to s3
    s3_ds = S3DataStore(bucket_name="weather-model-478492276227")
    existing_log = s3_ds.load_df("logs/daily_predictions.csv")
    if existing_log is None:
        existing_log = pd.DataFrame()
    existing_log = pd.concat([existing_log, weather_df], ignore_index=True)
    s3_ds.save_df(existing_log, "logs/daily_predictions.csv")
    
    return weather_code

def lambda_handler(event, context):
    weather_code = get_weather_code()
    return {
        'statusCode': 200,
        'weather': f"The weather code for today is : {weather_code}"
    }

lambda_handler({}, {})