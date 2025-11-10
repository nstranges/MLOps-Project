import io
import os
import json
import boto3
import pandas as pd
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class S3DataStore:

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        access_key = os.getenv("AWS_ACCESS_KEY")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if access_key and secret_key:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key
            )
        else:
            self.s3 = boto3.client("s3")

    def save_json(self, key: str, data: dict) -> None:
        body = json.dumps(data)
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=body)
        print(f"Saved JSON to {key}")

    def load_json(self, key: str) -> dict | None:
        try:
            obj = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(obj["Body"].read())
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error reading JSON {key}: {e}")
            return None
    
    def save_df(self, df: pd.DataFrame, key: str) -> None:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=buffer.getvalue())
        print(f"Saved DataFrame to {key}")
    
    def load_df(self, key: str) -> pd.DataFrame | None:
        try:
            obj = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return pd.read_csv(io.StringIO(obj["Body"].read().decode('utf-8')))
        except self.s3.exceptions.NoSuchKey:
            print(f"No such key: {key}")
            return None
        except Exception as e:
            print(f"Error reading DataFrame {key}: {e}")
            return None

