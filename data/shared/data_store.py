import io
import json
import pandas as pd
import boto3

class S3DataStore:

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3")

    def load_json(self, key) -> dict|None:
        try:
            obj = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            data = json.loads(obj['Body'].read())
            return data
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error loading JSON for {key}: {e}")
            return None
    
    def save_json(self, key, data) -> None:
        self.s3.put_object(
            Bucket = self.bucket_name,
            Key = key,
            Body = json.dumps(data)
        )
    
    def load_df(self, key) -> pd.DataFrame:
        obj = self.s3.get_object(Bucket=self.bucket_name, Key=key)
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    def save_df(self, df, key) -> None:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=csv_buffer.getvalue())
        print(f"Uploaded DataFrame to {key}")

    def list_s3_keys(self, prefix):
        keys = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys