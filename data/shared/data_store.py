import io
import os
import json
import boto3
import lakefs
import pandas as pd
from lakefs.client import Client

class LakeFSDataStore:

    def __init__(self, repo_name: str, endpoint: str, branch: str = "main"):
        self.repo_name = repo_name
        self.branch = branch
        access_key = os.getenv("LAKEFS_USERNAME")
        secret_key = os.getenv("LAKEFS_PASSWORD")
        self.repo = lakefs.repository(
            repository_id = repo_name,
            client = Client(
                username = access_key,
                password = secret_key,
                host = endpoint
            )
        )
        self.s3 = boto3.client(
            "s3",
            endpoint_url = endpoint,
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key,
            region_name="us-east-2"
        )

    def _key(self, path: str) -> str:
        return f"{self.branch}/{path.lstrip('/')}"

    def save_json(self, key: str, data: dict) -> None:
        key = self._key(key)
        body = json.dumps(data)
        self.s3.put_object(Bucket=self.repo_name, Key=key, Body=body)
        print(f"Saved JSON to {key}")

    def load_json(self, key: str) -> dict | None:
        key = self._key(key)
        try:
            obj = self.s3.get_object(Bucket=self.repo_name, Key=key)
            return json.loads(obj["Body"].read())
        except self.s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error reading JSON {key}: {e}")
            return None

    def save_df(self, df: pd.DataFrame, key: str) -> None:
        key = self._key(key)
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        self.s3.put_object(Bucket=self.repo_name, Key=key, Body=buffer.getvalue())
        print(f"Saved DataFrame to {key}")

    def load_df(self, key: str) -> pd.DataFrame:
        key = self._key(key)
        obj = self.s3.get_object(Bucket=self.repo_name, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))
    
    def checkout(self, branch: str):
        if branch not in [b.id for b in list(self.repo.branches())]:
            print(f"Checkout failed. {branch} does not exist")
        else:
            self.branch = branch
            print(f"Switched to branch '{branch}'")

    def commit(self, message: str) -> str | None:
        try:
            res = self.repo.branch(self.branch).commit(message)
            print(f"Commit ID: {res.id} to {self.branch}")
            return res.id
        except lakefs.exceptions.BadRequestException as e:
            print(f"Commit failed: {e} on {self.branch}")
            return None

    def create_branch(self, name: str, checkout: bool = False) -> str:
        res = self.repo.branch(name).create(source_reference = self.branch, exist_ok=True)
        print(f"Created {name} from {self.branch}")
        if checkout:
            self.checkout(name)
        return res.id

    def merge_branch(self, dest: str, delete_after_merge: bool = False) -> str:
        merge_commit = self.repo.branch(self.branch).merge_into(dest)
        print(f"Merged {self.branch} into {dest}. Merge commit: {merge_commit}")
        if delete_after_merge:
            self.repo.branch(self.branch).delete()
            print(f"Deleted {self.branch}")
            self.checkout(dest)
        return merge_commit
