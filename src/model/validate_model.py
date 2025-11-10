import json
import os
import io
import tarfile
import boto3

BUCKET = os.getenv("BUCKET", "weather-model-478492276227")
MODELS_PREFIX = os.getenv("MODELS_PREFIX", "models")
INNER_JSON = os.getenv("INNER_JSON", "accuracy.json")
DEFAULT_THRESHOLD = float(os.getenv("THRESHOLD", "0.8"))

s3 = boto3.client("s3")

def _find_member_by_name(tar: tarfile.TarFile, target_name: str):
    for m in tar.getmembers():
        if m.isfile() and m.name.rsplit("/", 1)[-1] == target_name:
            return m
    return None

def lambda_handler(event, context):
    model_name = event.get("ModelName")
    threshold = float(event.get("threshold", DEFAULT_THRESHOLD))
    inner_json = event.get("inner_json", INNER_JSON)

    if not model_name:
        # Always include validation_result so the Choice state works
        return {"validation_result": "Failed", "error": "Missing ModelName"}

    key = f"{MODELS_PREFIX}/{model_name}/output/output.tar.gz"

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        tar_bytes = obj["Body"].read()

        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:*") as tar:
            member = _find_member_by_name(tar, inner_json)
            if not member:
                return {
                    "validation_result": "Failed",
                    "error": f"{inner_json} not found in s3://{BUCKET}/{key}"
                }
            with tar.extractfile(member) as f:
                data = json.loads(f.read().decode("utf-8"))

        if "accuracy" not in data:
            return {"validation_result": "Failed", "error": "Missing 'accuracy' in JSON"}

        accuracy = float(data["accuracy"])
        result = "Passed" if accuracy > threshold else "Failed"
        return {"validation_result": result, "accuracy": accuracy}

    except Exception as e:
        # Keep a consistent shape for Step Functions
        return {
            "validation_result": "Failed",
            "error": f"Error processing s3://{BUCKET}/{key}: {e}"
        }