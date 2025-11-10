import os
import zipfile
import boto3
import argparse
from dotenv import load_dotenv
load_dotenv()

def create_lambda_zip(zip_path: str, paths: list[str]):
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for path in paths:
            if os.path.isfile(path):
                # Handle individual files
                arcname = os.path.relpath(path, start="..")
                print(f"Adding {path} as {arcname}")
                zf.write(path, arcname)
            elif os.path.isdir(path):
                # Handle directories (existing logic)
                for root, _, files in os.walk(path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, start="..")
                        print(f"Adding {file_path} as {arcname}")
                        zf.write(file_path, arcname)
            else:
                print(f"Warning: {path} is neither a file nor a directory")

def upload_lambda_function(function_name: str, zip_path: str):
    client = boto3.client(
        "lambda",
        region_name="us-east-2",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    with open(zip_path, "rb") as f:
        code_bytes = f.read()
    client.update_function_code(
        FunctionName=function_name,
        ZipFile=code_bytes,
        Publish=True,
    )

if __name__ == "__main__":
    
    # get function name from args
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--function-name", type=str, required=True, help="Name of the Lambda function to deploy")
    args = parser.parse_args()
    function_name = args.function_name
    
    zip_path = "../untracked/package.zip"
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)

    common = ["../src/api", "../src/ds", "../src/shared", "../src/data/utils.py"]
    function_to_folders = {
        "get-weather-data": common + ["../src/data/extract.py"],
        "process-weather-data": common + ["../src/data/transform.py"],
        "validate-raw-data": common + ["../src/data/validate_extract.py"],
        "validate-processed-data": common + ["../src/data/validate_transform.py"],
        "predict-weather-code": common + ["../src/model/predict.py", "../src/data/extract.py", "../src/data/transform.py"],
        "detect-data-drift": common + ["../src/monitoring/check_data_drift.py"],
        "model-accuracy": ["../src/model/validate_model.py"]

    }
    if function_name == "all":
        for fn in function_to_folders.keys():
            zip_path = f"../untracked/package.zip"
            create_lambda_zip(zip_path, function_to_folders[fn])
            upload_lambda_function(
                function_name=fn,
                zip_path=zip_path
            )
    else:
        create_lambda_zip(zip_path, function_to_folders[function_name])
        upload_lambda_function(
            function_name=function_name,
            zip_path=zip_path
        )

# to run this, cd into scripts/ and run:
# python deploy_to_lambda.py --function-name process-weather-data
# make sure AWS creds are set in .env file