import os
import boto3
import tarfile
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

BUCKET_NAME = "weather-model-478492276227"
S3_KEY = "code/source.tar.gz"


def create_source_archive(archive_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    model_dir = project_root / "src" / "model"
    train_file = model_dir / "train.py"
    inference_file = model_dir / "inference.py"

    if not train_file.exists():
        raise FileNotFoundError(f"train.py not found at {train_file}")
    if not inference_file.exists():
        raise FileNotFoundError(f"inference.py not found at {inference_file}")

    # Create the tar.gz archive
    with tarfile.open(archive_path, mode="w:gz") as tar:
        # arcname ensures the files are at the root of the archive
        tar.add(train_file, arcname="train.py")
        tar.add(inference_file, arcname="inference.py")

    print(f"Created archive at: {archive_path}")


def upload_to_s3(file_path: Path) -> None:
    """
    Upload the given file to a hardcoded S3 bucket/key.
    """
    s3_client = boto3.client(
        "s3",
        region_name="us-east-2",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    s3_client.upload_file(str(file_path), BUCKET_NAME, S3_KEY)
    print(f"Uploaded {file_path} to s3://{BUCKET_NAME}/{S3_KEY}")


if __name__ == "__main__":
    # source.tar.gz will be created inside the scripts/ directory
    scripts_dir = Path("../untracked")
    archive_path = scripts_dir / "source.tar.gz"
    os.makedirs(scripts_dir, exist_ok=True)
    create_source_archive(archive_path)
    upload_to_s3(archive_path)

# to run this, cd into scripts/ and run:
# python deploy_model_to_s3.py
# Ensure AWS credentials are set in your environment variables.