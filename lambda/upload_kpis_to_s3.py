import boto3
from pathlib import Path

# Config
BUCKET_NAME = "lab6kinesis"
REGION = "eu-north-1"
LOCAL_KPI_DIR = Path(__file__).resolve().parent.parent / "output" / "kpis"

# Create S3 client
s3 = boto3.client("s3", region_name=REGION)

def upload_kpi_file(file_path):
    key = f"kpis/{file_path.name}"
    try:
        s3.upload_file(
            Filename=str(file_path),
            Bucket=BUCKET_NAME,
            Key=key
        )
        print(f"✅ Uploaded: {file_path.name} → s3://{BUCKET_NAME}/{key}")
    except Exception as e:
        print(f"❌ Failed to upload {file_path.name}: {e}")

if __name__ == "__main__":
    kpi_files = list(LOCAL_KPI_DIR.glob("*.json"))
    if not kpi_files:
        print("⚠️ No KPI files found to upload.")
    else:
        for file_path in kpi_files:
            upload_kpi_file(file_path)
