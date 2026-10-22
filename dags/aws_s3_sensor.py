
from datetime import datetime, timedelta
from pathlib import Path
import shutil

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# Replace these values with your own S3 details
BUCKET_NAME = "airflow-project-s3-storage"
S3_KEY = "data.csv"
AWS_CONN_ID = "aws_s3"

# This directory is inside the local Astro Airflow container
LOCAL_FILE = Path("/usr/local/airflow/include/downloads/data.csv")


@dag(
    dag_id="s3_sensor_download",
    description="Wait for an S3 file, download it, and inspect it",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["aws", "s3", "airflow3"],
)
def s3_sensor_download():

    # Task 1: Wait until the specified S3 object exists
    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name=BUCKET_NAME,
        bucket_key=S3_KEY,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=60 * 60,
        mode="reschedule",
    )

    # Task 2: Download the object into the Astro container
    @task
    def download_s3_file():
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        LOCAL_FILE.parent.mkdir(parents=True, exist_ok=True)

        s3_object = hook.get_key(
            key=S3_KEY,
            bucket_name=BUCKET_NAME,
        )

        if s3_object is None:
            raise FileNotFoundError(
                f"S3 object not found: s3://{BUCKET_NAME}/{S3_KEY}"
            )

        response = s3_object.get()
        try:
            with LOCAL_FILE.open("wb") as local_file:
                shutil.copyfileobj(response["Body"], local_file)
        finally:
            response["Body"].close()

        print(f"Downloaded file to: {LOCAL_FILE}")
        print(f"File size: {LOCAL_FILE.stat().st_size} bytes")

        return str(LOCAL_FILE)

    # Task 3: Read a sample of the downloaded CSV
    @task
    def inspect_csv(file_path: str):
        import csv

        with open(file_path, "r", newline="", encoding="utf-8-sig") as file:
            reader = csv.reader(file)
            for row_number, row in enumerate(reader):
                print(row)
                if row_number >= 4:
                    break

    downloaded_file = download_s3_file()

    # Define the execution order
    wait_for_s3_file >> downloaded_file
    inspect_csv(downloaded_file)


s3_sensor_download()
