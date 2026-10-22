import csv
import logging
from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from pathlib import Path

DATA_PATH = Path("/usr/local/airflow/include/downloads/data.csv")
logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    tags=['sensor'],
    catchup=False
)
def local_file_sensor_dag():

    wait_for_files = FileSensor(
        task_id='wait_for_files',
        fs_conn_id='fs_default',
        filepath=str(DATA_PATH),
    )

    @task
    def process_file():
        try:
            with DATA_PATH.open(encoding="utf-8", newline="") as csv_file:
                rows = list(csv.reader(csv_file, strict=True))
        except (OSError, UnicodeDecodeError, csv.Error):
            logger.exception("Failed to read CSV file at %s", DATA_PATH)
            raise

        logger.info("CSV data from %s: %s", DATA_PATH, rows)

    wait_for_files >> process_file()

local_file_sensor_dag()