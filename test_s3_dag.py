from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import boto3

# AWS S3 Bucket Configuration
S3_BUCKET = "my-parquet-bucket"
S3_KEY = "data/output.parquet"
S3_PATH = f"s3://{S3_BUCKET}/{S3_KEY}"

# Function to generate large data and save as Parquet
def generate_large_data(**kwargs):
    df = pd.DataFrame({"col1": range(1000000), "col2": range(1000000, 2000000)})
    df.to_parquet("/tmp/data.parquet", engine="pyarrow")

    # Upload to S3 (or use GCS/HDFS)
    s3_client = boto3.client("s3")
    s3_client.upload_file("/tmp/data.parquet", S3_BUCKET, S3_KEY)

    return S3_PATH  # Pass only the file path

# Function to read and process Parquet file
def process_parquet_file(**kwargs):
    ti = kwargs["ti"]
    s3_path = ti.xcom_pull(task_ids="generate_data_task")  # Retrieve file path

    # Read Parquet file from S3
    df = pd.read_parquet(s3_path, engine="pyarrow")

    # Process data (example: sum of a column)
    total_sum = df["col1"].sum()
    print(f"Total Sum: {total_sum}")

with DAG(
    "parquet_airflow_example",
    start_date=datetime(2024, 2, 12),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    generate_data_task = PythonOperator(
        task_id="generate_data_task",
        python_callable=generate_large_data,
        provide_context=True,
    )

    process_data_task = PythonOperator(
        task_id="process_data_task",
        python_callable=process_parquet_file,
        provide_context=True,
    )

    generate_data_task >> process_data_task  # Task Dependency
