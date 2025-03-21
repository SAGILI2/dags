from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd
import numpy as np
from io import BytesIO
import logging

MINIO_CLIENT = Minio(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)
BUCKET_NAME = 'airflow-source-data'


logger = logging.getLogger("airflow.task")

def produce_and_upload_chunks(**kwargs):
    # Simulating large dataset (~1GB+)
    num_rows = 50_000_000
    df = pd.DataFrame({'data': range(num_rows)})
    num_parts = 5
    for i, part in enumerate(np.array_split(df, num_parts)):
        buffer = BytesIO()
        part.to_parquet(buffer, index=False)
        buffer.seek(0)
        MINIO_CLIENT.put_object(
            BUCKET_NAME,
            f"data_part_{i}.parquet",
            buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/parquet"
        )
        logger.info(f"Uploaded part {i} with {len(part)} rows")


def consume_chunk(part_index, **kwargs):
    response = MINIO_CLIENT.get_object(BUCKET_NAME, f"data_part_{part_index}.parquet")
    df = pd.read_parquet(BytesIO(response.read()))
    logger.info(f"Processing part {part_index} with {len(df)} rows")


with DAG(
    dag_id='parallel_processing_minio_dag',
    start_date=datetime(2024, 2, 13),
    schedule_interval=None,
    catchup=False
) as dag:

    produce_task = PythonOperator(
        task_id='produce_and_upload_chunks',
        python_callable=produce_and_upload_chunks
    )

    consume_tasks = []
    for i in range(5):
        consume_task = PythonOperator(
            task_id=f'consume_chunk_{i}',
            python_callable=consume_chunk,
            op_kwargs={'part_index': i}
        )
        consume_tasks.append(consume_task)

    produce_task >> consume_tasks
