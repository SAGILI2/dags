from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
import time

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 1),
    'retries': 1,
}

# Define functions
def fetch_data(task_id, **kwargs):
    """Simulates fetching data in parallel."""
    time.sleep(random.randint(1, 3))
    data = f"data_from_{task_id}"
    print(f"Fetched {data}")
    return data

def process_data(task_id, **kwargs):
    """Processes data in parallel after fetching is complete."""
    ti = kwargs['ti']
    fetched_data = ti.xcom_pull(task_ids=f'fetch_{task_id}')
    time.sleep(random.randint(1, 3))
    processed_data = f"processed_{fetched_data}"
    print(f"Processed {processed_data}")
    return processed_data

def aggregate_results(**kwargs):
    """Collects all processed data and aggregates it into a final result."""
    ti = kwargs['ti']
    processed_results = [ti.xcom_pull(task_ids=f'process_{i}') for i in range(3)]
    final_result = " | ".join(processed_results)
    print(f"Final Aggregated Result: {final_result}")
    return final_result

# Define DAG
with DAG('parallel_processing_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Step 1: Fetch data in parallel
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{i}',
            python_callable=fetch_data,
            op_kwargs={'task_id': i},
        ) for i in range(3)
    ]

    # Step 2: Process data in parallel after fetching
    process_tasks = [
        PythonOperator(
            task_id=f'process_{i}',
            python_callable=process_data,
            op_kwargs={'task_id': i},
        ) for i in range(3)
    ]

    # Step 3: Aggregate results sequentially after all processing is done
    aggregate_task = PythonOperator(
        task_id='aggregate_results',
        python_callable=aggregate_results,
    )

    # Corrected dependencies
    for fetch_task, process_task in zip(fetch_tasks, process_tasks):
        fetch_task >> process_task

    process_tasks >> aggregate_task
