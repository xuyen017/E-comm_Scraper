from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from storage.data_storage_aws import download_data_from_s3
from processing.transform import transform_data
from storage.db_storage import save_data_to_postgresql


def download_raw_data():
    # Implement tải dữ liệu từ S3
    raw_data = download_data_from_s3()
    return raw_data


def transform_raw_data(raw_data):
    # Implement xử lý transform
    transformed_data = transform_data(raw_data)
    return transformed_data


def load_to_postgresql(transformed_data):
    # Implement lưu dữ liệu vào PostgreSQL
    save_data_to_postgresql(transformed_data)


dag = DAG(
    dag_id="transform_and_load_data",
    start_date=datetime(2024, 11, 18),
    schedule_interval="@weekly",
    catchup=False
)

download_task = PythonOperator(
    task_id="download_raw_data",
    python_callable=download_raw_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_raw_data",
    python_callable=transform_raw_data,
    op_args=['{{ ti.xcom_pull(task_ids="download_raw_data") }}'],
    dag=dag
)

load_task = PythonOperator(
    task_id="load_to_postgresql",
    python_callable=load_to_postgresql,
    op_args=['{{ ti.xcom_pull(task_ids="transform_raw_data") }}'],
    dag=dag
)

download_task >> transform_task >> load_task
