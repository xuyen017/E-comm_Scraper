from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from storage.data_storage_aws import download_data_from_s3
from processing.transform import transform_raw_data_with_spark
from storage.db_storage import load_to_postgresql, check_file_in_log, update_data_processing_log


# Hàm kiểm tra log và tải dữ liệu nếu cần
def check_and_download_data(keyword):
    file_name = f"{keyword}_output.csv"
    if not check_file_in_log(file_name):
        return download_data_from_s3(file_name)
    else:
        print(f"File {file_name} đã được xử lý trước đó, bỏ qua tải dữ liệu.")
        return None


# Hàm transform dữ liệu với Spark
def transform_raw_data(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids="check_and_download_data")
    if raw_data is not None:
        return transform_raw_data_with_spark(raw_data)
    return None


# Hàm lưu dữ liệu vào PostgreSQL và cập nhật log
def load_and_log_data(**kwargs):
    transformed_data = kwargs['ti'].xcom_pull(task_ids="transform_raw_data")
    file_name = kwargs['file_name']
    if transformed_data is not None:
        load_to_postgresql(transformed_data, "ecommerce_data")
        update_data_processing_log(file_name)


# Khởi tạo DAG
dag = DAG(
    dag_id="transform_and_load_data",
    start_date=datetime(2024, 11, 18),
    schedule_interval="@weekly",
    catchup=False
)

keyword = "some_keyword"
file_name = f"{keyword}_output.csv"

# Các tasks trong DAG
check_and_download_task = PythonOperator(
    task_id="check_and_download_data",
    python_callable=check_and_download_data,
    op_args=[keyword],
    dag=dag
)

transform_task = PythonOperator(
    task_id="transform_raw_data",
    python_callable=transform_raw_data,
    provide_context=True,
    dag=dag
)

load_and_log_task = PythonOperator(
    task_id="load_and_log_data",
    python_callable=load_and_log_data,
    provide_context=True,
    op_kwargs={"file_name": file_name},
    dag=dag
)

check_and_download_task >> transform_task >> load_and_log_task
