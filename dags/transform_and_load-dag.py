from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from storage.data_storage_aws import download_data_from_s3
from processing.transform import transform_raw_data_with_spark
from storage.db_storage import load_to_postgresql
from storage.db_storage import check_file_in_log
import psycopg2
import os


# Hàm kiểm tra log và quyết định tải dữ liệu
def check_and_download_data(keyword):
    file_name = f"{keyword}_output.csv"

    # Kiểm tra xem file đã được xử lý chưa
    if not check_file_in_log(file_name):
        raw_data = download_data_from_s3(file_name)  # Tải dữ liệu nếu chưa xử lý
        return raw_data
    else:
        print(f"File {file_name} đã được xử lý trước đó, bỏ qua tải dữ liệu.")
        return None  # Trả về None nếu không cần tải lại


# Hàm transform dữ liệu với Spark
def transform_raw_data(raw_data):
    transformed_data = transform_raw_data_with_spark(raw_data)
    return transformed_data


# Hàm lưu dữ liệu vào PostgreSQL
def load_to_postgresql(transformed_data, file_name):
    if transformed_data is not None:
        save_data_to_postgresql(transformed_data)

        # Cập nhật log sau khi xử lý xong
        update_data_processing_log(file_name)


# Hàm cập nhật log trong PostgreSQL
def update_data_processing_log(file_name):
    # Lấy thông tin kết nối từ biến môi trường
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5433")
    pg_database = os.getenv("POSTGRES_DB", "ecommerce")
    pg_user = os.getenv("POSTGRES_USER", "admin")
    pg_password = os.getenv("POSTGRES_PASSWORD", "admin")

    # Kết nối đến PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    # Thực hiện truy vấn để cập nhật log
    cursor.execute("""
        INSERT INTO data_processing_log (file_name, processed_date, status, last_processed)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (file_name) DO UPDATE 
        SET processed_date = EXCLUDED.processed_date, 
            status = EXCLUDED.status,
            last_processed = EXCLUDED.last_processed
    """, (file_name, datetime.now(), "completed", datetime.now()))

    # Xác nhận và đóng kết nối
    conn.commit()
    cursor.close()
    conn.close()


dag = DAG(
    dag_id="transform_and_load_data",
    start_date=datetime(2024, 11, 18),
    schedule_interval="@weekly",
    catchup=False
)

keyword = "some_keyword"

# Kiểm tra log và tải dữ liệu nếu cần
check_and_download_task = PythonOperator(
    task_id="check_and_download_data",
    python_callable=check_and_download_data,
    op_args=[keyword],
    dag=dag
)

# Chỉ thực hiện transform và load nếu có dữ liệu
transform_task = PythonOperator(
    task_id="transform_raw_data",
    python_callable=transform_raw_data,
    op_args=['{{ ti.xcom_pull(task_ids="check_and_download_data") }}'],
    dag=dag
)

load_task = PythonOperator(
    task_id="load_to_postgresql",
    python_callable=load_to_postgresql,
    op_args=['{{ ti.xcom_pull(task_ids="transform_raw_data") }}', keyword],
    dag=dag
)

check_and_download_task >> transform_task >> load_task
