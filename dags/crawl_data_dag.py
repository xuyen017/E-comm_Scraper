from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.app import scrape_data_tiki, scrape_data_sendo
from utils.read_keyword import read_keyword
from storage.data_storage_aws import save_data_to_s3


def get_keyword_data(**kwargs):
    keyword, category = read_keyword(**kwargs)
    if keyword and category:
        kwargs['ti'].xcom_push(key='keyword', value=keyword)
        kwargs['ti'].xcom_push(key='category', value=category)
    else:
        raise ValueError("No keyword or category available for today.")


dag = DAG(
    dag_id="crawl_and_stogare",
    start_date=datetime(2024, 11, 11),
    schedule_interval="@daily",
    catchup=True
)

read_keyword_task = PythonOperator(
    task_id='get_keyword_data',
    python_callable=get_keyword_data,
    provide_context=True,
    dag=dag,
)


def run_scrape_data_tiki(**kwargs):
    keyword = kwargs['ti'].xcom_pull(
        task_ids='get_keyword_data', key='keyword')
    category = kwargs['ti'].xcom_pull(
        task_ids='get_keyword_data', key='category')
    tiki_data = scrape_data_tiki(keyword, category)
    kwargs['ti'].xcom_push(key='tiki_data', value=tiki_data)
    # Đảm bảo push dữ liệu vào XCom


def run_scrape_data_sendo(**kwargs):
    keyword = kwargs['ti'].xcom_pull(
        task_ids='get_keyword_data', key='keyword')
    category = kwargs['ti'].xcom_pull(
        task_ids='get_keyword_data', key='category')
    sendo_data = scrape_data_sendo(keyword, category)
    kwargs['ti'].xcom_push(key='sendo_data', value=sendo_data)
    # Đảm bảo push dữ liệu vào XCom


crawl_tiki_task = PythonOperator(
    task_id='scrape_tiki',
    python_callable=run_scrape_data_tiki,
    provide_context=True,
    dag=dag
)

crawl_sendo_task = PythonOperator(
    task_id='scrape_sendo',
    python_callable=run_scrape_data_sendo,
    provide_context=True,
    dag=dag
)


def save_to_s3(**kwargs):
    # Lấy dữ liệu từ XCom
    tiki_data = kwargs['ti'].xcom_pull(
        task_ids='scrape_tiki', key='tiki_data')
    sendo_data = kwargs['ti'].xcom_pull(
        task_ids='scrape_sendo', key='sendo_data')
    keyword = kwargs['ti'].xcom_pull(
        task_ids='get_keyword_data', key='keyword')  # Lấy keyword từ XCom

    # Xử lý trường hợp dữ liệu rỗng
    if tiki_data is None:
        tiki_data = []
    if sendo_data is None:
        sendo_data = []

    print("Tiki Data:", tiki_data)  # Kiểm tra dữ liệu Tiki
    print("Sendo Data:", sendo_data)  # Kiểm tra dữ liệu Sendo

    # Tiến hành lưu vào S3
    save_data_to_s3(tiki_data + sendo_data, keyword)


save_s3_task = PythonOperator(
    task_id='save_to_s3',
    python_callable=save_to_s3,
    provide_context=True,
    dag=dag
)

# Thiết lập thứ tự task
read_keyword_task >> [crawl_tiki_task, crawl_sendo_task] >> save_s3_task
