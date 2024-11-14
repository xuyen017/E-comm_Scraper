import psycopg2
from psycopg2 import sql, pool
import os
from datetime import datetime
import pandas as pd
from io import StringIO
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)

# Tạo một connection pool toàn cục (tái sử dụng kết nối này)
postgres_pool = None


# Hàm để tạo pool kết nối
def create_pool():
    global postgres_pool
    if postgres_pool is None:
        postgres_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,  # số kết nối tối thiểu
            maxconn=10,  # số kết nối tối đa
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5433"),
            database=os.getenv("POSTGRES_DB", "ecommerce"),
            user=os.getenv("POSTGRES_USER", "admin"),
            password=os.getenv("POSTGRES_PASSWORD", "admin")
        )
        logging.info("PostgreSQL connection pool created.")


# Hàm để kiểm tra xem file đã có trong log hay chưa
def check_file_in_log(file_name):
    create_pool()  # Đảm bảo rằng pool được khởi tạo trước khi sử dụng
    try:
        conn = postgres_pool.getconn()
        cursor = conn.cursor()

        # Truy vấn để kiểm tra file trong bảng log
        query = sql.SQL(
            "SELECT 1 FROM data_processing_log WHERE file_name = %s LIMIT 1")
        cursor.execute(query, (file_name,))

        # Kiểm tra kết quả truy vấn
        result = cursor.fetchone()

        # Đóng kết nối
        cursor.close()
        postgres_pool.putconn(conn)

        return result is not None

    except Exception as e:
        logging.error(f"Error while checking file in log: {e}")
        return False


# Hàm cập nhật log trong PostgreSQL
def update_data_processing_log(file_name):
    create_pool()  # Đảm bảo rằng pool được khởi tạo trước khi sử dụng
    try:
        conn = postgres_pool.getconn()
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

        conn.commit()
        logging.info(f"Data processing log for {file_name} updated successfully.")
    except Exception as e:
        logging.error(f"Error while updating data processing log for {file_name}: {e}")
    finally:
        cursor.close()
        postgres_pool.putconn(conn)


def load_to_postgresql(data, table_name):
    create_pool()  # Đảm bảo rằng pool được khởi tạo trước khi sử dụng

    # Chuyển dữ liệu từ list hoặc DataFrame thành CSV
    if isinstance(data, pd.DataFrame):
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
    else:
        raise ValueError("Data must be a pandas DataFrame")

    try:
        conn = postgres_pool.getconn()
        cursor = conn.cursor()

        # Chèn dữ liệu vào PostgreSQL (với phương pháp COPY cho tốc độ cao)
        cursor.copy_expert(f"COPY {table_name} FROM stdin WITH CSV HEADER", csv_buffer)
        conn.commit()
        logging.info(f"Data loaded successfully into table {table_name}.")
    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {e}")
        conn.rollback()
    finally:
        cursor.close()
        postgres_pool.putconn(conn)
