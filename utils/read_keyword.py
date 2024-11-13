import pandas as pd
from datetime import datetime


def read_keyword(**kwargs):
    file_path = '/opt/airflow/keyword.csv'
    try:
        # Đọc dữ liệu từ CSV
        df = pd.read_csv(file_path)

        # Lấy execution_date từ Airflow context
        execution_date = kwargs['execution_date'].date()
        start_date = datetime(2024, 11, 10).date()  # Ngày bắt đầu cố định

        # Tính số ngày đã qua từ ngày bắt đầu
        days_diff = (execution_date - start_date).days

        # Kiểm tra xem có đủ dữ liệu không
        if 0 <= days_diff < len(df):
            # Lấy dòng tương ứng với số ngày đã qua
            selected_row = df.iloc[days_diff]
            result = f"Catalog and Keywords for {execution_date}:"
            result += f"\nCategory: {selected_row['category']},\
                 Keyword: {selected_row['keyword']}"
            return selected_row['keyword'], selected_row['category']
        else:
            return None, None  # Trả về None nếu không có dữ liệu

    except Exception as e:
        raise Exception(f"Error reading CSV file: {str(e)}")
