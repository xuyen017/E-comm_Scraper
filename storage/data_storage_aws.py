import pandas as pd
import boto3
from io import StringIO
import os
from dotenv import load_dotenv


# Nạp biến môi trường từ .env
load_dotenv()

# Lấy cấu hình từ biến môi trường
bucket_name = os.getenv('BUCKET_NAME')
region_name = os.getenv('REGION_NAME')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
endpoint_url = os.getenv('ENDPOINT_URL')


# Hàm lưu dữ liệu lên S3
def save_data_to_s3(data, keyword):
    if isinstance(data, list):
        # Chuyển thành DataFrame
        df = pd.DataFrame.from_records(data, columns=[
            "product_id", "keyword", "platform", 
            "title", "price", "total_sales", 
            "location", "image_url", "product_url", 
            "category", "timecrawl"
        ])
    else:
        raise ValueError(
            "Data should be a list of records (tuples or dictionaries)")

    # Tạo kết nối S3 với boto3
    s3 = boto3.client('s3', 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key, 
                      endpoint_url=endpoint_url)

    # Ghi DataFrame vào CSV và upload lên S3
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Tạo tên file từ keyword
    file_name = f"{keyword}_output.csv"

    # Upload lên S3
    s3.put_object(
        Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    print(f"File uploaded successfully to {bucket_name}/{file_name}")
