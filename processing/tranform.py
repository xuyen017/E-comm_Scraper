from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, regexp_replace

# Hàm biến đổi dữ liệu thô với Spark
def transform_raw_data_with_spark(raw_data):
    # Khởi tạo SparkSession nếu chưa có
    spark = SparkSession.builder \
        .appName("Ecommerce Data Transformation") \
        .getOrCreate()

    # Đọc dữ liệu từ file CSV (raw_data là đường dẫn tới file CSV)
    df = spark.read.csv(raw_data, header=True, inferSchema=True)

    # Chuyển đổi cột 'timecrawl' thành kiểu timestamp
    df = df.withColumn("timecrawl", to_timestamp("timecrawl", "yyyy-MM-dd HH:mm:ss"))

    # Làm sạch dữ liệu nếu cần (ví dụ: loại bỏ kí tự không cần thiết trong 'price')
    df = df.withColumn("price", regexp_replace("price", "₫", "").cast("float"))

    # Thực hiện các phép biến đổi khác theo yêu cầu
    df = df.withColumn("total_sales", regexp_replace("total_sales", "Đã bán ", "").cast("int"))

    # Cung cấp một đường dẫn tạm thời để lưu dữ liệu đã biến đổi
    output_path = "/tmp/transformed_data.csv"
    df.write.option("header", "true").csv(output_path)

    # Dừng SparkSession khi hoàn thành
    spark.stop()

    return output_path  # Trả về đường dẫn tới tệp dữ liệu đã được biến đổi
