FROM apache/airflow:2.3.0-python3.9

# Thiết lập để có quyền root cho các lệnh tiếp theo
USER root

# Dọn dẹp và đảm bảo thư mục apt tồn tại trước khi cập nhật
RUN rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/apt/lists/partial \
    && apt-get update && apt-get install -y \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    wget \
    unzip \
    openjdk-11-jdk  # Cài đặt Java cho Spark

# Cài đặt PostgreSQL client
RUN apt-get install -y postgresql-client

# Tải và cài đặt Google Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt install -y ./google-chrome-stable_current_amd64.deb \
    && rm -f google-chrome-stable_current_amd64.deb

# Cài đặt Apache Spark
RUN wget https://dlcdn.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz \
    && tar -xvzf spark-3.5.3-bin-hadoop3.tgz \
    && mv spark-3.5.3-bin-hadoop3 /opt/spark \
    && rm spark-3.5.3-bin-hadoop3.tgz

# Thiết lập biến môi trường cho Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Chuyển sang người dùng airflow trước khi cài đặt các gói
USER airflow
# Cài đặt pip và các phụ thuộc trong requirements.txt
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Cài đặt thêm selenium và webdriver-manager
RUN pip install selenium webdriver-manager

# Sao chép các thư mục cần thiết vào container
COPY scraping /opt/airflow/scraping
COPY parsing /opt/airflow/parsing
COPY dags /opt/airflow/dags
COPY utils /opt/airflow/utils
COPY config /opt/airflow/config
COPY storage /opt/airflow/storage
COPY processing /opt/airflow/processing 

# Cập nhật PYTHONPATH để Python tìm kiếm trong các thư mục này
ENV PYTHONPATH="/opt/airflow/scraping:/opt/airflow/parsing:/opt/airflow/storage:/opt/airflow/processing:${PYTHONPATH}"

# Tạo các thư mục với quyền truy cập cần thiết nếu cần
RUN mkdir -p /opt/airflow/dags /opt/airflow/utils /opt/airflow/scraping /opt/airflow/parsing /opt/airflow/config /opt/airflow/storage /opt/airflow/processing 
