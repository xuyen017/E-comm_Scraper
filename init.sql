CREATE TABLE IF NOT EXISTS data_processing_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    last_processed TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ecommerce_data (
    product_id TEXT,
    keyword TEXT,
    platform TEXT,
    title TEXT,
    price DECIMAL,
    total_sales INT,
    location TEXT,
    image_url TEXT,
    product_url TEXT,
    category TEXT,
    timecrawl TIMESTAMP
);
