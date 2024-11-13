from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from parsing.parser_base import BaseParser
import json
import logging
import time
import re

logger = logging.getLogger()


class Tikiparser(BaseParser):
    def __init__(self, config_path):
        super().__init__(config_path)
        # Khởi tạo WebDriver với chế độ headless và các tuỳ chọn cần thiết
        chrome_options = Options()
        # Chạy Chrome ở chế độ headless
        chrome_options.add_argument("--headless")
        # Khuyến nghị trong Docker
        chrome_options.add_argument("--no-sandbox")
        # Giải quyết vấn đề bộ nhớ chia sẻ
        chrome_options.add_argument("--disable-dev-shm-usage")
        # Để Selenium có thể kết nối với Chrome
        chrome_options.add_argument("--remote-debugging-port=9222")
        self.driver = webdriver.Chrome(options=chrome_options)

    def process_data(self, keyword):
        tuple_data_page = []
        current_page = 1
        base_url = self.build_search_url('tiki', keyword)

        if base_url is None:
            raise ValueError(
                "Base URL is None. Check build_search_url for errors.")

        while True:
            search_url = f"{base_url}&page={current_page}"
            logger.info(f"Fetching page {current_page} for URL: {search_url}")

            # Mở trang bằng Selenium và chờ tải trang
            self.driver.get(search_url)
            time.sleep(3)

            try:
                # Chờ cho đến khi các sản phẩm được tải
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (By.CLASS_NAME, "CatalogProducts__Wrapper-sc-1r8ct7c-0")
                    )
                )
            except Exception as e:
                logger.error(f"Error waiting for products to load: {e}")
                break

            # Lấy mã nguồn HTML và phân tích bằng BeautifulSoup
            html_content = self.driver.page_source
            soup = self.parser_html(html_content)
            list_prod_id = soup.find_all(
                "div", attrs={"class": re.compile(
                    'styles__ProductItemContainerStyled-sc-bszvl7-0')}
            )

            if not list_prod_id:
                logger.warning(
                    f"No products found on page {current_page}.\
                         Stopping scrape.")
                break
            else:
                logger.info(
                    f"Found {len(list_prod_id)} products on page\
                         {current_page}.")

            for prod in list_prod_id:
                product_data = self.extract_product_data(prod, 'tiki', keyword)
                if product_data:
                    tuple_data_page.append((
                        product_data.prod_id,
                        product_data.keyword,
                        product_data.platform,
                        product_data.title,
                        product_data.price,
                        product_data.total_sales,
                        product_data.location,
                        product_data.image_url,
                        product_data.product_url
                    ))

            current_page += 1

        self.driver.quit()  # Đóng trình duyệt sau khi hoàn tất
        return tuple_data_page

    def extract_product_data(self, prod, platform, keyword):
        try:
            # Lấy product_id từ data-view-content
            data_content = prod.get("data-view-content")
            product_id = None
            if data_content:
                try:
                    data = json.loads(data_content.replace('&quot;', '"'))
                    product_id = data.get("click_data", {}).get("id")
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(
                        f"Error parsing JSON data for product_id: {e}")
                    product_id = None

            # Lấy tiêu đề sản phẩm (kiểm tra None)
            title_tag = prod.find(
                'h3', class_="style__NameStyled-sc-139nb47-8 ibOlar")
            title = title_tag.text.strip() if title_tag else "Unknown Title"

            # Lấy giá sản phẩm (kiểm tra None và đảm bảo gọi strip())
            price_tag = prod.find('div', class_='price-discount__price')
            price = price_tag.text.strip() if price_tag else "Unknown Price"

            # Lấy số lượng bán (kiểm tra None)
            total_sales_tag = prod.find('span', class_="quantity has-border")
            total_sales = total_sales_tag.text.strip() \
                if total_sales_tag else '0'

            # Đặt vị trí mặc định
            location = "VN"

            # Lấy URL hình ảnh (kiểm tra None và thuộc tính 'srcset')
            image_tag = prod.find('img')
            image_url = image_tag['srcset'] \
                if image_tag and 'srcset' in image_tag.attrs else "No Image"

            # Lấy URL sản phẩm (kiểm tra None và thuộc tính 'href')
            link_tag = prod.find('a', class_='style__ProductLink-sc-139nb47-2')
            product_url = "https://tiki.vn" + link_tag['href'] \
                if link_tag and 'href' in link_tag.attrs else "No URL"

            return self.ProductInfo(
                prod_id=product_id if product_id else hash(title),
                keyword=keyword,
                platform=platform,
                title=title,
                price=price,
                total_sales=total_sales,
                location=location,
                image_url=image_url,
                product_url=product_url
            )

        except Exception as e:
            logger.error(f"Error extracting product data: {e}")
            return None
