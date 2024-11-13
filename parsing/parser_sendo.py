
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.options import Options
from parsing.parser_base import BaseParser
import logging
import time
import re

logger = logging.getLogger()

# from selenium import webdriver


class SendoParser(BaseParser):
    def process_data(self, keyword):
        tuple_data_page = []
        base_url = self.build_search_url('sendo', keyword)

        if base_url is None:
            raise ValueError("Base URL is None")

        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Chạy chế độ headless để chạy trong Docker
        chrome_options.add_argument("--no-sandbox")  # Không sử dụng sandbox để tránh lỗi quyền
        chrome_options.add_argument("--disable-dev-shm-usage")  # Giảm dung lượng chia sẻ bộ nhớ
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-gpu")  # Vô hiệu hóa GPU (không cần thiết khi chạy headless)

        # Khởi tạo driver với ChromeDriverManager
        driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()), 
            options=chrome_options
        )
        driver.get(base_url)
        scroll_step = 500

        # Cuộn từ từ xuống để trang tải nội dung
        end_of_scroll = False
        while not end_of_scroll:
            driver.execute_script(
                "window.scrollBy(0, arguments[0]);", scroll_step)
            time.sleep(0.5)
            end_of_scroll = driver.execute_script(
                "return (window.innerHeight + window.pageYOffset)\
                     >= document.body.scrollHeight"
            )

        # Vòng lặp để tiếp tục nhấn nút "Xem thêm" cho đến khi không còn
        while True:
            try:
                load_more_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "button.d7ed-s0YDb1.d7ed-jQXTxb.d7ed-ZPZ4Mf.d7ed-YaJkXL.d7ed-bTLFAv"))
                )

                driver.execute_script("arguments[0].scrollIntoView();", load_more_button)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", load_more_button)
                time.sleep(3)

                html_content = driver.page_source
                soup = BeautifulSoup(html_content, "html.parser")

                products = soup.find_all("div", class_=re.compile(r'\bd7ed-d4keTB\b'))
                total_products_found = len(products)
                logger.info(f"Tìm thấy {total_products_found} sản phẩm.")

                end_of_scroll = False
                while not end_of_scroll:
                    driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_step)
                    time.sleep(0.5)
                    end_of_scroll = driver.execute_script(
                        "return (window.innerHeight + window.pageYOffset) >= document.body.scrollHeight"
                    )

            except Exception as e:
                logger.info(f"Không thấy nút 'Xem thêm' hoặc lỗi xảy ra: {e}")
                break

        html_content = driver.page_source
        soup = BeautifulSoup(html_content, "html.parser")

        list_prod_id = soup.find_all("div", class_="d7ed-d4keTB")
        for prod in list_prod_id:
            product_data = self.extract_product_data(prod, "sendo", keyword)
            if product_data:
                tuple_data = (
                    product_data.prod_id,
                    product_data.keyword,
                    product_data.platform,
                    product_data.title,
                    product_data.price,
                    product_data.total_sales,
                    product_data.location,
                    product_data.image_url,
                    product_data.product_url
                )
                tuple_data_page.append(tuple_data)
        driver.quit()
        return tuple_data_page

    def extract_product_data(self, prod, platform, keyword):
        def extract_product_id_from_url(url):
            match = re.search(r'-(\d+)\.html', url)
            return match.group(1) if match else None

        try:
            a_tag = prod.find("a", href=True)
            product_id = None
            if a_tag:
                product_url = a_tag["href"]
                product_id = extract_product_id_from_url(product_url)

                if product_id is None:
                    logger.error(
                        "Product ID not found in URL, skipping product")
                    return None

                product_id = int(product_id)

                title_tag = prod.find('span', class_="d7ed-Vp2Ugh")
                title = title_tag.text.strip() if title_tag else "N/A"

                price_tag = prod.find('span', class_='_0032-GpBMYp')
                price = price_tag.text.strip() if price_tag else "0"

                image_tag = prod.find('div', class_='d7ed-a1ulZz')
                if image_tag:
                    img = image_tag.find('img')
                    image_url = img.get('data-src') or img.get('src') \
                        if img else "N/A"
                else:
                    image_url = "N/A"

                # Tìm tất cả các thẻ span có lớp `d7ed-bm83Kw d7ed-mzOLVa`, bỏ lớp 'undefined'
                location_tag = prod.find('span', class_='d7ed-bm83Kw.d7ed-mzOLVa')
                # Nếu không tìm thấy thẻ span cụ thể, kiểm tra nếu có phần tử nào chứa địa chỉ
                # if not location_tag:
                #     location_tag = prod.find(
                #         'span', class_=lambda class_name: class_name and 'd7ed-mzOLVa' in class_name)
                # Nếu tìm thấy thẻ span có địa chỉ
                location = location_tag.text.strip() if location_tag else "VN"

                total_sales = (
                    prod.find("div", class_="d7ed-CX1CTf").text.strip()
                    if prod.find("div", class_="d7ed-CX1CTf")
                    else '0'
                )

                return self.ProductInfo(
                    prod_id=product_id,
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
            logger.error(f"Error extracting products data: {e}")
            return None