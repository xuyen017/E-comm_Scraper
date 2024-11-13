import yaml
import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass
import urllib.parse
import logging


logger = logging.getLogger()


class BaseParser:

    @dataclass
    class ProductInfo():
        prod_id: int      # ID sản phẩm
        keyword: str      # Từ khóa tìm kiếm
        platform: str     # Tiki hoặc Sendo)
        title: str        # Tên sản phẩm
        price: str        # Giá sản phẩm
        total_sales: str  # Tổng số lượng đã bán
        location: str     # Địa chỉ
        image_url: str    # URL hình ảnh sản phẩm
        product_url: str  # URL liên kết sản phẩm

    def __init__(self, config_path):
        # khởi tạo parser với đường dẫn tới tệp YAML
        # param config_path: Đường dẫn đến tệp parse_config.yml
        try:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            logger.error('Config file not found')

    def build_search_url(self, platform, keyword):
        # Tạo URL tìm kiếm dựa trên nền tảng và từ khóa tìm kiếm
        # :param platform: Nền tảng (vd: 'tiki', 'sendo')
        # :param keyword: Từ khóa tìm kiếm nhập từ terminal
        try:
            base_url = self.config[platform]['search_url']
            # Mã hóa từ khóa để sử dụng trong URL
            encoded_keyword = urllib.parse.quote_plus(keyword)
            # url hoàn chỉnh
            search_url = f"{base_url}{encoded_keyword}"
            # Ghi log cho URL đc tạo ra
            logger.info(f"Generated search URL for {platform}: {search_url}")

            return search_url

        except KeyError as e:
            logger.error(f"Platform {platform} not found in cf. Error: {e}")
            return None

    def get_html(self, url):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)\
             AppleWebKit/537.36 (KHTML, like Gecko)\
                 Chrome/130.0.0.0 Safari/537.36'}
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.text
            else:
                logging.error(
                    f"failed to fetch {url},\
                         status code:{response.status_code}")
                return None
        except requests.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
            return None

    def parser_html(self, html_content):
        # Phân tích cú pháp HTML và trả về đối tượng BeautifulSoup.
        # :param html_content: Nội dung HTML để phân tích
        return BeautifulSoup(html_content, 'html.parser')

    def process_data(self, keyword):
        # Phương thức chung để xử lý một trang dựa trên từ khóa
        # Phương thức này sẽ được override bởi các lớp con.
        # :param keyword: Từ khóa tìm kiếm
        # :return: Dữ liệu được phân tích từ trang
        raise NotImplementedError(
            "This method should be overridden in child classes")
