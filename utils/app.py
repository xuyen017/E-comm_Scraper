import logging
from datetime import datetime
from scraping.scraper_sendo import SendoScraper
from scraping.scraper_tiki import TikiScraper

# Cấu hình logging cho Airflow
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
config_path = "config/parse_config.yml"
time_crawl = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def scrape_data_tiki(keyword, category):

    # Tạo scraper cho Tiki và scrape
    tiki_scraper = TikiScraper(config_path)
    tiki_results = tiki_scraper.scrape(keyword)
    # Kiểm tra nếu item là tuple và thêm category vào cuối tuple
    for i, item in enumerate(tiki_results):
        if isinstance(item, tuple):  # Nếu item là tuple
            # Thêm category và timecrawl vào tuple
            item_with_category = item + (category, time_crawl)
            # Cập nhật lại item trong list
            tiki_results[i] = item_with_category
        else:
            logger.error(f"Item không phải là tuple: {item}")

    logger.info("Kết quả tìm kiếm từ Sendo:")
    for result in tiki_results:
        logger.info(result)

    return tiki_results


def scrape_data_sendo(keyword, category):
    # Tạo scraper cho Sendo và scrape
    sendo_scraper = SendoScraper(config_path)
    sendo_results = sendo_scraper.scrape(keyword)

    # Kiểm tra nếu item là tuple và thêm category vào cuối tuple
    for i, item in enumerate(sendo_results):
        if isinstance(item, tuple):  # Nếu item là tuple
            # Thêm category và timecrawl vào tuple
            item_with_category = item + (category, time_crawl)
            # Cập nhật lại item trong list
            sendo_results[i] = item_with_category
        else:
            logger.error(f"Item không phải là tuple: {item}")

    logger.info("Kết quả tìm kiếm từ Sendo:")
    for result in sendo_results:
        logger.info(result)

    return sendo_results