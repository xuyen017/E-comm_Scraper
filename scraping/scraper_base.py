import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseScraper:
    def __init__(self, parser_class, config_path):
        self.parser = parser_class(config_path)
        self.config_path = config_path

    def scrape(self, keyword):
        logger.info(
            f"Starting scrape for keyword '{keyword}' on\
                 {self.parser.__class__.__name__}")
        try:
            result_data = self.parser.process_data(keyword)
            logger.info(
                f"Scraped {len(result_data)} products for keyword '{keyword}'")
            return result_data
        except Exception as e:
            logger.error(f"Scraping failed for keyword '{keyword}': {e}")
            return []
