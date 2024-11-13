from parsing.parser_sendo import SendoParser
from .scraper_base import BaseScraper


class SendoScraper(BaseScraper):
    def __init__(self, config_path):
        super().__init__(SendoParser, config_path)  # Sendo
