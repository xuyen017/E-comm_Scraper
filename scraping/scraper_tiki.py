from parsing.parser_tiki import Tikiparser
from .scraper_base import BaseScraper


class TikiScraper(BaseScraper):
    def __init__(self, config_path):
        super().__init__(Tikiparser, config_path)  # Tiki
