import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

class UfcFightSpider(scrapy.Spider):
    name = 'ufc_fight'

    def __init__(self, urls=None, results=None, *args, **kwargs):
        super(UfcFightSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls
        self.results = results

    def parse(self, response):
        # Extracting text from fight details
        fight_details = response.css('body > section > div > div > div.b-fight-details__fight > div.b-fight-details__content ::text').getall()

        # Extracting text from total strikes section
        total_strikes = response.css('body > section > div > div > section:nth-child(4) ::text').getall()

        # Extracting text from total strikes per round section
        total_strikes_per_round = response.css('body > section > div > div > section:nth-child(5) ::text').getall()

        # Cleaning the extracted text
        fight_details = [text.strip() for text in fight_details if text.strip()]
        total_strikes = [text.strip() for text in total_strikes if text.strip()]
        total_strikes_per_round = [text.strip() for text in total_strikes_per_round if text.strip()]

        data = {
            'url': response.url,
            'fight_details': fight_details,
            'total_strikes': total_strikes,
            'total_strikes_per_round': total_strikes_per_round
        }

        self.results[response.url].append(data)

class UfcCrawler:
    def scrape_multiple_sites(self, urls):
        results = {url: [] for url in urls}
        process = CrawlerProcess({
            'USER_AGENT': 'Mozilla/5.0 (compatible; Scrapy/1.0; +http://scrapy.org)'
        })

        process.crawl(UfcFightSpider, urls=urls, results=results)
        process.start()

        return results

if __name__ == "__main__":
    urls = [
        "http://ufcstats.com/fight-details/3fa8ee3fdc04fe36",
        # Add more URLs here
    ]
    crawler = UfcCrawler()
    results = crawler.scrape_multiple_sites(urls)

    for url, data in results.items():
        print(f"Data from {url}:\n", data)

    print("ready")
