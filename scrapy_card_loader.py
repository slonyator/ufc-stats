import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

class UfcCardSpider(scrapy.Spider):
    name = 'ufc_card_spider'

    def __init__(self, urls, results):
        self.start_urls = urls
        self.results = results

    def parse(self, response):
        headers = response.css('thead.b-fight-details__table-head > tr > th::text').getall()
        headers = [header.strip() for header in headers]

        for row in response.css('tbody.b-fight-details__table-body > tr'):
            row_data = []
            for i in range(len(headers)):
                cell_data = row.css('td:nth-child({}) *::text'.format(i + 1)).getall()
                cell_data = ' '.join(text.strip() for text in cell_data)
                row_data.append(cell_data.strip())
            self.results[response.url].append(dict(zip(headers, row_data)))

class UfcCrawler:
    def scrape_multiple_sites(self, urls):
        results = {url: [] for url in urls}
        process = CrawlerProcess({
            'USER_AGENT': 'Mozilla/5.0 (compatible; Scrapy/1.0; +http://scrapy.org)'
        })

        process.crawl(UfcCardSpider, urls=urls, results=results)
        process.start()

        dataframes = {}
        for url, data in results.items():
            if data:
                df = pd.DataFrame(data)
                dataframes[url] = df
            else:
                dataframes[url] = pd.DataFrame()

        return dataframes

if __name__ == "__main__":
    urls = [
        "http://ufcstats.com/event-details/a8e8587a06e73c87",
        # Add more URLs here
    ]
    crawler = UfcCrawler()
    results = crawler.scrape_multiple_sites(urls)

    for url, df in results.items():
        print(f"Data from {url}:\n", df)

    print("ready!")
