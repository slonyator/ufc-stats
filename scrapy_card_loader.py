import scrapy
import pandas as pd
from scrapy.crawler import CrawlerProcess

class UfcCardSpider(scrapy.Spider):
    name = 'ufc_card_spider'
    start_urls = ['http://ufcstats.com/event-details/a8e8587a06e73c87']

    def parse(self, response):
        # Extracting headers
        headers = response.css('body > section > div > div > table > thead > tr > th::text').getall()
        # Cleaning headers
        headers = [header.strip() for header in headers]

        # Extracting table rows
        table_rows = response.css('body > section > div > div > table > tbody > tr')
        for row in table_rows:
            data = row.css('td::text').getall()
            data = [d.strip() for d in data]
            yield dict(zip(headers, data))

class UfcCardPipeline:
    def __init__(self):
        self.items = []

    def process_item(self, item, spider):
        self.items.append(item)
        return item

def main():
    process = CrawlerProcess({
        'ITEM_PIPELINES': {'__main__.UfcCardPipeline': 1}
    })

    process.crawl(UfcCardSpider)  # Pass the class, not an instance
    process.start()

    # Convert the scraped data to a DataFrame
    data_frame = pd.DataFrame(process.crawler.engine.scraper.spider.pipeline.items)
    print(data_frame)

if __name__ == "__main__":
    main()

