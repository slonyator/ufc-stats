import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

class UfcCardSpider(scrapy.Spider):
    name = 'ufc_card_spider'
    start_urls = ['http://ufcstats.com/event-details/a8e8587a06e73c87']

    def __init__(self, results):
        self.results = results

    def parse(self, response):
        # Extracting headers
        headers = response.css('thead.b-fight-details__table-head > tr > th::text').getall()
        headers = [header.strip() for header in headers]

        # Extracting rows
        for row in response.css('tbody.b-fight-details__table-body > tr'):
            row_data = []
            for i in range(len(headers)):
                cell_data = row.css('td:nth-child({}) *::text'.format(i + 1)).getall()
                cell_data = ' '.join(text.strip() for text in cell_data)
                row_data.append(cell_data.strip())
            self.results.append(dict(zip(headers, row_data)))

def scrape_and_convert_to_dataframe():
    results = []
    process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/5.0 (compatible; Scrapy/1.0; +http://scrapy.org)'
    })

    process.crawl(UfcCardSpider, results=results)
    process.start()  # the script will block here until the crawling is finished

    # Convert the results into a DataFrame
    if results:
        df = pd.DataFrame(results)
        return df
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    df = scrape_and_convert_to_dataframe()
    print(df)
