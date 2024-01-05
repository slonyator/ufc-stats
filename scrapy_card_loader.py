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

            data_link = row.attrib.get('data-link')
            row_data_dict = dict(zip(headers, row_data))
            row_data_dict['data_link'] = data_link

            self.results[response.url].append(row_data_dict)

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

    # Clean up the data
    # Example

    df = results['http://ufcstats.com/event-details/a8e8587a06e73c87']

    df_cleaned = (
        df
        .assign(
            Kd_1=lambda x: x['Kd'].str.split('  ', expand=True)[0],
            Kd_2=lambda x: x['Kd'].str.split('  ', expand=True)[1],
            Str_1=lambda x: x['Str'].str.split('  ', expand=True)[0],
            Str_2=lambda x: x['Str'].str.split('  ', expand=True)[1],
            Td_1=lambda x: x['Td'].str.split('  ', expand=True)[0],
            Td_2=lambda x: x['Td'].str.split('  ', expand=True)[1],
            Sub_1=lambda x: x['Sub'].str.split('  ', expand=True)[0],
            Sub_2=lambda x: x['Sub'].str.split('  ', expand=True)[1]
        )
        .drop(columns=['Fighter', 'Kd', 'Str', 'Td', 'Sub'])
    )

    print("ready!")
