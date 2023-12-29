import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

class UfcEventsSpider(scrapy.Spider):
    name = 'ufc_events'
    start_urls = ['http://ufcstats.com/statistics/events/completed']

    def __init__(self, results_list):
        self.results_list = results_list

    def parse(self, response):
        for event_row in response.xpath('//tr[contains(@class, "b-statistics__table-row")]'):
            link = event_row.xpath('.//a/@href').extract_first()
            event_name = event_row.xpath('.//a/text()').extract_first()
            event_date = event_row.xpath('.//span[@class="b-statistics__date"]/text()').extract_first()
            location = event_row.xpath('following-sibling::tr[1]/td/text()').extract_first()

            item = {
                'event_name': event_name.strip() if event_name else '',
                'date': event_date.strip() if event_date else '',
                'location': location.strip() if location else 'Unknown',
                'link': link.strip() if link else ''
            }
            self.results_list.append(item)
            yield item

        # Pagination
        next_page = response.css('a.b-link_style_black::attr(href)').extract()[-1]
        if next_page and response.urljoin(next_page) != response.url:
            yield response.follow(next_page, self.parse)

def run_spider():
    scraped_items = []
    process = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG'})
    process.crawl(UfcEventsSpider, results_list=scraped_items)
    process.start()
    return scraped_items

if __name__ == "__main__":
    scraped_data = run_spider()
    df = pd.DataFrame(scraped_data)
    print(df)
