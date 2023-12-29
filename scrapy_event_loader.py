import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd

# Define the Scrapy spider
class UfcEventsSpider(scrapy.Spider):
    name = 'ufc_events'
    start_urls = ['http://ufcstats.com/statistics/events/completed']

    def __init__(self, results_list):
        self.results_list = results_list

    def parse(self, response):
        for event_row in response.xpath('//tr[contains(@class, "b-statistics__table-row")]'):
            link = event_row.xpath('.//a/@href').get()
            event_name = event_row.xpath('.//a/text()').get()
            event_date = event_row.xpath('.//span[@class="b-statistics__date"]/text()').get()
            location = event_row.xpath('following-sibling::tr[1]/td/text()').get()

            if link and event_name and event_date:
                item = {
                    'event_name': event_name.strip(),
                    'date': event_date.strip(),
                    'location': location.strip() if location else 'Unknown'
                }
                print(item)
                self.results_list.append(item)
                yield item

        next_page = response.css('a.b-link_style_black[href*="page"]:last-child::attr(href)').get()
        if next_page:
            yield response.follow(next_page, self.parse)

# Function to run the spider and collect the data
def run_spider():
    scraped_items = []
    process = CrawlerProcess(settings={'LOG_LEVEL': 'DEBUG'})
    process.crawl(UfcEventsSpider, results_list=scraped_items)
    process.start()  # This will block until the spider is finished
    return scraped_items

if __name__ == "__main__":
    scraped_data = run_spider()
    df = pd.DataFrame(scraped_data)
    print(df)