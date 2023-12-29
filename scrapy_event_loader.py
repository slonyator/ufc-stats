import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd


class UfcEventsSpider(scrapy.Spider):
    name = "ufc_events"
    start_urls = ["http://ufcstats.com/statistics/events/completed"]

    def __init__(self, results_list, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.results_list = results_list

    def parse(self, response):
        # Iterate over each event row
        for event_row in response.css("tr.b-statistics__table-row"):
            link = event_row.css("a::attr(href)").get()
            event_name = event_row.css("a::text").get()
            event_date = event_row.css("span.b-statistics__date::text").get()
            location = event_row.xpath(
                'following-sibling::tr[1]/td[@class="b-statistics__table-col b-statistics__table-col_style_big-top-padding"]/text()'
            ).get()

            item = {
                "event_name": event_name.strip() if event_name else "",
                "date": event_date.strip() if event_date else "",
                "location": location.strip() if location else "Unknown",
                "link": link,
            }
            self.results_list.append(item)
            yield item

        # Handle pagination
        current_page = response.css(
            "li.b-statistics__paginate-item.b-statistics__paginate-item_state_current::text"
        ).get()
        next_page = response.css(
            "li.b-statistics__paginate-item > a::attr(href)"
        ).extract()
        if current_page and next_page:
            next_page_url = next_page[
                -1
            ]  # Assuming the last link is the 'Next' button
            if (
                next_page_url
                and response.urljoin(next_page_url) != response.url
            ):
                yield response.follow(next_page_url, self.parse)


class UfcCrawler:
    def __init__(self):
        self.scraped_events = []

    def get_events(self):
        process = CrawlerProcess(settings={"LOG_LEVEL": "INFO"})
        process.crawl(UfcEventsSpider, results_list=self.scraped_events)
        process.start()
        return self.scraped_events


if __name__ == "__main__":
    scraped_data = UfcCrawler().get_events()
    df = pd.DataFrame(scraped_data)
    print(df)
