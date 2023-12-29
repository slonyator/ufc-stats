import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


class UfcEventItem(scrapy.Item):
    event_name = scrapy.Field()
    date = scrapy.Field()
    location = scrapy.Field()


class UfcEventsSpider(scrapy.Spider):
    name = "ufc_events"
    allowed_domains = ["ufcstats.com"]
    start_urls = ["http://ufcstats.com/statistics/events/completed"]

    def parse(self, response):
        # Process each event row in the table
        for event_row in response.xpath(
            '//tr[contains(@class, "b-statistics__table-row")]'
        ):
            # Extract event details using XPath
            link = event_row.xpath(".//a/@href").get()
            event_name = event_row.xpath(".//a/text()").get()
            event_date = event_row.xpath(
                './/span[@class="b-statistics__date"]/text()'
            ).get()
            location = event_row.xpath(
                "following-sibling::tr[1]/td/text()"
            ).get()

            if link and event_name and event_date:
                item = UfcEventItem()
                item["event_name"] = event_name.strip()
                item["date"] = event_date.strip()
                item["location"] = location.strip() if location else "Unknown"

                yield item

        # Handling pagination
        next_page = response.css(
            'a.b-link_style_black[href*="page"]:last-child::attr(href)'
        ).get()
        if next_page:
            yield scrapy.Request(
                url=response.urljoin(next_page), callback=self.parse
            )


if __name__ == "__main__":
    # Get project settings
    settings = get_project_settings()

    # Create a crawler process with those settings
    process = CrawlerProcess(settings=settings)

    # Add the UfcEventsSpider to the process
    process.crawl(UfcEventsSpider)

    # Start the crawling process
    process.start()
    