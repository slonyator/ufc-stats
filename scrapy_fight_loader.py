import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess


class UfcFightSpider(scrapy.Spider):
    name = "ufc_fight"

    def __init__(self, urls=None, results=None, *args, **kwargs):
        super(UfcFightSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls
        self.results = results

    def parse(self, response):
        # Extracting and cleaning fight details
        fight_details_raw = response.css(
            "body > section > div > div > div.b-fight-details__fight > div.b-fight-details__content ::text"
        ).getall()
        fight_details_raw = [
            text.strip() for text in fight_details_raw if text.strip()
        ]

        # Parsing fight details into a structured format
        fight_details = {}
        current_key = None
        for detail in fight_details_raw:
            if detail.endswith(":"):
                if current_key is not None and current_key != "Details":
                    fight_details[current_key] = " ".join(
                        fight_details[current_key]
                    )
                current_key = detail[:-1]  # Remove colon
                fight_details[current_key] = []
            elif current_key:
                fight_details[current_key].append(detail)

        # Special handling for 'Details' key
        if "Details" in fight_details:
            fight_details["Details"] = " ".join(fight_details["Details"])

        # Extracting headers for the total strikes table
        header_selector = 'body > section > div > div > section:nth-child(4) > table > thead > tr'
        headers = response.css(f'{header_selector} th ::text').getall()
        headers = [header.strip() for header in headers if header.strip()]

        # Extracting rows for the total strikes table
        row_selector = 'body > section > div > div > section:nth-child(4) > table > tbody'
        row_data = response.css(f'{row_selector} td ::text').getall()
        row_data = [data.strip() for data in row_data if data.strip()]

        # Assuming the first two elements are fighter names and they don't have colons
        fighter_names = row_data[:2]
        stats_data = row_data[2:]  # the rest is stats data

        # Split the stats data into two parts, one for each fighter
        half = len(headers) - 1  # minus one because fighter name is already taken as separate
        fighter_one_stats = stats_data[:half]
        fighter_two_stats = stats_data[half:]

        # Prepend fighter names to their respective stats
        fighter_one_row = [fighter_names[0]] + fighter_one_stats
        fighter_two_row = [fighter_names[1]] + fighter_two_stats

        # Combine rows into a list to create a DataFrame
        total_strikes_data = [fighter_one_row, fighter_two_row]

        # Convert the list of rows to a DataFrame and store under 'total_strikes'
        total_strikes_df = pd.DataFrame(total_strikes_data, columns=headers)

        # Storing the structured data
        self.results[response.url].append({
            'fight_details': fight_details,  # from the previous parsing
            'total_strikes': total_strikes_df
        })


class UfcCrawler:
    def scrape_multiple_sites(self, urls):
        results = {url: [] for url in urls}
        process = CrawlerProcess({
            'USER_AGENT': 'Mozilla/5.0 (compatible; Scrapy/1.0; +http://scrapy.org)'
        })

        process.crawl(UfcFightSpider, urls=urls, results=results)
        process.start()

        nested_dataframes = {}
        for url, data_list in results.items():
            dataframes = {}
            for data in data_list:
                for key, value in data.items():
                    if key != 'url':
                        if key == 'total_strikes':
                            # Convert total_strikes list of dicts to a DataFrame
                            dataframes[key] = pd.DataFrame(value)
                        elif isinstance(value, dict):
                            # Convert other dict types to a DataFrame
                            dataframes[key] = pd.DataFrame([value])
                        else:
                            # Handle other types of data (like lists)
                            dataframes[key] = pd.DataFrame([value])
            nested_dataframes[url] = dataframes

        return nested_dataframes


if __name__ == "__main__":
    urls = [
        "http://ufcstats.com/fight-details/3fa8ee3fdc04fe36",
        "http://ufcstats.com/fight-details/1efc813ccd7a5aba"
        # Add more URLs here
    ]
    crawler = UfcCrawler()
    results = crawler.scrape_multiple_sites(urls)

    for url, dfs in results.items():
        print(f"Data from {url}:")
        for key, df in dfs.items():
            print(f"\n{key}:\n", df)

    print("ready")
