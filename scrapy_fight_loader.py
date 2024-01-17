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
        headers = response.css(
            'body > section > div > div > section:nth-child(4) > table > thead > tr > th ::text').getall()
        headers = [header.strip() for header in headers if header.strip()]

        # Initialize a list to hold all rows of fighter data
        total_strikes_data = []

        # We know there are two rows of fighter data based on the selectors provided
        for row_index in range(1, 3):  # Two rows for two fighters
            fighter_row_data = []
            for col_index, header in enumerate(headers, start=1):
                # Construct the selector for each cell in the row
                cell_selector = (
                    f'body > section > div > div > section:nth-child(4) > '
                    f'table > tbody > tr > '
                    f'td:nth-child({col_index}) > '
                    f'p:nth-child({row_index}) ::text'
                )
                # Extract the cell data
                cell_data = response.css(cell_selector).getall()
                cell_data_clean = ' '.join(cell_data).strip()
                fighter_row_data.append(cell_data_clean)

            # Append this fighter's data to the total strikes data list
            total_strikes_data.append(fighter_row_data)

        # Convert the list of data into a DataFrame and store under 'total_strikes'
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
