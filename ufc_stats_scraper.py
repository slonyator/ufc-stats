import requests
from bs4 import BeautifulSoup
from loguru import logger


class UFCStatsScraper:
    def __init__(self, url):
        self.url = url
        self.soup = None
        self._fetch_html()

    def _fetch_html(self):
        """Fetch the HTML content from the URL and parse it."""
        response = requests.get(self.url)
        if response.status_code == 200:
            self.soup = BeautifulSoup(response.content, "html.parser")
        else:
            raise Exception(f"Failed to fetch data from URL: {self.url}")

    def get_meta_information(self):
        meta_info = {}
        fighters = self.soup.find_all(
            "h3", class_="b-fight-details__person-name"
        )
        meta_info["fighters"] = [
            fighter.get_text().strip() for fighter in fighters
        ]

        # Extracting the method of victory, round, time, time format, and referee
        fight_details = self.soup.find_all(
            "i", class_="b-fight-details__text-item"
        )
        for detail in fight_details:
            label = detail.find("i", class_="b-fight-details__label")
            if label:
                key = label.get_text().strip().replace(":", "")
                value = detail.get_text().replace(label.get_text(), "").strip()
                meta_info[key.lower().replace(" ", "_")] = value

        return meta_info

    def get_totals(self):
        totals = {}
        # Trying a broader search for the 'Totals' section
        potential_sections = self.soup.find_all(
            ["section", "div", "span"]
        )  # Including more tag types

        for section in potential_sections:
            if "Totals" in section.get_text():
                table = section.find_next("table")
                header_row = table.find("tr").find_all("th")
                headers = [
                    header.get_text()
                    .strip()
                    .lower()
                    .replace(" ", "_")
                    .replace(".", "")
                    for header in header_row
                ]

                rows = table.find_all("tr")[1:]  # skipping header row

                for row in rows:
                    columns = row.find_all("td")
                    fighter_name = columns[0].get_text().strip()
                    fighter_data = {}

                    for idx, col in enumerate(columns[1:], start=1):
                        header = headers[idx]
                        fighter_data[header] = col.get_text().strip()

                    totals[fighter_name] = fighter_data

                totals = self._split_and_reorganize(totals)

                return totals

        print("Totals section not found in the HTML.")
        return totals

    def get_significant_strikes(self):
        significant_strikes = {}
        # Adjust the identifier (like 'div', 'p', etc.) and string based on your HTML structure
        section = self.soup.find(
            "p", string=lambda text: text and "Significant Strikes" in text
        )

        if section:
            table = section.find_next("table")
            header_row = table.find("tr").find_all("th")
            headers = [
                header.get_text()
                .strip()
                .lower()
                .replace(" ", "_")
                .replace(".", "")
                for header in header_row
            ]

            rows = table.find_all("tr")[1:]  # skipping header row

            for row in rows:
                columns = row.find_all("td")
                fighter_name = columns[0].get_text().strip()
                fighter_data = {}

                for idx, col in enumerate(
                    columns[1:], start=1
                ):  # Start from 1 to skip the fighter's name
                    header = headers[idx]
                    fighter_data[header] = col.get_text().strip()

                significant_strikes[fighter_name] = fighter_data
        else:
            print("Significant Strikes section not found in the HTML.")

        return significant_strikes

    def get_significant_strikes_details(self):
        significant_strikes_details = {
            "landed_by_target": self._fetch_landed_by_target(),
            "landed_by_position": self._fetch_landed_by_position(),
            "per_round": self._fetch_per_round(),
        }

        return significant_strikes_details

    def _fetch_landed_by_target(self):
        data = {}
        section = self.soup.find(
            "h4", string=lambda text: text and "Landed by target" in text
        )

        if section:
            chart_rows = section.find_next_sibling("div").find_all(
                "div", class_="b-fight-details__charts-row"
            )

            for row in chart_rows:
                target = (
                    row.find("i", class_="b-fight-details__charts-row-title")
                    .get_text()
                    .strip()
                )
                values = row.find_all(
                    "i", class_="b-fight-details__charts-num"
                )
                if len(values) == 2:
                    data[target] = {
                        "red_fighter": values[0].get_text().strip(),
                        "blue_fighter": values[1].get_text().strip(),
                    }
        else:
            print("Landed by Target section not found.")

        return data

    def _fetch_landed_by_position(self):
        data = {}
        section = self.soup.find(
            "h4", string=lambda text: text and "Landed by position" in text
        )

        if section:
            chart_rows = section.find_next_sibling("div").find_all(
                "div", class_="b-fight-details__charts-row"
            )

            for row in chart_rows:
                position = (
                    row.find("i", class_="b-fight-details__charts-row-title")
                    .get_text()
                    .strip()
                )
                values = row.find_all(
                    "i", class_="b-fight-details__charts-num"
                )
                if len(values) == 2:
                    data[position] = {
                        "red_fighter": values[0].get_text().strip(),
                        "blue_fighter": values[1].get_text().strip(),
                    }
        else:
            print("Landed by Position section not found.")

        return data

    def _fetch_per_round(self):
        # Find the 'Per Round' section
        per_round_section = self.soup.find(
            "a",
            class_="b-fight-details__collapse-link_rnd js-fight-collapse-link",
        )
        if not per_round_section:
            return "Per Round section not found."

        # Find the table next to the 'Per Round' section
        table = per_round_section.find_next(
            "table", class_="b-fight-details__table js-fight-table"
        )
        if not table:
            return "Per Round table not found."

        rounds_data = []
        # Each round has its own thead with round number and a subsequent tbody with details
        round_headers = table.find_all(
            "thead",
            class_="b-fight-details__table-row b-fight-details__table-row_type_head",
        )
        for round_header in round_headers:
            round_number = round_header.th.text.strip()
            round_rows = round_header.find_next("tbody").find_all("tr")

            # Extracting details for each fighter in the round
            for row in round_rows:
                columns = row.find_all("td")
                round_data = {
                    "round": round_number,
                    "fighter": columns[0].get_text(),
                    "kd": columns[1].get_text(),
                    "significant_strikes": columns[2].get_text(),
                    "significant_strike_percentage": columns[3].get_text(),
                    "total_strikes": columns[4].get_text(),
                    "td_percentage": columns[5].get_text(),
                    "sub_attempts": columns[6].get_text(),
                    "reversals": columns[7].get_text(),
                    "control": columns[8].get_text(),
                }
                rounds_data.append(round_data)

        return rounds_data

    def _parse_strikes_table(self, table):
        data = {}
        if table:
            header_row = table.find("tr").find_all("th")
            headers = [
                header.get_text()
                .strip()
                .lower()
                .replace(" ", "_")
                .replace(".", "")
                for header in header_row
            ]

            rows = table.find_all("tr")[1:]  # skipping header row

            for row in rows:
                columns = row.find_all("td")
                fighter_name = columns[0].get_text().strip()
                fighter_data = {}

                for idx, col in enumerate(columns[1:], start=1):
                    header = headers[idx]
                    fighter_data[header] = col.get_text().strip()

                data[fighter_name] = fighter_data
        return data

    def _split_and_reorganize(self, data):
        new_dict = {}

        for combined_key, stats in data.items():
            # Splitting the main key into individual keys
            keys = combined_key.split('\n\n\n')

            # Splitting and reorganizing each stat for the individual keys
            for key in keys:
                new_dict[key] = {}

            for stat_key, combined_value in stats.items():
                values = combined_value.split('\n    \n\n')

                for key, value in zip(keys, values):
                    new_dict[key][stat_key] = value.strip()

        return new_dict



if __name__ == "__main__":
    scraper = UFCStatsScraper(
        "http://ufcstats.com/fight-details/3fa8ee3fdc04fe36"
    )

    # Get meta information
    logger.info("Meta Information:")
    meta_info = scraper.get_meta_information()
    logger.info(meta_info)

    # Get totals
    logger.info("\nTotals:")
    totals = scraper.get_totals()
    logger.info(totals)

    # Get significant strikes
    logger.info("\nSignificant Strikes:")
    significant_strikes = scraper.get_significant_strikes()
    logger.info(significant_strikes)

    # Get detailed significant strikes data
    logger.info("\nSignificant Strikes Details:")
    significant_strikes_details = scraper.get_significant_strikes_details()
    logger.info(significant_strikes_details)

    logger.success("ready")
