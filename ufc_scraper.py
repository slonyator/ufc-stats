import re

import requests
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger


class UfcScraper:
    @staticmethod
    def scrape_cards(link: str) -> pd.DataFrame:
        response = requests.get(link)
        soup = BeautifulSoup(response.content, "html.parser")
        cards = soup.select(".b-link_style_black")

        card_links = [card.get("href") for card in cards]
        cards_df = pd.DataFrame({"card": card_links})

        return cards_df

    @staticmethod
    def scrape_dates(link: str) -> pd.DataFrame:
        page = requests.get(link)
        soup = BeautifulSoup(page.content, "html.parser")
        fight_dates = [
            item.get_text()
            for item in soup.select(".b-list__box-list-item:nth-child(1)")
        ]
        df = pd.DataFrame({"fight_date": fight_dates})
        df[["key", "value"]] = df["fight_date"].str.split(
            ":", n=1, expand=True
        )
        df["date"] = df["value"].str.replace("\n", "").str.strip()
        df = df[["date"]]
        df["card"] = link
        df = df.reset_index(drop=True)

        return df

    @staticmethod
    def scrape_fights(link: str) -> pd.DataFrame:
        response = requests.get(link)
        soup = BeautifulSoup(response.content, "html.parser")
        fight_links = soup.find_all("a", href=True)

        fights = [
            fight["href"]
            for fight in fight_links
            if "fight-details" in fight["href"]
        ]
        fights_df = pd.DataFrame({"fights": fights, "card": link})

        return fights_df

    @staticmethod
    def _scrape_fight_summary_table(link: str) -> pd.DataFrame:
        response = requests.get(link)
        soup = BeautifulSoup(response.content, "html.parser")
        tables = soup.find_all("table")

        if tables:
            table_html = str(tables[0])
            df = pd.read_html(table_html, header=0)[0]

            for col in ['Sig. str. %', 'Td %']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: x.replace('---', '0%') if isinstance(x, str) else x)

            return df
        else:
            return pd.DataFrame()

    @staticmethod
    def _process_fight_summary_data(df: pd.DataFrame) -> pd.DataFrame:
        # Rename columns
        column_names = [
            "Fighter",
            "KD",
            "Sig_Strike",
            "Sig_Strike_Percent",
            "Total_Strikes",
            "TD",
            "TD_Percent",
            "Sub_Attempts",
            "Pass",
            "Rev",
        ]
        df.columns = column_names

        # Melt and separate fighter data
        df = df.melt(id_vars=["Fighter"], var_name="key", value_name="value")
        df[["fighter_1", "fighter_2"]] = df["value"].str.split(
            "  ", n=1, expand=True
        )

        # Cleaning and trimming strings
        df["fighter_1"] = df["fighter_1"].str.replace("\n", "").str.strip()
        df["fighter_2"] = df["fighter_2"].str.replace("\n", "").str.strip()

        # Pivot the DataFrame
        df = df.pivot_table(
            index="Fighter",
            columns="key",
            values=["fighter_1", "fighter_2"],
            aggfunc="first",
        )

        # Separate specific columns and convert percentages
        for col in ["Sig_Strike", "Total_Strikes", "TD"]:
            for fighter in ["fighter_1", "fighter_2"]:
                df[
                    [f"{fighter}_{col}_Landed", f"{fighter}_{col}_Attempts"]
                ] = df[fighter, col].str.split(" of ", expand=True)
                df.drop((fighter, col), axis=1, inplace=True)

        # Convert percentage columns
        for col in df.columns:
            if "Percent" in col[1]:
                df[col] = df[col].str.replace("%", "").astype(float) * 0.01

        # Convert other columns to numeric
        for col in df.columns:
            if "Fighter" not in col:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df.columns = ["_".join(col).strip() for col in df.columns]

        return df.reset_index(drop=True)

    @staticmethod
    def _scrape_fight_details(link: str) -> pd.DataFrame:
        response = requests.get(link)
        soup = BeautifulSoup(response.content, "html.parser")

        # Find elements based on class name, equivalent to the provided XPath
        details_elements = soup.select(".b-fight-details__text > i")

        # Extract text from each element
        details_text = [elem.get_text(strip=True) for elem in details_elements]

        # Convert the list of texts into a DataFrame
        fight_details_df = pd.DataFrame({"fight_details": details_text})

        return fight_details_df

    @staticmethod
    def _process_fight_details(df: pd.DataFrame) -> pd.DataFrame:
        # Clean and trim the 'value' column
        df["value"] = df["fight_details"].str.replace("\n", "").str.strip()

        # Split the 'value' column into 'feature' and 'value'
        df[["feature", "value"]] = df["value"].str.split(":", n=1, expand=True)
        df["value"] = df["value"].str.strip()

        # Replace NaNs with empty strings
        df["value"].fillna("", inplace=True)

        # Group by 'feature', filter out empty 'value', and reset index
        df = df[df["value"] != ""].groupby("feature").first().reset_index()

        # Pivot the DataFrame
        df_pivot = df.pivot_table(
            index=None, columns="feature", values="value", aggfunc="first"
        )

        # Flatten the column index and format column names
        df_pivot.columns = [
            col.lower().replace(" ", "_").replace("/", "_")
            for col in df_pivot.columns
        ]

        return df_pivot.reset_index(drop=True)

    @staticmethod
    def _add_additional_fight_details(
        df: pd.DataFrame, link: str
    ) -> pd.DataFrame:
        response = requests.get(link)
        soup = BeautifulSoup(response.content, "html.parser")

        # Extracting fighter_1_res
        fighter_1_res_text = soup.select_one(
            ".b-fight-details__persons"
        ).get_text()
        fighter_1_res = re.findall(r"[:upper:]{1}", fighter_1_res_text)[0]
        df["fighter_1_res"] = fighter_1_res

        # Determining fighter_2_res based on fighter_1_res
        df["fighter_2_res"] = df["fighter_1_res"].apply(
            lambda x: "W" if x == "L" else "L" if x == "W" else "D"
        )

        # Rename 'round' column to 'round_finished'
        df.rename(columns={"round": "round_finished"}, inplace=True)

        # Extracting and adding 'weight_class'
        weight_class_text = (
            soup.select_one(".b-fight-details__fight-title")
            .get_text()
            .strip()
            .replace("\n", "")
        )
        df["weight_class"] = weight_class_text

        return df

    def scrape_fight_summary_data(self, link: str) -> pd.DataFrame:
        logger.info(f"Start scraping fight summary data for {link}")

        # Scrape the fight summary table
        summary_table = self._scrape_fight_summary_table(link)
        logger.info(f"Scraping fight summary data for {link} finished")

        # Process the fight summary data
        processed_summary_data = self._process_fight_summary_data(
            summary_table
        )
        logger.info(f"Processing fight summary data for {link} finished")

        # Scrape additional fight details
        fight_details = self._scrape_fight_details(link)
        logger.info(f"Scraping fight details for {link} finished")

        # Process fight details
        processed_fight_details = self._process_fight_details(fight_details)
        logger.info(f"Processing fight details for {link} finished")

        # Add additional fight details
        final_data = self._add_additional_fight_details(
            processed_fight_details, link
        )
        logger.info(f"Adding additional fight details for {link} finished")

        # Combine summary data with fight details
        combined_data = pd.concat([processed_summary_data, final_data], axis=1)
        logger.info(f"Scraping fight summary data for {link} finished")

        return combined_data


if __name__ == "__main__":
    link = "http://ufcstats.com/statistics/events/completed?page=all"

    # Scrape Fight Cards from the link
    cards_df = (
        UfcScraper.scrape_cards(link=link).reset_index(drop=True).head(10)
    )
    logger.info("Scraping cards finished")

    # Scrape fight dates from every Fight Card
    dates_df = pd.concat(
        [UfcScraper.scrape_dates(link) for link in cards_df["card"]],
        ignore_index=True,
    )
    logger.info("Scraping dates finished")

    # Scrape Fights from every Fight Card
    fights_df = pd.concat(
        [UfcScraper.scrape_fights(link) for link in cards_df["card"]],
        ignore_index=True,
    )
    logger.info("Scraping fights finished")

    # Merge all the DataFrames
    meta_df = cards_df.merge(dates_df, how="inner").merge(
        fights_df, how="inner"
    )
    logger.info("Meta DataFrame created")

    # Scrape fight summary data
    fight_summary_data = pd.concat(
        [
            UfcScraper().scrape_fight_summary_data(link)
            for link in meta_df["fights"]
        ],
        ignore_index=True,
    )
    logger.info("Fight summary data scraped")
