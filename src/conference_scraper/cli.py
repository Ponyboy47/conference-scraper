"""Command-line interface for the conference scraper."""

import json
import logging
import time
import unicodedata

import pandas as pd
import typer
from tqdm import tqdm

from .config import setup_logging
from .database import save_sql, setup_sql
from .scraper import scrape_conference_pages, scrape_talk_data_parallel, scrape_talk_urls

app = typer.Typer()


def main_scrape_process() -> None:
    """Main scraping process orchestration."""
    logger = logging.getLogger(__name__)

    # Doing this first to make the feedback loop for SQL schema changes faster
    con, cur = setup_sql()

    main_url = "https://www.churchofjesuschrist.org/study/general-conference?lang=eng"
    conference_urls = scrape_conference_pages(main_url)

    all_talk_urls = []
    for conference_url in tqdm(conference_urls, desc="Scraping conferences"):
        all_talk_urls.extend(scrape_talk_urls(conference_url))

    logger.info(f"Total talks found: {len(all_talk_urls)}")

    # Scrape talks in parallel
    conference_talks = scrape_talk_data_parallel(all_talk_urls)

    # Create DataFrame from the scraped data
    conference_df = pd.DataFrame(conference_talks)

    # Normalize Unicode and clean data
    for col in conference_df.columns:
        conference_df[col] = conference_df[col].apply(
            lambda x: unicodedata.normalize("NFD", x).replace("\t", "    ").replace("\xa0", " ")
            if isinstance(x, str)
            else x
        )
    conference_df.sort_values(
        ["year", "season", "url", "speaker", "title"],
        ascending=[True, True, True, True, True],
        inplace=True,
    )
    logger.info("Scraping complete")

    # Save to JSON and sqlite db
    conference_json = conference_df.to_dict(orient="records")
    with open("conference_talks.json", "w") as f:
        json.dump(conference_json, f, indent=2, sort_keys=True)
    logger.info("JSON data saved to 'conference_talks.json'.")

    save_sql(con, cur, conference_df)
    logger.info("SQLite data saved to 'conference_talks.db'.")


@app.command()
def scrape(verbose: bool = False, log_file: str | None = None):
    """Run the conference scraper."""
    setup_logging(verbose, log_file)

    logger = logging.getLogger(__name__)
    logger.info("Starting conference scraper")

    # Run the scraper
    start = time.time()
    main_scrape_process()
    end = time.time()

    logger.info(f"Total time taken: {end - start:.2f} seconds")


if __name__ == "__main__":
    app()
