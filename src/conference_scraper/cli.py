"""Command-line interface for the conference scraper."""

import json
import logging
import os
import shutil
import sqlite3
import time
import unicodedata
from pathlib import Path

import pandas as pd
import typer
from groq import Groq
from tqdm import tqdm

from .config import setup_logging
from .database import insert_data_with_topics, setup_sql
from .scraper import scrape_conference_pages, scrape_talk_data_parallel, scrape_talk_urls

app = typer.Typer()


def main_scrape_process(outputs_dir: Path, extract_topics: bool = False, groq_api_key: str | None = None) -> None:
    """Main scraping process orchestration.

    Args:
        extract_topics: Whether to extract topics from talk texts
        groq_api_key: Groq API key for topic extraction (if None, uses GROQ_API_KEY env var)
    """
    logger = logging.getLogger(__name__)

    # Doing this first to make the feedback loop for SQL schema changes faster
    con, cur, db_file = setup_sql(outputs_dir, extract_topics)

    main_url = "https://www.churchofjesuschrist.org/study/general-conference?lang=eng"
    conference_urls = scrape_conference_pages(main_url)

    all_talk_urls = []
    for conference_url in tqdm(conference_urls, desc="Scraping conferences", unit="conferences"):
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

    # Save to JSON
    json_file = outputs_dir / "conference_talks.json"
    conference_json = conference_df.to_dict(orient="records")
    with open(json_file, "w") as f:
        json.dump(conference_json, f, indent=2, sort_keys=True)
    logger.info(f"JSON data saved to '{json_file}'.")

    # Set up topic extraction client if needed
    topic_client = None
    if extract_topics:
        topic_client = Groq(api_key=groq_api_key)
        logger.info("Topic extraction enabled - processing talks sequentially")

    # Process talks one by one, extracting topics immediately for new talks
    total_new_talks = 0
    for idx, row in tqdm(conference_df.iterrows(), desc="Processing talks", unit="talk", total=len(conference_df)):
        try:
            # Insert talk data and get whether it's new
            is_new = insert_data_with_topics(cur, row, topic_client)
            if is_new:
                total_new_talks += 1
                con.commit()  # Commit after each new talk to ensure progress is saved

        except Exception as e:
            logger.error(f"Failed to process talk '{row.title}' ({row.year} {row.season}): {e}")
            continue

    if extract_topics:
        logger.info(f"Processed {total_new_talks} new talks with topic extraction")
    else:
        logger.info("Processed all talks (topic extraction disabled)")

    cur.execute("VACUUM")
    con.commit()
    logger.info("SQLite data saved to 'conference_talks.db'.")

    # Duplicate db without the talk text to save space
    no_text_db = db_file.parent / "conference_talks_no_text.db"
    shutil.copy2(str(db_file), str(no_text_db))
    con = sqlite3.connect(no_text_db)
    cur = con.cursor()
    cur.execute("DROP TABLE talk_texts")
    cur.execute("VACUUM")
    con.commit()


@app.command()
def scrape(
    outputs_dir: str = "data",
    verbose: bool = False,
    log_file: str | None = None,
    extract_topics: bool = typer.Option(
        False, "--extract-topics", help="Extract 3-10 topics from talk texts using Groq API"
    ),
    groq_api_key: str | None = typer.Option(None, "--groq-api-key", help="Groq API key (or set GROQ_API_KEY env var)"),
):
    """Run the conference scraper."""
    api_key = groq_api_key or os.getenv("GROQ_API_KEY")
    if extract_topics:
        if not api_key:
            raise AttributeError(
                "Topic extraction requested but no API key provided. Set GROQ_API_KEY or use --groq-api-key"
            )

    outputs_dir = Path(outputs_dir) if outputs_dir else Path("data")
    if not outputs_dir.exists():
        outputs_dir.mkdir(parents=True)
    setup_logging(verbose, log_file)

    logger = logging.getLogger(__name__)
    logger.info("Starting conference scraper")

    # Run the scraper
    start = time.time()
    main_scrape_process(outputs_dir=outputs_dir, extract_topics=extract_topics, groq_api_key=api_key)
    end = time.time()

    logger.info(f"Total time taken: {end - start:.2f} seconds")


if __name__ == "__main__":
    app()
