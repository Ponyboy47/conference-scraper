"""Command-line interface for the conference scraper."""

import json
import logging
import os
import time
import unicodedata

import pandas as pd
import typer
from groq import Groq
from tqdm import tqdm

from .config import setup_logging
from .database import save_sql, setup_sql
from .scraper import scrape_conference_pages, scrape_talk_data_parallel, scrape_talk_urls
from .topic_extractor import extract_topics_batch

app = typer.Typer()


def main_scrape_process(extract_topics: bool = False, groq_api_key: str | None = None) -> None:
    """Main scraping process orchestration.

    Args:
        extract_topics: Whether to extract topics from talk texts
        groq_api_key: Groq API key for topic extraction (if None, uses GROQ_API_KEY env var)
    """
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

    # Extract topics if requested
    topics_df = None
    if extract_topics:
        logger.info("Extracting topics from talks (this may take a while due to rate limiting)...")
        api_key = groq_api_key or os.getenv("GROQ_API_KEY")
        if not api_key:
            raise AttributeError(
                "Topic extraction requested but no API key provided. Set GROQ_API_KEY or use --groq-api-key"
            )

        client = Groq(api_key=api_key)

        # Extract texts for topic analysis
        talk_texts = []
        for idx, row in conference_df.iterrows():
            if pd.notna(row.talk) and row.talk and row.talk.strip():
                talk_texts.append(row.talk)
            else:
                talk_texts.append("")

        # Use batch extraction with rate limiting
        topics_list = extract_topics_batch(talk_texts, client, batch_size=10)

        # Filter out empty topics for talks without text
        filtered_topics = []
        for text, topics in zip(talk_texts, topics_list):
            if text.strip():  # Only include topics for talks with actual content
                filtered_topics.append(topics)
            else:
                filtered_topics.append([])

        topics_df = pd.DataFrame({"topics": filtered_topics}, index=conference_df.index)
        logger.info("Topic extraction complete")

    save_sql(con, cur, conference_df, topics_df)
    logger.info("SQLite data saved to 'conference_talks.db'.")


@app.command()
def scrape(
    verbose: bool = False,
    log_file: str | None = None,
    extract_topics: bool = typer.Option(
        False, "--extract-topics", help="Extract 3-10 topics from talk texts using Groq API"
    ),
    groq_api_key: str | None = typer.Option(None, "--groq-api-key", help="Groq API key (or set GROQ_API_KEY env var)"),
):
    """Run the conference scraper."""
    setup_logging(verbose, log_file)

    logger = logging.getLogger(__name__)
    logger.info("Starting conference scraper")

    # Run the scraper
    start = time.time()
    main_scrape_process(extract_topics=extract_topics, groq_api_key=groq_api_key)
    end = time.time()

    logger.info(f"Total time taken: {end - start:.2f} seconds")


if __name__ == "__main__":
    app()
