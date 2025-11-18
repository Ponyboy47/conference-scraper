"""Web scraping functionality for conference talks."""

import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_soup(url: str) -> BeautifulSoup | None:
    """Create a tree structure (BeautifulSoup) out of a GET request's HTML."""
    logger = logging.getLogger(__name__)
    try:
        r = requests.get(url, allow_redirects=True)
        r.raise_for_status()
        logger.debug(f"Successfully fetched {r.url}")
        return BeautifulSoup(r.content, "html5lib")
    except requests.RequestException as e:
        logger.error(f"Error fetching {url}: {e}")
        return None


def is_decade_page(url: str) -> bool:
    """Check if a page is a decade selection page."""
    return bool(re.search(r"/study/general-conference/\d{4}\d{4}", url))


def scrape_conference_pages(main_page_url: str) -> list[str]:
    """Retrieve a list of URLs for each conference (year/month) from the main page."""
    logger = logging.getLogger(__name__)
    logger.info(f"Scraping conference pages from {main_page_url}")
    soup = get_soup(main_page_url)
    if soup is None:
        logger.error(f"Failed to fetch content from {main_page_url}")
        return []

    all_conference_links = []

    # Find all the links to individual conferences or decades
    links = [
        "https://www.churchofjesuschrist.org" + a["href"]
        for a in soup.find_all("a", href=True)
        if re.search(r"/study/general-conference/(\d{4}/(04|10)|\d{4}\d{4})", a["href"])
    ]

    for link in links:
        if is_decade_page(link):
            # Handle decade page
            decade_soup = get_soup(link)
            if decade_soup:
                year_links = [
                    "https://www.churchofjesuschrist.org" + a["href"]
                    for a in decade_soup.find_all("a", href=True)
                    if re.search(r"/study/general-conference/\d{4}/(04|10)", a["href"])
                ]
                all_conference_links.extend(year_links)
        else:
            all_conference_links.append(link)

    logger.info(f"Total conference links found: {len(all_conference_links)}")
    logger.debug(f"Sample conference links: {all_conference_links[:5]}")
    return all_conference_links


def scrape_talk_urls(conference_url: str) -> list[str]:
    """Retrieve a list of URLs for each talk in a specific conference."""
    logger = logging.getLogger(__name__)
    soup = get_soup(conference_url)
    if soup is None:
        return []

    talk_links = [
        "https://www.churchofjesuschrist.org" + a["href"]
        for a in soup.find_all("a", href=True)
        if re.search(r"/study/general-conference/\d{4}/(04|10)/.+", a["href"])
    ]

    # Remove duplicate links and session links
    talk_links = list(set(talk_links))
    talk_links = [link for link in talk_links if not link.endswith("session?lang=eng")]

    logger.debug(f"Found {len(talk_links)} talk links in {conference_url}")
    if talk_links:
        logger.debug(f"Sample talk links: {talk_links[:3]}")
    return talk_links


def scrape_talk_data(url: str) -> dict[str, str | None]:
    """Scrapes a single talk for data such as: title, conference, calling, speaker, content."""
    logger = logging.getLogger(__name__)
    try:
        soup = get_soup(url)
        if soup is None:
            return {}

        title_tag = soup.find("h1", {"id": "title1"})
        if title_tag:
            title = title_tag.text.strip()
        else:
            title_tag = soup.find("title")
            title = title_tag.text.strip() if title_tag else None

        # Don't include full sessions, sustainings, reports, etc
        prefixes = [
            "Church Auditing Department Report",
            "Statistical Report",
            "Audit Report",
            "The Annual Report of the Church",
            "Church Finance Committee Report",
            "The Sustaining of Church Officers",
            "The Church Audit Committee Report",
            "Sustaining of ",
            "Video:",
            "Saturday Morning",
            "Proclamation",
        ]
        # Fix prefix checking (line 120)
        if any(title.startswith(prefix) for prefix in prefixes) or title.endswith("Session"):
            return {}

        author_tag = soup.find("p", {"class": "author-name"})
        speaker = author_tag.text.strip() if author_tag else None

        calling_tag = soup.find("p", {"class": "author-role"})
        calling = calling_tag.text.strip() if calling_tag else None

        content_array = soup.find("div", {"class": "body-block"})
        content = (
            "\n\n".join(paragraph.text.strip() for paragraph in content_array.find_all("p")) if content_array else None
        )

        # Fix unsafe regex (line 134)
        year_match = re.search(r"/(\d{4})/", url)
        if not year_match:
            logger.error(f"Could not extract year from URL: {url}")
            return {}
        year = year_match.group(1)
        season = "April" if "/04/" in url else "October"

        return {
            "title": title,
            "speaker": speaker,
            "calling": calling,
            "year": year,
            "season": season,
            "url": url,
            "talk": content,
        }
    except Exception as e:
        logger.error(f"Failed to scrape {url}: {e}")
        return {}


def scrape_talk_data_parallel(urls: list[str]) -> list[dict[str, str | None]]:
    """Scrapes all talks in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(
            tqdm(
                executor.map(scrape_talk_data, urls),
                total=len(urls),
                desc="Scraping talks in parallel",
                unit="talks",
            )
        )
    return [result for result in results if result]  # Filter out empty results
