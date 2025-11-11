import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import unicodedata
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import sys
from pathlib import Path
import os
from dataclasses import dataclass


def get_soup(url: str) -> BeautifulSoup | None:
    """Create a tree structure (BeautifulSoup) out of a GET request's HTML."""
    try:
        r = requests.get(url, allow_redirects=True)
        r.raise_for_status()
        print(f"Successfully fetched {r.url}")
        return BeautifulSoup(r.content, "html5lib")
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


def is_decade_page(url: str) -> bool:
    """Check if a page is a decade selection page."""
    return bool(re.search(r"/study/general-conference/\d{4}\d{4}", url))


def scrape_conference_pages(main_page_url: str) -> list[str]:
    """Retrieve a list of URLs for each conference (year/month) from the main page."""
    soup = get_soup(main_page_url)
    if soup is None:
        print(f"Failed to fetch content from {main_page_url}")
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

    print(f"Total conference links found: {len(all_conference_links)}")
    print("Sample conference links:", all_conference_links[:5])
    return all_conference_links


def scrape_talk_urls(conference_url: str) -> list[set]:
    """Retrieve a list of URLs for each talk in a specific conference."""
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

    print(f"Found {len(talk_links)} talk links in {conference_url}")
    if talk_links:
        print("Sample talk links:", talk_links[:3])
    return talk_links


def scrape_talk_data(url: str) -> dict[str, str | None]:
    """Scrapes a single talk for data such as: title, conference, calling, speaker, content."""
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
        ]
        if any(
            map(lambda prefix: title.startswith(prefix), prefixes)
        ) or title.endswith("Session"):
            return {}

        author_tag = soup.find("p", {"class": "author-name"})
        speaker = author_tag.text.strip() if author_tag else None

        calling_tag = soup.find("p", {"class": "author-role"})
        calling = calling_tag.text.strip() if calling_tag else None

        content_array = soup.find("div", {"class": "body-block"})
        content = (
            "\n\n".join(
                paragraph.text.strip() for paragraph in content_array.find_all("p")
            )
            if content_array
            else None
        )

        year = re.search(r"/(\d{4})/", url).group(1)
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
        print(f"Failed to scrape {url}: {e}")
        return {}


def scrape_talk_data_parallel(urls: list[str]) -> list[dict[str, str | None]]:
    """Scrapes all talks in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(
        max_workers=os.cpu_count()
    ) as executor:  # Adjust `max_workers` as needed
        results = list(
            tqdm(
                executor.map(scrape_talk_data, urls),
                total=len(urls),
                desc="Scraping talks in parallel",
            )
        )
    return [result for result in results if result]  # Filter out empty results


def setup_sql() -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    db_path = Path("conference_talks.db")
    if db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute(
        "CREATE TABLE speakers(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE organization(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE callings(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL, organization INTEGER UNIQUE NOT NULL, rank INTEGER NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE conferences(id INTEGER PRIMARY KEY AUTOINCREMENT, year INTEGER UNIQUE NOT NULL, season TEXT UNIQUE NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE talks(id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, emeritus INTEGER NOT NULL DEFAULT 0, speaker INTEGER NOT NULL, conference INTEGER NOT NULL, calling INTEGER NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE talk_texts(id INTEGER PRIMARY KEY AUTOINCREMENT, talk INTEGER UNIQUE NOT NULL, text TEXT NOT NULL)"
    )
    cur.execute(
        "CREATE TABLE talk_urls(id INTEGER PRIMARY KEY AUTOINCREMENT, talk INTEGER UNIQUE NOT NULL, url TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind in ('audio', 'video', 'text')))"
    )
    cur.execute(
        "CREATE TABLE talk_topics(id INTEGER PRIMARY KEY AUTOINCREMENT, talk INTEGER UNIQUE NOT NULL, name TEXT UNIQUE NOT NULL)"
    )

    return con, cur


calling_re = re.compile(
    r"(?P<emeritus>(recently )?((released|former) )?((as|member of the) )?)(?P<calling>[a-zA-Z, ]+)"
)
org_re = re.compile(r"[a-zA-Z ]+(, | in the )(?P<group>[a-zA-Z ]+)")


class Calling:
    def __init__(self, full_calling: str | None):
        if not full_calling:
            self.calling = "Unknown"
            self.organization = "Unknown"
            self.rank = 1000
            self.emeritus = False
            return

        matches = calling_re.match(full_calling, re.I)
        if not matches:
            raise ValueError(f"Unsupported calling: {full_calling}")

        self.calling = matches.group("calling")
        self.organization, self.rank = Calling.get_org_and_rank(self.calling)
        self.emeritus = len(matches.group("emeritus")) > 0

    @staticmethod
    def get_org_and_rank(calling: str) -> tuple[str, int]:
        org = "Local"
        rank = 99
        lowered = calling.lower()
        if "president of the church" in lowered:
            org = "First Presidency"
            rank = 0
        elif "first presidency" in lowered:
            org = "First Presidency"
            rank = 1
        elif "of the twelve" in lowered:
            org = "Quorum of the Twelve Apostles"
            rank = 2
        elif "of the seventy" in lowered:
            org = "Quorum of the Seventy"
            rank = 3
        elif "presiding bishop" in lowered:
            org = "Presiding Bishopric"
            rank = 4
        elif lowered.endswith("general presidency"):
            if "young men" in lowered:
                rank = 5
            elif "sunday school in lowered":
                rank = 6
            elif "relief society" in lowered:
                rank = 7
            elif "young women" in lowered:
                rank = 8
            elif "primary" in lowered:
                rank = 9
            else:
                raise ValueError(f"Unsupported calling for organization: {calling}")
            org = org_re.match(lowered, re.I).group("org").title()
        elif lowered.endswith("general president"):
            if "young men" in lowered:
                org = "Young Men General Presidency"
                rank = 5
            elif "sunday school in lowered":
                org = "Sunday School General Presidency"
                rank = 6
            elif "relief society" in lowered:
                org = "Relief Society General Presidency"
                rank = 7
            elif "young women" in lowered:
                org = "Young Women General Presidency"
                rank = 8
            elif "primary" in lowered:
                org = "Primary General Presidency"
                rank = 9
            else:
                raise ValueError(f"Unsupported calling for organization: {calling}")
        elif any(
            map(
                lambda field: field in lowered,
                [
                    "church audit committee",
                    "church leadership committee",
                ],
            )
        ):
            org = calling.title()
        return org, rank


@dataclass
class Conference:
    year: int
    season: str

    def __hash__(self) -> int:
        return hash((self.year, self.season))

    def __eq__(self, other) -> bool:
        return self.year == other.year and self.season == other.season


def save_sql(conference_df: pd.DataFrame) -> None:
    con, cur = setup_sql()
    speakers: set[str] = set()
    orgs: set[str] = set()
    conferences: set[Conference] = set()
    for idx, row in conference_df.iterrows():
        speakers.add(row.speaker)
        calling = Calling(row.calling)
        orgs.add(calling.organization)
        conferences.add(Conference(row.year, row.season))
    cur.executemany("INSERT INTO speakers (name) VALUES (?)", speakers)
    cur.executemany(
        "INSERT INTO conferences (year, season) VALUES (:year, :season)", conferences
    )
    con.commit()
    sys.exit(1)


def main_scrape_process():
    main_url = "https://www.churchofjesuschrist.org/study/general-conference?lang=eng"
    conference_urls = scrape_conference_pages(main_url)

    all_talk_urls = []
    for conference_url in tqdm(conference_urls, desc="Scraping conferences"):
        all_talk_urls.extend(scrape_talk_urls(conference_url))

    print(f"Total talks found: {len(all_talk_urls)}")

    # Scrape talks in parallel
    conference_talks = scrape_talk_data_parallel(all_talk_urls)

    # Create DataFrame from the scraped data
    conference_df = pd.DataFrame(conference_talks)

    # Normalize Unicode and clean data
    for col in conference_df.columns:
        conference_df[col] = conference_df[col].apply(
            lambda x: unicodedata.normalize("NFD", x) if isinstance(x, str) else x
        )
        conference_df[col] = conference_df[col].apply(
            lambda x: x.replace("\t", "") if isinstance(x, str) else x
        )
    conference_df.sort_values(
        ["year", "season", "url", "speaker", "title"],
        ascending=[True, True, True, True, True],
        inplace=True,
    )

    save_sql(conference_df)

    # Save to JSON and sqlite db
    conference_df.to_json(
        "conference_talks.json", orient="records", indent=2, sort_keys=True
    )
    print("Scraping complete. Data saved to 'conference_talks.json'.")


def main():
    # Run the scraper
    start = time.time()
    main_scrape_process()
    end = time.time()
    print(f"Total time taken: {end - start} seconds")


if __name__ == "__main__":
    main()
