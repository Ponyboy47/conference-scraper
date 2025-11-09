import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import unicodedata
import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def get_soup(url):
    """Create a tree structure (BeautifulSoup) out of a GET request's HTML."""
    try:
        r = requests.get(url, allow_redirects=True)
        r.raise_for_status()
        print(f"Successfully fetched {r.url}")
        return BeautifulSoup(r.content, "html5lib")
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None


def is_decade_page(url):
    """Check if a page is a decade selection page."""
    return bool(re.search(r"/study/general-conference/\d{4}\d{4}", url))


def scrape_conference_pages(main_page_url):
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


def scrape_talk_urls(conference_url):
    """Retrieve a list of URLs for each talk in a specific conference."""
    soup = get_soup(conference_url)
    if soup is None:
        return []

    talk_links = [
        "https://www.churchofjesuschrist.org" + a["href"]
        for a in soup.find_all("a", href=True)
        if re.search(r"/study/general-conference/\d{4}/(04|10)/.*", a["href"])
    ]

    # Remove duplicate links and session links
    talk_links = list(set(talk_links))
    talk_links = [link for link in talk_links if not link.endswith("session?lang=eng")]

    print(f"Found {len(talk_links)} talk links in {conference_url}")
    if talk_links:
        print("Sample talk links:", talk_links[:3])
    return talk_links


def scrape_talk_data(url):
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
            title = title_tag.text.strip() if title_tag else "No Title Found"

        # Don't include full sessions, sustainings, reports, etc
        prefixes = [
            "Church Auditing Department Report",
            "Statistical Report",
            "Audit Report",
            "The Annual Report of the Church",
            "Church Finance Committee Report",
            "The Sustaining of Church Officers"
        ]
        if any(map(lambda prefix: title.startswith(prefix), prefixes)) or title.endswith("Session"):
            return {}

        conference_tag = soup.find("p", {"class": "subtitle"})
        # conference = (
        #     conference_tag.text.strip() if conference_tag else "No Conference Found"
        # )
        # print(f"Conference: {conference}")

        author_tag = soup.find("p", {"class": "author-name"})
        speaker = author_tag.text.strip() if author_tag else "No Speaker Found"

        calling_tag = soup.find("p", {"class": "author-role"})
        calling = calling_tag.text.strip() if calling_tag else "No Calling Found"

        content_array = soup.find("div", {"class": "body-block"})
        content = (
            "\n\n".join(
                paragraph.text.strip() for paragraph in content_array.find_all("p")
            )
            if content_array
            else "No Content Found"
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


def scrape_talk_data_parallel(urls):
    """Scrapes all talks in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(
        max_workers=10
    ) as executor:  # Adjust `max_workers` as needed
        results = list(
            tqdm(
                executor.map(scrape_talk_data, urls),
                total=len(urls),
                desc="Scraping talks in parallel",
            )
        )
    return [result for result in results if result]  # Filter out empty results


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

    # Save to CSV and JSON
    conference_df.to_csv("conference_talks.csv", index=False)
    print("Scraping complete. Data saved to 'conference_talks.csv'.")

    conference_df.to_json("conference_talks.json", orient="records", indent=4)
    print("Data also saved to 'conference_talks.json'.")


def main():
    # Run the scraper
    start = time.time()
    main_scrape_process()
    end = time.time()
    print(f"Total time taken: {end - start} seconds")


if __name__ == "__main__":
    main()
