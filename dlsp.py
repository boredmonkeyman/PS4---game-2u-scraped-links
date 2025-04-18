import requests
from bs4 import BeautifulSoup
import re
import os
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import HTTPError

# Constants
BASE_URL = "https://game-2u.com/Category/game/ps4"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Referer': 'https://google.com'
}
TOTAL_PAGES = 122
RESULTS_DIR = "results"
REQUEST_DELAY = 1  # Delay between requests in seconds
MAX_RETRIES = 10   # Maximum number of retries for failed requests
RETRY_DELAY = 5   # Delay before retrying after a failure (seconds)
THREADS = 10        # Number of concurrent threads

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# Create results directory if it doesn't exist
os.makedirs(RESULTS_DIR, exist_ok=True)

# Session to maintain cookies/headers
session = requests.Session()
session.headers.update(HEADERS)

def sanitize_filename(filename):
    """Remove invalid characters from filename"""
    return re.sub(r'[\\/*?:"<>|]', "", filename).strip()

def make_request_with_retry(url, max_retries=MAX_RETRIES):
    """Make HTTP request with retry logic"""
    for attempt in range(max_retries + 1):
        try:
            time.sleep(REQUEST_DELAY)
            response = session.get(url, timeout=10)
            response.raise_for_status()  # Raises HTTPError for bad responses
            return response
        except (HTTPError, requests.exceptions.RequestException) as e:
            if attempt < max_retries:
                logging.warning(f"Attempt {attempt + 1} failed for {url}. Error: {e}. Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logging.error(f"Failed to fetch {url} after {max_retries} attempts. Error: {e}")
                raise
    return None

def parse_and_save_game(url):
    try:
        response = make_request_with_retry(url)
        if not response:
            return False

        soup = BeautifulSoup(response.content, "html.parser")

        title = soup.find("h1", class_="entry-title")
        game_name = title.text.strip() if title else "Unknown"
        logging.info(f"Processing game: {game_name}")

        table_data = {}
        info_tables = soup.select("table")
        if info_tables:
            for row in info_tables[0].find_all("tr"):
                cells = row.find_all("td")
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).replace(":", "")
                    value = cells[1].get_text(strip=True)
                    table_data[key] = value

        version = table_data.get("Game Version", "Unknown")
        language = table_data.get("Language", "Unknown")
        firmware = table_data.get("Required firmware", "Unknown")

        content_text = soup.get_text()
        size_matches = re.findall(r"(\d{1,3}\.\d{1,2}\s?GB)", content_text)

        section_links = {
            "Base Game": [],
            "Update": [],
            "Fix": [],
            "Torrent": [],
        }

        for table in info_tables:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) >= 4:
                    label = tds[0].text.strip().lower()
                    links = [
                        f"{a['href']} - {a.get_text(strip=True)}"
                        for a in tds[3].find_all("a", href=True)
                    ]
                    if "base" in label:
                        section_links["Base Game"].extend(links)
                    elif "update" in label and "fix" not in label:
                        section_links["Update"].extend(links)
                    elif "fix" in label:
                        section_links["Fix"].extend(links)
                elif len(tds) == 2 and "torrent" in tds[0].text.lower():
                    a = tds[0].find("a")
                    if a:
                        section_links["Torrent"].append(f"{a['href']} - {a.get_text(strip=True)}")

        lines = [
            f"URL: {url}",
            f"Game Name: {game_name}",
            f"Game Version: {version}",
            f"Language: {language}",
            f"Required firmware: {firmware}",
            "\nDetected Sizes:"
        ]

        if size_matches:
            for size in size_matches:
                lines.append(f"- {size}")
        else:
            lines.append("- Unknown")

        lines.append("\nDownload Links:")
        for section, links in section_links.items():
            if links:
                lines.append(f"\n[{section}]")
                for link in links:
                    lines.append(f"- {link}")

        # Save immediately
        filename = f"{sanitize_filename(game_name)}.txt"
        filepath = os.path.join(RESULTS_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
            
        return True

    except Exception as e:
        logging.error(f"Error parsing {url}: {e}")
        return False

def process_page(page_number):
    try:
        url = f"{BASE_URL}/page/{page_number}" if page_number > 1 else BASE_URL
        response = make_request_with_retry(url)
        if not response:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.select("h2.entry-title a")
        game_links = []

        for link in links:
            href = link.get("href")
            if href and "/20" in href and "ps4" in href.lower():
                game_links.append(href)

        return game_links
    except Exception as e:
        logging.warning(f"Failed to process page {page_number}: {e}")
        return []

def main():
    logging.info("Starting the scraper with 5 threads...")
    start_time = time.time()
    total_games = 0
    
    # First collect all game URLs using threads
    all_game_urls = []
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_page = {executor.submit(process_page, page): page for page in range(1, TOTAL_PAGES + 1)}
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                game_urls = future.result()
                all_game_urls.extend(game_urls)
                logging.info(f"Page {page} processed - found {len(game_urls)} games")
            except Exception as e:
                logging.error(f"Page {page} generated an exception: {e}")

    # Then process all game pages using threads
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_url = {executor.submit(parse_and_save_game, url): url for url in all_game_urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                success = future.result()
                if success:
                    total_games += 1
            except Exception as e:
                logging.error(f"Game {url} generated an exception: {e}")

    elapsed_time = time.time() - start_time
    logging.info(f"Completed processing {total_games} games in {elapsed_time:.2f} seconds")
    logging.info(f"Results saved to: {RESULTS_DIR}")

if __name__ == "__main__":
    main()