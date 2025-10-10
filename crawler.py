import time
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import urllib.robotparser as robotparser
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
)
from typing import List, Tuple

BASE_URL = "https://www.basketball-reference.com/"
SPECIFIC_DIRECTORY = "/players/"
ROBOTS_URL = BASE_URL + "robots.txt"
FILENAME = "player_links.csv"
USER_AGENT = "*"

def init_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Initialize the Selenium Chrome WebDriver
    """
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), 
                              options=chrome_options)
    return driver

def check_robots_txt(base_url: str = BASE_URL, 
                     path: str = SPECIFIC_DIRECTORY) -> bool:
    """
    Verify the site's robots.txt to ensure the specific directory is allowed to be crawled
    """
    print(f"[INFO] Checking robots.txt: {ROBOTS_URL}")

    robotParser = robotparser.RobotFileParser()
    robotParser.set_url(ROBOTS_URL)
    robotParser.read()

    # Checks if the crawler can access the path
    can_fetch = robotParser.can_fetch(USER_AGENT, base_url + path)
    if can_fetch:
        print(f"[SUCCESS] Crawling allowed for path: {path}")
    else:
        print(f"[ERROR] Crawling disallowed for path: {path}")
    return can_fetch

def close_popups(driver: webdriver.Chrome) -> None:
    """
    Attempts to close site-specific banner
    """
    try:
        accept_button = driver.find_element(By.CLASS_NAME, "osano-cm-accept-all") # close site-specific cookie banner
        accept_button.click()
        print("[INFO] Closed cookie banner")
        time.sleep(1)
    except NoSuchElementException:
        pass

def get_all_players_urls() -> List[Tuple[str, str]]:
    """
    Crawl Basketball Reference player pages (A-Z) and collect all player profile URLs
    """
    # Respects robots.txt before crawling
    if not check_robots_txt(BASE_URL):
        print(f"[WARNING] Crawler stopped — robots.txt disallows crawling in directoy {SPECIFIC_DIRECTORY}")
        return []

    driver = init_driver()
    player_links = []
    alphabet = [chr(i) for i in range(ord("a"), ord("z") + 1)] # Creates an alphabet list [A-Z]

    driver.get(BASE_URL)

    # Handle possible cookie banners
    close_popups(driver)

    for letter in alphabet:
        url = f"{BASE_URL.rstrip('/')}{SPECIFIC_DIRECTORY}{letter}/"
        print(f"[INFO] Crawling: {url}")
        driver.get(url)

        time.sleep(2)

        try:
            # Wait for the player table to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "players"))
            )
        except TimeoutException:
            print(f"[WARNING] Timeout waiting for table on {url}")
            continue

        # Extract all table rows for players
        rows = driver.find_elements(By.CSS_SELECTOR, "#players tbody tr")
        for row in rows:
            try:
                player_cell = row.find_element(By.CSS_SELECTOR, "th[data-stat='player'] a")
                player_name = player_cell.text.strip()
                player_url = player_cell.get_attribute("href")
                player_links.append((player_name, player_url))
            except NoSuchElementException:
                continue

        print(f"[INFO] Found {len(player_links)} players so far...")

    driver.quit()
    return player_links


def save_links_to_csv(links: List[Tuple[str, str]], filename: str = FILENAME) -> None:
    """
    Save collected player URLs to a CSV file
    """
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Player Name", "Profile URL"])
        writer.writerows(links)
    print(f"[SUCCESS] Saved {len(links)} player URLs to {filename}")


if __name__ == "__main__":
    print("[INFO] Starting Basketball Reference web crawler...")
    player_links = get_all_players_urls()
    if player_links:
        save_links_to_csv(player_links)
        print("[SUCCESS] Crawling completed")
