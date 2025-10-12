# scraper.py
import csv
import time
from typing import Dict, List, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

from bs4 import BeautifulSoup, Comment  # pip install beautifulsoup4


# === Einstellungen ===
LINKS_CSV = "player_links.csv"     # Eingabe
OUT_CSV   = "players_data3.csv"     # Ausgabe (eine Zeile pro Spieler)
HEADLESS  = True
PAGE_TIMEOUT = 20


def init_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)


def close_popups(driver: webdriver.Chrome) -> None:
    try:
        btn = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "osano-cm-accept-all"))
        )
        btn.click()
        time.sleep(0.4)
    except (TimeoutException, NoSuchElementException):
        pass


def read_links_from_csv(filename: str) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    with open(filename, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("Player Name") or "").strip()
            url  = (row.get("Profile URL") or "").strip()
            if name and url:
                links.append((name, url))
    return links


def wait_for_main(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, PAGE_TIMEOUT).until(EC.presence_of_element_located((By.ID, "wrap")))


def extract_meta_from_dom(driver: webdriver.Chrome) -> Dict[str, str]:
    """
    Extrahiert Meta-Daten aus #meta.
    WICHTIG: 'Name' und 'Shoots' werden GAR NICHT mehr aufgenommen.
    """
    import re
    meta: Dict[str, str] = {}
    try:
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "meta")))
    except TimeoutException:
        pass

    soup = BeautifulSoup(driver.page_source, "html.parser")
    meta_div = soup.find(id="meta")
    if not meta_div:
        return meta

    # (1) <p><strong>Key:</strong> Value</p>
    for p in meta_div.find_all("p"):
        strong = p.find("strong")
        if strong and strong.get_text(strip=True).endswith(":"):
            key = strong.get_text(strip=True).rstrip(":")
            strong.extract()
            value = p.get_text(" ", strip=True)
            if value:
                # 'Shoots' bewusst NICHT übernehmen
                if key.lower() == "shoots":
                    continue
                meta[key] = value

    # (2) Height/Weight über itemprop (wenn vorhanden)
    span_h = meta_div.find("span", attrs={"itemprop": "height"})
    span_w = meta_div.find("span", attrs={"itemprop": "weight"})
    if span_h:
        h_text = span_h.get_text(" ", strip=True)
        after = span_h.find_next(string=True)
        h_suffix = ""
        if after and isinstance(after, str):
            m = re.search(r"\(\s*\d{2,3}\s*cm\s*\)", after)
            if m:
                h_suffix = f" {m.group(0)}"
        meta["Height"] = (h_text + h_suffix).strip()

    if span_w:
        w_text = span_w.get_text(" ", strip=True)
        after = span_w.find_next(string=True)
        w_suffix = ""
        if after and isinstance(after, str):
            m = re.search(r"\(\s*\d{2,3}\s*kg\s*\)", after)
            if m:
                w_suffix = f" {m.group(0)}"
        meta["Weight"] = (w_text + w_suffix).strip()

    # (3) Fallback: "Key: Value"-Zeilen heuristisch — 'Shoots' überspringen
    for line in [t.strip() for t in meta_div.get_text("\n").split("\n") if t.strip()]:
        if ":" in line:
            key, val = line.split(":", 1)
            key, val = key.strip(), val.strip()
            if key and val and key not in meta and key.lower() != "shoots":
                meta[key] = val

    # (4) Weitere Fallbacks nur für Height/Weight via Regex
    full_text = meta_div.get_text(" ", strip=True)
    if "Height" not in meta:
        m = re.search(r"(\d{1,2}-\d{1,2})(\s*\(\s*\d{2,3}\s*cm\s*\))?", full_text)
        if m:
            meta["Height"] = (m.group(1) + (m.group(2) or "")).strip()
    if "Weight" not in meta:
        m = re.search(r"(\d{2,3})\s*lb(\s*\(\s*\d{2,3}\s*kg\s*\))?", full_text, flags=re.IGNORECASE)
        if m:
            meta["Weight"] = (f"{m.group(1)}lb" + (m.group(2) or "")).strip()

    # (5) KEIN Name-Feld setzen
    return meta


def extract_transactions_raw(driver: webdriver.Chrome) -> str:
    """
    Liefert den *kompletten Text* aus all_transactions (inkl. kommentierter Tabelle).
    Rückgabe: ein einziger String (Zeilen mit '\n' getrennt).
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")

    container = soup.find("div", class_="all_transactions") or soup.find(id="all_transactions")
    if not container:
        return ""

    comments = container.find_all(string=lambda t: isinstance(t, Comment))
    if comments:
        joined = "\n".join(c for c in comments)
        inner = BeautifulSoup(joined, "html.parser")
        table = inner.find("table")
        if table:
            text = table.get_text("\n", strip=True)
            return normalize_ws(text)
        return normalize_ws(inner.get_text("\n", strip=True))

    return normalize_ws(container.get_text("\n", strip=True))


def normalize_ws(text: str) -> str:
    if not text:
        return ""
    lines = [ln.strip() for ln in text.replace("\r", "").split("\n")]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines)


def main() -> None:
    print("[INFO] Starte Scraper (eine Zeile pro Spieler; ohne Name/Shoots in CSV)…")
    links = read_links_from_csv(LINKS_CSV)
    if not links:
        print("[ERROR] Keine Links gefunden.")
        return

    # KEIN Name, KEIN Shoots in der CSV
    common_meta = ["Position", "Born", "College", "High School", "Draft", "Height", "Weight"]
    fieldnames = ["Player Name", "Profile URL", *common_meta, "MetaRaw", "TransactionsRaw"]

    driver = init_driver(HEADLESS)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for idx, (player_name, url) in enumerate(links, start=1):
            print(f"[{idx}/{len(links)}] {player_name} -> {url}")
            try:
                driver.get(url)
            except Exception as e:
                print(f"[WARN] Laden fehlgeschlagen: {e}")
                continue

            close_popups(driver)
            try:
                wait_for_main(driver)
            except TimeoutException:
                print("[WARN] Seite evtl. unvollständig – fahre fort.")

            base = {"Player Name": player_name, "Profile URL": url}

            # META
            try:
                meta = extract_meta_from_dom(driver)
            except Exception as e:
                print(f"[WARN] META-Parsing-Fehler: {e}")
                meta = {}

            row = {**base}
            # MetaRaw + gewünschte Meta-Felder (ohne Name/Shoots)
            row["MetaRaw"] = "; ".join([f"{k}: {v}" for k, v in meta.items()])
            for k in common_meta:
                if k in meta:
                    row[k] = meta[k]

            # TRANSACTIONS: kompletter Text
            try:
                tx_raw = extract_transactions_raw(driver)
            except Exception as e:
                print(f"[WARN] Transactions-Parsing-Fehler: {e}")
                tx_raw = ""
            row["TransactionsRaw"] = tx_raw

            writer.writerow(row)
            time.sleep(1.0)

    driver.quit()
    print(f"[SUCCESS] Fertig. Gespeichert in '{OUT_CSV}'.")


if __name__ == "__main__":
    main()
