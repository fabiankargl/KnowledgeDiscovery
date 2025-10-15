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
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)
from webdriver_manager.chrome import ChromeDriverManager

# Optional: für #meta-Parsing und Transactions in HTML-Kommentaren
from bs4 import BeautifulSoup, Comment  # pip install beautifulsoup4

# === Einstellungen ===
LINKS_CSV    = "player_links.csv"     # Eingabe
OUT_CSV      = "players_data6.csv"    # Ausgabe (eine Zeile pro Spieler)
HEADLESS     = True
PAGE_TIMEOUT = 15                      # Seiten-Timeout
WAIT_META    = 8                       # Wartezeit auf #meta (schlanker als #wrap)


def init_driver(headless: bool = True) -> webdriver.Chrome:
    """Schneller, stabiler Driver (eager, Ressourcen blocken, kürzere Timeouts)."""
    opts = Options()
    if headless:
        # Wenn Probleme: "--headless" statt "--headless=new"
        opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    # schneller: wartet nicht auf alle Subressourcen
    opts.page_load_strategy = "eager"

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)

    # Zeitlimits enger setzen
    driver.set_page_load_timeout(PAGE_TIMEOUT)
    driver.set_script_timeout(10)

    # Selenium-HTTP-Client Timeout (verhindert 120s-Hänger auf localhost)
    try:
        driver.command_executor._client_config.timeout = 30
    except Exception:
        pass

    # Große Ressourcen blocken (macht Seiten deutlich kleiner/schneller)
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd(
            "Network.setBlockedURLs",
            {
                "urls": [
                    "*.png",
                    "*.jpg",
                    "*.jpeg",
                    "*.gif",
                    "*.webp",
                    "*.svg",
                    "*.css",
                    "*.woff",
                    "*.woff2",
                    "*.ttf",
                ]
            },
        )
    except Exception:
        pass

    return driver


def close_popups(driver: webdriver.Chrome, total_timeout: float = 1.0) -> None:
    """
    Nicht blockierend. Klickt gängige Consent/OK-Buttons, sonst weiter.
    Max. ~1.0 s.
    """
    deadline = time.time() + total_timeout
    selectors = [
        (By.CLASS_NAME, "osano-cm-accept-all"),
        (By.CSS_SELECTOR, "button.osano-cm-accept-all"),
        (By.CSS_SELECTOR, "button[aria-label*='Accept' i]"),
        (By.CSS_SELECTOR, "button[id*='accept' i], button[class*='accept' i]"),
        (By.XPATH, "//button[contains(translate(., 'ACCEPT','accept'),'accept')]"),
        (By.XPATH, "//button[contains(., 'I Accept') or contains(., 'I agree')]"),
        (By.XPATH, "//input[@type='submit' and contains(@value, 'I Accept')]"),
    ]
    while time.time() < deadline:
        clicked = False
        for by, sel in selectors:
            try:
                for el in driver.find_elements(by, sel):
                    if el.is_displayed() and el.is_enabled():
                        try:
                            el.click()
                            clicked = True
                            break
                        except Exception:
                            continue
            except Exception:
                continue
        if clicked:
            return
        time.sleep(0.08)

    # JS-Fallback (einmalig, nicht blockierend)
    try:
        driver.execute_script(
            """
(function(){
  function v(el){try{const r=el.getBoundingClientRect();return r.width>0&&r.height>0}catch(e){return false}}
  const keys=['accept','agree','ok'];
  const nodes=[...document.querySelectorAll('button,input[type=button],input[type=submit]')];
  for(const n of nodes){
    const t=((n.innerText||n.value||'')+' '+(n.getAttribute('aria-label')||'')+' '+(n.id||'')+' '+(n.className||'')).toLowerCase();
    if(keys.some(k=>t.includes(k)) && v(n)){ try{ n.click(); return; }catch(e){} }
  }
})();"""
        )
    except Exception:
        pass


def read_links_from_csv(filename: str) -> List[Tuple[str, str]]:
    links: List[Tuple[str, str]] = []
    with open(filename, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("Player Name") or "").strip()
            url = (row.get("Profile URL") or "").strip()
            if name and url:
                links.append((name, url))
    return links


def wait_for_meta(driver: webdriver.Chrome) -> None:
    WebDriverWait(driver, WAIT_META).until(
        EC.presence_of_element_located((By.ID, "meta"))
    )


def extract_meta_from_dom(driver: webdriver.Chrome) -> Dict[str, str]:
    """
    Extrahiert Meta-Daten aus #meta.
    'Shoots' wird absichtlich NICHT übernommen.
    """
    import re

    meta: Dict[str, str] = {}
    try:
        wait_for_meta(driver)
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
            if value and key.lower() != "shoots":
                meta[key] = value

    # (2) Height/Weight via itemprop
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

    # (3) Fallback: "Key: Value" — 'Shoots' überspringen
    for line in [t.strip() for t in meta_div.get_text("\n").split("\n") if t.strip()]:
        if ":" in line:
            key, val = line.split(":", 1)
            key, val = key.strip(), val.strip()
            if key and val and key not in meta and key.lower() != "shoots":
                meta[key] = val

    # (4) Regex-Fallbacks für Height/Weight
    full_text = meta_div.get_text(" ", strip=True)
    if "Height" not in meta:
        m = re.search(r"(\d{1,2}-\d{1,2})(\s*\(\s*\d{2,3}\s*cm\s*\))?", full_text)
        if m:
            meta["Height"] = (m.group(1) + (m.group(2) or "")).strip()
    if "Weight" not in meta:
        m = re.search(
            r"(\d{2,3})\s*lb(\s*\(\s*\d{2,3}\s*kg\s*\))?",
            full_text,
            flags=re.IGNORECASE,
        )
        if m:
            meta["Weight"] = (f"{m.group(1)}lb" + (m.group(2) or "")).strip()

    return meta


def extract_transactions_raw(driver: webdriver.Chrome) -> str:
    """
    Holt den kompletten Text der Transactions (inkl. kommentierter Tabelle).
    Rückgabe: String mit '\n' als Zeilentrenner.
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")

    container = soup.find("div", class_="all_transactions") or soup.find(
        id="all_transactions"
    )
    if not container:
        return ""

    # Tabelle steckt oft in HTML-Kommentaren
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
    print("[INFO] Starte Scraper (eine Zeile pro Spieler; 'Shoots' wird nicht geschrieben)…")
    links = read_links_from_csv(LINKS_CSV)
    if not links:
        print("[ERROR] Keine Links gefunden.")
        return

    common_meta = ["Position", "Born", "College", "High School", "Draft", "Height", "Weight"]
    fieldnames = ["Player Name", "Profile URL", *common_meta, "MetaRaw", "TransactionsRaw"]

    driver = init_driver(HEADLESS)
    try:
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()

            for idx, (player_name, url) in enumerate(links, start=1):
                print(f"[{idx}/{len(links)}] {player_name} -> {url}")

                # Kurzer Lade-Retry (vermeidet harte Abbrüche)
                ok = False
                for i in range(2):
                    try:
                        driver.get(url)
                        ok = True
                        break
                    except WebDriverException as e:
                        if i == 1:
                            print(f"[WARN] Laden fehlgeschlagen: {e}")
                        time.sleep(0.6 + 0.4 * i)
                if not ok:
                    continue

                # Cookie/Consent unblocking, aber nicht blockierend
                close_popups(driver)

                # Schlankes Warten auf #meta (statt #wrap)
                try:
                    wait_for_meta(driver)
                except TimeoutException:
                    print("[WARN] #meta nicht eindeutig – fahre fort.")

                base = {"Player Name": player_name, "Profile URL": url}

                # META
                try:
                    meta = extract_meta_from_dom(driver)
                except Exception as e:
                    print(f"[WARN] META-Parsing-Fehler: {e}")
                    meta = {}

                row = {**base}
                row["MetaRaw"] = "; ".join([f"{k}: {v}" for k, v in meta.items()])
                for k in common_meta:
                    if k in meta:
                        row[k] = meta[k]

                # TRANSACTIONS
                try:
                    tx_raw = extract_transactions_raw(driver)
                except Exception as e:
                    print(f"[WARN] Transactions-Parsing-Fehler: {e}")
                    tx_raw = ""
                row["TransactionsRaw"] = tx_raw

                writer.writerow(row)
                # kurze Pause (bei Bedarf 0.15–0.2 testen)
                time.sleep(0.25)

        print(f"[SUCCESS] Fertig. Gespeichert in '{OUT_CSV}'.")
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
