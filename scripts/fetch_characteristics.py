from __future__ import annotations

import os
import sys
import time
import re
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable

import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


CHAR_URL = "https://plants.usda.gov/plant-profile/{symbol}/characteristics"
PROFILE_URL = "https://plants.usda.gov/plant-profile/{symbol}"

CHAR_SECTIONS = [
    "Morphology/Physiology",
    "Growth Requirements",
    "Reproduction",
    "Suitability/Use",
]

PROFILE_GENERAL_SECTION = "Profile / General Information"
PROFILE_CLASS_SECTION = "Classification"
DIRECT_LOOKUP_SECTION = "Direct Trait Lookup"

# Traits you care about but which may appear in unexpected locations
DIRECT_TRAITS = [
    "Leaf Retention",
    "Flower Conspicuous",
    "Fall Conspicuous",
    "Bloom Period",
    "Shade Tolerance",
    "Moisture Use",
]

CLASS_TRAITS = [
    "Kingdom",
    "Subkingdom",
    "Superdivision",
    "Division",
    "Class",
    "Subclass",
    "Order",
    "Family",
    "Genus",
    "Species",
]

GENERAL_PROFILE_TRAITS = [
    "Symbol",
    "Group",
    "Duration",
    "Growth Habits",
    "Native Status",
    "Family",   # often duplicates canonical family; still useful
    "Genus",    # sometimes appears outside classification too
    "Species",  # ditto
]


def load_env() -> None:
    env_path = Path.cwd() / ".env"
    if not env_path.exists():
        raise FileNotFoundError(f"Expected .env at {env_path}. Run from project root.")
    load_dotenv(dotenv_path=env_path)


def get_db_config() -> dict:
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE", "native_plants")
    port = int(os.getenv("MYSQL_PORT", "3306"))

    missing = [k for k, v in {"MYSQL_USER": user, "MYSQL_PASSWORD": password}.items() if not v]
    if missing:
        raise ValueError(f"Missing required env vars in .env: {', '.join(missing)}")

    return {"host": host, "user": user, "password": password, "database": database, "port": port}


def get_symbols_for_state(cur, state_code: str, fetch_id: int | None) -> list[str]:
    if fetch_id is None:
        cur.execute(
            """
            SELECT DISTINCT symbol
            FROM plant_state_presence
            WHERE state_code = %s
            ORDER BY symbol
            """,
            (state_code,),
        )
    else:
        cur.execute(
            """
            SELECT DISTINCT symbol
            FROM plant_state_presence
            WHERE state_code = %s AND fetch_id = %s
            ORDER BY symbol
            """,
            (state_code, fetch_id),
        )

    rows = cur.fetchall() or []
    if not rows:
        return []

    first = rows[0]
    if isinstance(first, dict):
        return [r["symbol"] for r in rows]
    return [r[0] for r in rows]


def build_driver(headful: bool) -> webdriver.Chrome:
    opts = Options()
    if not headful:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,900")
    # Make JS-heavy pages more stable
    opts.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver


def get_rendered_html(driver: webdriver.Chrome, url: str, timeout_s: int) -> str:
    driver.get(url)
    # Wait for any table rows to appear (more robust than waiting on text headers)
    WebDriverWait(driver, timeout_s).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table tr"))
    )
    time.sleep(0.8)  # small extra hydration buffer
    return driver.page_source


def clean_text(s: str) -> str:
    return " ".join(s.replace("\u00a0", " ").split()).strip()


def normalize_trait_name(name: str) -> str:
    n = clean_text(name)
    aliases = {
        "Leaf retention": "Leaf Retention",
        "Flowers Conspicuous": "Flower Conspicuous",
        "Flower conspicuous": "Flower Conspicuous",
        "Fall conspicuous": "Fall Conspicuous",
    }
    return aliases.get(n, n)


def extract_rows_anywhere(soup: BeautifulSoup) -> dict[str, str]:
    """
    Scan all <tr> rows and build a mapping of trait_name -> trait_value
    based on the first/second cell, using normalized names.
    If duplicates exist, keep the first non-empty value.
    """
    out: dict[str, str] = {}
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue

        k = normalize_trait_name(cells[0].get_text(" ", strip=True))
        v = clean_text(cells[1].get_text(" ", strip=True))
        if not k or not v:
            continue

        # Skip common junk
        if k == "Name":
            continue

        if k not in out:
            out[k] = v
    return out


def extract_characteristics_tables(char_html: str) -> list[tuple[str, str, str]]:
    soup = BeautifulSoup(char_html, "lxml")
    out: list[tuple[str, str, str]] = []

    for section in CHAR_SECTIONS:
        header = soup.find(lambda tag: tag.get_text(strip=True) == section)
        if not header:
            continue

        table = header.find_next("table")
        if not table:
            continue

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            trait_name = normalize_trait_name(cells[0].get_text(" ", strip=True))
            trait_value = clean_text(cells[1].get_text(" ", strip=True))

            if not trait_name or not trait_value:
                continue
            if trait_name == "Name":
                continue

            out.append((section, trait_name, trait_value))

    return out


def extract_profile_general(profile_html: str) -> list[tuple[str, str, str]]:
    soup = BeautifulSoup(profile_html, "lxml")
    out: list[tuple[str, str, str]] = []

    # Global scan is more reliable than hunting for a specific “General Information” header
    kv = extract_rows_anywhere(soup)

    for k in GENERAL_PROFILE_TRAITS:
        v = kv.get(k)
        if v:
            out.append((PROFILE_GENERAL_SECTION, k, v))

    return out


def extract_profile_classification(profile_html: str) -> list[tuple[str, str, str]]:
    soup = BeautifulSoup(profile_html, "lxml")
    out: list[tuple[str, str, str]] = []

    header = soup.find(lambda tag: tag.get_text(strip=True) == "Classification")
    if not header:
        return out

    table = header.find_next("table")
    if not table:
        return out

    for tr in table.find_all("tr"):
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        trait_name = normalize_trait_name(cells[0].get_text(" ", strip=True))
        trait_value = clean_text(cells[1].get_text(" ", strip=True))

        if not trait_name or not trait_value:
            continue
        if trait_name == "Name":
            continue

        if trait_name in CLASS_TRAITS:
            out.append((PROFILE_CLASS_SECTION, trait_name, trait_value))

    return out


def extract_direct_traits(html: str) -> list[tuple[str, str, str]]:
    soup = BeautifulSoup(html, "lxml")
    kv = extract_rows_anywhere(soup)
    out: list[tuple[str, str, str]] = []
    for name in DIRECT_TRAITS:
        v = kv.get(name)
        if v:
            out.append((DIRECT_LOOKUP_SECTION, name, v))
    return out


def upsert_fetch_status(cur, table: str, symbol: str, url: str, fetched_at, status: str, error: str | None) -> None:
    cur.execute(
        f"""
        INSERT INTO {table}
          (symbol, profile_url, fetched_at, fetch_status, error)
        VALUES
          (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          profile_url = VALUES(profile_url),
          fetched_at = VALUES(fetched_at),
          fetch_status = VALUES(fetch_status),
          error = VALUES(error)
        """,
        (symbol, url, fetched_at, status, error),
    )


def upsert_kv_rows(cur, symbol: str, url: str, fetched_at, rows: list[tuple[str, str, str]]) -> int:
    if not rows:
        return 0

    cur.executemany(
        """
        INSERT INTO plant_characteristics_kv
          (symbol, section, trait_name, trait_value, profile_url, fetched_at)
        VALUES
          (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          trait_value = VALUES(trait_value),
          profile_url = VALUES(profile_url),
          fetched_at = VALUES(fetched_at)
        """,
        [(symbol, section, name, val, url, fetched_at) for (section, name, val) in rows],
    )
    return cur.rowcount


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch USDA profile + characteristics using Selenium, store KV.")
    parser.add_argument("--state-code", required=True)
    parser.add_argument("--fetch-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--refetch", action="store_true")
    parser.add_argument("--timeout-s", type=int, default=25)
    parser.add_argument("--sleep", type=float, default=0.4)
    args = parser.parse_args()

    state_code = args.state_code.strip().upper()
    fetch_id = args.fetch_id
    limit = args.limit
    headful = args.headful
    refetch = args.refetch
    timeout_s = args.timeout_s
    sleep_time = args.sleep

    try:
        load_env()
        cfg = get_db_config()
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor(dictionary=True)

        symbols = get_symbols_for_state(cur, state_code, fetch_id)
        if limit and limit > 0:
            symbols = symbols[:limit]
        if not symbols:
            raise RuntimeError(f"No symbols found for {state_code} (fetch_id={fetch_id}).")

        driver = build_driver(headful=headful)

        total_kv = 0
        prof_ok = prof_no = char_ok = char_no = failed = 0

        try:
            for i, symbol in enumerate(symbols, start=1):
                fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)

                profile_url = PROFILE_URL.format(symbol=symbol)
                char_url = CHAR_URL.format(symbol=symbol)

                # PROFILE
                try:
                    profile_html = get_rendered_html(driver, profile_url, timeout_s)
                    prof_rows = []
                    prof_rows.extend(extract_profile_general(profile_html))
                    prof_rows.extend(extract_profile_classification(profile_html))
                    prof_rows.extend(extract_direct_traits(profile_html))  # direct fallback

                    if prof_rows:
                        upsert_fetch_status(cur, "plant_profile_fetches", symbol, profile_url, fetched_at, "HAS_DATA", None)
                        total_kv += upsert_kv_rows(cur, symbol, profile_url, fetched_at, prof_rows)
                        prof_ok += 1
                    else:
                        upsert_fetch_status(cur, "plant_profile_fetches", symbol, profile_url, fetched_at, "NO_DATA", None)
                        prof_no += 1
                except Exception as e:
                    upsert_fetch_status(cur, "plant_profile_fetches", symbol, profile_url, fetched_at, "ERROR", repr(e))
                    failed += 1

                # CHARACTERISTICS
                try:
                    char_html = get_rendered_html(driver, char_url, timeout_s)
                    char_rows = []
                    char_rows.extend(extract_characteristics_tables(char_html))
                    char_rows.extend(extract_direct_traits(char_html))  # direct fallback

                    if char_rows:
                        upsert_fetch_status(cur, "plant_characteristics_fetches", symbol, char_url, fetched_at, "HAS_DATA", None)
                        total_kv += upsert_kv_rows(cur, symbol, char_url, fetched_at, char_rows)
                        char_ok += 1
                    else:
                        upsert_fetch_status(cur, "plant_characteristics_fetches", symbol, char_url, fetched_at, "NO_DATA", None)
                        char_no += 1
                except Exception as e:
                    upsert_fetch_status(cur, "plant_characteristics_fetches", symbol, char_url, fetched_at, "ERROR", repr(e))
                    failed += 1

                if i % 25 == 0:
                    conn.commit()

                time.sleep(max(sleep_time, 0.0))

            conn.commit()

        finally:
            driver.quit()
            cur.close()
            conn.close()

        print(f"Completed. State={state_code} symbols={len(symbols)}")
        print(f"Profile HAS_DATA={prof_ok} NO_DATA={prof_no}")
        print(f"Char   HAS_DATA={char_ok} NO_DATA={char_no}")
        print(f"KV upserts (rowcount sum): {total_kv}  Errors: {failed}")
        return 0

    except (FileNotFoundError, ValueError, RuntimeError, MySQLError) as e:
        print(f"Error: {e!r}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
