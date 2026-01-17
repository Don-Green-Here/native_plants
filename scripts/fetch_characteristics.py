from __future__ import annotations
import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone

import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
#Using playwright to load since the USDA pages are loaded dynamically though js

PROFILE_SECTIONS = ["Symbol", "Group", "Duration", "Growth Habits", "Native Status",]
# The table that exists on the plants profile page, contains duration (annual, biennial, perrenial), native status
CHAR_SECTIONS = ["Morphology/Physiology", "Growth Requirements", "Reproduction", "Suitability/Use",]
# Sections that exist on characteristic pages for plants as table headers, 4 total

PROFILE_URL = "https://plants.usda.gov/plant-profile/{symbol}"
#Main page for the plant
CHAR_URL = "https://plants.usda.gov/plant-profile/{symbol}/characteristics"
#Page of characteristics, all plants have the page, but not all are populated with tables

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

    # Support both dictionary=True and default tuple cursor row formats
    first = rows[0]
    if isinstance(first, dict):
        return [r["symbol"] for r in rows]
    return [r[0] for r in rows]


def should_skip_symbol(cur, symbol: str, refetch: bool) -> bool:
    #Skip if refetch passed, looking for whether there's data or not
    if refetch:
        return False
    cur.execute(
            """
            SELECT fetch_status FROM 
            plant_characteristics_fetches
            WHERE symbol = %s
            """,
            (symbol,),
    )
    row = cur.fetchone()
    if not row:
        return False
    return row["fetch_status"] in ("HAS_DATA", "NO_DATA")

def render_characteristics(page, symbol: str, timeout_ms: int) -> str:
    char_url = CHAR_URL.format(symbol=symbol)
    page.goto(char_url, wait_until="domcontentloaded",timeout=timeout_ms)
    #Wait for dynamic (document object model (DOM)) content to load or for timeout to expire
    #Does not rely on 404 since all pages exist, but they may not have content

    try:
        page.wait_for_timeout(1200)
        #Gives time for js to populate the table
    except Exception:
        pass
    # If it never appears, we still capture the DOM and classify as NO_DATA.
    for header in CHAR_SECTIONS:
        try:
            page.wait_for_selector(f"text={header}", timeout = 1500)
            break
        except PlaywrightTimeoutError:
            continue

    return page.content()

def extract_char_tables(char_html: str)->list[tuple[str, str, str]]:

    soup = BeautifulSoup(char_html, "lxml")
    out: list[tuple[str, str, str]] = []
    #Expect list of tuples that takes in 3 strings, currently empty

    #Parse each section of the lxml, down to the individual cell
    #First we're looking for the header titles "sections" of the tables we know we will find
    for section in CHAR_SECTIONS:
        header = soup.find(lambda tag: tag.get_text(strip=True) == section)
        if not header:
            continue
        #We're now searching for a table near our known section
        table = header.find_next("table")
        if not table:
            continue
        #We're now searching for table cells and table headers inside the table
        for tr in table.find_all("tr"):
            cells = tr.find_all(["td","th"])
            if len(cells) < 2:
                continue
                #cells smaller than 2 do not hold key value pairs
            trait_name = cells[0].get_text(" ", strip = True)
            trait_value = cells[1].get_text(" ", strip = True)
            # Strip trailing/leading white space
            out.append((section, trait_name, trait_value))


    return out

def render_profile(page, symbol: str, timeout_ms: int) -> str:
    profile_url = PROFILE_URL.format(symbol=symbol)
    page.goto(profile_url, wait_until="networkidle",timeout=timeout_ms)
    #Wait for page to load
    #Does not rely on 404 since all pages exist

    return page.content()

def extract_profile_tables(profile_html: str)->list[tuple[str, str, str]]:

    soup = BeautifulSoup(profile_html, "lxml")
    out: list[tuple[str, str, str]] = []
    #Expect list of tuples that takes in 3 strings, currently empty

    #Parse each section of the lxml, down to the individual cell
    for tr in soup.find_all("tr"):
        cells = tr.find_all(["td","th"])
        if len(cells) < 2:
            continue
            #cells smaller than 2 do not hold key value pairs
        trait_name = cells[0].get_text(" ", strip = True)
        trait_value = cells[1].get_text(" ", strip = True)
        # Strip trailing/leading white space
        if not trait_name or not trait_value:
            continue
        if trait_name in PROFILE_SECTIONS:
            out.append(("Profile / General Information", trait_name, trait_value))


    return out

def upsert_fetch_char_status(cur, symbol: str, url: str, fetched_at, status: str, error: str | None) -> None:
    cur.execute(
        """
        INSERT INTO plant_characteristics_fetches
        (symbol, profile_url, fetched_at, fetch_status, error) 
        VALUES (%s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        profile_url = VALUES(profile_url), 
        fetched_at = VALUES(fetched_at), 
        fetch_status = VALUES(fetch_status), 
        error = VALUES(error)
        """,
        (symbol, url, fetched_at, status, error),
    )

def upsert_fetch_profile_status(cur, symbol: str, url: str, fetched_at, status: str, error: str | None) -> None:
    cur.execute(
        """
        INSERT INTO plant_profile_fetches
        (symbol, profile_url, fetched_at, fetch_status, error) 
        VALUES (%s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        profile_url = VALUES(profile_url), 
        fetched_at = VALUES(fetched_at), 
        fetch_status = VALUES(fetch_status), 
        error = VALUES(error)
        """,
        (symbol, url, fetched_at, status, error),
    )

def insert_kv_rows(cur, symbol: str, url: str, fetched_at, rows: list[tuple[str,str,str]]) -> int:
    if not rows:
        return 0
    cur.executemany(
        """
        INSERT IGNORE INTO plant_characteristics_kv 
        (symbol, section, trait_name, trait_value, profile_url, fetched_at) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        """,
        [(symbol, s, n, v, url, fetched_at) for (s, n, v) in rows]
    )
    #return the row count to id how many new primary keys inserted
    return cur.rowcount

def main()-> int:

    parser = argparse.ArgumentParser(description="Find plants with characteristic data on USDA")
    parser.add_argument("--state-code", required=True, help="USDA state code")
    parser.add_argument("--fetch-id", type=int, help="Optional. Fetch index")
    parser.add_argument("--limit", type=int, help="Optional. Fetch limit")
    parser.add_argument("--headful", action="store_true", help="Optional. Run header visually (debug).")
    parser.add_argument("--refetch", action="store_true", help="Optional. Run refetch even if we have values.")
    parser.add_argument("--sleep", type=float, default=0.5, help = "Optional. Sleep time in seconds between requests.")
    parser.add_argument("--timeout-ms", type=int, default = 30000, help="Optional. Page timeout in milliseconds.")
    args = parser.parse_args()

    state_code = args.state_code.strip().upper()
    fetch_id = args.fetch_id
    limit = args.limit
    headful = args.headful
    refetch = args.refetch
    sleep_time = args.sleep
    timeout_ms = args.timeout_ms
    #Arguments passed during command line

    try:
        load_env()
        cfg = get_db_config()
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor(dictionary=True)

        symbols = get_symbols_for_state(cur, state_code, fetch_id)
        #Returns all distinct symbols

        if limit and limit > 0:
            symbols = symbols[:limit]

        if not symbols:
            raise RuntimeError(f"No symbols found for {state_code} (fetch_id={fetch_id}).")

        data_found = 0
        no_data = 0
        failed = 0
        profile_found = 0
        no_profile_found = 0
        total_inserted_kv = 0
        #Set our statuses to zero

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=not headful)
            page = browser.new_page()
            # launches a chrome/microsoft edge browser instance as a headless client

            for i, symbol in enumerate(symbols, start=1):
                char_url = CHAR_URL.format(symbol=symbol)
                profile_url = PROFILE_URL.format(symbol=symbol)

                #if should_skip_symbol(cur, symbol, refetch):
                #    continue

                fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)

                try:
                    char_html = render_characteristics(page, symbol, timeout_ms)
                    #Will attempt to open characteristics web page and test if headers for the data exist
                    char_rows = extract_char_tables(char_html)
                    #Will return characteristic rows containing (section, trait_name, trait_value)
                    if char_rows:
                        upsert_fetch_char_status(cur, symbol, char_url, fetched_at, "HAS_DATA", None)
                        #this will update the table plant_characteristics_fetches, duplicate entry will update values
                        inserted = insert_kv_rows(cur, symbol, char_url, fetched_at, char_rows)
                        #this will update the table plant_characteristics_kv, it will ignore duplicates
                        total_inserted_kv += inserted
                        data_found += 1
                    else:
                        upsert_fetch_char_status(cur, symbol, char_url, fetched_at, "NO_DATA", None)
                        #Update the fetch table to show no data at this link
                        no_data += 1
                except Exception as e:
                    upsert_fetch_char_status(cur, symbol, char_url, fetched_at, "ERROR", str(e))
                    #No url able to be loaded
                    failed += 1

                try:
                    profile_html = render_profile(page, symbol, timeout_ms)
                    #Will attempt to open characteristics web page and test if headers for the data exist
                    profile_rows = extract_profile_tables(profile_html)
                    #Will return characteristic rows containing (section, trait_name, trait_value)
                    if profile_rows:
                        upsert_fetch_profile_status(cur, symbol, profile_url, fetched_at, "HAS_DATA", None)
                        #this will update the table plant_characteristics_fetches, duplicate entry will update values
                        inserted = insert_kv_rows(cur, symbol, profile_url, fetched_at, profile_rows)
                        #this will update the table plant_characteristics_kv, it will ignore duplicates
                        total_inserted_kv += inserted
                        profile_found += 1
                    else:
                        upsert_fetch_profile_status(cur, symbol, profile_url, fetched_at, "NO_DATA", None)
                        #Update the fetch table to show no data at this link
                        no_profile_found += 1
                except Exception as e:
                    upsert_fetch_profile_status(cur, symbol, profile_url, fetched_at, "ERROR", str(e))
                    #No url able to be loaded
                    failed += 1

                #We will commit to our tables in chunks to keep progress
                #We're expecting thousands of pages
                if i % 25 == 0:
                    conn.commit()
                time.sleep(max(sleep_time,0.0))

            conn.commit()
            browser.close()

        cur.close()
        conn.close()

        print(f"Completed. State = {state_code}")
        print(f"Key Values inserted (deduped): {total_inserted_kv}")
        print(f"Profiles found: {profile_found}, No profile found: {no_profile_found}, Issues: {failed}")
        print(f"KV pairs found: {data_found}, No KVs Available: {no_data}, Issues: {failed}")

        return 0

    except (FileNotFoundError, ValueError, RuntimeError, MySQLError) as e:
        try:
            if "conn" in locals() and conn.is_connected():
                conn.rollback()
        except Exception:
            pass
        print(f"Error: {e}", file = sys.stderr)
        return 1

if __name__ == "__main__":
    raise SystemExit(main())




