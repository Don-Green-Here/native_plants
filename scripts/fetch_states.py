from __future__ import annotations

import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timezone

import requests
import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv


USDA_BASE = "https://plants.sc.egov.usda.gov/DocumentLibrary/Txt"


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


def get_state(cur, state_code: str) -> dict:
    cur.execute(
        """
        SELECT state_code, state_name, state_slug, is_active
        FROM states
        WHERE state_code = %s
        """,
        (state_code,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"State not found in states table: {state_code}")
    if int(row.get("is_active", 1)) != 1:
        raise RuntimeError(f"State is inactive in states table: {state_code}")
    if not row.get("state_slug"):
        raise RuntimeError(f"State slug missing for {state_code}. Populate states.state_slug.")
    return row


def build_url(state_slug: str) -> str:
    return f"{USDA_BASE}/{state_slug}_NRCS_csv.txt"


def fetch_url(url: str, timeout_s: int = 60) -> tuple[int | None, str | None, str | None, str | None]:
    """
    Returns: (http_status, content_type, body, error)
    """
    try:
        resp = requests.get(url, timeout=timeout_s)
        content_type = resp.headers.get("Content-Type")
        # store text even if non-200 so you can debug, but you may choose otherwise
        body = resp.text
        return resp.status_code, content_type, body, None
    except Exception as e:
        return None, None, None, str(e)


def insert_fetch(cur, state_code: str, url: str, http_status, content_type, body, error) -> int:
    fetched_at = datetime.now(timezone.utc).replace(tzinfo=None)  # store naive UTC
    cur.execute(
        """
        INSERT INTO state_fetches
          (state_code, url, fetched_at, http_status, content_type, body, error)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s)
        """,
        (state_code, url, fetched_at, http_status, content_type, body, error),
    )
    return int(cur.lastrowid)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch USDA NRCS state txt into state_fetches.")
    parser.add_argument("--state-code", required=True, help="Two-letter code, e.g., VA, NJ")
    args = parser.parse_args()

    state_code = args.state_code.strip().upper()

    try:
        load_env()
        cfg = get_db_config()

        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor(dictionary=True)

        state = get_state(cur, state_code)
        url = build_url(state["state_slug"])

        http_status, content_type, body, error = fetch_url(url)

        fetch_id = insert_fetch(cur, state_code, url, http_status, content_type, body, error)
        conn.commit()

        print(f"Inserted fetch record for {state_code}")
        print(f"Fetch ID: {fetch_id}")
        print(f"URL: {url}")
        print(f"HTTP: {http_status} Content-Type: {content_type}")
        print(f"Body length: {len(body) if body else 0} Error: {error}")

        cur.close()
        conn.close()
        return 0

    except (FileNotFoundError, ValueError, RuntimeError, MySQLError) as e:
        try:
            if "conn" in locals() and conn.is_connected():
                conn.rollback()
        except Exception:
            pass
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())