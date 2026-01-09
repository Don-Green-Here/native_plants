from __future__ import annotations

import os
import sys
import csv
import io
import argparse
from pathlib import Path
from datetime import datetime, timezone

import mysql.connector
from mysql.connector import Error as MySQLError
from dotenv import load_dotenv


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


def get_latest_fetch_for_state(cur, state_code: str) -> dict:
    cur.execute(
        """
        SELECT id, state_code, url, fetched_at, http_status, content_type, body
        FROM state_fetches
        WHERE state_code = %s
          AND http_status = 200
          AND body IS NOT NULL
        ORDER BY fetched_at DESC
        LIMIT 1
        """,
        (state_code,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No successful fetch found for state_code={state_code}")
    return row


def get_fetch_by_id(cur, fetch_id: int) -> dict:
    cur.execute(
        """
        SELECT id, state_code, url, fetched_at, http_status, content_type, body
        FROM state_fetches
        WHERE id = %s
        """,
        (fetch_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Fetch not found for id={fetch_id}")
    if row["http_status"] != 200 or row["body"] is None:
        raise RuntimeError(f"Fetch id={fetch_id} is not usable (status={row['http_status']}, body is null?).")
    return row


def parse_csv_text(body: str) -> list[dict]:

    f = io.StringIO(body)
    reader = csv.DictReader(f)

    required_cols = ["Symbol", "Synonym Symbol", "Scientific Name with Author", "State Common Name", "Family"]
    for col in required_cols:
        if col not in (reader.fieldnames or []):
            raise RuntimeError(f"Missing expected column '{col}'. Found: {reader.fieldnames}")

    out = []
    for r in reader:
        symbol = (r.get("Symbol") or "").strip()
        if not symbol:
            continue

        out.append(
            {
                "symbol": symbol,
                "synonym_symbol": (r.get("Synonym Symbol") or "").strip() or None,
                "scientific_name_with_author": (r.get("Scientific Name with Author") or "").strip(),
                "state_common_name": (r.get("State Common Name") or "").strip() or None,
                "family": (r.get("Family") or "").strip() or None,
            }
        )
    return out


def insert_raw_rows(cur, fetch_id: int, state_code: str, rows: list[dict], batch_size: int = 1000) -> int:
    sql = """
    INSERT INTO raw_usda_state_plants
      (fetch_id, state_code, symbol, synonym_symbol, scientific_name_with_author, state_common_name, family, created_at)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    created_at = datetime.now(timezone.utc).replace(tzinfo=None)

    total = 0
    batch = []
    for r in rows:
        batch.append(
            (
                fetch_id,
                state_code,
                r["symbol"],
                r["synonym_symbol"],
                r["scientific_name_with_author"],
                r["state_common_name"],
                r["family"],
                created_at,
            )
        )
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            total += cur.rowcount
            batch.clear()

    if batch:
        cur.executemany(sql, batch)
        total += cur.rowcount

    return total


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse USDA state fetch body into raw_usda_state_plants.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--fetch-id", type=int, help="Parse a specific fetch_id (reproducible).")
    group.add_argument("--state-code", help="Parse latest successful fetch for a state code (convenience).")
    parser.add_argument("--latest", action="store_true", help="Required when using --state-code (guardrail).")
    args = parser.parse_args()

    try:
        load_env()
        cfg = get_db_config()

        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor(dictionary=True)

        if args.fetch_id is not None:
            fetch = get_fetch_by_id(cur, args.fetch_id)
        else:
            state_code = args.state_code.strip().upper()
            if not args.latest:
                raise ValueError("When using --state-code you must also pass --latest (explicit guardrail).")
            fetch = get_latest_fetch_for_state(cur, state_code)

        fetch_id = int(fetch["id"])
        state_code = fetch["state_code"]
        body = fetch["body"]

        rows = parse_csv_text(body)
        inserted = insert_raw_rows(cur, fetch_id, state_code, rows)
        conn.commit()

        print("Parse complete.")
        print(f"Fetch ID: {fetch_id} State: {state_code}")
        print(f"Parsed rows: {len(rows)} Inserted rows: {inserted}")

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