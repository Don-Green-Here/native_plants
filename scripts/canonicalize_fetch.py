from __future__ import annotations
import os
import sys
import argparse
from pathlib import Path
from xml.etree.ElementTree import canonicalize

import mysql.connector
#Imports the driver installed in command line as mysql-connector-pyton
from mysql.connector import Error as MySQLError
#We alias exceptions as MySQLError to catch SQL specific errors
from dotenv import load_dotenv

def load_env() -> None:
    """
    Load .env file from the project root directory.
    Using an explicit path will avoid auto-discovery issues.
    """
    env_path = Path.cwd() / ".env"
    #cwd = current working directory
    # / ".env" will append .env to that path
    if not env_path.exists():
        raise FileNotFoundError(
            f"Expected .env at {env_path}."
        )
    load_dotenv(dotenv_path=env_path)
    #loads the .env file explicitly from the env_path, avoids auto discovery

def get_db_config() -> dict:
    """
    Build MySQL connection configuration from the .env variables
    """
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    database = os.getenv("MYSQL_DATABASE", "native_plants")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    #port 3306 is the default for MySQL

    missing = [k for k, v in{
        "MYSQL_USER": user,
        "MYSQL_PASSWORD": password,
    }.items() if not v]
    #.items returns an iterable of key-value pairs
    if missing:
        raise ValueError(f"Missing required environment variables: in .env: {', '.join(missing)}")

    return {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port,
    }
def require_fetch(cur, fetch_id: int) -> dict:
    cur.execute(
        """
        SELECT id, state_code, http_status, content_type, fetched_at
        FROM state_fetches
        WHERE id = %s
        """,
        (fetch_id,),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"fetch_id = {fetch_id} not found in state fetch table")
    if row["http_status"] != 200:
        raise RuntimeError(f"fetch_id = {fetch_id} has http_status = {row['http_status']} expected 200.")
    return row

def canonicalize_plants(cur, fetch_id: int) -> dict:
    cur.execute(
        """
        INSERT INTO canonical_plants (symbol, scientific_name_with_author, family, preferred_common_name) 
        SELECT
            r.symbol,
            MAX(r.scientific_name_with_author) AS scientific_name_with_author,
            MAX(r.family) AS family,
            MAX(NULLIF(r.state_common_name, '')) AS preferred_common_name
        FROM raw_usda_state_plants r
        WHERE r.fetch_id = %s
        GROUP BY r.symbol
        ON DUPLICATE KEY UPDATE
            scientific_name_with_author = VALUES(scientific_name_with_author), 
            family = VALUES(family), 
            preferred_common_name = COALESCE(VALUES(preferred_common_name),
            canonical_plants.preferred_common_name
            );
        """,
        (fetch_id,),
    )
    return cur.rowcount

def build_state_presence(cur, fetch_id: int) -> int:
    """
    Inserts state presence rows (fetch_id, state_code, symbol).
    Indepotent via UNIQUE + INSERT IGNORE
    """
    cur.execute(
        """
        INSERT IGNORE INTO plant_state_presence(fetch_id, state_code, symbol)
        SELECT DISTINCT
            r.fetch_id, r.state_code, r.symbol 
            FROM raw_usda_state_plants r
            WHERE r.fetch_id = %s
            """,
        (fetch_id,),
    )
    return cur.rowcount

def populate_common_names(cur, fetch_id: int) -> int:
    sql = """
    INSERT IGNORE INTO plant_common_names
      (symbol, common_name, state_code, source_system, is_preferred)
    SELECT DISTINCT
      r.symbol,
      r.state_common_name AS common_name,
      r.state_code,
      'USDA_STATE_FILE' AS source_system,
      0 AS is_preferred
    FROM raw_usda_state_plants r
    WHERE r.fetch_id = %s
      AND r.state_common_name IS NOT NULL
      AND TRIM(r.state_common_name) <> ''
    """
    cur.execute(sql, (fetch_id,))
    return cur.rowcount

def count_symbols_in_fetch(cur, fetch_id: int) -> int:
    cur.execute(
        """
        SELECT COUNT(DISTINCT symbol) AS n
        FROM raw_usda_state_plants 
        WHERE fetch_id = %s
        """,
        (fetch_id,),
    )
    return int(cur.fetchone()["n"])

def count_presence_rows(cur, fetch_id: int) -> int:
    cur.execute(
        """
        SELECT COUNT(*) AS n
        FROM plant_state_presence
        WHERE fetch_id = %s
        """,
        (fetch_id,),
    )
    return int(cur.fetchone()["n"])

def main() -> int:
    parser = argparse.ArgumentParser(description="Canonicalize USDA fetch_id for reproducibility.")
    parser.add_argument("--fetch-id", type=int, required=True, help="state_fetches.id to canonicalize")
    parser.add_argument(
        "--with-common-names",
        action="store_true",
        help="Also populate plant_common_names (requires metadata table).",)
    args = parser.parse_args()

    try:
        load_env()
        cfg = get_db_config()

        conn = mysql.connector.connect(**cfg)
        #dict = TRUE makes fetches and counts easier
        cur = conn.cursor(dictionary=True)

        fetch = require_fetch(cur, args.fetch_id)

        # Pre-counts for deterministic reporting
        raw_distinct_symbols = count_symbols_in_fetch(cur, args.fetch_id)

        #conn.autocommit = False
        #will stop any current transactions
        #conn.start_transaction()

        affected_canonical = canonicalize_plants(cur, args.fetch_id)
        inserted_presence = build_state_presence(cur, args.fetch_id)

        inserted_common_names = None
        if args.with_common_names:
            inserted_common_names = populate_common_names(cur, args.fetch_id)

        conn.commit()

        #Post count sanity check
        presence_rows = count_presence_rows(cur, args.fetch_id)

        print("Canonicalization complete")
        print(f"Fetch ID: {args.fetch_id}")
        print(f"State: {fetch['state_code']}  Fetched at: {fetch['fetched_at']}  Content-Type: {fetch['content_type']}")
        print(f"Raw distinct symbols: {raw_distinct_symbols}")
        print(f"Table canonical_plants affected rows: {affected_canonical}")
        print(f"Table plant_state_presence inserted rows: {inserted_presence}")
        print(f"Table plant_state_presence total rows for fetch: {presence_rows}")

        if inserted_common_names is not None:
            print(f"Table plant_common_names inserted rows for fetch: {inserted_common_names}")
        cur.close()
        conn.close()
        return 0

    except (FileNotFoundError, ValueError, RuntimeError, MySQLError) as e:
        try:
            #If we have a connection open, attempt to rollback, rollback prevents partial writes
            if "conn" in locals() and conn.is_connected():
                conn.rollback()
        except Exception:
            pass
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
if __name__ == "__main__":
    raise SystemExit(main())
#Close our CLF
