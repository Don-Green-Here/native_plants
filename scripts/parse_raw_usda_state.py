from __future__ import annotations
#Makes python treat type annotations more flexibly
#Future compatibility feature that avoids type-edge cases
import os
import sys
import csv
#Adds the ability to read csv
import io
from pathlib import Path
#Safer way to work with file system paths
import mysql.connector
#Imports the driver installed in command line as mysql-connector-pyton
from mysql.connector import Error as MySQLError
#We alias exceptions as MySQLError to catch SQL specific errors
from dotenv import load_dotenv
#Imports the function we added on command line that can actually read .env files
#Loads .env key/value pairs into the environment
#Helps us to obscure sensitive information from our .env file

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
    # port 3306 is the default for MySQL

    missing = [k for k, v in {
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
def fetch_latest_body(cur, state_code: str) -> tuple[int, str]:
    cur.execute(
        """
        SELECT id, body 
        FROM state_fetches WHERE state_code = %s AND http_status = 200 AND body is NOT NULL
        ORDER BY fetched_at DESC 
        LIMIT 1
        """,
        (state_code,),
    )
    row = cur.fetchone()
    #reads one row from the result, which will be a dict because we set it to TRUE when setting up cur
    if not row:
        raise RuntimeError(f"No successful fetch found for {state_code}")

    return int(row["id"]), row["body"]

def parse_csv_text(body: str) -> list[dict]:
    f = io.StringIO(body)
    #treat the string as a file object so that csv module can read it
    reader = csv.DictReader(f)

    required_cols = [
        "Symbol",
        "Synonym Symbol",
        "Scientific Name with Author",
        "State Common Name",
        "Family",
    ]
    for col in required_cols:
        if col not in reader.fieldnames:
            raise RuntimeError(f"Missing required column: '{col}'. Found: {reader.fieldnames}")
    rows =[]
    for r in reader:
        rows.append(
            {
            "symbol": (r.get("Symbol") or "").strip(),
            "synonym_symbol": (r.get("Synonym Symbol") or "").strip() or None,
            "scientific_name_with_author": (r.get("Scientific Name with Author") or "").strip(),
            "state_common_name": (r.get("State Common Name") or "").strip() or None,
            "family": (r.get("Family") or "").strip() or None,
            }
        )
        """
        Builds a normalized dict with your preferred snake_case keys.
        (r.get(...) or "").strip() ensures if key missing or value is None, 
        use empty string .strip() removes whitespace or None converts empty 
        strings to actual None so MySQL stores them as NULL.
        """
    return rows

def insert_rows(cur, state_code: str, fetch_id: int, rows: list[dict], batch_size: int = 1000) -> int:
    sql = """
    INSERT INTO raw_usda_state_plants
    (state_code, fetch_id, symbol, synonym_symbol, 
    scientific_name_with_author, state_common_name, family)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
     """
    total_inserted = 0
    batch = []
    for r in rows:
        #Skip empty symbol rows
        if not r["symbol"]:
            continue
        batch.append(
            (
            state_code,
            fetch_id,
            r["symbol"],
            r["synonym_symbol"],
            r["scientific_name_with_author"],
            r["state_common_name"],
            r["family"],
            )
        )
        if len(batch) >= batch_size:
            cur.executemany(sql, batch)
            #execute many is far faster than individual insertions
            total_inserted += cur.rowcount
            batch.clear()
    if batch:
        cur.executemany(sql, batch)
        total_inserted += cur.rowcount
    return total_inserted

def main() -> int:
    try:
        load_env()
        # load our function we created to get our .env variables
        cfg = get_db_config()
        # load our function to build a connection config dictionary from our .env variables
        conn = mysql.connector.connect(**cfg)
        # open the connection to MySQL
        # **cfg means expand the dictionary into keyword arguments
        # Same as: mysql.connector.connect(host=..., user=..., password=..., database=..., port=...)

        cur = conn.cursor(dictionary=True)
        # cursor is the object used to run SQL queries
        # dictionary = True means returned rows come back as dictionaries
        # Dicts like {"state_code": "VA", "state_name": "Virginia", "state_slug": "virginia"}
        # We are doing dicts instead of tuples since it is easier to read and debug than ("VA", "Virginia", "virginia")
        state_code = "VA"
        #starting with Virginia, later it will be a CLI argument
        fetch_id, body = fetch_latest_body(cur, state_code)
        parsed = parse_csv_text(body)

        inserted = insert_rows(cur, state_code, fetch_id, parsed)
        conn.commit()

        print(f"Parsed rows: {len(parsed)}")
        print(f"Inserted rows, will be 0 if duplicates prevented: {inserted}")
        print(f"State: {state_code} Fetch ID: {fetch_id}")


        cur.close()
        conn.close()
        # always close your cursor and connection to prevent leaks
        return 0

    except (ValueError, MySQLError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
    #Exits the program with exit code returned by main()
    #Standard for CLI scripts