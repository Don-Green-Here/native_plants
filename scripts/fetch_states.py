from __future__ import annotations
#Makes python treat type annotations more flexibly
#Future compatibility feature that avoids type-edge cases
import os
import sys
from pathlib import Path
#Safer way to work with file system paths
from datetime import datetime, timezone
import requests
#Requests will handle http status codes
import mysql.connector
#Imports the driver installed in command line as mysql-connector-pyton
from mysql.connector import Error as MySQLError
#We alias exceptions as MySQLError to catch SQL specific errors
from dotenv import load_dotenv
#Imports the function we added on command line that can actually read .env files
#Loads .env key/value pairs into the environment
#Helps us to obscure sensitive information from our .env file

USDA_STATE_URL_TEMPLATE = "https://plants.sc.egov.usda.gov/DocumentLibrary/Txt/{StateName}_NRCS_csv.txt"

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

    """
    How to read the 'list comprehension' above:
    1. Loop over env_vars.items()
    2. Unpack each item into key and value
    3. If value is missing... Put key into the result list 
    
    By ending with a comma (,) we are creating a tuple. 
    This is the same code as the 'tuple unpacking' above, but easier to understand for beginners: 
    missing = []
    env_vars = {
     "MYSQL_USER": user,
     "MYSQL_PASSWORD": password,
    }

    for key, value in env_vars.items():
        if not value:
            missing.append(key)
    """

    if missing:
        raise ValueError(f"Missing required environment variables: in .env: {', '.join(missing)}")

    return {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port,
    }

def build_usda_state_url(state_name: str) -> str:
    state_token = state_name.replace(" ", "")
    #Simplist case is a single word state name, i.e. Virginia. Remove all spaces.
    return USDA_STATE_URL_TEMPLATE.format(StateName=state_token)
    #StateName is used in world variable USDA_STATE_URL_TEMPLATE
    #format() method formats the specified value and inserts them into the string placeholder inside the {}

def fetch_text(url: str, timeout: int = 30) -> tuple[int | None, str | None, str | None, str | None]:
    #This will return http_status, content_type, body_text, error_message
    try:
        response = requests.get(url, timeout = 30, headers = {"User-Agent": "native-plants-pipeline/1.0"})
        http_status = response.status_code
        content_type = response.headers.get("Content-Type")
        body_text = response.text
        return http_status, content_type, body_text, None
    except requests.RequestException as e:
        return None, None, None, str(e)

def insert_fetch(
        cur,
        state_code: str,
        url: str,
        fetched_at_utc: datetime,
        http_status: int | None,
        content_type: str | None,
        body: str | None,
        error: str | None,
) -> None:
    sql = """
    INSERT INTO state_fetches
    (state_code, url, fetched_at, http_status, content_type, body, error)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    fetched_at_naive_utc = fetched_at_utc.replace(tzinfo=None)
    #Naive UTC is when we do not know the datetime exactly
    cur.execute(sql, (state_code, url, fetched_at_naive_utc, http_status, content_type, body, error))

def main() -> int:
    try:
        load_env()
        #load our function we created to get our .env variables
        cfg = get_db_config()
        #load our function to build a connection config dictionary from our .env variables
        conn = mysql.connector.connect(**cfg)
        #open the connection to MySQL
        #**cfg means expand the dictionary into keyword arguments
        #Same as: mysql.connector.connect(host=..., user=..., password=..., database=..., port=...)

        cur = conn.cursor(dictionary=True)
        #cursor is the object used to run SQL queries
        #dictionary = True means returned rows come back as dictionaries
        #Dicts like {"state_code": "VA", "state_name": "Virginia", "state_slug": "virginia"}
        #We are doing dicts instead of tuples since it is easier to read and debug than ("VA", "Virginia", "virginia")
        """
        cur.execute("SELECT * FROM states;")
        rows = cur.fetchall()
        
        #Retrieves all rows returned by the query into a Pyton list called rows

        print(f"Connected to MySQL database. DB = {cfg['database']}.states rows={len(rows)}")
        for row in rows:
            print(row)
        """
        cur.execute("SELECT state_code, state_name FROM states WHERE state_code = 'VA' AND is_active = 1;")
        row = cur.fetchone()
        #first attempt to select a state
        #Query sent to MySQL
        if not row:
            print("No active VA row found in states.")
            return 3
        state_code = row["state_code"]
        state_name = row["state_name"]

        url = build_usda_state_url(state_name)
        #calls on our build_usda_state_url function to return a full URL
        fetched_at = datetime.now(timezone.utc)

        http_status, content_type, body, error = fetch_text(url)

        #Insert the ingestion record
        insert_fetch(
            cur=cur,
            state_code=state_code,
            url=url,
            fetched_at_utc=fetched_at,
            http_status=http_status,
            content_type=content_type,
            body=body,
            error=error,
        )
        conn.commit()

        print(f"Inserted fetch record for {state_code}")
        print(f"URL: {url}")
        print(f"HTTP: {http_status} Content-Type: {content_type}")
        print(f"Body length: {len(body) if body else 0} Error: {error}")

        cur.close()
        conn.close()
        #always close your cursor and connection to prevent leaks
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

