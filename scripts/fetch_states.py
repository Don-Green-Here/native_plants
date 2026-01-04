from __future__ import annotations
#Makes python treat type annotations more flexibly
#Future compatibility feature that avoids type-edge cases
import os
import sys
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

        cur.execute("SELECT * FROM states;")
        #Query sent to MySQL

        rows = cur.fetchall()
        #Retrieves all rows returned by the query into a Pyton list called rows

        print(f"Connected to MySQL database. DB = {cfg['database']}.states rows={len(rows)}")
        for row in rows:
            print(row)

        cur.close()
        conn.close()
        #always close your cursor and connection to prevent leakes
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

