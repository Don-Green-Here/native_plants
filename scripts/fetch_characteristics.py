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

SECTIONS = ["Morphology/Physiology", "Growth Requirements", "Reproduction", "Suitability/Use",]
# Sections that exist on characteristic pages for plants for tables

BASE_URL = "https://plants.usda.gov/plant-profile/{symbol}/characteristics"

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
