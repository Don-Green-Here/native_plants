from __future__ import annotations

import os
import re
import sys
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

def upsert_trait(
    cur,
    symbol: str,
    trait_key: str,
    trait_value: str,
    value_type: str,
    source_system: str,
    trait_name_raw: str | None,
    trait_value_raw: str | None,
    now,
) -> None:
    cur.execute(
        """
        INSERT INTO plant_traits_normalized
          (symbol, trait_key, trait_value, value_type, source_system,
           trait_name_raw, trait_value_raw, last_computed_at)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          trait_value=VALUES(trait_value),
          value_type=VALUES(value_type),
          source_system=VALUES(source_system),
          trait_name_raw=VALUES(trait_name_raw),
          trait_value_raw=VALUES(trait_value_raw),
          last_computed_at=VALUES(last_computed_at)
        """,
        (symbol, trait_key, trait_value, value_type,
         source_system, trait_name_raw, trait_value_raw, now),
    )

def normalize_yes_no(value: str | None)-> str | None:
    if not value:
        return "Unknown"
    v = " ".join(value.split()).strip()
    if v.startswith("yes"):
        return "Yes"
    if v.startswith("no"):
        return "No"
    return None

def normalize_enum(value: str | None) -> str:
    if not value:
        return "Unknown"
    v = " ".join(value.split()).strip().title() #Capitalize each word
    allowed = {"Early Spring", "Spring", "Mid Spring", "Late Spring",
              "Early Summer", "Summer", "Mid Summer", "Late Summer",
              "Early Fall", "Fall", "Mid Fall", "Late Fall",
              "Early Winter", "Winter", "Mid Winter", "Late Winter",
               "None", "Slight", "Moderate", "High", #Toxicity
               "Slow", "Moderate", "Rapid", #Spread Rate/Growth
               "Low", "Medium",#Appears in various enums
               "Short", "Long",
               "Blue","Brown", "Green", #Flower colors
               "Orange", "Purple", "Red",
               "White", "Yellow",}
    return v if v in allowed else "Unknown"

def normalize_number(value: str | None) -> str | None:
    if not value:
        return None
    v = " ".join(value.split()).strip()
    string_num = re.search(r"(-?d+)",v)
    if not string_num:
        return None
    return string_num.group(1) #store as a string number, e.g. -10

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
    return [r["symbol"] for r in (cur.fetchall() or [])]

def fetch_latest_kv(cur, symbol: str, trait_names: list[str]) -> dict[str, dict]:
    """
    Returns trait_name -> {value, section, fetched_at}
    Prefer:
      1) section='Direct Trait Lookup'
      2) non-empty
      3) newest fetched_at
    """
    placeholders = ", ".join(["%s"] * len(trait_names))
    cur.execute(
        f"""
        SELECT trait_name, trait_value, section, fetched_at
        FROM (
          SELECT
            trait_name,
            trait_value,
            section,
            fetched_at,
            ROW_NUMBER() OVER (
              PARTITION BY trait_name
              ORDER BY
                (section = 'Direct Trait Lookup') DESC,
                (NULLIF(TRIM(trait_value), '') IS NULL) ASC,
                fetched_at DESC
            ) rn
          FROM plant_characteristics_kv
          WHERE symbol = %s
            AND trait_name IN ({placeholders})
        ) t
        WHERE rn = 1
        """,
        (symbol, *trait_names),
    )
    out: dict[str, dict] = {}
    for r in cur.fetchall() or []:
        out[r["trait_name"]] = {
            "value": r["trait_value"],
            "section": r["section"],
            "fetched_at": r["fetched_at"],
        }
    return out

def main() -> int:
    parser = argparse.ArgumentParser(description="Build normalized traits KV for UI chips and secondary filters.")
    parser.add_argument("--state-code", required=True)
    parser.add_argument("--fetch-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--commit-every", type=int, default=200)
    args = parser.parse_args()

    state_code = args.state_code.strip().upper()
    fetch_id = args.fetch_id
    limit = args.limit
    commit_every = args.commit_every

    # Map USDA raw trait_name -> (trait_key, normalizer, value_type)
    TRAIT_MAP: dict[str, tuple[str, callable, str]] = {
        #Aestetics
        "Flower Color": ("bloom_color", normalize_enum, "enum"),
        "Hedge Tolerance": ("hedge_tolerance", normalize_enum, "enum"),
        # Sizing/hedging
        # Typically only long-lived trees and shrubs will have a height at 20 years.
        "Height, Mature (feet)": ("height_mature", normalize_number, "number"),
        "Height at 20 Years, Maximum (feet)": ("height_20yr", normalize_number, "number"),


        # Speed of spread
        # If seed/vegetative spread is fast should alert user how it spreads
        "Vegetative Spread Rate": ("colonizing", normalize_enum, "enum"),
        "Seed Spread Rate":("reseeding", normalize_enum, "enum"),
        "Propagated by Corm": ("corms", normalize_enum, "bool"),
        "Propagated by Sprigs": ("sprigs", normalize_enum, "bool"),
        "Propagated by Tubers": ("tubers", normalize_enum, "bool"),

        # Edibility / wildlife
        "Palatable Human": ("palatable_human", normalize_yes_no, "bool"),
        "Palatable Browse Animal": ("palatable_browse_animal", normalize_enum, "enum"),
        "Palatable Graze Animal": ("palatable_graze_animal", normalize_enum, "enum"),
        "Fruit/Seed Period Begin": ("fruit_ready", normalize_enum, "enum"),
        "Toxicity": ("toxicity", normalize_enum, "bool"),

        # Soil texture acceptance
        "Adapted to Coarse Textured Soils": ("soil_sand", normalize_yes_no, "bool"),
        "Adapted to Medium Textured Soils": ("soil_loam", normalize_yes_no, "bool"),
        "Adapted to Fine Textured Soils": ("soil_clay", normalize_yes_no, "bool"),
        "Salinity Tolerance": ("salinity", normalize_enum, "enum"),

        # Growing Zone Determination
        "Temperature, Minimum (°F)": ("min_temp", normalize_number, "number"),
        "Frost Free Days, Minimum": ("min_frost", normalize_number, "number"),
        "Cold Stratification Required": ("cold_req", normalize_yes_no, "bool"),
    }

    trait_names = list(TRAIT_MAP.keys())
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    source_system = "USDA_SELENIUM_BS4"

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

        upserts = 0

        for i, symbol in enumerate(symbols, start=1):
            kv = fetch_latest_kv(cur, symbol, trait_names)

            for raw_name, (trait_key, normalizer, value_type) in TRAIT_MAP.items():
                row = kv.get(raw_name)
                if not row:
                    continue

                raw_val = row["value"]
                normalized = normalizer(raw_val)
                if not normalized:
                    continue

                upsert_trait(
                    cur=cur,
                    symbol=symbol,
                    trait_key=trait_key,
                    trait_value=normalized,
                    value_type=value_type,
                    source_system=source_system,
                    trait_name_raw=raw_name,
                    trait_value_raw=raw_val,
                    now=now,
                )
                upserts += 1

            if i % commit_every == 25:
                conn.commit()

        conn.commit()
        cur.close()
        conn.close()

        print(f"Traits build complete. State={state_code} symbols={len(symbols)} upserts={upserts}")
        return 0

    except (FileNotFoundError, ValueError, RuntimeError, MySQLError) as e:
        print(f"ERROR: {e!r}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())