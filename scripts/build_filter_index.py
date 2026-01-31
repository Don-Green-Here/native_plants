from __future__ import annotations

import os
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

def normalize_enum(value: str | None, allowed: set[str]) -> str:
    if not value:
        return "Unknown"
    v = " ".join(value.split()).strip()
    return v if v in allowed else "Unknown"

def normalize_moisture_use(value: str | None) -> str:
    allowed = {"Low", "Medium", "High"}
    v = normalize_enum(value, allowed)
    return v

def normalize_bloom_period(value: str | None) -> str:
    allowed = {"Early Spring", "Spring", "Mid Spring", "Late Spring",
              "Early Summer", "Summer", "Mid Summer", "Late Summer",
              "Early Fall", "Fall", "Mid Fall", "Late Fall",
              "Early Winter", "Winter", "Mid Winter", "Late Winter",}
    v = normalize_enum(value, allowed)
    return v

def normalize_shade_tolerance(value: str | None) -> str:
    allowed = {"Tolerant", "Intermediate", "Intolerant"}
    v = normalize_enum(value, allowed)
    return v

def normalize_duration_primary(duration_raw: str | None) -> str:
    if not duration_raw:
        return "Unknown"
    txt = duration_raw.lower()
    if "perennial" in txt:
        return "Perennial"
    if "biennial" in txt:
        return "Biennial"
    if "annual" in txt:
        return "Annual"
    return "Unknown"

def split_duration_set(duration_raw: str | None) -> set[str]:
    if not duration_raw:
        return set()
    txt = duration_raw.lower()
    found = set()
    if "perennial" in txt:
        found.add("Perennial")
    if "biennial" in txt:
        found.add("Biennial")
    if "annual" in txt:
        found.add("Annual")
    return found

def is_shade_tolerant(shade_enum: str) -> int:
    return 1 if shade_enum in ("Tolerant", "Intermediate") else 0

def fetch_latest_kv_map(cur, symbol: str) -> dict[str, str]:
    """t is the alias for the subquery"""
    cur.execute(
        """
        SELECT trait_name, trait_value
        FROM (SELECT trait_name, trait_value, ROW_NUMBER() 
        OVER (PARTITION BY trait_name
        ORDER BY (NULLIF(TRIM(trait_value),'')iS NULL)ASC, fetched_at DESC) 
        AS rn 
        FROM plant_characteristics_kv
        WHERE symbol =%s
        AND trait_name IN ('Group', 'Duration', 'Growth Habits', 'Native Status',
        'Fall Conspicuous', 'Leaf Retention', 'Flower Conspicuous',
        'Shade Tolerance', 'Moisture Use', 'Bloom Period', 'Family')
        ) t
        WHERE rn = 1
        """,
        (symbol,),
    )
    rows = cur.fetchall() or []
    return {r["trait_name"]: r["trait_value"] for r in rows}

def has_any_profile_kv(cur, symbol: str) -> int:
    cur.execute(
        """
        SELECT 1
        FROM plant_characteristics_kv
        WHERE symbol=%s AND section IN ('Profile / General Information', 'Characteristics')
        LIMIT 1
        """,
        (symbol,),
    )
    return 1 if cur.fetchone() else 0

def has_any_characteristics_kv(cur, symbol: str) -> int:
    cur.execute(
        """
        SELECT 1
        FROM plant_characteristics_kv
        WHERE symbol=%s AND section IN 
        ('Morphology/Physiology', 'Growth Requirements', 
        'Reproduction','Suitability/Use') 
        LIMIT 1
        """,
        (symbol,),
    )
    return 1 if cur.fetchone() else 0

def normalize_unknown(value: str | None) -> str:
    if not value:
        return "Unknown"
    v = " ".join(value.split()).strip().lower()
    # Strong yes signals
    if v.startswith("yes") or v in ("y", "true", "t", "1"):
        return "Yes"
    # Strong no signals
    if v.startswith("no") or v in ("n", "false", "f", "0"):
        return "No"
    print("WARN: Unknown yes/no value:", repr(value), flush=True)
    return "Unknown"

def normalize_seasonal_interest(family: str | None) -> str:
    if not family:
        return "Unknown"
    family.removeprefix("x") # Some hybrid plants start with an "x"
    v = "".join(family.split()[0]).strip().lower()
    # What family does this belong to?
    if v in ("arecaceae","taxaceae", "araucariaceae", "podocarpaceae"):
        return "Yes" #These are families with only evergreen members
    return "Unknown"

#              "pinaceae", "cupressaceae", "aquifoliaceae", "ericaceae",
#             "theaceae", "oleaceae", "rubiaceae", "fagaceae", "asteraceae",
#             "lamiaceae", "saxifragaceae", "cyperaceae", "arecaceae",
#             "taxaceae", "araucariaceae", "podocarpaceae", "magnoliaceae")
# Theses families contain evergreen members, but may not exclusively be evergreen, saving names for later


def showy_bloomer(v: str) -> int:
    if v == "Yes":
        return 1
    if v == "No":
        return 0
    if v == "Unknown":
        return 2


def fall_interest(v: str) -> int:
    if v == "Yes":
        return 1
    if v == "No":
        return 0
    if v == "Unknown":
        return 2

def evergreen(v: str) -> int:
    if v == "Yes":
        return 1
    if v == "No":
        return 0
    if v == "Unknown":
        return 2

def upsert_filter_index(
    cur,
    symbol: str,
    canonical: dict,
    plant_group: str | None,
    growth_habits_raw: str | None,
    native_status_raw: str | None,
    duration_raw: str | None,
    duration_primary: str,
    shade_tolerance: str,
    moisture_use: str,
    bloom_period: str,
    shade_tolerant: int,
    has_profile_kv: int,
    has_characteristics_kv: int,
    flower_conspicuous: str,
    fall_conspicuous: str,
    is_showy_bloomer: int,
    has_fall_interest: int,
    is_evergreen: int,
) -> None:
    last_indexed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    cur.execute(
        """
        INSERT INTO plant_filter_index (
          symbol,
          preferred_common_name, scientific_name_with_author, family,
          plant_group, growth_habits_raw, native_status_raw, duration_raw,
          duration_primary, shade_tolerance, moisture_use, bloom_period,
          is_shade_tolerant, has_profile_kv, has_characteristics_kv,
          flower_conspicuous, fall_conspicuous, is_showy_bloomer, has_fall_interest, is_evergreen,
          last_indexed_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)
        ON DUPLICATE KEY UPDATE
          preferred_common_name=VALUES(preferred_common_name),
          scientific_name_with_author=VALUES(scientific_name_with_author),
          family=VALUES(family),
          plant_group=VALUES(plant_group),
          growth_habits_raw=VALUES(growth_habits_raw),
          native_status_raw=VALUES(native_status_raw),
          duration_raw=VALUES(duration_raw),
          duration_primary=VALUES(duration_primary),
          shade_tolerance=VALUES(shade_tolerance),
          moisture_use=VALUES(moisture_use),
          bloom_period=VALUES(bloom_period),
          is_shade_tolerant=VALUES(is_shade_tolerant),
          has_profile_kv=VALUES(has_profile_kv),
          has_characteristics_kv=VALUES(has_characteristics_kv),
          flower_conspicuous=VALUES(flower_conspicuous),
          fall_conspicuous=VALUES(fall_conspicuous),
          is_showy_bloomer=VALUES(is_showy_bloomer),
          has_fall_interest=VALUES(has_fall_interest),
          is_evergreen=VALUES(is_evergreen),
          last_indexed_at=VALUES(last_indexed_at)
        """,
        (
            symbol,
            canonical.get("preferred_common_name"),
            canonical.get("scientific_name_with_author"),
            canonical.get("family"),
            plant_group,
            growth_habits_raw,
            native_status_raw,
            duration_raw,
            duration_primary,
            shade_tolerance,
            moisture_use,
            bloom_period,
            shade_tolerant,
            has_profile_kv,
            has_characteristics_kv,
            flower_conspicuous,
            fall_conspicuous,
            is_showy_bloomer,
            has_fall_interest,
            is_evergreen,
            last_indexed_at,
        ),
    )

def refresh_child_tables(cur, symbol: str, duration_secondary: set[str], growth_habits_raw: str | None) -> tuple[int, int]:
    cur.execute(
        """
        DELETE FROM plant_durations WHERE symbol =%s
        """,
        (symbol,)
    )
    duration_ins = 0
    if duration_secondary:
        cur.executemany(
            "INSERT INTO plant_durations (symbol, duration) VALUES (%s,%s)",
            [(symbol, d) for d in sorted(duration_secondary)],
        )
        duration_ins = cur.rowcount
    cur.execute(
        "DELETE FROM plant_growth_habits WHERE symbol = %s",
        (symbol,)
    )
    habit_ins = 0
    if growth_habits_raw:
        parts = [p.strip() for p in growth_habits_raw.split(",") if p.strip()]
        expanded: list[str] = []
        for p in parts:
            expanded.extend([q.strip() for q in p.split(";") if q.strip()])
        habits = sorted(set(expanded))
        if habits:
            cur.executemany(
                "INSERT INTO plant_growth_habits (symbol, growth_habit)  VALUES (%s,%s)",
                [(symbol, h) for h in habits]
            )
            habit_ins = cur.rowcount
    return duration_ins, habit_ins

def get_canonical_fields(cur, symbol: str) -> dict:
    cur.execute(
        """
        SELECT symbol, scientific_name_with_author, family, preferred_common_name
        FROM canonical_plants
        WHERE symbol = %s
        """,
        (symbol,),
    )
    row = cur.fetchone()
    return row or {}

def main() -> int:
    ap = argparse.ArgumentParser(description="Build plant_filter_index from plant_characteristics_kv")
    ap.add_argument("--state-code", required=True)
    ap.add_argument("--fetch-id", type=int, required=True)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--commit-every", type=int, default=200)
    ap.add_argument("--rebuild-children", action="store_true", help="Rebuild plant_durations and plant_growth_habits.")
    args = ap.parse_args()

    state_code = args.state_code.strip().upper()
    fetch_id = args.fetch_id

    try:
        load_env()
        cfg = get_db_config()
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor(dictionary=True)

        symbols = get_symbols_for_state(cur, state_code, args.fetch_id)
        if not symbols:
            raise RuntimeError(f"No symbols found for {state_code} (fetch_id={fetch_id}).")

        if args.limit and args.limit > 0:
            symbols = symbols[:args.limit]

        upserts = 0
        duration_rows = 0
        habit_rows = 0

        for i, symbol in enumerate(symbols, start=1):
            canonical = get_canonical_fields(cur, symbol)
            kv = fetch_latest_kv_map(cur, symbol)

            plant_group = kv.get("Group")
            growth_habits_raw = kv.get("Growth Habits")
            native_status_raw = kv.get("Native Status")
            duration_raw = kv.get("Duration")

            duration_primary = normalize_duration_primary(duration_raw)
            duration_secondary = split_duration_set(duration_raw)

            shade_tolerance = normalize_shade_tolerance(kv.get("Shade Tolerance"))
            moisture_use = normalize_moisture_use(kv.get("Moisture Use"))
            bloom_period = normalize_bloom_period(kv.get("Bloom Period"))

            shade_tolerant = is_shade_tolerant(shade_tolerance)
            has_profile_kv = has_any_profile_kv(cur, symbol)
            has_characteristics_kv = has_any_characteristics_kv(cur, symbol)

            # Debugging
            #raw_fc = kv.get("Leaf Retention")
            #if i <= 50:  # first 10 symbols
            #    print(symbol, "Evergreen =", repr(raw_fc), flush=True)

            flower_conspicuous = normalize_unknown(kv.get("Flower Conspicuous"))
            fall_conspicuous = normalize_unknown(kv.get("Fall Conspicuous"))
            leaf_retention = normalize_unknown(kv.get("Leaf Retention"))

            family = normalize_seasonal_interest(kv.get("Family"))
            is_showy_bloomer = showy_bloomer(flower_conspicuous)
            has_fall_interest = fall_interest(fall_conspicuous)
            is_evergreen = evergreen(leaf_retention)

            if i <= 50:  # first 50 symbols
                print(canonical.get("preferred_common_name"), "Evergreen =", repr(is_evergreen), flush=True)
                print(symbol, "Fall Interest =", repr(has_fall_interest), flush=True)
                print(symbol, "Showy Flower Bloom=", repr(is_showy_bloomer), flush=True)

            upsert_filter_index(
                cur=cur,
                symbol=symbol,
                canonical=canonical,
                plant_group=plant_group,
                growth_habits_raw=growth_habits_raw,
                native_status_raw=native_status_raw,
                duration_raw=duration_raw,
                duration_primary=duration_primary,
                shade_tolerance=shade_tolerance,
                moisture_use=moisture_use,
                bloom_period=bloom_period,
                shade_tolerant=shade_tolerant,
                has_profile_kv=has_profile_kv,
                has_characteristics_kv=has_characteristics_kv,
                flower_conspicuous=flower_conspicuous,
                fall_conspicuous=fall_conspicuous,
                is_showy_bloomer=is_showy_bloomer,
                has_fall_interest=has_fall_interest,
                is_evergreen=is_evergreen,
            )
            upserts +=1

            if args.rebuild_children:
                duration_ins, habit_ins = refresh_child_tables(cur, symbol, duration_secondary, growth_habits_raw)
                duration_rows += duration_ins
                habit_rows += habit_ins

            if i % args.commit_every == 0:
                conn.commit()

        conn.commit()
        cur.close()
        conn.close()
        print(f"Filter indexing complete.")
        print(f"State: {state_code}  Fetch ID: {args.fetch_id}")
        print(f"Symbols processed: {len(symbols)}  Upserts: {upserts}")
        if args.rebuild_children:
            print(f"Child tables rebuilt: plant_durations rows={duration_rows}, plant_growth_habits rows={habit_rows}")
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



