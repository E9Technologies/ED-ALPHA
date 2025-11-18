import argparse
import logging
import re
from typing import Iterable, List, Tuple

import psycopg2
import requests
from psycopg2.extras import execute_values

from config import build_user_agent, load_configuration


MASTERFILE_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
GKG_SUFFIX = ".gkg.csv.zip"
TIME_PATTERN = re.compile(r"/(\d{14})\.gkg\.csv\.zip$")
DEFAULT_BATCH_SIZE = 2000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch GDELT masterfile list and upsert time strings for .gkg.csv.zip entries."
        )
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of rows per database batch insert (default: {DEFAULT_BATCH_SIZE}).",
    )
    return parser.parse_args()


def fetch_masterfile(user_agent: str) -> Iterable[str]:
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/plain",
    }
    response = requests.get(MASTERFILE_URL, headers=headers, timeout=60)
    response.raise_for_status()
    text = response.text
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            yield stripped


def parse_masterfile_lines(lines: Iterable[str]) -> List[Tuple[str, str, int, str]]:
    dedup: dict[str, Tuple[str, str, int, str]] = {}
    for line in lines:
        parts = line.split()
        if len(parts) != 3:
            logging.debug("Skipping malformed line: %s", line)
            continue
        size_str, md5_hash, url = parts
        if not url.endswith(GKG_SUFFIX):
            continue
        match = TIME_PATTERN.search(url)
        if not match:
            logging.debug("Skipping URL without time_str: %s", url)
            continue
        time_str = match.group(1)
        if len(time_str) != 14:
            logging.debug("Unexpected time_str length for %s", url)
            continue
        try:
            size_value = int(size_str)
        except ValueError:
            size_value = None
        dedup[time_str] = (time_str, url, size_value, md5_hash)
    return list(dedup.values())


def upsert_master_times(
    connection: psycopg2.extensions.connection,
    rows: List[Tuple[str, str, int, str]],
) -> None:
    if not rows:
        return

    insert_query = """
        INSERT INTO gdelt_master_times (time_str, source_url, file_size_bytes, md5_hash)
        VALUES %s
        ON CONFLICT (time_str)
        DO UPDATE SET
            source_url = EXCLUDED.source_url,
            file_size_bytes = EXCLUDED.file_size_bytes,
            md5_hash = EXCLUDED.md5_hash,
            last_seen_at = NOW()
    """

    payload = [
        (
            time_str,
            source_url,
            file_size if file_size is not None else None,
            md5_hash,
        )
        for time_str, source_url, file_size, md5_hash in rows
    ]

    with connection.cursor() as cursor:
        execute_values(cursor, insert_query, payload, page_size=len(rows))
    connection.commit()


def chunk_rows(rows: List[Tuple[str, str, int, str]], batch_size: int) -> Iterable[List[Tuple[str, str, int, str]]]:
    for index in range(0, len(rows), batch_size):
        yield rows[index : index + batch_size]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    if args.batch_size <= 0:
        raise ValueError("batch-size must be a positive integer.")

    config = load_configuration()
    user_agent = build_user_agent(config["user_email"])

    logging.info("Fetching %s ...", MASTERFILE_URL)
    lines = list(fetch_masterfile(user_agent))
    logging.info("Retrieved %d lines from masterfile.", len(lines))

    entries = parse_masterfile_lines(lines)
    logging.info("Found %d .gkg.csv.zip entries.", len(entries))

    connection = psycopg2.connect(**config["database_config"])

    try:
        inserted_total = 0
        for batch in chunk_rows(entries, args.batch_size):
            upsert_master_times(connection, batch)
            inserted_total += len(batch)
        logging.info("Upserted %d entries into gdelt_master_times.", inserted_total)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
