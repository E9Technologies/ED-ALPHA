import argparse
import csv
import io
import logging
import zipfile
from typing import Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values
import requests

from config import build_user_agent, load_configuration

# _csv.Error: field larger than field limit (131072) is raised for large GKG fields; increase limit.
OVER_SIZE_LIMIT = 200_000_000
csv.field_size_limit(OVER_SIZE_LIMIT)

GDELT_GKG_URL_TEMPLATE = "http://data.gdeltproject.org/gdeltv2/{time_str}.gkg.csv.zip"
EXPECTED_COLUMN_COUNT = 27


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download GDELT GKG CSVs for all timestamps between --start-time and --end-time "
            "(inclusive) found in gdelt_master_times, and upsert selected fields into PostgreSQL."
        )
    )
    parser.add_argument(
        "--start-time",
        required=True,
        help="Inclusive start of the range (YYYYMMDDHHMM or YYYYMMDDHHMMSS).",
    )
    parser.add_argument(
        "--end-time",
        required=True,
        help="Inclusive end of the range (YYYYMMDDHHMM or YYYYMMDDHHMMSS).",
    )
    return parser.parse_args()


def validate_time_str(value: str) -> str:
    if len(value) != 14 or not value.isdigit():
        raise ValueError("time_str must be a 14-digit numeric string (YYYYMMDDHHMMSS).")
    return value


def normalise_time_input(value: str, label: str) -> str:
    digits = value.strip()
    if len(digits) == 12 and digits.isdigit():
        digits = f"{digits}00"
    if len(digits) != 14 or not digits.isdigit():
        raise ValueError(f"{label} must be a 12- or 14-digit numeric string (YYYYMMDDHHMM[SS]).")
    return digits


def ensure_time_order(start_time: str, end_time: str) -> None:
    if start_time > end_time:
        raise ValueError("start_time must be less than or equal to end_time.")


def build_gdelt_headers(user_agent: str) -> dict:
    return {
        "User-Agent": user_agent,
        "Accept": "application/octet-stream",
    }


def download_gdelt_zip(
    time_str: str,
    headers: dict,
    session: Optional[requests.sessions.Session] = None,
) -> bytes:
    url = GDELT_GKG_URL_TEMPLATE.format(time_str=time_str)
    requester = session or requests
    response = requester.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    return response.content


def extract_csv_rows(zip_bytes: bytes) -> Iterable[Tuple[int, List[str]]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zip_file:
        members = zip_file.namelist()
        if not members:
            raise ValueError("Downloaded ZIP file does not contain any members.")

        target_name = members[0]
        with zip_file.open(target_name) as member:
            text_stream = io.TextIOWrapper(member, encoding="utf-8", errors="replace", newline="")
            reader = csv.reader(text_stream, delimiter="\t")
            for line_num, row in enumerate(reader, start=1):
                yield line_num, row


def filter_valid_rows(rows: Iterable[Tuple[int, List[str]]]) -> Iterable[Tuple[int, List[str]]]:
    for line_num, row in rows:
        if len(row) != EXPECTED_COLUMN_COUNT:
            logging.warning(
                "Skipping line %d due to unexpected column count (expected %d, got %d).",
                line_num,
                EXPECTED_COLUMN_COUNT,
                len(row),
            )
            continue
        yield line_num, row


def prepare_records(
    time_str: str,
    rows: Iterable[Tuple[int, List[str]]],
) -> List[Tuple[str, int, str, str, str, str]]:
    records: List[Tuple[str, int, str, str, str, str]] = []
    for line_num, row in rows:
        records.append(
            (
                time_str,
                line_num,
                row[0],
                row[4],
                row[7],
                row[13],
            )
        )
    return records


def upsert_gdelt_records(
    connection: psycopg2.extensions.connection,
    records: List[Tuple[str, int, str, str, str, str]],
) -> None:
    if not records:
        logging.info("No valid records to upsert.")
        return

    upsert_query = """
        INSERT INTO gdelt_gkg_records (
            time_str,
            line_num,
            gkg_record_id,
            v2_document_identifier,
            v1_themes,
            v1_organizations
        )
        VALUES %s
        ON CONFLICT (time_str, line_num)
        DO UPDATE
        SET gkg_record_id = EXCLUDED.gkg_record_id,
            v2_document_identifier = EXCLUDED.v2_document_identifier,
            v1_themes = EXCLUDED.v1_themes,
            v1_organizations = EXCLUDED.v1_organizations
    """

    with connection.cursor() as cursor:
        execute_values(cursor, upsert_query, records)

    connection.commit()


def fetch_time_range(
    connection: psycopg2.extensions.connection,
    start_time: str,
    end_time: str,
) -> List[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT time_str
            FROM gdelt_master_times
            WHERE time_str BETWEEN %s AND %s
            ORDER BY time_str
            """,
            (start_time, end_time),
        )
        rows = cursor.fetchall()
    return [row[0] for row in rows]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    args = parse_args()
    start_time = normalise_time_input(args.start_time, "start_time")
    end_time = normalise_time_input(args.end_time, "end_time")
    ensure_time_order(start_time, end_time)

    config = load_configuration()
    user_agent = build_user_agent(config["user_email"])

    try:
        connection = psycopg2.connect(**config["database_config"])
    except Exception as exc:  # noqa: BLE001
        logging.error("Failed to connect to database: %s", exc)
        raise

    try:
        time_values = fetch_time_range(connection, start_time, end_time)
        if not time_values:
            logging.info(
                "No gdelt_master_times entries found between %s and %s.",
                start_time,
                end_time,
            )
            return

        headers = build_gdelt_headers(user_agent)
        session = requests.Session()

        total_success = 0
        total_rows = 0
        failures: List[Tuple[str, str]] = []

        for index, time_str in enumerate(time_values, start=1):
            logging.info("Processing %s (%d/%d).", time_str, index, len(time_values))
            try:
                validate_time_str(time_str)
                zip_bytes = download_gdelt_zip(time_str, headers, session=session)
                rows = extract_csv_rows(zip_bytes)
                valid_rows = filter_valid_rows(rows)
                records = prepare_records(time_str, valid_rows)

                if not records:
                    logging.info("No records produced for time %s.", time_str)
                    continue

                upsert_gdelt_records(connection, records)
                total_success += 1
                total_rows += len(records)
            except Exception as exc:  # noqa: BLE001
                logging.error("Failed to process %s: %s", time_str, exc)
                failures.append((time_str, str(exc)))

        logging.info(
            "Completed processing. Successful timestamps: %d/%d. Rows upserted: %d.",
            total_success,
            len(time_values),
            total_rows,
        )
        if failures:
            logging.warning("Encountered failures for %d timestamps:", len(failures))
            for failed_time, message in failures:
                logging.warning("  %s -> %s", failed_time, message)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
