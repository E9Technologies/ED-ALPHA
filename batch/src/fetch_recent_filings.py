import argparse
import json
import logging
import os
import tempfile
from datetime import datetime, date
from typing import Iterable, Iterator, List, Optional, Tuple
import zipfile
import re

import psycopg2
from psycopg2.extras import execute_values
import requests

from config import build_user_agent, load_configuration


BULK_FILENAME_CIK_REGEX = re.compile(r"CIK(\d+)")
SUBMISSIONS_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
BATCH_SIZE = 1000000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download the SEC submissions bulk archive and upsert recent filings for all CIKs."
    )
    parser.add_argument(
        "--archive",
        help="Path to a local submissions.zip archive. If omitted, the archive will be downloaded.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help="Number of filings to accumulate before performing a database upsert (default: 1000000).",
    )
    return parser.parse_args()


def download_submissions_archive(url: str, user_agent: str) -> str:
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/zip",
    }

    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_file:
        logging.info("Downloading submissions archive from %s ...", url)
        with requests.get(url, headers=headers, stream=True, timeout=120) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    temp_file.write(chunk)
        temp_path = temp_file.name

    logging.info("Archive saved to %s", temp_path)
    return temp_path


def iter_submission_entries(archive_path: str) -> Iterator[Tuple[int, str, dict]]:
    with zipfile.ZipFile(archive_path) as archive:
        members = [name for name in archive.namelist() if name.endswith(".json")]
        logging.info("Processing %d submission files from archive.", len(members))

        for index, member in enumerate(members, start=1):
            with archive.open(member) as fp:
                try:
                    data = json.load(fp)
                except json.JSONDecodeError as exc:
                    logging.warning("Skipping %s due to JSON decode error: %s", member, exc)
                    continue
            yield index, member, data


def parse_recent_filings(
    cik: int, recent: Optional[dict]
) -> List[Tuple[int, str, str, Optional[date], str, Optional[str]]]:
    if not recent:
        return []

    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    primary_documents = recent.get("primaryDocument", [])
    accession_numbers = recent.get("accessionNumber", [])
    items_list = recent.get("items", [])

    records: List[Tuple[int, str, str, Optional[date], str, Optional[str]]] = []

    for form, filing_date, document, accession, items in zip(
        forms, filing_dates, primary_documents, accession_numbers, items_list
    ):
        if not accession:
            continue

        filing_date_value = _parse_filing_date(filing_date)
        items_value = _normalize_items(items)

        records.append(
            (
                cik,
                str(accession),
                str(form) if form is not None else "",
                filing_date_value,
                str(document) if document is not None else "",
                items_value,
            )
        )

    return records


def _parse_filing_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        logging.warning("Invalid filingDate '%s'; storing NULL.", value)
        return None


def _normalize_items(items: Optional[Iterable[str]]) -> Optional[str]:
    if items is None:
        return None
    if isinstance(items, str):
        stripped = items.strip()
        return stripped or None
    try:
        joined = ", ".join(str(part).strip() for part in items if part)
    except TypeError:
        return str(items)
    return joined or None


def _extract_cik_from_filename(filename: str) -> Optional[str]:
    match = BULK_FILENAME_CIK_REGEX.search(os.path.basename(filename))
    if match:
        return match.group(1)
    return None


def upsert_recent_filings(
    connection: psycopg2.extensions.connection,
    filings: List[Tuple[int, str, str, Optional[date], str, Optional[str]]],
) -> None:
    if not filings:
        return

    upsert_query = """
        INSERT INTO company_recent_filings (
            cik,
            accession_number,
            form,
            filing_date,
            primary_document,
            items
        )
        VALUES %s
        ON CONFLICT (cik, accession_number)
        DO UPDATE
        SET form = EXCLUDED.form,
            filing_date = EXCLUDED.filing_date,
            primary_document = EXCLUDED.primary_document,
            items = EXCLUDED.items
    """

    with connection.cursor() as cursor:
        execute_values(cursor, upsert_query, filings)

    connection.commit()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    config = load_configuration()
    user_agent = build_user_agent(config["user_email"])

    archive_path = args.archive
    cleanup_archive = False
    if not archive_path:
        archive_path = download_submissions_archive(SUBMISSIONS_ARCHIVE_URL, user_agent)
        cleanup_archive = True
    else:
        if not os.path.exists(archive_path):
            raise FileNotFoundError(f"Archive file not found: {archive_path}")

    total_files = 0
    total_filings = 0

    connection = psycopg2.connect(**config["database_config"])
    try:
        batch: List[Tuple[int, str, str, Optional[date], str, Optional[str]]] = []

        for total_files, entry in enumerate(iter_submission_entries(archive_path), start=1):
            _, filename, payload = entry

            cik_raw = _extract_cik_from_filename(filename)
            if cik_raw is None:
                logging.warning(
                    "Could not determine CIK from filename '%s'; skipping.",
                    filename,
                )
                continue

            try:
                cik_int = int(str(cik_raw).lstrip("0") or "0")
            except ValueError:
                logging.warning("Invalid CIK '%s' (file '%s'); skipping.", cik_raw, filename)
                continue
            if cik_int <= 0:
                logging.warning("Non-positive CIK '%s' (file '%s'); skipping.", cik_raw, filename)
                continue

            recent = payload.get("filings", {}).get("recent")
            filings = parse_recent_filings(cik_int, recent)

            if not filings:
                continue

            batch.extend(filings)
            total_filings += len(filings)

            if len(batch) >= args.batch_size:
                upsert_recent_filings(connection, batch)
                logging.info("Upserted %d filings so far.", total_filings)
                batch.clear()

        if batch:
            upsert_recent_filings(connection, batch)
            logging.info("Upserted %d filings in total.", total_filings)
        else:
            logging.info("No filings to process.")

    finally:
        connection.close()
        if cleanup_archive and archive_path:
            os.unlink(archive_path)

    logging.info(
        "Processed %d submission files and upserted %d filings.",
        total_files,
        total_filings,
    )


if __name__ == "__main__":
    main()
