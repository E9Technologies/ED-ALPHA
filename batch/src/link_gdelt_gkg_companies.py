import argparse
import logging
from bisect import bisect_left
from typing import Iterable, List, Sequence, Set, Tuple

import psycopg2
from psycopg2.extras import execute_values

from config import load_configuration

LinkRow = Tuple[str, str, int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Link GDELT GKG records to company CIKs by matching organization names to company titles."
        )
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000000,
        help="Number of GDELT records to process per database fetch.",
    )
    return parser.parse_args()


def normalize_text(value: str) -> str:
    return " ".join(value.strip().lower().split())


def build_company_title_index(
    cursor: psycopg2.extensions.cursor,
) -> Tuple[Sequence[str], Sequence[int]]:
    cursor.execute("SELECT cik, title FROM company_profiles WHERE title IS NOT NULL")
    normalized_titles: List[Tuple[str, int]] = []

    for cik, title in cursor.fetchall():
        normal_title = normalize_text(title or "")
        if not normal_title:
            continue
        normalized_titles.append((normal_title, cik))

    if not normalized_titles:
        return [], []

    normalized_titles.sort(key=lambda item: item[0])
    titles_only = [title for title, _ in normalized_titles]
    ciks_only = [cik for _, cik in normalized_titles]
    return titles_only, ciks_only


def find_matching_ciks(
    organization: str,
    titles: Sequence[str],
    ciks: Sequence[int],
) -> Set[int]:
    if not organization or not titles:
        return set()

    upper_bound = f"{organization}{chr(0x10FFFF)}"
    start_index = bisect_left(titles, organization)
    end_index = bisect_left(titles, upper_bound)

    matches: Set[int] = set()
    for idx in range(start_index, end_index):
        title = titles[idx]
        if title.startswith(organization):
            if organization == "united states":
                # "UNITED STATES ..." org strings overmatch (CIK 101538 = UNITED STATES ANTIMONY CORP); skip entirely.
                continue
            matches.add(ciks[idx])

    return matches


def iter_organizations(raw_value: str) -> Iterable[str]:
    for entry in raw_value.split(";"):
        normalized = normalize_text(entry)
        if normalized:
            yield normalized


def insert_links(
    connection: psycopg2.extensions.connection,
    rows: Sequence[LinkRow],
) -> None:
    if not rows:
        return

    insert_query = """
        INSERT INTO gdelt_gkg_company_links (time_str, gkg_record_id, cik)
        VALUES %s
        ON CONFLICT (time_str, gkg_record_id, cik)
        DO NOTHING
    """

    with connection.cursor() as cursor:
        execute_values(cursor, insert_query, rows)


def link_gdelt_records(
    connection: psycopg2.extensions.connection,
    batch_size: int,
) -> Tuple[int, int]:
    with connection.cursor() as cursor:
        titles, ciks = build_company_title_index(cursor)

    if not titles:
        logging.info("No company titles found; nothing to link.")
        return 0, 0

    matched_records = 0
    inserted_rows = 0
    pending_rows: Set[LinkRow] = set()

    with connection.cursor(name="gdelt_scan") as cursor:
        cursor.itersize = batch_size
        cursor.execute(
            """
            SELECT time_str, gkg_record_id, v1_organizations
            FROM gdelt_gkg_records
            WHERE v1_organizations IS NOT NULL AND v1_organizations <> ''
            """
        )

        for time_str, gkg_record_id, organizations in cursor:
            organization_ciks: Set[int] = set()
            for organization in iter_organizations(organizations or ""):
                organization_ciks.update(find_matching_ciks(organization, titles, ciks))

            if not organization_ciks:
                continue

            matched_records += 1

            for cik in organization_ciks:
                pending_rows.add((time_str, gkg_record_id, cik))

            if len(pending_rows) >= batch_size:
                insert_links(connection, list(pending_rows))
                inserted_rows += len(pending_rows)
                pending_rows.clear()

    if pending_rows:
        insert_links(connection, list(pending_rows))
        inserted_rows += len(pending_rows)

    connection.commit()

    return matched_records, inserted_rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    if args.batch_size <= 0:
        raise ValueError("batch_size must be a positive integer.")

    config = load_configuration()
    connection = psycopg2.connect(**config["database_config"])

    try:
        matched_records, inserted_rows = link_gdelt_records(connection, args.batch_size)
        logging.info(
            "Processed records with organization matches: %d; inserted link rows: %d.",
            matched_records,
            inserted_rows,
        )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
