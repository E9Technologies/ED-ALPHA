import logging
from typing import Dict, Iterable, List, Set, Tuple

import psycopg2
from psycopg2.extras import execute_values
import requests

from config import build_user_agent, load_configuration


SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def fetch_company_tickers(user_agent: str) -> List[Dict[str, str]]:
    headers = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }

    response = requests.get(SEC_COMPANY_TICKERS_URL, headers=headers, timeout=30)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected response structure from SEC API")

    companies: List[Dict[str, str]] = []
    for entry in payload.values():
        if not isinstance(entry, dict):
            continue
        cik = entry.get("cik_str")
        ticker = entry.get("ticker")
        title = entry.get("title")

        if cik is None or ticker is None or title is None:
            continue

        companies.append(
            {
                "cik": cik,
                "ticker": ticker,
                "title": title,
            }
        )

    return companies


def partition_companies(
    companies: Iterable[Dict[str, str]]
) -> Tuple[List[Tuple[int, str]], List[Tuple[int, str]]]:
    """
    Split company payload into (cik, title) profiles and (cik, ticker) associations.
    If conflicting titles are encountered for the same CIK, the conflicting ticker is skipped with a warning.
    """
    titles_by_cik: Dict[int, str] = {}
    ticker_pairs: Set[Tuple[int, str]] = set()

    for company in companies:
        cik = company["cik"]
        title = company["title"]
        ticker = company["ticker"]

        existing_title = titles_by_cik.get(cik)
        if existing_title is None:
            titles_by_cik[cik] = title
        elif existing_title != title:
            logging.warning(
                "Skipping ticker '%s' for CIK %s due to conflicting title '%s' (existing '%s')",
                ticker,
                cik,
                title,
                existing_title,
            )
            continue

        ticker_pairs.add((cik, ticker))

    profiles = sorted(((cik, title) for cik, title in titles_by_cik.items()), key=lambda item: item[0])
    tickers = sorted(ticker_pairs)

    return profiles, tickers


def upsert_company_profiles(
    connection: psycopg2.extensions.connection, profiles: List[Tuple[int, str]]
) -> None:
    if not profiles:
        return

    upsert_query = """
        INSERT INTO company_profiles (cik, title)
        VALUES %s
        ON CONFLICT (cik)
        DO UPDATE
        SET title = EXCLUDED.title
    """

    with connection.cursor() as cursor:
        execute_values(cursor, upsert_query, profiles)


def upsert_company_tickers(
    connection: psycopg2.extensions.connection, ticker_pairs: List[Tuple[int, str]]
) -> None:
    if not ticker_pairs:
        return

    upsert_query = """
        INSERT INTO company_tickers (cik, ticker)
        VALUES %s
        ON CONFLICT (cik, ticker)
        DO NOTHING
    """

    with connection.cursor() as cursor:
        execute_values(cursor, upsert_query, ticker_pairs)

    connection.commit()


def main() -> None:
    config = load_configuration()

    user_agent = build_user_agent(config["user_email"])
    companies = fetch_company_tickers(user_agent)

    if not companies:
        print("No companies were retrieved from the SEC endpoint.")
        return

    profiles, ticker_pairs = partition_companies(companies)

    db_config = config["database_config"]
    connection = psycopg2.connect(**db_config)

    try:
        upsert_company_profiles(connection, profiles)
        upsert_company_tickers(connection, ticker_pairs)
        print(
            f"Upserted {len(profiles)} company profiles and {len(ticker_pairs)} ticker associations."
        )
    finally:
        connection.close()


if __name__ == "__main__":
    main()
