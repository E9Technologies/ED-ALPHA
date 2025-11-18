import argparse
import logging
from typing import List, Tuple

import psycopg2
from psycopg2.extras import execute_values

from config import load_configuration


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate LLM scores for a scoring run by summing gdelt_article_scores "
            "per CIK and storing the totals in gdelt_run_cik_scores."
        )
    )
    parser.add_argument(
        "--run-id",
        type=int,
        required=True,
        help="Target gdelt_scoring_runs.id.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute totals and log them without writing to the database.",
    )
    return parser.parse_args()


def fetch_run_metadata(cursor: psycopg2.extensions.cursor, run_id: int) -> Tuple[int]:
    cursor.execute(
        """
        SELECT experiment_id
        FROM gdelt_scoring_runs
        WHERE id = %s
        """,
        (run_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"gdelt_scoring_runs.id {run_id} was not found.")
    return row[0]


def fetch_labelled_ciks(
    cursor: psycopg2.extensions.cursor,
    experiment_id: int,
) -> List[Tuple[int, int]]:
    cursor.execute(
        """
        SELECT cik, label
        FROM filing_experiment_labels
        WHERE experiment_id = %s
        """,
        (experiment_id,),
    )
    return cursor.fetchall()


def aggregate_scores(
    cursor: psycopg2.extensions.cursor,
    run_id: int,
    labelled_ciks: List[Tuple[int, int]],
) -> List[Tuple[int, int, int]]:
    if not labelled_ciks:
        return []

    cursor.execute(
        """
        SELECT
            cik,
            SUM(llm_score - 1) AS total_score
        FROM gdelt_article_scores
        WHERE run_id = %s
        GROUP BY cik
        """,
        (run_id,),
    )
    score_map = {row[0]: int(row[1]) for row in cursor.fetchall()}

    results: List[Tuple[int, int, int]] = []
    for cik, label in labelled_ciks:
        total = score_map.get(cik, 0)
        results.append((cik, label, total))
    return results


def upsert_totals(
    connection: psycopg2.extensions.connection,
    run_id: int,
    experiment_id: int,
    totals: List[Tuple[int, int, int]],
) -> None:
    if not totals:
        logging.info("No labelled CIKs found; nothing to upsert.")
        return

    payload = [
        (run_id, experiment_id, cik, label, total)
        for cik, label, total in totals
    ]

    insert_query = """
        INSERT INTO gdelt_run_cik_scores (
            run_id,
            experiment_id,
            cik,
            label,
            total_score
        )
        VALUES %s
        ON CONFLICT (run_id, cik)
        DO UPDATE SET
            label = EXCLUDED.label,
            total_score = EXCLUDED.total_score,
            computed_at = NOW()
    """

    with connection.cursor() as cursor:
        execute_values(cursor, insert_query, payload)
    connection.commit()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    config = load_configuration()
    connection = psycopg2.connect(**config["database_config"])

    try:
        with connection.cursor() as cursor:
            experiment_id = fetch_run_metadata(cursor, args.run_id)
            labelled_ciks = fetch_labelled_ciks(cursor, experiment_id)

        logging.info(
            "Aggregating run %d linked to experiment %d for %d labelled CIKs.",
            args.run_id,
            experiment_id,
            len(labelled_ciks),
        )

        with connection.cursor() as cursor:
            totals = aggregate_scores(cursor, args.run_id, labelled_ciks)

        if args.dry_run:
            logging.info("Dry-run enabled; results will not be persisted.")
            for cik, label, total in totals:
                logging.info("CIK %s (label=%s): total_score=%s", cik, label, total)
            return

        upsert_totals(connection, args.run_id, experiment_id, totals)
        logging.info("Upserted totals for %d CIKs.", len(totals))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
