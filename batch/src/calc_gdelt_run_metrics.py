import argparse
import logging
from typing import Iterable, List, Sequence, Tuple

import psycopg2
from psycopg2.extras import execute_values

from config import load_configuration


DEFAULT_K_VALUES = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute top-K metrics (top list, recall@K, precision@K) from gdelt_run_cik_scores "
            "for a given run and store them in gdelt_run_metrics."
        )
    )
    parser.add_argument("--run-id", type=int, required=True, help="Target gdelt_scoring_runs.id.")
    parser.add_argument(
        "--k-values",
        nargs="*",
        type=int,
        default=DEFAULT_K_VALUES,
        help="List of K values (positive integers). Defaults to 10 20 ... 100.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute metrics and print them without writing to the database.",
    )
    return parser.parse_args()


def sanitize_k_values(values: Sequence[int]) -> List[int]:
    unique_sorted = sorted({k for k in values if k > 0})
    if not unique_sorted:
        raise ValueError("At least one positive K value must be provided.")
    return unique_sorted


def fetch_run_header(cursor: psycopg2.extensions.cursor, run_id: int) -> Tuple[int]:
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


def fetch_ranked_scores(
    cursor: psycopg2.extensions.cursor,
    run_id: int,
) -> List[Tuple[int, int, int]]:
    cursor.execute(
        """
        SELECT cik, COALESCE(label, 0) AS label, total_score
        FROM gdelt_run_cik_scores
        WHERE run_id = %s
        ORDER BY total_score DESC, cik ASC
        """,
        (run_id,),
    )
    return cursor.fetchall()


def compute_metrics(
    ranked_scores: Sequence[Tuple[int, int, int]],
    k_values: Iterable[int],
) -> List[Tuple[int, List[int], List[int], int, int, float, float]]:
    total_positives = sum(1 for _, label, _ in ranked_scores if label == 1)
    if total_positives == 0:
        logging.warning("No positive labels found; recall will be 0 for all K.")

    metrics: List[Tuple[int, List[int], List[int], int, int, float, float]] = []
    for k in k_values:
        top_slice = ranked_scores[: min(k, len(ranked_scores))]
        top_ciks = [cik for cik, _, _ in top_slice]
        top_scores = [score for _, _, score in top_slice]
        positives_in_top = sum(1 for _, label, _ in top_slice if label == 1)
        actual_k = len(top_slice)
        recall = (positives_in_top / total_positives) if total_positives > 0 else 0.0
        precision = (positives_in_top / actual_k) if actual_k > 0 else 0.0
        metrics.append(
            (
                k,
                top_ciks,
                top_scores,
                positives_in_top,
                total_positives,
                recall,
                precision,
            )
        )
    return metrics


def upsert_metrics(
    connection: psycopg2.extensions.connection,
    run_id: int,
    metrics: Sequence[Tuple[int, List[int], List[int], int, int, float, float]],
) -> None:
    if not metrics:
        logging.info("No metrics to upsert.")
        return

    payload = [
        (
            run_id,
            k,
            top_ciks,
            top_scores,
            positives_in_top,
            total_positives,
            recall,
            precision,
        )
        for (k, top_ciks, top_scores, positives_in_top, total_positives, recall, precision) in metrics
    ]

    insert_query = """
        INSERT INTO gdelt_run_metrics (
            run_id,
            k,
            top_ciks,
            top_scores,
            positives_in_top,
            total_positives,
            recall,
            precision
        )
        VALUES %s
        ON CONFLICT (run_id, k)
        DO UPDATE SET
            top_ciks = EXCLUDED.top_ciks,
            top_scores = EXCLUDED.top_scores,
            positives_in_top = EXCLUDED.positives_in_top,
            total_positives = EXCLUDED.total_positives,
            recall = EXCLUDED.recall,
            precision = EXCLUDED.precision,
            computed_at = NOW()
    """

    with connection.cursor() as cursor:
        execute_values(cursor, insert_query, payload)
    connection.commit()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    k_values = sanitize_k_values(args.k_values)

    config = load_configuration()
    connection = psycopg2.connect(**config["database_config"])

    try:
        with connection.cursor() as cursor:
            experiment_id = fetch_run_header(cursor, args.run_id)
            ranked_scores = fetch_ranked_scores(cursor, args.run_id)

        if not ranked_scores:
            logging.warning("No run scores found for run_id=%d. Nothing to do.", args.run_id)
            return

        logging.info(
            "Computing metrics for run %d (experiment %d) across %d CIKs.",
            args.run_id,
            experiment_id,
            len(ranked_scores),
        )

        metrics = compute_metrics(ranked_scores, k_values)

        for k, _, _, pos_top, total_pos, recall, precision in metrics:
            logging.info(
                "K=%d: positives_in_top=%d total_positives=%d recall=%.4f precision=%.4f",
                k,
                pos_top,
                total_pos,
                recall,
                precision,
            )

        if args.dry_run:
            logging.info("Dry-run enabled; metrics will not be persisted.")
            return

        upsert_metrics(connection, args.run_id, metrics)
        logging.info("Stored metrics for %d K values.", len(metrics))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
