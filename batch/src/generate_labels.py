import argparse
import json
import logging
import os
import re
import math
import random
from bisect import bisect_left
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import DefaultDict, Dict, List, Optional, Sequence, Set, Tuple

import psycopg2
from psycopg2.extras import execute_values

from config import load_configuration


CONFIG_DEFAULT_PATH = "config/predict_config.json"
DEFAULT_ITEM_CODES = ["2.01"]
DEFAULT_HORIZON_DAYS = 30
DEFAULT_MIN_DAYS_BEFORE = 31
DEFAULT_MAX_DAYS_BEFORE = 1
LOG_MATCH_THRESHOLD = 0.2
DEFAULT_NEG_MULT = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate labeled CIK sets based on company_recent_filings within a future horizon."
    )
    parser.add_argument("--config", help="Path to config JSON file.", default=CONFIG_DEFAULT_PATH)
    parser.add_argument("--predict-date", dest="predict_date", help="予測日 (YYYYMMDD)。例: 20250701")
    parser.add_argument(
        "--horizon-days",
        type=int,
        dest="horizon_days",
        help="将来判定の期間（日）",
    )
    parser.add_argument(
        "--item-codes",
        nargs="+",
        dest="item_codes",
        help="対象の item code のリスト。指定しなければ絞り込まない。",
    )
    parser.add_argument(
        "--min-days-before",
        type=int,
        dest="min_days_before",
        help="predict_date から見て GDELT を最大で何日前まで遡るか（日）",
    )
    parser.add_argument(
        "--max-days-before",
        type=int,
        dest="max_days_before",
        help="predict_date から見て GDELT を最小で何日前まで遡るか（日）",
    )
    parser.add_argument(
        "--max-positive-samples",
        type=int,
        dest="max_positive_samples",
        help="正例（positive CIK）の上限数。指定しなければ全件を使用。",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="ランダムサンプリングで使用するシード（未指定ならランダム）。",
    )
    return parser.parse_args()


def load_config_from_file(path: Optional[str]) -> Dict[str, object]:
    if not path:
        return {}
    if not os.path.exists(path):
        logging.info("Config file '%s' not found; proceeding with CLI defaults.", path)
        return {}
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def coalesce(
    cli_value: Optional[object],
    config_value: Optional[object],
    default_value: Optional[object] = None,
) -> Optional[object]:
    if cli_value is not None:
        return cli_value
    if config_value is not None:
        return config_value
    return default_value


def parse_predict_date(raw: str) -> date:
    try:
        return datetime.strptime(raw, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError("predict_date must follow YYYYMMDD format.") from exc


def normalize_item_codes(codes: Optional[Sequence[str]]) -> Optional[List[str]]:
    if codes is None:
        return None
    normalized = [code.strip() for code in codes if code.strip()]
    return normalized or None


def extract_item_codes(items_value: str) -> Set[str]:
    """
    Extract item codes (e.g., 2.01) from an items field using regex.
    """
    return set(match.group(0) for match in ITEM_CODE_REGEX.finditer(items_value))


def fetch_filings_within_horizon(
    cursor: psycopg2.extensions.cursor,
    start_date: date,
    end_date: date,
) -> List[Tuple[int, str, str, Optional[date], Optional[str]]]:
    cursor.execute(
        """
        SELECT cik, accession_number, primary_document, filing_date, items
        FROM company_recent_filings
        WHERE filing_date BETWEEN %s AND %s
        """,
        (start_date, end_date),
    )
    return cursor.fetchall()


def fetch_news_counts(
    cursor: psycopg2.extensions.cursor,
    start_time: str,
    end_time: str,
) -> Dict[int, int]:
    cursor.execute(
        """
        SELECT g.cik, COUNT(*)::INT AS cnt
        FROM gdelt_gkg_company_links g
        INNER JOIN company_profiles p
            ON g.cik = p.cik
        WHERE g.cik IS NOT NULL
          AND g.time_str BETWEEN %s AND %s
          AND p.title NOT ILIKE 'UNITED STATES%%'
        GROUP BY g.cik
        """,
        (start_time, end_time),
    )
    return {row[0]: row[1] for row in cursor.fetchall()}


def match_negatives_to_positives(
    positive_counts: Dict[int, int],
    negative_counts: Dict[int, int],
    max_log_diff: float,
) -> Tuple[List[Tuple[int, int]], List[int]]:
    if not positive_counts or not negative_counts:
        return [], sorted(positive_counts.keys())

    negative_entries = sorted(
        (math.log1p(count), cik) for cik, count in negative_counts.items()
    )
    negative_logs = [entry[0] for entry in negative_entries]
    used_negatives: Set[int] = set()
    pairs: List[Tuple[int, int]] = []
    unmatched: List[int] = []

    positive_order = sorted(
        positive_counts.items(),
        key=lambda item: item[1],
        reverse=True,
    )

    for positive_cik, positive_count in positive_order:
        target_log = math.log1p(positive_count)
        insert_index = bisect_left(negative_logs, target_log)
        best_candidate: Optional[Tuple[float, int, int]] = None  # (diff, idx, cik)

        def consider(index: int) -> None:
            nonlocal best_candidate
            if index < 0 or index >= len(negative_entries):
                return
            log_value, candidate_cik = negative_entries[index]
            if candidate_cik in used_negatives:
                return
            diff = abs(log_value - target_log)
            if diff >= max_log_diff:
                return
            if best_candidate is None or diff < best_candidate[0]:
                best_candidate = (diff, index, candidate_cik)

        # Examine the immediate neighbor at insert_index and to the left/right while within threshold.
        right_index = insert_index
        while right_index < len(negative_entries):
            log_value, _ = negative_entries[right_index]
            if log_value - target_log >= max_log_diff:
                break
            consider(right_index)
            right_index += 1

        left_index = insert_index - 1
        while left_index >= 0:
            log_value, _ = negative_entries[left_index]
            if target_log - log_value >= max_log_diff:
                break
            consider(left_index)
            left_index -= 1

        if best_candidate is None:
            unmatched.append(positive_cik)
            continue

        _, chosen_idx, chosen_cik = best_candidate
        used_negatives.add(chosen_cik)
        pairs.append((positive_cik, chosen_cik))

        # Remove used negative to keep future scans lightweight.
        negative_entries.pop(chosen_idx)
        negative_logs.pop(chosen_idx)

    return pairs, unmatched


def insert_experiment_record(
    connection: psycopg2.extensions.connection,
    predict_date: date,
    horizon_days: int,
    item_codes: Optional[List[str]],
    neg_mult: int,
    seed: Optional[int],
    config_payload: Dict[str, object],
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO filing_experiments (
                predict_date,
                horizon_days,
                item_codes,
                neg_multiplier,
                seed,
                config
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                predict_date,
                horizon_days,
                item_codes,
                neg_mult,
                seed,
                json.dumps(config_payload),
            ),
        )
        experiment_id = cursor.fetchone()[0]
    connection.commit()
    return experiment_id


def insert_experiment_labels(
    connection: psycopg2.extensions.connection,
    experiment_id: int,
    labels: List[Tuple[int, int]],
) -> None:
    if not labels:
        return

    insert_query = """
        INSERT INTO filing_experiment_labels (experiment_id, cik, label)
        VALUES %s
        ON CONFLICT (experiment_id, cik)
        DO UPDATE SET label = EXCLUDED.label
    """

    payload = [(experiment_id, cik, label) for cik, label in labels]

    with connection.cursor() as cursor:
        execute_values(cursor, insert_query, payload)
    connection.commit()


def insert_label_evidence(
    connection: psycopg2.extensions.connection,
    experiment_id: int,
    evidence_rows: List[Tuple[int, str, str, Optional[date], str]],
) -> None:
    if not evidence_rows:
        return

    insert_query = """
        INSERT INTO filing_experiment_label_evidence (
            experiment_id,
            cik,
            accession_number,
            primary_document,
            filing_date,
            matching_item_code
        )
        VALUES %s
        ON CONFLICT (experiment_id, cik, accession_number, matching_item_code)
        DO NOTHING
    """

    with connection.cursor() as cursor:
        execute_values(cursor, insert_query, [(experiment_id, *row) for row in evidence_rows])
    connection.commit()


def validate_parameters(
    predict_date: Optional[str],
    horizon_days: Optional[int],
    item_codes: Optional[List[str]],
    min_days_before: Optional[int],
    max_days_before: Optional[int],
) -> Tuple[date, int, List[str], int, int]:
    if not predict_date:
        raise ValueError("predict_date must be provided via CLI or config file.")

    parsed_predict_date = parse_predict_date(predict_date)

    horizon = horizon_days if horizon_days and horizon_days > 0 else DEFAULT_HORIZON_DAYS

    normalized_items = normalize_item_codes(item_codes)
    if normalized_items is None:
        normalized_items = DEFAULT_ITEM_CODES.copy()

    min_days = min_days_before if min_days_before is not None else DEFAULT_MIN_DAYS_BEFORE
    max_days = max_days_before if max_days_before is not None else DEFAULT_MAX_DAYS_BEFORE

    if min_days < 0 or max_days < 0:
        raise ValueError("min_days_before and max_days_before must be zero or positive integers.")
    if min_days < max_days:
        raise ValueError("min_days_before must be greater than or equal to max_days_before.")

    return parsed_predict_date, horizon, normalized_items, min_days, max_days


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    config_file_values = load_config_from_file(args.config)

    merged_params = {
        "predict_date": coalesce(args.predict_date, config_file_values.get("predict_date")),
        "horizon_days": coalesce(args.horizon_days, config_file_values.get("horizon_days")),
        "item_codes": coalesce(args.item_codes, config_file_values.get("item_codes")),
        "min_days_before": coalesce(args.min_days_before, config_file_values.get("min_days_before")),
        "max_days_before": coalesce(args.max_days_before, config_file_values.get("max_days_before")),
        "max_positive_samples": coalesce(
            args.max_positive_samples,
            config_file_values.get("max_positive_samples"),
        ),
        "seed": coalesce(args.seed, config_file_values.get("seed")),
    }

    (
        predict_date,
        horizon_days,
        item_codes,
        min_days_before,
        max_days_before,
    ) = validate_parameters(
        merged_params["predict_date"],
        merged_params["horizon_days"],
        merged_params["item_codes"],
        merged_params["min_days_before"],
        merged_params["max_days_before"],
    )
    max_positive_samples_raw = merged_params["max_positive_samples"]
    if max_positive_samples_raw is not None:
        max_positive_samples = int(max_positive_samples_raw)
        if max_positive_samples <= 0:
            raise ValueError("max_positive_samples must be a positive integer when provided.")
    else:
        max_positive_samples = None
    sample_seed_raw = merged_params["seed"]
    sample_seed = int(sample_seed_raw) if sample_seed_raw is not None else None

    start_date = predict_date
    end_date = predict_date + timedelta(days=horizon_days - 1)

    logging.info(
        "Evaluating filings from %s to %s for item codes %s.",
        start_date,
        end_date,
        ", ".join(item_codes),
    )

    db_config = load_configuration()["database_config"]
    connection = psycopg2.connect(**db_config)

    news_window_start = predict_date - timedelta(days=min_days_before)
    news_window_end = predict_date - timedelta(days=max_days_before)
    news_start_dt = datetime.combine(news_window_start, time(0, 0, 0))
    news_end_dt = datetime.combine(news_window_end, time(23, 59, 59))
    news_start_str = news_start_dt.strftime("%Y%m%d%H%M%S")
    news_end_str = news_end_dt.strftime("%Y%m%d%H%M%S")

    logging.info(
        "Collecting GDELT company link counts between %s and %s.",
        news_start_dt,
        news_end_dt,
    )

    try:
        with connection.cursor() as cursor:
            news_counts = fetch_news_counts(cursor, news_start_str, news_end_str)
            filing_rows = fetch_filings_within_horizon(cursor, start_date, end_date)

        if not news_counts:
            logging.warning(
                "No gdelt_gkg_company_links rows found between %s and %s. Aborting.",
                news_start_dt,
                news_end_dt,
            )
            return

        target_codes = set(item_codes)
        positives: Set[int] = set()
        evidence_by_cik: DefaultDict[int, List[Tuple[str, str, Optional[date], List[str]]]] = defaultdict(list)
        codes_by_cik: DefaultDict[int, Set[str]] = defaultdict(set)
        skipped_due_to_missing_news: Set[int] = set()

        for cik, accession_number, primary_document, filing_dt, items in filing_rows:
            if not items:
                continue
            codes_in_row = extract_item_codes(items)
            matching_codes = sorted(code for code in codes_in_row if code in target_codes)
            if not matching_codes:
                continue
            if cik not in news_counts:
                skipped_due_to_missing_news.add(cik)
                continue
            positives.add(cik)
            codes_by_cik[cik].update(matching_codes)
            evidence_by_cik[cik].append(
                (accession_number, primary_document or "", filing_dt, matching_codes)
            )

        if skipped_due_to_missing_news:
            logging.info(
                "Skipped %d positive candidates outside the news window.",
                len(skipped_due_to_missing_news),
            )

        if not positives:
            logging.warning("No positive CIKs aligned with news counts. Aborting.")
            return

        logging.info(
            "Identified %d positive CIKs producing %d matching item codes.",
            len(positives),
            sum(len(codes_by_cik[cik]) for cik in positives),
        )

        positive_counts_full = {cik: news_counts[cik] for cik in positives}
        if max_positive_samples is not None and len(positives) > max_positive_samples:
            rng = random.Random(sample_seed)
            ordered_ciks = sorted(positives)
            selected_ciks_list = rng.sample(ordered_ciks, max_positive_samples)
            selected_ciks = set(selected_ciks_list)
            dropped_ciks = positives - selected_ciks
            positives = selected_ciks
            for cik in dropped_ciks:
                evidence_by_cik.pop(cik, None)
                codes_by_cik.pop(cik, None)
            logging.info(
                "Randomly sampled %d positives (from %d) using max_positive_samples=%d seed=%s.",
                len(positives),
                len(ordered_ciks),
                max_positive_samples,
                "None" if sample_seed is None else sample_seed,
            )

        positive_counts = {cik: positive_counts_full[cik] for cik in positives}
        negative_counts = {cik: count for cik, count in news_counts.items() if cik not in positives}

        if not negative_counts:
            logging.warning("No negative candidates available within the news window. Aborting.")
            return

        pairs, unmatched = match_negatives_to_positives(
            positive_counts,
            negative_counts,
            LOG_MATCH_THRESHOLD,
        )

        if not pairs:
            logging.error(
                "Failed to match any negatives within log1p threshold %.2f. Aborting.",
                LOG_MATCH_THRESHOLD,
            )
            raise SystemExit(1)

        if unmatched:
            logging.warning(
                "Dropped %d positive CIKs due to lack of close negative matches.",
                len(unmatched),
            )

        matched_positive_ciks = {pos for pos, _ in pairs}
        matched_negative_ciks = {neg for _, neg in pairs}

        for cik in unmatched:
            evidence_by_cik.pop(cik, None)
            codes_by_cik.pop(cik, None)

        neg_mult = DEFAULT_NEG_MULT
        seed = sample_seed

        experiment_payload = {
            "predict_date": predict_date.strftime("%Y%m%d"),
            "horizon_days": horizon_days,
            "item_codes": item_codes,
            "max_positive_samples": max_positive_samples,
            "neg_mult": neg_mult,
            "seed": seed,
            "min_days_before": min_days_before,
            "max_days_before": max_days_before,
            "log_match_threshold": LOG_MATCH_THRESHOLD,
            "actual_positive_count": len(matched_positive_ciks),
            "actual_positive_item_codes": sum(len(codes_by_cik.get(cik, set())) for cik in matched_positive_ciks),
            "actual_negative_count": len(matched_negative_ciks),
            "news_window_start": news_start_str,
            "news_window_end": news_end_str,
            "unmatched_positive_count": len(unmatched),
            "pair_count": len(pairs),
        }

        experiment_id = insert_experiment_record(
            connection,
            predict_date,
            horizon_days,
            item_codes,
            neg_mult,
            seed,
            experiment_payload,
        )

        label_rows: List[Tuple[int, int]] = []
        label_rows.extend((cik, 1) for cik in sorted(matched_positive_ciks))
        label_rows.extend((cik, 0) for cik in sorted(matched_negative_ciks))

        insert_experiment_labels(connection, experiment_id, label_rows)

        evidence_rows: List[Tuple[int, str, str, Optional[date], str]] = []
        for cik in sorted(matched_positive_ciks):
            for accession_number, primary_document, filing_dt, matching_codes in evidence_by_cik.get(cik, []):
                for code in matching_codes:
                    evidence_rows.append(
                        (cik, accession_number, primary_document, filing_dt, code)
                    )

        insert_label_evidence(connection, experiment_id, evidence_rows)

        logging.info(
            "Experiment %d stored with %d positives, %d negatives, and %d evidence rows.",
            experiment_id,
            len(matched_positive_ciks),
            len(matched_negative_ciks),
            len(evidence_rows),
        )
        print(f"experiment_id={experiment_id}")
    finally:
        connection.close()


ITEM_CODE_REGEX = re.compile(r"\d+\.\d+")


if __name__ == "__main__":
    main()
