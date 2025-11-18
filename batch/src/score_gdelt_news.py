import argparse
import html
import os
import re
import sys
import time
from datetime import date, datetime, time as dt_time, timedelta
from typing import Dict, Iterator, Optional, Tuple

import psycopg2
import requests
from bs4 import BeautifulSoup

from config import build_user_agent, load_configuration
from llm_methods import (
    BaseLLMMethod,
    LLMMethodError,
    LLMResponseFormatError,
    create_llm_method,
)


MAX_SNIPPET_CHARS = 2000
DEFAULT_BATCH_SIZE = 200
LLM_MAX_RETRIES = 3
LLM_RETRY_BASE_SECONDS = 5
LLM_RETRY_MAX_SECONDS = 30


class ArticleFetchError(Exception):
    pass


class ArticleContentUnavailable(Exception):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score GDELT articles linked to filing experiment CIKs using an LLM."
    )
    parser.add_argument("--experiment-id", type=int, required=True, help="Target filing_experiments.id")
    parser.add_argument(
        "--min-days-before",
        type=int,
        required=True,
        help="Maximum lookback in days before predict_date (e.g. 60).",
    )
    parser.add_argument(
        "--max-days-before",
        type=int,
        required=True,
        help="Minimum lookback in days before predict_date (e.g. 1).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of records to process per fetch (default {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--model",
        required=True,
        help="LLM model name (must match one of the approved OpenRouter models).",
    )
    parser.add_argument(
        "--reasoning-mode",
        choices=["none", "thinking"],
        required=True,
        help="Reasoning mode flag for OpenRouter models (must be 'none' or 'thinking').",
    )
    parser.add_argument(
        "--run-label",
        help="Optional label to record alongside this scoring run.",
    )
    return parser.parse_args()


def validate_day_window(min_days_before: int, max_days_before: int) -> None:
    if min_days_before < 0 or max_days_before < 0:
        raise ValueError("min_days_before and max_days_before must be non-negative integers.")
    if min_days_before < max_days_before:
        raise ValueError("min_days_before must be greater than or equal to max_days_before.")


def compute_time_bounds(predict_date: date, min_days_before: int, max_days_before: int) -> Tuple[str, str]:
    start_date = predict_date - timedelta(days=min_days_before)
    end_date = predict_date - timedelta(days=max_days_before)
    if start_date > end_date:
        raise ValueError("Computed start_date is later than end_date; verify day window parameters.")
    start_time = datetime.combine(start_date, dt_time.min)
    end_time = datetime.combine(end_date, dt_time.max)
    return start_time.strftime("%Y%m%d%H%M%S"), end_time.strftime("%Y%m%d%H%M%S")


def fetch_predict_date(cursor: psycopg2.extensions.cursor, experiment_id: int) -> date:
    cursor.execute(
        "SELECT predict_date FROM filing_experiments WHERE id = %s",
        (experiment_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"filing_experiments.id {experiment_id} was not found.")
    predict_date = row[0]
    if not isinstance(predict_date, date):
        raise ValueError("predict_date must be a date column.")
    return predict_date


def assert_labels_exist(cursor: psycopg2.extensions.cursor, experiment_id: int) -> None:
    cursor.execute(
        "SELECT COUNT(*) FROM filing_experiment_labels WHERE experiment_id = %s",
        (experiment_id,),
    )
    count = cursor.fetchone()[0]
    if count == 0:
        raise ValueError(f"filing_experiment_labels has no rows for experiment_id {experiment_id}.")


def iter_target_records(
    cursor: psycopg2.extensions.cursor,
    experiment_id: int,
    start_time_str: str,
    end_time_str: str,
    fetch_size: int,
) -> Iterator[Tuple[str, str, str, int, Optional[int]]]:
    cursor.execute(
        """
        SELECT g.time_str, g.gkg_record_id, g.v2_document_identifier, fel.cik, fel.label
        FROM gdelt_gkg_company_links AS gl
        JOIN gdelt_gkg_records AS g
          ON g.time_str = gl.time_str
         AND g.gkg_record_id = gl.gkg_record_id
        JOIN filing_experiment_labels AS fel
          ON fel.cik = gl.cik
        WHERE fel.experiment_id = %s
          AND g.v2_document_identifier IS NOT NULL
          AND g.v2_document_identifier <> ''
          AND g.time_str BETWEEN %s AND %s
        ORDER BY g.time_str, g.gkg_record_id, fel.cik
        """,
        (experiment_id, start_time_str, end_time_str),
    )
    while True:
        rows = cursor.fetchmany(fetch_size)
        if not rows:
            break
        for row in rows:
            yield row



def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def extract_title_and_snippet(html_text: str) -> Tuple[Optional[str], Optional[str]]:
    soup = BeautifulSoup(html_text, "html.parser")

    title_text: Optional[str] = None
    if soup.title and soup.title.string:
        title_text = soup.title.string
    if not title_text:
        og_title = soup.find("meta", attrs={"property": "og:title"})
        if og_title and og_title.get("content"):
            title_text = og_title["content"]

    snippet_text: Optional[str] = None
    article_tag = soup.find("article")
    if article_tag:
        snippet_text = article_tag.get_text(separator=" ", strip=True)

    if not snippet_text:
        meta_tag = soup.find("meta", attrs={"name": "description"})
        if meta_tag and meta_tag.get("content"):
            snippet_text = meta_tag["content"]
    if not snippet_text:
        og_desc = soup.find("meta", attrs={"property": "og:description"})
        if og_desc and og_desc.get("content"):
            snippet_text = og_desc["content"]
    if not snippet_text:
        snippet_text = soup.get_text(separator=" ", strip=True)

    def normalize(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        unescaped = html.unescape(value).replace("\x00", "")
        normalized = normalize_whitespace(unescaped)
        if not normalized:
            return None
        return normalized

    title = normalize(title_text)
    snippet = normalize(snippet_text)
    if snippet is not None:
        snippet = snippet[:MAX_SNIPPET_CHARS]

    return title, snippet

def upsert_article(
    connection: psycopg2.extensions.connection,
    article_url: str,
    title: Optional[str],
    snippet: Optional[str],
    fetch_error: Optional[str],
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO gdelt_articles2 (article_url, title, snippet, last_fetched_at, fetch_error)
            VALUES (%s, %s, %s, NOW(), %s)
            ON CONFLICT (article_url)
            DO UPDATE SET
                title = EXCLUDED.title,
                snippet = EXCLUDED.snippet,
                last_fetched_at = EXCLUDED.last_fetched_at,
                fetch_error = EXCLUDED.fetch_error
            """,
            (article_url, title, snippet, fetch_error),
        )
    connection.commit()


def get_article_content(
    connection: psycopg2.extensions.connection,
    session: requests.Session,
    article_url: str,
    user_agent: str,
) -> Tuple[str, str]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT title, snippet, fetch_error FROM gdelt_articles2 WHERE article_url = %s",
            (article_url,),
        )
        row = cursor.fetchone()

    if row is not None:
        title, snippet, fetch_error = row
        if fetch_error:
            raise ArticleFetchError(fetch_error)
        if title or snippet:
            return (title or "").strip(), (snippet or "").strip()

    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    }
    try:
        response = session.get(article_url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.RequestException as exc:
        err_msg = f"HTTP error: {exc}"
        upsert_article(connection, article_url, None, None, err_msg[:512])
        raise ArticleFetchError(err_msg) from exc

    response.encoding = response.encoding or response.apparent_encoding
    html_text = response.text
    title, snippet = extract_title_and_snippet(html_text)
    if not title and not snippet:
        err_msg = "Failed to extract title or snippet."
        upsert_article(connection, article_url, None, None, err_msg)
        raise ArticleContentUnavailable(err_msg)

    upsert_article(connection, article_url, title, snippet, None)
    return title or "", snippet or ""


def score_article(
    llm_method: BaseLLMMethod,
    title: Optional[str],
    snippet: Optional[str],
) -> Tuple[int, str]:
    last_error: Optional[Exception] = None
    method_name = type(llm_method).__name__
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            result = llm_method.score(title, snippet)
            return result.score, result.reason
        except (LLMMethodError, LLMResponseFormatError, requests.RequestException) as exc:
            last_error = exc
            if attempt == LLM_MAX_RETRIES:
                raise RuntimeError(
                    f"LLM request failed after {LLM_MAX_RETRIES} attempts: {exc}"
                ) from exc
            wait_seconds = min(LLM_RETRY_BASE_SECONDS * (2 ** (attempt - 1)), LLM_RETRY_MAX_SECONDS)
            print(
                f"LLM request failed via {method_name} (attempt {attempt}/{LLM_MAX_RETRIES}): "
                f"{exc}. Retrying in {wait_seconds}s.",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)
    if last_error is not None:
        raise RuntimeError(f"LLM scoring failed: {last_error}") from last_error
    raise RuntimeError("LLM scoring failed without receiving an error.")


def upsert_score(
    connection: psycopg2.extensions.connection,
    run_id: int,
    experiment_id: int,
    cik: int,
    gkg_record_id: str,
    time_str: str,
    article_url: str,
    label: Optional[int],
    score: int,
    reason: str,
) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO gdelt_article_scores (
                run_id,
                experiment_id,
                cik,
                gkg_record_id,
                time_str,
                article_url,
                label,
                llm_score,
                llm_reason,
                evaluated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (run_id, cik, gkg_record_id)
            DO UPDATE SET
                time_str = EXCLUDED.time_str,
                article_url = EXCLUDED.article_url,
                label = EXCLUDED.label,
                llm_score = EXCLUDED.llm_score,
                llm_reason = EXCLUDED.llm_reason,
                evaluated_at = EXCLUDED.evaluated_at
            """,
            (run_id, experiment_id, cik, gkg_record_id, time_str, article_url, label, score, reason),
        )
    connection.commit()


def create_scoring_run(
    connection: psycopg2.extensions.connection,
    experiment_id: int,
    min_days_before: int,
    max_days_before: int,
    batch_size: int,
    model_name: str,
    run_label: Optional[str],
) -> int:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO gdelt_scoring_runs (
                experiment_id,
                min_days_before,
                max_days_before,
                batch_size,
                model_name,
                run_label
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                experiment_id,
                min_days_before,
                max_days_before,
                batch_size,
                model_name,
                run_label,
            ),
        )
        run_id = cursor.fetchone()[0]
    connection.commit()
    return run_id


def main() -> None:
    args = parse_args()
    validate_day_window(args.min_days_before, args.max_days_before)

    try:
        config = load_configuration()
    except Exception as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    user_agent = build_user_agent(config["user_email"])

    reasoning_mode = args.reasoning_mode.lower()
    if reasoning_mode not in {"none", "thinking"}:
        print("Reasoning mode must be 'none' or 'thinking'.", file=sys.stderr)
        sys.exit(1)

    model_name = args.model.strip()
    if not model_name:
        print("LLM model name is required via --model.", file=sys.stderr)
        sys.exit(1)

    batch_size = max(args.batch_size, 1)

    try:
        connection = psycopg2.connect(**config["database_config"])
    except Exception as exc:
        print(f"Database connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        with connection.cursor() as cursor:
            predict_date = fetch_predict_date(cursor, args.experiment_id)
            assert_labels_exist(cursor, args.experiment_id)
            start_time_str, end_time_str = compute_time_bounds(
                predict_date,
                args.min_days_before,
                args.max_days_before,
            )
        print(f"time_window_start={start_time_str}")
        print(f"time_window_end={end_time_str}")

        api_key = os.getenv("OPENROUTER_API_KEY")
        if not api_key:
            print("OPENROUTER_API_KEY is required.", file=sys.stderr)
            sys.exit(1)

        article_session = requests.Session()
        llm_session = requests.Session()
        supports_json = not model_name.startswith("anthropic/claude-sonnet-4.5")
        if not supports_json:
            print(
                "Note: Selected model does not support JSON response_format; "
                "falling back to text parsing.",
                file=sys.stderr,
            )
        llm_method = create_llm_method(
            llm_session,
            api_key=api_key,
            model=model_name,
            reasoning_mode=reasoning_mode,
            supports_json_format=supports_json,
        )

        run_id = create_scoring_run(
            connection,
            args.experiment_id,
            args.min_days_before,
            args.max_days_before,
            batch_size,
            model_name,
            args.run_label,
        )
        print(f"run_id={run_id}")

        print(
            f"Scoring articles for experiment {args.experiment_id} (run {run_id}) "
            f"between {start_time_str} and {end_time_str}.",
            file=sys.stderr,
        )
        print(f"LLM provider=openrouter model={model_name} reasoning_mode={reasoning_mode}.", file=sys.stderr)

        score_cache: Dict[str, Tuple[int, str]] = {}
        failed_articles: set[str] = set()
        processed = 0
        scored = 0
        skipped_fetch = 0
        skipped_missing = 0
        skipped_llm = 0

        with connection.cursor() as cursor:
            for (
                time_str,
                gkg_record_id,
                article_url,
                cik,
                label,
            ) in iter_target_records(
                cursor,
                args.experiment_id,
                start_time_str,
                end_time_str,
                batch_size,
            ):
                processed += 1
                fetch_start = time.perf_counter()
                try:
                    title, snippet = get_article_content(
                        connection,
                        article_session,
                        article_url,
                        user_agent,
                    )
                    fetch_duration = time.perf_counter() - fetch_start
                except ArticleFetchError as exc:
                    fetch_duration = time.perf_counter() - fetch_start
                    skipped_fetch += 1
                    print(
                        f"Skip (fetch error): {article_url} ({exc}) fetch={fetch_duration:.2f}s",
                        file=sys.stderr,
                    )
                    continue
                except ArticleContentUnavailable as exc:
                    fetch_duration = time.perf_counter() - fetch_start
                    skipped_missing += 1
                    print(
                        f"Skip (no content): {article_url} ({exc}) fetch={fetch_duration:.2f}s",
                        file=sys.stderr,
                    )
                    continue

                cache_key = article_url
                cache_hit = False
                score_duration = 0.0
                if cache_key in score_cache:
                    score, reason = score_cache[cache_key]
                    cache_hit = True
                else:
                    score_start = time.perf_counter()
                    try:
                        score, reason = score_article(
                            llm_method,
                            title,
                            snippet,
                        )
                    except RuntimeError as exc:
                        skipped_llm += 1
                        failed_articles.add(cache_key)
                        print(f"Skip (LLM error after retries): {article_url} ({exc})", file=sys.stderr)
                        continue
                    except Exception as exc:
                        score_duration = time.perf_counter() - score_start
                        print(f"LLM scoring failed for {article_url}: {exc}", file=sys.stderr)
                        raise
                    score_duration = time.perf_counter() - score_start
                    score_cache[cache_key] = (score, reason)

                upsert_start = time.perf_counter()
                upsert_score(
                    connection,
                    run_id,
                    args.experiment_id,
                    cik,
                    gkg_record_id,
                    time_str,
                    article_url,
                    label,
                    score,
                    reason,
                )
                upsert_duration = time.perf_counter() - upsert_start
                scored += 1
                score_duration_repr = "cache" if cache_hit else f"{score_duration:.2f}s"
                print(
                    f"Timing run_id={run_id} article_url={article_url} "
                    f"fetch={fetch_duration:.2f}s score={score_duration_repr} "
                    f"db={upsert_duration:.2f}s",
                    file=sys.stderr,
                )

        print(
            f"Run {run_id} processed {processed} records; scored {scored}; "
            f"skipped (fetch errors) {skipped_fetch}; skipped (missing content) {skipped_missing}; "
            f"skipped (LLM failures) {skipped_llm}.",
            file=sys.stderr,
        )
    finally:
        connection.close()
        article_session.close()
        llm_session.close()


if __name__ == "__main__":
    main()
