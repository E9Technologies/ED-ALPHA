import argparse
import logging
import re
import time
import warnings
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional, Sequence, Tuple

import psycopg2
import requests
from bs4.builder import XMLParsedAsHTMLWarning
from psycopg2.extras import execute_values

from bs4 import BeautifulSoup, NavigableString, Tag
from bs4.element import Comment

from config import load_configuration

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

SEC_BASE_URL = "https://www.sec.gov/Archives/edgar/data"
ITEM_SECTION_REGEX = re.compile(r"^Item\s+(\d+(?:\.\d+)+)\.?$", re.I)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape SEC filings for experiment item codes and store per-item sections."
    )
    parser.add_argument("--experiment-id", type=int, required=True, help="Target filing_experiments.id")
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds to sleep between SEC requests (default 1.0).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of rows to upsert per batch.",
    )
    return parser.parse_args()


def fetch_targets(
    cursor: psycopg2.extensions.cursor,
    experiment_id: int,
) -> Tuple[List[Tuple[int, str, str, Optional[str]]], Dict[Tuple[int, str], Sequence[str]]]:
    cursor.execute(
        """
        SELECT DISTINCT
            evidence.cik,
            evidence.accession_number,
            evidence.primary_document,
            evidence.filing_date::TEXT
        FROM filing_experiment_label_evidence AS evidence
        WHERE evidence.experiment_id = %s
        ORDER BY evidence.cik, evidence.accession_number
        """,
        (experiment_id,),
    )
    rows = cursor.fetchall()
    if not rows:
        raise ValueError(f"No filing_experiment_label_evidence rows found for experiment_id {experiment_id}.")

    return rows


def build_filing_url(cik: int, accession_number: str, primary_document: str) -> str:
    clean_cik = str(int(cik))
    accession_fragment = accession_number.replace("-", "")
    return f"{SEC_BASE_URL}/{clean_cik}/{accession_fragment}/{primary_document}"


def fetch_html(url: str) -> str:
    headers = {
        # SEC は UA 明示を推奨
        "User-Agent": "Mozilla/5.0 (compatible; FilingItemScraper/1.0; +https://example.com/)",
        "Accept": "text/html,application/xhtml+xml",
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "utf-8"
    return response.text


def upsert_item_sections(
    connection: psycopg2.extensions.connection,
    rows: Sequence[Tuple[int, int, str, str, str, Optional[str], str, str]],
    batch_size: int,
) -> None:
    if not rows:
        return

    insert_query = """
        INSERT INTO filing_item_sections (
            experiment_id,
            cik,
            accession_number,
            item_code,
            primary_document,
            filing_date,
            title,
            body
        )
        VALUES %s
        ON CONFLICT (cik, accession_number, item_code)
        DO UPDATE SET
            experiment_id = EXCLUDED.experiment_id,
            primary_document = EXCLUDED.primary_document,
            filing_date = EXCLUDED.filing_date,
            title = EXCLUDED.title,
            body = EXCLUDED.body,
            scraped_at = NOW()
    """

    start = 0
    total = len(rows)
    while start < total:
        chunk = rows[start : start + batch_size]
        with connection.cursor() as cursor:
            execute_values(cursor, insert_query, chunk)
        connection.commit()
        start += len(chunk)


def normalize(s: str) -> str:
    """NBSP→半角空白、連続空白を1つに、前後trim"""
    if not s:
        return ""
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def get_cell_text_bold_first(td: Tag) -> str:
    """セル内の<b>テキストを優先。無ければセル全体のテキスト。"""
    bolds = td.find_all("b")
    if bolds:
        return normalize(" ".join(b.get_text(" ", strip=True) for b in bolds))
    return normalize(td.get_text(" ", strip=True))

def item_header_from_table(table: Tag):
    """
    テーブルが「Item x.xx」見出しかを判定し、番号とタイトルを抽出。
    タイトルは右側セルの<b>（無ければセルテキスト）を連結。
    右側セルが存在せず同一セル内にタイトルが混在する場合はフォールバックし、その旨print。
    戻り値: dict or None
    """
    tr = table.find("tr")
    if not tr:
        return None
    cells = tr.find_all(["td", "th"])
    if not cells:
        return None

    idx = -1
    number = None
    for i, cell in enumerate(cells):
        t = normalize(cell.get_text(" ", strip=True))
        m = ITEM_SECTION_REGEX.search(t)
        if m:
            idx = i
            number = m.group(1)
            break

    if idx == -1 or not number:
        return None

    # タイトル：右側セルの<b>優先で取得
    right_cells = cells[idx + 1 :]
    title = ""
    if right_cells:
        pieces = []
        for td in right_cells:
            txt = get_cell_text_bold_first(td)
            if txt:
                pieces.append(txt)
        title = normalize(" ".join(pieces))

    marker = f"Item {number}."
    return {
        "type": "item",
        "node": table,
        "marker": marker,
        "number": number,
        "title": title or marker,
    }

def extract_anchors(soup: BeautifulSoup):
    """
    文書順でアンカー（Item見出しテーブル & SIGNATURES）を収集。
    """
    anchors = []
    for el in soup.find_all(True):  # すべてのタグを文書順に
        # Item見出しテーブル
        if el.name == "table":
            meta = item_header_from_table(el)
            if meta:
                anchors.append(meta)
                continue

        # SIGNATURES（太字/強調）
        if el.name in ("b", "strong"):
            txt = normalize(el.get_text(" ", strip=True)).upper()
            if txt.startswith("SIGNATURE"):
                p = el.find_parent("p")
                anchors.append(
                    {
                        "type": "signatures",
                        "node": p or el,
                        "marker": "SIGNATURES",
                        "number": None,
                        "title": "SIGNATURES",
                    }
                )
    return anchors

def extract_span_fallback_anchors(soup: BeautifulSoup):
    """
    span/divタグのfont-weight:bold/700指定を頼りにItem / SIGNATURESのアンカーを抽出。
    """
    anchors = []
    rx_item_start = re.compile(r"^Item\s+(\d+(?:\.\d+)+)\.?", re.I)
    def has_bold_weight(style: str) -> bool:
        style_lower = style.lower()
        match = re.search(r"font-weight\s*:\s*([0-9]+|bold)", style_lower)
        if not match:
            return False
        value = match.group(1)
        if value == "bold":
            return True
        try:
            weight = int(value)
        except ValueError:
            return False
        return weight >= 600

    for el in soup.find_all(["span", "div"]):
        style = el.get("style")
        if not style:
            continue
        if not has_bold_weight(style):
            continue
        text = normalize(el.get_text(" ", strip=True))
        if not text:
            continue

        m = rx_item_start.search(text)
        if m:
            number = m.group(1)
            marker = f"Item {number}."
            title = normalize(rx_item_start.sub("", text)) or marker
            container = el.find_parent(["p", "div"]) if el.name == "span" else el
            anchors.append(
                {
                    "type": "item",
                    "node": container or el,
                    "marker": marker,
                    "number": number,
                    "title": title,
                }
            )
            continue

        if text.upper().startswith("SIGNATURE"):
            container = el.find_parent(["p", "div"]) if el.name == "span" else el
            anchors.append(
                {
                    "type": "signatures",
                    "node": container or el,
                    "marker": "SIGNATURES",
                    "number": None,
                    "title": "SIGNATURES",
                }
            )
    return anchors

def text_between_nodes(start_node: Tag, stop_node: Tag | None):
    """
    start_node直後から、stop_node直前までの全テキストを連結（文書順）。
    """
    chunks = []
    for obj in start_node.next_elements:
        # stop_nodeに到達したら終了
        if isinstance(obj, Tag) and stop_node is not None and obj is stop_node:
            break
        # コメントやscript/styleは無視
        if isinstance(obj, Comment):
            continue
        if isinstance(obj, Tag) and obj.name in ("script", "style"):
            continue
        if isinstance(obj, NavigableString):
            chunks.append(str(obj))
    return normalize(" ".join(chunks))

def scrape_to_rows(html: str):
    soup = BeautifulSoup(html, "lxml")
    anchors = extract_anchors(soup)
    if not anchors:
        span_anchors = extract_span_fallback_anchors(soup)
        if span_anchors:
            print("[info] using fallback span anchors (font-weight:700)")
            anchors = span_anchors
        else:
            print("[warn] no anchors found (no Item tables / SIGNATURES)")
            return []

    rows = []
    for i, a in enumerate(anchors):
        start = a["node"]
        next_node = anchors[i + 1]["node"] if i + 1 < len(anchors) else None
        text = text_between_nodes(start, next_node)
        rows.append(
            {
                "section": a["marker"],  # e.g., "Item 1.01." or "SIGNATURES"
                "title": a["title"],
                "text": text,
            }
        )
    return rows

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()

    config = load_configuration()
    connection = psycopg2.connect(**config["database_config"])

    try:
        with connection.cursor() as cursor:
            filings = fetch_targets(cursor, args.experiment_id)

        logging.info(
            "Found %d filings with evidence for experiment %d.",
            len(filings),
            args.experiment_id,
        )

        upsert_rows: List[Tuple[int, int, str, str, str, Optional[str], str, str]] = []

        for cik, accession_number, primary_document, filing_date in filings:

            url = build_filing_url(cik, accession_number, primary_document)
            logging.info("Fetching %s", url)

            try:
                html = fetch_html(url)
            except requests.RequestException as exc:
                logging.warning("Failed to fetch %s: %s", url, exc)
                continue

            sections = scrape_to_rows(html)
            for section in sections:
                m = ITEM_SECTION_REGEX.search(section['section'])
                if not m:
                    continue

                item_code = m.group(1)

                title = section['title']
                text = section['text']

                upsert_rows.append(
                    (
                        args.experiment_id,
                        cik,
                        accession_number,
                        item_code,
                        primary_document,
                        filing_date,
                        title,
                        text,
                    )
                )

            if args.delay > 0:
                time.sleep(args.delay)

        upsert_item_sections(connection, upsert_rows, args.batch_size)
        logging.info("Stored %d item sections.", len(upsert_rows))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
