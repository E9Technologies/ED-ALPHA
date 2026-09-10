"""Export the paper's averaged rankings from PostgreSQL without writing to it."""
import argparse
import hashlib
import json
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from pathlib import Path

import psycopg
from dotenv import dotenv_values
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

EXPERIMENT_ID = 20
PAPER_COMMIT = '434e376c6a6b52f36683d70ab6b75c305cf37795'
PAPER_RUNS = {
    50: ('anthropic/claude-sonnet-4.5', 'Sonnet 4.5', [4, 18, 30]),
    52: ('openai/gpt-5', 'GPT-5', [4, 19, 32]),
    54: ('google/gemini-2.5-pro', 'Gemini Pro 2.5', [5, 20, 34]),
}
K_VALUES = [5, 25, 50]


def connect(env_file):
    env = dotenv_values(env_file, interpolate=False)
    config = conninfo_to_dict(env['DATABASE_URL']) if env.get('DATABASE_URL') else {
        field: env[key] for key, field in [
            ('PGHOST', 'host'), ('PGPORT', 'port'), ('PGDATABASE', 'dbname'),
            ('PGUSER', 'user'), ('PGPASSWORD', 'password'),
        ] if env.get(key)
    }
    if not all(config.get(key) for key in ['host', 'dbname', 'user']):
        raise ValueError('Missing database connection settings')
    config['sslmode'] = env.get('PGSSLMODE') or env.get('PGSSL') or config.get('sslmode', 'require')
    if env.get('PGSSLROOTCERT') or env.get('PGSSL_ROOT_CERT'):
        config['sslrootcert'] = env.get('PGSSLROOTCERT') or env['PGSSL_ROOT_CERT']
    config.update(connect_timeout=10, application_name='ed-alpha-paper-snapshot-export',
                  options='-c default_transaction_read_only=on -c statement_timeout=20000 '
                          '-c lock_timeout=2000 -c idle_in_transaction_session_timeout=45000')
    conn = psycopg.connect(**config, row_factory=dict_row)
    conn.read_only = True
    conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
    return conn


def fetch(conn, sql, params=None):
    return conn.execute(sql, params).fetchall()


def export(conn):
    assert conn.execute("SHOW transaction_read_only").fetchone()['transaction_read_only'] == 'on'
    experiment = fetch(conn, '''
        SELECT id,predict_date,horizon_days,item_codes,neg_multiplier,seed,created_at
        FROM public.filing_experiments WHERE id=%s
    ''', (EXPERIMENT_ID,))[0]
    experiment['run_ids'] = list(PAPER_RUNS)
    end_date = experiment['predict_date'] + timedelta(days=experiment['horizon_days'] - 1)
    labels = fetch(conn, '''
        SELECT l.cik,l.label,p.title AS company_name
        FROM public.filing_experiment_labels l
        LEFT JOIN public.company_profiles p ON p.cik=l.cik
        WHERE l.experiment_id=%s ORDER BY l.cik
    ''', (EXPERIMENT_ID,))
    assert len(labels) == 100 and sum(r['label'] for r in labels) == 50
    # Use the actual evidence for the experiment labels, not any nearby 8-K.
    event_rows = fetch(conn, '''
        SELECT e.cik,e.accession_number,e.primary_document,e.filing_date,
               ARRAY_AGG(DISTINCT e.matching_item_code ORDER BY e.matching_item_code) AS items
        FROM public.filing_experiment_label_evidence e
        JOIN public.filing_experiment_labels l ON l.experiment_id=e.experiment_id AND l.cik=e.cik
        WHERE e.experiment_id=%s AND l.label=1
          AND e.filing_date BETWEEN %s AND %s
          AND e.matching_item_code=ANY(%s)
        GROUP BY e.cik,e.accession_number,e.primary_document,e.filing_date
        ORDER BY e.cik,e.filing_date,e.accession_number
    ''', (EXPERIMENT_ID, experiment['predict_date'], end_date, experiment['item_codes']))
    events = {}
    for event in event_rows:
        cik = event.pop('cik')
        event['form'] = '8-K'
        event['url'] = (f"https://www.sec.gov/Archives/edgar/data/{cik}/"
                        f"{event['accession_number'].replace('-', '')}/{event['primary_document']}")
        events.setdefault(cik, event)
    assert set(events) == {r['cik'] for r in labels if r['label'] == 1}
    now = datetime.now(timezone.utc).isoformat()
    runs = []
    for run_id, (model_name, model_label, expected_tp) in PAPER_RUNS.items():
        header = fetch(conn, 'SELECT model_name,experiment_id FROM public.gdelt_scoring_runs WHERE id=%s', (run_id,))[0]
        assert header == {'model_name': model_name, 'experiment_id': EXPERIMENT_ID}
        scores = fetch(conn, '''
            SELECT cik,COUNT(*) AS article_count,COUNT(llm_score) AS score_count,
                   SUM(llm_score) AS score_sum,MIN(time_str) AS first_news,MAX(time_str) AS last_news
            FROM public.gdelt_article_scores WHERE run_id=%s GROUP BY cik ORDER BY cik
        ''', (run_id,))
        assert all(r['article_count'] == r['score_count'] for r in scores)
        score_map = {r['cik']: r for r in scores}
        assert set(score_map).issubset({r['cik'] for r in labels})
        # Break evidence ties deterministically, and keep the highest-scored three articles.
        evidence_rows = fetch(conn, '''
            SELECT cik,llm_score,llm_reason AS summary,article_url AS url,title,evaluated_at
            FROM (
                SELECT s.*,COALESCE(NULLIF(a.title,''),s.article_url) AS title,
                       ROW_NUMBER() OVER (PARTITION BY s.cik ORDER BY s.llm_score DESC,
                           s.evaluated_at DESC,s.article_url,s.gkg_record_id) AS position
                FROM public.gdelt_article_scores s
                LEFT JOIN public.gdelt_articles a ON a.article_url=s.article_url
                WHERE s.run_id=%s
            ) evidence WHERE position<=3 ORDER BY cik,position
        ''', (run_id,))
        evidence = {}
        for article in evidence_rows:
            evidence.setdefault(article.pop('cik'), []).append(article)
        rows = []
        for label in labels:
            score = score_map.get(label['cik'])
            count = score['article_count'] if score else 0
            score_sum = score['score_sum'] if score else 0
            rows.append({**label, 'article_count': count, 'score_sum': score_sum,
                         'average_score': float(Fraction(score_sum, count)) if count else 0,
                         'evidence': evidence.get(label['cik'], []),
                         'event': events.get(label['cik'])})
        rows.sort(key=lambda r: (-Fraction(r['score_sum'], r['article_count']) if r['article_count'] else 0, r['cik']))
        metrics = []
        for k in K_VALUES:
            top = rows[:k]
            tp = sum(r['label'] for r in top)
            metrics.append({'k': k, 'top_ciks': [r['cik'] for r in top],
                            'top_scores': [r['average_score'] for r in top],
                            'positives_in_top': tp, 'total_positives': 50,
                            'precision': tp / k, 'recall': tp / 50, 'computed_at': now})
        assert [m['positives_in_top'] for m in metrics] == expected_tp, f'Paper results changed for Run {run_id}'
        runs.append({'id': run_id, 'model_name': model_name, 'model_label': model_label,
                     'metrics': metrics, 'topResults': rows})
    return {
        'provenance': {'paper_commit': PAPER_COMMIT, 'exported_at': now,
                       'aggregation': 'mean_article_score',
                       'formula': 'SUM(llm_score) / COUNT(llm_score)',
                       'no_articles_score': 0, 'tie_break': 'cik_ascending',
                       'event_window_end': end_date,
                       'source_digest': hashlib.sha256(json.dumps(
                           [(run['id'], run['topResults']) for run in runs],
                           default=str, sort_keys=True).encode()).hexdigest()},
        'experiment': experiment, 'runs': runs,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parents[1] / 'data/demo-data.json')
    args = parser.parse_args()
    with connect(args.env_file.expanduser()) as conn:
        snapshot = export(conn)
        conn.rollback()
    # Publish the local file only after every paper result and event label passes.
    temporary = args.output.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False, default=lambda value: value.isoformat()) + '\n')
    temporary.replace(args.output)
    print('Exported 3 paper runs, 100 companies each, and verified K=5/25/50. Database read-only.')


if __name__ == '__main__':
    try:
        main()
    except psycopg.Error as error:
        # Connection errors can include the connection string. Do not echo them.
        raise SystemExit(f'Database export failed ({type(error).__name__}, SQLSTATE {error.sqlstate or "unavailable"}); connection details withheld.') from None
