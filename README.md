<p align="center">
  <img src="docs/images/ED-Alpha_logo.png" alt="ED-Alpha logo" width="360" />
</p>

<p align="center">
  <img src="docs/images/demo.gif" alt="ED-Alpha demo" width="760" />
</p>

# ED-Alpha

ED-Alpha is a Python 3.12.11 pipeline that ingests SEC filings and GDELT news, links articles to companies, scores event importance with LLMs, and evaluates recall@k / precision@k against actual filing events. The project combines a batch data pipeline with a lightweight demo UI.

## Overview

- Pipeline steps: (1) sync SEC company data and filings to build labels, (2) ingest GDELT GKG news and link them to companies, (3) score article importance with an LLM, (4) aggregate run-level scores to extract likely events and measure metrics.
- Tech stack: Python 3.12.11, PostgreSQL, OpenAI Chat Completions API, FastAPI demo backend, Vite/PNPM frontend.

<p align="center">
  <img src="docs/images/platform.png" alt="Platform architecture" width="240" />
</p>

## Batch Pipeline

### Prerequisites

- Python 3.12.11
- PostgreSQL with INSERT/UPDATE privileges
- `psql` (or another SQL client) to apply schemas

### Setup

1) Create and activate a virtual environment (optional).
```bash
python3.12 -m venv .venv
source .venv/bin/activate
```
2) Install dependencies.
```bash
pip install -r requirements.txt
```
3) Prepare environment variables.
```bash
cp .env.example .env
```
- `USER_EMAIL` is required for the SEC User-Agent header (`Ed-Alpha/0.1 (<your email>)`).
- `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` should point to your PostgreSQL instance.
- `OPENAI_API_KEY` is required for scoring news; override `LLM_MODEL` if needed (e.g., `gpt-4o-mini`).
4) Apply database schemas (first run only).
```bash
psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -f sql/create_company_tickers.sql
psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -f sql/create_recent_filings.sql
psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -f sql/create_gdelt_gkg_records.sql
psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -f sql/create_gdelt_gkg_company_links.sql
psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -f sql/create_filing_experiments.sql
psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -f sql/create_gdelt_article_scores.sql
```

### How it Works (main commands)

Run the scripts below in order to populate and evaluate the dataset. CLI options from `JPN_README.md` remain available for customization.

1) Sync company tickers.
```bash
python src/fetch_company_tickers.py
```
2) Fetch latest filings from SEC bulk data.
```bash
python src/fetch_recent_filings.py
```
3) Pull GDELT master times (e.g., hourly intervals).
```bash
python src/fetch_gdelt_master_times.py --start-date 20250101 --end-date 20250131
```
4) Download and store GDELT GKG records for the desired window.
```bash
python src/fetch_gdelt_gkg.py --start-time 202501010000 --end-time 202501020000
```
5) Link GDELT organizations to companies.
```bash
python src/link_gdelt_gkg_companies.py
```
6) Generate labels for filings by Item code.
```bash
python src/generate_labels.py \
  --predict-date 20251001 \
  --horizon-days 30 \
  --item-codes 1.01 1.02 1.03 2.01 2.03 2.04 3.01 3.02 3.03 4.02 5.01 5.03 8.01
```
7) Score linked GDELT news with the LLM.
```bash
python src/score_gdelt_news.py \
  --experiment-id 123 \
  --min-days-before 60 \
  --max-days-before 1 \
  --batch-size 200 \
  --run-label "baseline-score" \
  --model gpt-4o-mini
```
8) Aggregate per-run scores by CIK.
```bash
python src/aggregate_gdelt_run_scores.py --run-id 42
```
9) Calculate recall@k / precision@k metrics.
```bash
python src/calc_gdelt_run_metrics.py --run-id 42 --k-values 10 25 50 100
```
10) (Optional) Scrape filing item sections for inspection.
```bash
python src/scrape_filing_items.py --experiment-id 123 --delay 0.5
```

Tip: use `config/predict_config.example.json` as a template and pass `--config` to re-use the same parameters across runs. Add `--dry-run` to reporting scripts to log without writing.

## UI

<p align="center">
  <img src="docs/images/demo.png" alt="UI walkthrough" width="240" />
</p>

The demo UI shows filing predictions and news scores backed by the batch outputs.

### Backend
```bash
cd demo/backend
uvicorn main:app --reload --port 5000
```

### Frontend
```bash
cd demo/frontend
pnpm install
pnpm dev
```

Once both services are running, open the frontend URL shown by `pnpm dev` to interact with the demo.

---
