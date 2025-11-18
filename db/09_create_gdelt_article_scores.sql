ALTER TABLE IF EXISTS gdelt_ma_articles RENAME TO gdelt_articles;
ALTER TABLE IF EXISTS gdelt_ma_article_scores RENAME TO gdelt_article_scores;
ALTER INDEX IF EXISTS idx_gdelt_ma_article_scores_score RENAME TO idx_gdelt_article_scores_score;
ALTER INDEX IF EXISTS idx_gdelt_ma_article_scores_cik RENAME TO idx_gdelt_article_scores_cik;

CREATE TABLE IF NOT EXISTS gdelt_articles (
    article_url TEXT PRIMARY KEY,
    title TEXT,
    snippet TEXT,
    last_fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fetch_error TEXT
);

CREATE TABLE IF NOT EXISTS gdelt_scoring_runs (
    id BIGSERIAL PRIMARY KEY,
    experiment_id BIGINT NOT NULL REFERENCES filing_experiments (id) ON DELETE CASCADE,
    min_days_before INTEGER,
    max_days_before INTEGER,
    batch_size INTEGER,
    model_name TEXT,
    run_label TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gdelt_article_scores (
    run_id BIGINT NOT NULL REFERENCES gdelt_scoring_runs (id) ON DELETE CASCADE,
    experiment_id BIGINT NOT NULL REFERENCES filing_experiments (id) ON DELETE CASCADE,
    cik BIGINT NOT NULL,
    gkg_record_id TEXT NOT NULL,
    time_str TEXT NOT NULL,
    article_url TEXT NOT NULL REFERENCES gdelt_articles (article_url) ON DELETE CASCADE,
    label SMALLINT,
    llm_score SMALLINT NOT NULL,
    llm_reason TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, cik, gkg_record_id)
);

CREATE INDEX IF NOT EXISTS idx_gdelt_article_scores_score
    ON gdelt_article_scores (llm_score);

CREATE INDEX IF NOT EXISTS idx_gdelt_article_scores_cik
    ON gdelt_article_scores (cik);

CREATE INDEX IF NOT EXISTS idx_gdelt_article_scores_run
    ON gdelt_article_scores (run_id);

CREATE TABLE IF NOT EXISTS gdelt_run_cik_scores (
    run_id BIGINT NOT NULL REFERENCES gdelt_scoring_runs (id) ON DELETE CASCADE,
    experiment_id BIGINT NOT NULL REFERENCES filing_experiments (id) ON DELETE CASCADE,
    cik BIGINT NOT NULL,
    label SMALLINT,
    total_score INTEGER NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, cik)
);

CREATE INDEX IF NOT EXISTS idx_gdelt_run_cik_scores_label
    ON gdelt_run_cik_scores (label);

CREATE TABLE IF NOT EXISTS gdelt_run_metrics (
    run_id BIGINT NOT NULL REFERENCES gdelt_scoring_runs (id) ON DELETE CASCADE,
    k INTEGER NOT NULL,
    top_ciks BIGINT[] NOT NULL,
    top_scores INTEGER[] NOT NULL,
    positives_in_top INTEGER NOT NULL,
    total_positives INTEGER NOT NULL,
    recall NUMERIC NOT NULL,
    precision NUMERIC NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, k)
);
