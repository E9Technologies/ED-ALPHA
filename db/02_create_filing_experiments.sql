CREATE TABLE IF NOT EXISTS filing_experiments (
    id BIGSERIAL PRIMARY KEY,
    predict_date DATE NOT NULL,
    horizon_days INTEGER NOT NULL,
    item_codes TEXT[],
    neg_multiplier INTEGER NOT NULL,
    seed INTEGER,
    config JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS filing_experiment_labels (
    experiment_id BIGINT NOT NULL REFERENCES filing_experiments (id) ON DELETE CASCADE,
    cik BIGINT NOT NULL,
    label SMALLINT NOT NULL,
    PRIMARY KEY (experiment_id, cik)
);

CREATE INDEX IF NOT EXISTS idx_filing_experiment_labels_label ON filing_experiment_labels (label);
CREATE INDEX IF NOT EXISTS idx_filing_experiment_labels_cik ON filing_experiment_labels (cik);
