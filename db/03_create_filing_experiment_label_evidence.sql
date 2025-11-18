CREATE TABLE IF NOT EXISTS filing_experiment_label_evidence (
    id BIGSERIAL PRIMARY KEY,
    experiment_id BIGINT NOT NULL REFERENCES filing_experiments (id) ON DELETE CASCADE,
    cik BIGINT NOT NULL,
    accession_number TEXT NOT NULL,
    primary_document TEXT NOT NULL,
    filing_date DATE,
    matching_item_code TEXT NOT NULL,
    UNIQUE (experiment_id, cik, accession_number, matching_item_code)
);

CREATE INDEX IF NOT EXISTS idx_filing_experiment_label_evidence_item_code
    ON filing_experiment_label_evidence (matching_item_code);
