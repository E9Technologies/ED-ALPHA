CREATE TABLE IF NOT EXISTS filing_item_sections (
    id BIGSERIAL PRIMARY KEY,
    experiment_id BIGINT NOT NULL REFERENCES filing_experiments (id) ON DELETE CASCADE,
    cik BIGINT NOT NULL,
    accession_number TEXT NOT NULL,
    item_code TEXT NOT NULL,
    primary_document TEXT NOT NULL,
    filing_date DATE,
    title TEXT,
    body TEXT,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cik, accession_number, item_code)
);

CREATE INDEX IF NOT EXISTS idx_filing_item_sections_cik ON filing_item_sections (cik);
