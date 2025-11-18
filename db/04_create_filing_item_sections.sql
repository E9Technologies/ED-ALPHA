CREATE TABLE IF NOT EXISTS filing_item_sections (
    accession_number TEXT NOT NULL,
    primary_document TEXT NOT NULL,
    cik BIGINT NOT NULL,
    filing_date DATE,
    item_code TEXT NOT NULL,
    section_title TEXT,
    text_content TEXT,
    PRIMARY KEY (accession_number, item_code)
);

CREATE INDEX IF NOT EXISTS idx_filing_item_sections_cik ON filing_item_sections (cik);
