CREATE TABLE IF NOT EXISTS company_recent_filings (
    cik BIGINT NOT NULL,
    accession_number TEXT NOT NULL,
    form TEXT NOT NULL,
    filing_date DATE,
    primary_document TEXT NOT NULL,
    items TEXT,
    PRIMARY KEY (cik, accession_number)
);

CREATE INDEX IF NOT EXISTS idx_company_recent_filings_form ON company_recent_filings (form);
CREATE INDEX IF NOT EXISTS idx_company_recent_filings_filing_date ON company_recent_filings (filing_date);
