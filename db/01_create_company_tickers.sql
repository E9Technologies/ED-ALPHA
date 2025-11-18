CREATE TABLE IF NOT EXISTS company_profiles (
    cik BIGINT PRIMARY KEY,
    title TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS company_tickers (
    cik BIGINT NOT NULL REFERENCES company_profiles (cik) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    PRIMARY KEY (cik, ticker)
);

CREATE INDEX IF NOT EXISTS idx_company_tickers_ticker ON company_tickers (ticker);
