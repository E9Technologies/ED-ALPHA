CREATE TABLE IF NOT EXISTS gdelt_gkg_company_links (
    time_str TEXT NOT NULL,
    gkg_record_id TEXT NOT NULL,
    cik BIGINT NOT NULL REFERENCES company_profiles (cik) ON DELETE CASCADE,
    PRIMARY KEY (time_str, gkg_record_id, cik)
);

CREATE INDEX IF NOT EXISTS idx_gdelt_gkg_company_links_cik ON gdelt_gkg_company_links (cik);
CREATE INDEX IF NOT EXISTS idx_gdelt_gkg_company_links_gkg_record ON gdelt_gkg_company_links (gkg_record_id);
