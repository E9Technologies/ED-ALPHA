CREATE TABLE IF NOT EXISTS gdelt_gkg_records (
    time_str TEXT NOT NULL,
    line_num INTEGER NOT NULL,
    gkg_record_id TEXT NOT NULL,
    v2_document_identifier TEXT,
    v1_themes TEXT,
    v1_organizations TEXT,
    PRIMARY KEY (time_str, line_num)
);

CREATE INDEX IF NOT EXISTS idx_gdelt_gkg_records_gkg_record_id ON gdelt_gkg_records (gkg_record_id);
CREATE INDEX IF NOT EXISTS idx_gdelt_gkg_records_v2_document_identifier ON gdelt_gkg_records (v2_document_identifier);
