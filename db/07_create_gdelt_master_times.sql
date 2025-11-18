CREATE TABLE IF NOT EXISTS gdelt_master_times (
    time_str TEXT PRIMARY KEY,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    download_attempts INTEGER NOT NULL DEFAULT 0,
    last_download_attempt TIMESTAMPTZ,
    last_download_error TEXT
);
