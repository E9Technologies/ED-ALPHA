CREATE TABLE IF NOT EXISTS gdelt_master_times (
    time_str TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    file_size_bytes BIGINT,
    md5_hash TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
