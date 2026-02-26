CREATE DATABASE dwh;
\c dwh;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS dm;


-- СЫРОЙ СЛОЙ (RAW)
CREATE TABLE IF NOT EXISTS raw.crypto_snapshot (
    id SERIAL PRIMARY KEY,
    snapshot_time TIMESTAMP NOT NULL,
    coin_id VARCHAR(50) NOT NULL,
    price_usd NUMERIC(20, 4),
    market_cap_usd NUMERIC(30, 4),
    volume_24h_usd NUMERIC(30, 4),
    price_change_24h NUMERIC(10, 2),
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_raw_time ON raw.crypto_snapshot(snapshot_time);

-- ДЕТАЛЬНЫЙ СЛОЙ (DDS)
CREATE TABLE IF NOT EXISTS dds.dim_coin (
    coin_id SERIAL PRIMARY KEY,
    coin_code VARCHAR(50) UNIQUE NOT NULL,
    coin_name VARCHAR(100),
    symbol VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dds.fact_market_data (
    fact_id SERIAL PRIMARY KEY,
    coin_id INTEGER REFERENCES dds.dim_coin(coin_id),
    snapshot_time TIMESTAMP NOT NULL,
    price_usd NUMERIC(20, 4),
    market_cap_usd NUMERIC(30, 4),
    volume_24h_usd NUMERIC(30, 4),
    price_change_24h NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(coin_id, snapshot_time)
);

-- ВИТРИНЫ (DM)
CREATE TABLE IF NOT EXISTS dm.market_summary (
    snapshot_time TIMESTAMP PRIMARY KEY,
    total_market_cap NUMERIC(30, 4),
    btc_price NUMERIC(20, 4),
    btc_dominance NUMERIC(10, 2),
    total_volume NUMERIC(30, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dm.quality_checks (
    check_id SERIAL PRIMARY KEY,
    check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    check_name VARCHAR(100),
    status VARCHAR(20),
    details TEXT
);
