-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Raw prices table
CREATE TABLE IF NOT EXISTS raw_prices (
    id          UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL,
    price_eur   NUMERIC(10, 2) NOT NULL,
    currency    VARCHAR(10) NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Raw generation table
CREATE TABLE IF NOT EXISTS raw_generation (
    id              UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    energy_type     VARCHAR(50) NOT NULL,
    quantity_mw     NUMERIC(12, 2) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Raw load table
CREATE TABLE IF NOT EXISTS raw_load (
    id          UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL,
    load_mw     NUMERIC(12, 2) NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for time-based queries
CREATE INDEX IF NOT EXISTS idx_prices_timestamp 
    ON raw_prices(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_generation_timestamp 
    ON raw_generation(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_load_timestamp 
    ON raw_load(timestamp DESC);