-- Cinema 360 PostgreSQL Schema
-- Data Warehouse initialization script

-- Fact table: Movie Analytics
CREATE TABLE IF NOT EXISTS movies_analytics (
    tconst VARCHAR(20) PRIMARY KEY,        -- IMDb ID
    primary_title VARCHAR(500),
    original_title VARCHAR(500),
    title_type VARCHAR(50),
    start_year INTEGER,
    runtime_minutes INTEGER,
    genres VARCHAR(200),

    -- Ratings
    imdb_rating DECIMAL(3, 1),
    num_votes INTEGER,

    -- Financial (from TMDB)
    budget BIGINT,
    revenue BIGINT,
    profit BIGINT,
    roi DECIMAL(10, 2),

    -- Internal
    internal_rating DECIMAL(3, 1),
    rating_diff DECIMAL(3, 1),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for common queries
CREATE INDEX IF NOT EXISTS idx_start_year ON movies_analytics(start_year);
CREATE INDEX IF NOT EXISTS idx_genres ON movies_analytics(genres);
CREATE INDEX IF NOT EXISTS idx_profit ON movies_analytics(profit);
