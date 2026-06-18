-- File: infrastructure/postgres/init.sql

CREATE TYPE article_status AS ENUM ('pending', 'processing', 'completed', 'failed');

CREATE TABLE IF NOT EXISTS articles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_content TEXT NOT NULL,
    stemmed_content TEXT,
    wordcloud_url TEXT, -- Hanya menyimpan referensi/URL gambar
    status article_status DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index untuk mempercepat query saat fitur Forwarding mencari artikel
CREATE INDEX idx_articles_status ON articles(status);
