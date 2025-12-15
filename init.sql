-- init.sql

-- Включаем расширение для триграмного поиска
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Создаем таблицу accounts
CREATE TABLE IF NOT EXISTS accounts (
  id SERIAL PRIMARY KEY,
  source VARCHAR(20) NOT NULL CHECK(source IN ('source', 'dimensions', 'salesforce')),
  name TEXT,
  normalized_name TEXT,
  phone TEXT,
  normalized_phone TEXT,
  website TEXT,
  normalized_website TEXT,
  billing_street TEXT,
  normalized_billing_street TEXT,
  billing_city TEXT,
  billing_postal_code TEXT,
  billing_country TEXT,
  raw_data JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем индексы
CREATE INDEX IF NOT EXISTS idx_accounts_source ON accounts(source);
CREATE INDEX IF NOT EXISTS idx_accounts_normalized_name ON accounts USING gin(normalized_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_accounts_normalized_phone ON accounts(normalized_phone);
CREATE INDEX IF NOT EXISTS idx_accounts_normalized_website ON accounts(normalized_website);
CREATE INDEX IF NOT EXISTS idx_accounts_normalized_billing_street ON accounts(normalized_billing_street);
CREATE INDEX IF NOT EXISTS idx_accounts_billing_city ON accounts(billing_city);

-- Индекс для быстрого поиска по raw_data (JSONB)
CREATE INDEX IF NOT EXISTS idx_accounts_raw_data ON accounts USING gin(raw_data);