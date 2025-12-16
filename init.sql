-- Enable pg_trgm extension for trigram similarity search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create accounts table
CREATE TABLE IF NOT EXISTS accounts (
  id SERIAL PRIMARY KEY,
  source VARCHAR(20) NOT NULL CHECK(source IN ('source', 'dimensions', 'salesforce')),
  
  name TEXT,
  normalized_name TEXT,
  
  phone TEXT,
  normalized_phone TEXT,
  
  website TEXT,               -- Original website OR email (for reference)
  normalized_website TEXT,    -- Normalized domain (from website OR email)
  
  billing_street TEXT,
  normalized_billing_street TEXT,
  billing_city TEXT,
  billing_postal_code TEXT,
  billing_country TEXT,
  
  raw_data JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_accounts_source ON accounts(source);

CREATE INDEX IF NOT EXISTS idx_accounts_normalized_name 
ON accounts USING gin(normalized_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_accounts_normalized_phone 
ON accounts(normalized_phone) 
WHERE normalized_phone IS NOT NULL AND normalized_phone != '';

CREATE INDEX IF NOT EXISTS idx_accounts_normalized_website 
ON accounts(normalized_website) 
WHERE normalized_website IS NOT NULL AND normalized_website != '';

CREATE INDEX IF NOT EXISTS idx_accounts_normalized_billing_street 
ON accounts(normalized_billing_street) 
WHERE normalized_billing_street IS NOT NULL AND normalized_billing_street != '';

CREATE INDEX IF NOT EXISTS idx_accounts_billing_city 
ON accounts(billing_city) 
WHERE billing_city IS NOT NULL AND billing_city != '';

CREATE INDEX IF NOT EXISTS idx_accounts_raw_data 
ON accounts USING gin(raw_data);

-- Partial indexes for faster source-specific name searches
CREATE INDEX IF NOT EXISTS idx_accounts_sf_name_trgm 
ON accounts USING gin(normalized_name gin_trgm_ops) 
WHERE source = 'salesforce';

CREATE INDEX IF NOT EXISTS idx_accounts_dim_name_trgm 
ON accounts USING gin(normalized_name gin_trgm_ops) 
WHERE source = 'dimensions';