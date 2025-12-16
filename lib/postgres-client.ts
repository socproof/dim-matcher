// lib/postgres-client.ts

import { 
  normalizeCompanyName, 
  normalizePhone, 
  normalizeWebsite, 
  normalizeAddress,
  extractEmailDomain 
} from './normalize';

export interface AccountDBRow {
  id: number;
  source: string;
  name: string | null;
  normalized_name: string | null;
  phone: string | null;
  normalized_phone: string | null;
  website: string | null;
  normalized_website: string | null;
  billing_street: string | null;
  normalized_billing_street: string | null;
  billing_city: string | null;
  billing_postal_code: string | null;
  billing_country: string | null;
  raw_data: any;
  created_at: string;
}

interface NormalizedAccount {
  Name: string;
  NormalizedName: string;
  Phone: string;
  NormalizedPhone: string;
  Website: string;
  NormalizedWebsite: string;
  BillingStreet: string;
  NormalizedBillingStreet: string;
  BillingCity: string;
  BillingPostalCode: string;
  BillingCountry: string;
}

interface QueryResult<T> {
  rows: T[];
  rowCount: number;
}

// Sanitize value to remove null bytes and invalid UTF-8 characters
function sanitizeValue(value: any): any {
  if (value === null || value === undefined) {
    return null;
  }
  
  if (typeof value === 'string') {
    // Remove null bytes and control characters except newlines and tabs
    return value
      .replace(/\u0000/g, '') // Remove null bytes
      .replace(/[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]/g, ''); // Remove other control chars
  }
  
  if (typeof value === 'object') {
    // Recursively sanitize objects and arrays
    if (Array.isArray(value)) {
      return value.map(item => sanitizeValue(item));
    }
    
    const sanitized: any = {};
    for (const [key, val] of Object.entries(value)) {
      sanitized[key] = sanitizeValue(val);
    }
    return sanitized;
  }
  
  return value;
}

// Safely stringify JSON with sanitization
function safeJsonStringify(obj: any): string {
  const sanitized = sanitizeValue(obj);
  return JSON.stringify(sanitized);
}

class PostgresClient {
  private baseUrl: string;

  constructor() {
    this.baseUrl = '/api/postgres';
  }

  private async query<T = QueryResult<any>>(sql: string, params: any[] = []): Promise<T> {
    const response = await fetch(this.baseUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql, params }),
    });

    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Database query failed');
    }

    return response.json();
  }

  private normalizeAccount(account: any): NormalizedAccount {
    const name = sanitizeValue(account.Name || account.name || account.cuname || '');
    const phone = sanitizeValue(account.Phone || account.phone || account.cuphone || '');
    const website = sanitizeValue(account.Website || account.website || '');
    const email = sanitizeValue(account.Email || account.email || account.cu_email || '');
    const billingStreet = sanitizeValue(account.BillingStreet || account.billing_street || account.cuaddress || '');
    const billingCity = sanitizeValue(account.BillingCity || account.billing_city || account.cu_address_user1 || '');
    const billingPostalCode = sanitizeValue(account.BillingPostalCode || account.billing_postal_code || account.cupostcode || '');
    const billingCountry = sanitizeValue(account.BillingCountry || account.billing_country || account.cu_country || '');

    // Normalize website, or extract domain from email if no website
    let normalizedWebsite = '';
    if (website) {
      normalizedWebsite = normalizeWebsite(website);
    } else if (email) {
      // No website - try to get domain from email
      const emailDomain = extractEmailDomain(email);
      if (emailDomain) {
        normalizedWebsite = emailDomain;
      }
    }

    return {
      Name: name,
      NormalizedName: normalizeCompanyName(name),
      Phone: phone,
      NormalizedPhone: normalizePhone(phone, billingCountry),
      Website: website || email, // Store original website or email for reference
      NormalizedWebsite: normalizedWebsite,
      BillingStreet: billingStreet,
      NormalizedBillingStreet: normalizeAddress(billingStreet),
      BillingCity: billingCity,
      BillingPostalCode: billingPostalCode,
      BillingCountry: billingCountry,
    };
  }

  public async checkConnection(): Promise<boolean> {
    try {
      const result = await this.query<{ rows: any[] }>('SELECT 1 as connected');
      return result.rows?.[0]?.connected === 1;
    } catch (error) {
      console.error('[PostgresClient] Connection check failed:', error);
      return false;
    }
  }

  public async clearAllAccounts(): Promise<void> {
    await this.query('TRUNCATE TABLE accounts RESTART IDENTITY');
  }

  public async clearAccountsBySource(source: 'source' | 'dimensions' | 'salesforce'): Promise<void> {
    await this.query('DELETE FROM accounts WHERE source = $1', [source]);
  }

  public async insertAccount(
    source: 'source' | 'dimensions' | 'salesforce',
    account: any
  ): Promise<void> {
    const normalized = this.normalizeAccount(account);

    const sql = `
      INSERT INTO accounts (
        source, name, normalized_name, 
        phone, normalized_phone,
        website, normalized_website,
        billing_street, normalized_billing_street,
        billing_city, billing_postal_code, billing_country,
        raw_data
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    `;

    await this.query(sql, [
      source,
      sanitizeValue(normalized.Name),
      sanitizeValue(normalized.NormalizedName),
      sanitizeValue(normalized.Phone),
      sanitizeValue(normalized.NormalizedPhone),
      sanitizeValue(normalized.Website),
      sanitizeValue(normalized.NormalizedWebsite),
      sanitizeValue(normalized.BillingStreet),
      sanitizeValue(normalized.NormalizedBillingStreet),
      sanitizeValue(normalized.BillingCity),
      sanitizeValue(normalized.BillingPostalCode),
      sanitizeValue(normalized.BillingCountry),
      safeJsonStringify(account)
    ]);
  }

  public async insertAccountsBatch(
    source: 'source' | 'dimensions' | 'salesforce',
    accounts: any[]
  ): Promise<void> {
    if (accounts.length === 0) return;

    console.log(`[PostgresClient] Inserting ${accounts.length} ${source} accounts...`);

    const values: any[] = [];
    const placeholders: string[] = [];
    let paramIndex = 1;

    for (const account of accounts) {
      const normalized = this.normalizeAccount(account);
      
      placeholders.push(`($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6}, $${paramIndex + 7}, $${paramIndex + 8}, $${paramIndex + 9}, $${paramIndex + 10}, $${paramIndex + 11}, $${paramIndex + 12})`);
      
      values.push(
        source,
        sanitizeValue(normalized.Name),
        sanitizeValue(normalized.NormalizedName),
        sanitizeValue(normalized.Phone),
        sanitizeValue(normalized.NormalizedPhone),
        sanitizeValue(normalized.Website),
        sanitizeValue(normalized.NormalizedWebsite),
        sanitizeValue(normalized.BillingStreet),
        sanitizeValue(normalized.NormalizedBillingStreet),
        sanitizeValue(normalized.BillingCity),
        sanitizeValue(normalized.BillingPostalCode),
        sanitizeValue(normalized.BillingCountry),
        safeJsonStringify(account) // Use safe stringify
      );
      
      paramIndex += 13;
    }

    const sql = `
      INSERT INTO accounts (
        source, name, normalized_name,
        phone, normalized_phone,
        website, normalized_website,
        billing_street, normalized_billing_street,
        billing_city, billing_postal_code, billing_country,
        raw_data
      ) VALUES ${placeholders.join(', ')}
    `;

    await this.query(sql, values);
    console.log(`[PostgresClient] Successfully inserted ${accounts.length} ${source} accounts`);
  }

  public async getAccountCounts(): Promise<{ source: number; dimensions: number; salesforce: number, total: number; }> {
    const result = await this.query<{ rows: { source: string; count: string }[] }>(`
      SELECT source, COUNT(*) as count 
      FROM accounts 
      GROUP BY source
    `);

    const counts = { source: 0, dimensions: 0, salesforce: 0, total: 0 };
    for (const row of result.rows || []) {
      if (row.source in counts) {
        counts[row.source as keyof typeof counts] = parseInt(row.count);
      }
    }

     counts.total = counts.source + counts.dimensions + counts.salesforce;

    return counts;
  }

  public async getTotalSourceCount(): Promise<number> {
    const result = await this.query<{ rows: { count: string }[] }>(`
      SELECT COUNT(*) as count FROM accounts WHERE source = 'source'
    `);
    return parseInt(result.rows?.[0]?.count || '0');
  }

  public async getSourceAccountsChunk(limit: number, offset: number): Promise<AccountDBRow[]> {
    const result = await this.query<{ rows: AccountDBRow[] }>(`
      SELECT * FROM accounts 
      WHERE source = 'source' 
      ORDER BY id 
      LIMIT $1 OFFSET $2
    `, [limit, offset]);
    return result.rows || [];
  }

  public async findPotentialMatches(
    account: any,
    targetSource: 'dimensions' | 'salesforce' = 'salesforce',
    limit: number = 50
  ): Promise<AccountDBRow[]> {
    const normalized = this.normalizeAccount(account);

    const hasName = normalized.NormalizedName && normalized.NormalizedName.length > 2;
    const hasPhone = normalized.NormalizedPhone && normalized.NormalizedPhone.length > 5;
    const hasWebsite = normalized.NormalizedWebsite && normalized.NormalizedWebsite.length > 3;

    if (!hasName && !hasPhone && !hasWebsite) {
      return [];
    }

    const unionParts: string[] = [];
    const params: any[] = [];
    let paramIdx = 1;

    const selectFields = `
      id, source, name, normalized_name, phone, normalized_phone,
      website, normalized_website,
      billing_street, normalized_billing_street,
      billing_city, billing_postal_code, billing_country, 
      raw_data, created_at
    `;

    // Priority 1: Exact phone match (most reliable)
    if (hasPhone) {
      unionParts.push(`
        (SELECT ${selectFields}, 100 as priority
         FROM accounts 
         WHERE source = $${paramIdx} AND normalized_phone = $${paramIdx + 1}
         LIMIT 5)
      `);
      params.push(targetSource, normalized.NormalizedPhone);
      paramIdx += 2;
    }

    // Priority 2: Website/domain match (includes email domain!)
    if (hasWebsite) {
      unionParts.push(`
        (SELECT ${selectFields}, 95 as priority
         FROM accounts 
         WHERE source = $${paramIdx} AND normalized_website = $${paramIdx + 1}
         LIMIT 5)
      `);
      params.push(targetSource, normalized.NormalizedWebsite);
      paramIdx += 2;
    }

    // Priority 3: Name similarity (uses GIN trigram index)
    if (hasName) {
      unionParts.push(`
        (SELECT ${selectFields}, (similarity(normalized_name, $${paramIdx + 1}) * 80)::int as priority
         FROM accounts 
         WHERE source = $${paramIdx} 
           AND normalized_name % $${paramIdx + 1}
         ORDER BY normalized_name <-> $${paramIdx + 1}
         LIMIT 20)
      `);
      params.push(targetSource, normalized.NormalizedName);
      paramIdx += 2;
    }

    if (unionParts.length === 0) {
      return [];
    }

    const sql = `
      WITH candidates AS (
        ${unionParts.join(' UNION ALL ')}
      )
      SELECT DISTINCT ON (id) 
             id, source, name, normalized_name, phone, normalized_phone,
             website, normalized_website,
             billing_street, normalized_billing_street,
             billing_city, billing_postal_code, billing_country,
             raw_data, created_at
      FROM candidates
      ORDER BY id, priority DESC
      LIMIT ${limit}
    `;

    try {
      const result = await this.query<{ rows: AccountDBRow[] }>(sql, params);
      return result.rows || [];
    } catch (error) {
      console.error('[findPotentialMatches] Error:', error);
      return [];
    }
  }

  public async findPotentialMatchesBatch(
    sourceAccounts: { id: number; account: any }[],
    targetSource: 'dimensions' | 'salesforce',
    limitPerAccount: number = 30
  ): Promise<Map<number, AccountDBRow[]>> {
    const results = new Map<number, AccountDBRow[]>();
    
    if (sourceAccounts.length === 0) return results;

    const searchTerms: {
      id: number;
      name: string | null;
      phone: string | null;
      website: string | null;
    }[] = [];
    
    for (const { id, account } of sourceAccounts) {
      const normalized = this.normalizeAccount(account);
      searchTerms.push({
        id,
        name: normalized.NormalizedName && normalized.NormalizedName.length > 2 
          ? normalized.NormalizedName : null,
        phone: normalized.NormalizedPhone && normalized.NormalizedPhone.length > 5 
          ? normalized.NormalizedPhone : null,
        website: normalized.NormalizedWebsite && normalized.NormalizedWebsite.length > 3 
          ? normalized.NormalizedWebsite : null
      });
    }

    const sql = `
      WITH search_terms AS (
        SELECT * FROM jsonb_to_recordset($1::jsonb) 
        AS t(id int, name text, phone text, website text)
      ),
      phone_matches AS (
        SELECT st.id as source_id, a.*, 100 as priority
        FROM search_terms st
        JOIN accounts a ON a.source = $2 AND a.normalized_phone = st.phone
        WHERE st.phone IS NOT NULL
      ),
      website_matches AS (
        SELECT st.id as source_id, a.*, 95 as priority
        FROM search_terms st
        JOIN accounts a ON a.source = $2 AND a.normalized_website = st.website
        WHERE st.website IS NOT NULL
      ),
      name_matches AS (
        SELECT st.id as source_id, a.*, 
               (similarity(a.normalized_name, st.name) * 80)::int as priority
        FROM search_terms st
        JOIN LATERAL (
          SELECT * FROM accounts 
          WHERE source = $2 
            AND normalized_name % st.name
          ORDER BY normalized_name <-> st.name
          LIMIT ${limitPerAccount}
        ) a ON true
        WHERE st.name IS NOT NULL
      ),
      all_matches AS (
        SELECT * FROM phone_matches
        UNION ALL SELECT * FROM website_matches
        UNION ALL SELECT * FROM name_matches
      )
      SELECT DISTINCT ON (source_id, id) 
             source_id,
             id, source, name, normalized_name, phone, normalized_phone,
             website, normalized_website,
             billing_street, normalized_billing_street,
             billing_city, billing_postal_code, billing_country, 
             raw_data, created_at
      FROM all_matches
      ORDER BY source_id, id, priority DESC
    `;

    try {
      const result = await this.query<{ rows: (AccountDBRow & { source_id: number })[] }>(
        sql, 
        [JSON.stringify(searchTerms), targetSource]
      );

      for (const row of (result.rows || [])) {
        const sourceId = row.source_id;
        if (!results.has(sourceId)) {
          results.set(sourceId, []);
        }
        results.get(sourceId)!.push(row);
      }

      return results;
    } catch (error) {
      console.error('[findPotentialMatchesBatch] Error:', error);
      return results;
    }
  }
}

let postgresClientInstance: PostgresClient | null = null;

export const getPostgresClient = (): PostgresClient => {
  if (!postgresClientInstance) {
    postgresClientInstance = new PostgresClient();
  }
  return postgresClientInstance;
};