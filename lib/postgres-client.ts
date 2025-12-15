// lib/postgres-client.ts

import { normalizeCompanyName, normalizePhone, normalizeWebsite, normalizeAddress } from './normalize';

export type AccountDBRow = {
    id: number;
    source: 'source' | 'dimensions' | 'salesforce';
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
};


function sanitizeString(value: any): string | null {
    if (value === null || value === undefined) {
        return null;
    }

    const str = String(value);

    return str
        .replace(/\u0000/g, '') // null byte
        .replace(/\\/g, '\\\\') // экранируем обратные слэши
        .replace(/[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]/g, '') // удаляем control characters
        .trim();
}


function sanitizeObject(obj: any): any {
    if (obj === null || obj === undefined) {
        return null;
    }

    if (typeof obj === 'string') {
        return sanitizeString(obj);
    }

    if (typeof obj === 'number' || typeof obj === 'boolean') {
        return obj;
    }

    if (Array.isArray(obj)) {
        return obj.map(item => sanitizeObject(item));
    }

    if (typeof obj === 'object') {
        const cleaned: any = {};
        for (const key in obj) {
            if (obj.hasOwnProperty(key)) {
                cleaned[key] = sanitizeObject(obj[key]);
            }
        }
        return cleaned;
    }

    return obj;
}


function mapAccountFields(account: any, fieldMapping: Record<string, string> | null | undefined): any {

    // If no mapping provided, return account as-is
    if (!fieldMapping || typeof fieldMapping !== 'object') {
        return account;
    }

    const entries = Object.entries(fieldMapping);

    // If mapping is empty, return account as-is
    if (entries.length === 0) {
        return account;
    }

    const mapped: any = {};

    for (const [sourceField, targetField] of entries) {
        if (account[sourceField] !== undefined) {
            mapped[targetField] = account[sourceField];
        }
    }

    // Also copy any fields that are already in standard format
    const standardFields = ['Name', 'Phone', 'Website', 'BillingStreet', 'BillingCity', 'BillingPostalCode', 'BillingCountry', 'AccountNumber'];
    for (const field of standardFields) {
        if (account[field] !== undefined && mapped[field] === undefined) {
            mapped[field] = account[field];
        }
    }

    return mapped;
}

export class PostgresClient {
    private apiUrl: string = '/api/postgres';

    constructor() { }

    public async testConnection(): Promise<{ connected: boolean; error?: string; details?: any }> {
        try {
            const response = await fetch('/api/postgres', {
                method: 'GET',
            });

            if (!response.ok) {
                const error = await response.json();
                return {
                    connected: false,
                    error: error.error || 'Connection failed',
                    details: error
                };
            }

            const data = await response.json();
            console.log('[PostgresClient] Connection test:', data);

            return {
                connected: data.status === 'connected',
                details: data
            };
        } catch (error) {
            return {
                connected: false,
                error: error instanceof Error ? error.message : 'Unknown error'
            };
        }
    }

    private async query<T = any>(sql: string, params: any[] = []): Promise<T> {
        const response = await fetch(this.apiUrl, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sql, params }),
        });

        if (!response.ok) {
            const error = await response.json();
            console.error('[PostgresClient] Query failed:', error);
            throw new Error(error.details || error.error || 'Database query failed');
        }

        const result = await response.json();

        return result;
    }

    public async insertAccountBatch(
        accounts: any[],
        source: 'source' | 'dimensions' | 'salesforce',
        sourceFieldMapping: Record<string, string>
    ) {
        if (accounts.length === 0) {
            console.log('[PostgresClient] No accounts to insert');
            return;
        }

        console.log(`[PostgresClient] Inserting ${accounts.length} ${source} accounts...`);

        // Разбиваем на батчи по 500 записей для оптимизации
        const batchSize = 500;
        let totalInserted = 0;

        for (let i = 0; i < accounts.length; i += batchSize) {
            const batch = accounts.slice(i, i + batchSize);
            console.log(`[PostgresClient] Inserting batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(accounts.length / batchSize)} (${batch.length} records)...`);

            try {
                await this.insertBatch(batch, source, sourceFieldMapping);
                totalInserted += batch.length;
                console.log(`[PostgresClient] Progress: ${totalInserted}/${accounts.length}`);
            } catch (error) {
                console.error(`[PostgresClient] Failed to insert batch at offset ${i}:`, error);
                // Пробуем вставить по одной записи из проблемного батча
                console.log(`[PostgresClient] Attempting individual inserts for failed batch...`);
                for (let j = 0; j < batch.length; j++) {
                    try {
                        await this.insertBatch([batch[j]], source, sourceFieldMapping);
                        totalInserted++;
                    } catch (singleError) {
                        console.error(`[PostgresClient] Failed to insert single record at index ${i + j}:`, singleError);
                        console.error(`[PostgresClient] Problematic record:`, batch[j]);
                    }
                }
            }
        }

        console.log(`[PostgresClient] Successfully inserted ${totalInserted}/${accounts.length} ${source} accounts`);
    }

    private async insertBatch(
        accounts: any[],
        source: 'source' | 'dimensions' | 'salesforce',
        sourceFieldMapping: Record<string, string>
    ) {
        if (accounts.length === 0) return;

        const values: string[] = [];
        const params: any[] = [];
        let paramIndex = 1;

        for (const account of accounts) {
            const sanitizedAccount = sanitizeObject(account);
            const mappedAccount = mapAccountFields(sanitizedAccount, sourceFieldMapping);
            const normalized = this.normalizeAccount(mappedAccount);

            values.push(
                `($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6}, $${paramIndex + 7}, $${paramIndex + 8}, $${paramIndex + 9}, $${paramIndex + 10}, $${paramIndex + 11}, $${paramIndex + 12})`
            );

            params.push(
                source,
                mappedAccount.Name || null,
                normalized.Name,
                mappedAccount.Phone || null,
                normalized.Phone,
                mappedAccount.Website || null,
                normalized.Website,
                mappedAccount.BillingStreet || null,
                normalized.BillingStreet,
                mappedAccount.BillingCity || null,
                mappedAccount.BillingPostalCode || null,
                mappedAccount.BillingCountry || null,
                JSON.stringify(sanitizedAccount)
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
    ) VALUES ${values.join(', ')}
  `;

        const result = await this.query(sql, params);
        console.log(`[PostgresClient] Batch insert completed: ${result.rowCount} rows`);
    }

    private normalizeAccount(account: any) {
        // Просто берем поля напрямую, без маппинга
        return {
            Name: normalizeCompanyName(account.Name || ''),
            Phone: normalizePhone(account.Phone || ''),
            Website: normalizeWebsite(account.Website || ''),
            BillingStreet: normalizeAddress(account.BillingStreet || '')
        };
    }

    public async findPotentialMatches(
        account: any,
        targetSource: 'dimensions' | 'salesforce' = 'salesforce',
        limit: number = 100
    ): Promise<AccountDBRow[]> {
        const normalizedInputAccount = this.normalizeAccount(account);

        console.log('[findPotentialMatches] Input account:', account);
        console.log('[findPotentialMatches] Normalized:', normalizedInputAccount);
        console.log('[findPotentialMatches] Target source:', targetSource);

        const conditions: string[] = [];
        const params: any[] = [targetSource];
        let paramIndex = 2;

        // Поиск по имени (триграмное сходство)
        if (normalizedInputAccount.Name && normalizedInputAccount.Name.length > 0) {
            conditions.push(`normalized_name % $${paramIndex}`);
            params.push(normalizedInputAccount.Name);
            console.log(`[findPotentialMatches] Added Name condition: ${normalizedInputAccount.Name}`);
            paramIndex++;
        }

        // Поиск по телефону
        if (normalizedInputAccount.Phone && normalizedInputAccount.Phone.length > 0) {
            conditions.push(`normalized_phone = $${paramIndex}`);
            params.push(normalizedInputAccount.Phone);
            console.log(`[findPotentialMatches] Added Phone condition: ${normalizedInputAccount.Phone}`);
            paramIndex++;
        }

        // Поиск по вебсайту
        if (normalizedInputAccount.Website && normalizedInputAccount.Website.length > 0) {
            conditions.push(`normalized_website = $${paramIndex}`);
            params.push(normalizedInputAccount.Website);
            console.log(`[findPotentialMatches] Added Website condition: ${normalizedInputAccount.Website}`);
            paramIndex++;
        }

        // Поиск по адресу
        if (normalizedInputAccount.BillingStreet && normalizedInputAccount.BillingStreet.length > 0) {
            conditions.push(`normalized_billing_street = $${paramIndex}`);
            params.push(normalizedInputAccount.BillingStreet);
            console.log(`[findPotentialMatches] Added BillingStreet condition: ${normalizedInputAccount.BillingStreet}`);
            paramIndex++;
        }

        if (conditions.length === 0) {
            console.log('[findPotentialMatches] No conditions - returning empty array');
            return [];
        }

        const sql = `
    SELECT * FROM accounts
    WHERE source = $1
    AND (${conditions.join(' OR ')})
    LIMIT ${limit}
  `;

        console.log('[findPotentialMatches] SQL:', sql);
        console.log('[findPotentialMatches] Params:', params);

        const result = await this.query<{ rows: AccountDBRow[] }>(sql, params);

        console.log('[findPotentialMatches] Found matches:', result.rows?.length || 0);

        if (result.rows && result.rows.length > 0) {
            console.log('[findPotentialMatches] First match:', result.rows[0]);
        }

        return result.rows || [];
    }

    public async getAccountCounts(): Promise<{
        source: number;
        dimensions: number;
        salesforce: number;
        total: number
    }> {
        const sql = `
      SELECT 
        source,
        COUNT(*) as count
      FROM accounts
      GROUP BY source
    `;

        const result = await this.query<{ rows: { source: string; count: string }[] }>(sql);
        const rows = result.rows || [];

        console.log('[PostgresClient] Account counts raw:', rows);

        const source = parseInt(rows.find(r => r.source === 'source')?.count || '0');
        const dimensions = parseInt(rows.find(r => r.source === 'dimensions')?.count || '0');
        const salesforce = parseInt(rows.find(r => r.source === 'salesforce')?.count || '0');

        return {
            source,
            dimensions,
            salesforce,
            total: source + dimensions + salesforce
        };
    }

    public async clearAccounts(source?: 'source' | 'dimensions' | 'salesforce'): Promise<void> {
        let sql = "DELETE FROM accounts";
        const params: string[] = [];

        if (source) {
            sql += " WHERE source = $1";
            params.push(source);
        }

        console.log(`[PostgresClient] Clearing accounts${source ? ` for source: ${source}` : ' (ALL)'}`);
        const result = await this.query(sql, params);
        console.log(`[PostgresClient] Cleared ${result.rowCount} accounts`);
    }

    public async getSourceAccountsChunk(limit: number, offset: number): Promise<AccountDBRow[]> {
        const sql = `
      SELECT * FROM accounts
      WHERE source = 'source'
      ORDER BY id
      LIMIT $1 OFFSET $2
    `;

        const result = await this.query<{ rows: AccountDBRow[] }>(sql, [limit, offset]);
        return result.rows || [];
    }

    public async getTotalSourceCount(): Promise<number> {
        const sql = "SELECT COUNT(*) as count FROM accounts WHERE source = 'source'";
        const result = await this.query<{ rows: { count: string }[] }>(sql);
        return parseInt(result.rows?.[0]?.count || '0');
    }
}

let postgresClientInstance: PostgresClient | undefined;

export const getPostgresClient = (): PostgresClient => {
    if (!postgresClientInstance) {
        postgresClientInstance = new PostgresClient();
    }
    return postgresClientInstance;
};