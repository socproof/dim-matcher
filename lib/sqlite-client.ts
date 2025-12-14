// lib/sqlite-client.ts

import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import { normalizeCompanyName, normalizePhone, normalizeWebsite, normalizeAddress } from './normalize';

export type AccountDBRow = {
  id: number;
  source: 'source' | 'dimensions' | 'salesforce';
  Name: string | null;
  normalizedName: string | null;
  Phone: string | null;
  normalizedPhone: string | null;
  Website: string | null;
  normalizedWebsite: string | null;
  BillingStreet: string | null;
  normalizedBillingStreet: string | null;
  BillingCity: string | null;
  BillingState: string | null;
  BillingPostalCode: string | null;
  BillingCountry: string | null;
  raw_data: string;
  created_at: string;
};

function getSourceFieldValue(
  account: any,
  sourceFieldMapping: Record<string, string>,
  conceptualFieldName: string
): any {
  if (!account || !sourceFieldMapping) {
    return '';
  }

  let sourceSpecificFieldKey = conceptualFieldName;
  for (const key in sourceFieldMapping) {
    if (sourceFieldMapping[key] === conceptualFieldName) {
      sourceSpecificFieldKey = key;
      break;
    }
  }

  let currentValue: any = account;
  const keys = sourceSpecificFieldKey.split('.');
  let pathFound = true;

  for (const key of keys) {
    if (currentValue && typeof currentValue === 'object' && key in currentValue) {
      currentValue = currentValue[key];
    } else {
      if (conceptualFieldName !== sourceSpecificFieldKey && account && typeof account === 'object' && conceptualFieldName in account) {
         currentValue = account[conceptualFieldName];
         pathFound = true;
         break;
      }
      pathFound = false;
      break;
    }
  }

  if (pathFound) {
    return (currentValue !== undefined && currentValue !== null) ? currentValue : '';
  }

  if (account && typeof account === 'object' && conceptualFieldName in account) {
      const fallbackValue = account[conceptualFieldName];
      return (fallbackValue !== undefined && fallbackValue !== null) ? fallbackValue : '';
  }

  return '';
}

interface SQLiteDB {
  exec: (options: {
    sql: string;
    bind?: any[];
    rowMode?: 'array' | 'object';
    callback?: (row: any, columnNames?: string[]) => void;
  }) => SQLiteDB;
  close: () => void;
}

export type QueryOptions = {
  returnFirst?: boolean;
  timeout?: number;
};

const DEFAULT_DB_FILENAME = 'app_match_database.sqlite3';

export class SQLiteClient {
  private db: SQLiteDB | null = null;
  private sqlite3: any;
  private initPromise: Promise<void> | null = null;
  public dbName: string;

  constructor(dbName: string = DEFAULT_DB_FILENAME) {
    this.dbName = dbName;
    this.initPromise = this.initDatabase();
  }

  private async initDatabase() {
    try {
      console.log('Initializing SQLite WASM module...');
      this.sqlite3 = await sqlite3InitModule({
        print: console.log,
        printErr: console.error,
      });
      console.log('SQLite WASM module initialized.');
    } catch (err) {
      console.error('FATAL: Failed to initialize SQLite WASM module:', err);
      this.initPromise = Promise.reject(err);
      throw err;
    }
  }

  public async ensureReady() {
    if (this.initPromise) {
      try {
        await this.initPromise;
        this.initPromise = null;
      } catch (initError) {
        console.error('Initialization failed in ensureReady:', initError);
        throw new Error('SQLiteClient failed to initialize properly.');
      }
    }
    if (!this.db) {
      await this.connect();
    }
    if (!this.db) {
        throw new Error('Database connection could not be established after ensureReady.');
    }
  }

  public async connect(dbName?: string) {
    const targetDbName = dbName || this.dbName;

    if (!this.sqlite3) {
      if (this.initPromise) await this.initPromise;
      if (!this.sqlite3) {
         console.error('SQLite module still not available after waiting for initialization.');
         throw new Error('SQLite module not initialized even after waiting.');
      }
    }

    try {
      if (this.db) {
          this.db.close();
          this.db = null;
      }
      this.db = new this.sqlite3.oo1.DB(targetDbName, 'c');
      this.dbName = targetDbName;

      if (targetDbName !== ':memory:') {
        const vfsInfo = this.sqlite3.capi.sqlite3_vfs_find(null);
        if (!vfsInfo || vfsInfo.name !== 'opfs') {
            console.warn(`Using VFS: ${vfsInfo?.name || 'unknown'}. For data persistence, 'opfs' is recommended.`);
        }
      }
    } catch (err) {
      console.error(`Failed to connect to database "${targetDbName}":`, err);
      this.db = null;
      throw err;
    }
  }

  public async query<T = any>(
    sql: string,
    params: any[] = [],
    options: QueryOptions = {}
  ): Promise<T[] | T | null> {
    await this.ensureReady();
    if (!this.db) {
        console.error("Query attempted but database connection is not available.");
        throw new Error('Database not connected after recovery attempt.');
    }

    return new Promise((resolve, reject) => {
      const results: T[] = [];
      const startTime = performance.now();
      let timeoutId: NodeJS.Timeout | null = null;

      if (options.timeout) {
        timeoutId = setTimeout(() => {
          reject(new Error(`Query timeout after ${options.timeout}ms: ${sql.substring(0, 100)}...`));
        }, options.timeout);
      }

      try {
        this.db!.exec({
          sql,
          bind: params,
          rowMode: 'object',
          callback: (row: T) => {
            results.push(row);
          }
        });

        if (timeoutId) clearTimeout(timeoutId);
        const duration = performance.now() - startTime;

        if (options.returnFirst) {
          resolve(results.length > 0 ? results[0] : null);
        } else {
          resolve(results);
        }
      } catch (err) {
        if (timeoutId) clearTimeout(timeoutId);
        console.error('SQLite Query failed:', { sql: sql.substring(0,100), params, error: err });
        reject(err);
      }
    });
  }

  public async createAccountsTable() {
    await this.ensureReady();
    const sql = `
      PRAGMA journal_mode=WAL;

      CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL CHECK(source IN ('source', 'dimensions', 'salesforce')),
        Name TEXT,
        normalizedName TEXT,
        Phone TEXT,
        normalizedPhone TEXT,
        Website TEXT,
        normalizedWebsite TEXT,
        BillingStreet TEXT,
        normalizedBillingStreet TEXT,
        BillingCity TEXT,
        BillingState TEXT,
        BillingPostalCode TEXT,
        BillingCountry TEXT,
        raw_data TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      CREATE INDEX IF NOT EXISTS idx_accounts_source ON accounts(source);
      CREATE INDEX IF NOT EXISTS idx_accounts_normalizedName ON accounts(normalizedName);
      CREATE INDEX IF NOT EXISTS idx_accounts_normalizedPhone ON accounts(normalizedPhone);
      CREATE INDEX IF NOT EXISTS idx_accounts_normalizedWebsite ON accounts(normalizedWebsite);
      CREATE INDEX IF NOT EXISTS idx_accounts_normalizedBillingStreet ON accounts(normalizedBillingStreet);
      CREATE INDEX IF NOT EXISTS idx_accounts_billingCity ON accounts(BillingCity);
      CREATE INDEX IF NOT EXISTS idx_accounts_billingState ON accounts(BillingState);
    `;
    try {
        await this.ensureReady();
        this.db!.exec({ sql });
    } catch (error) {
        console.error("Failed to execute 'accounts' table creation schema:", error);
        throw error;
    }
  }

  public async insertAccount(
    account: any,
    source: 'source' | 'dimensions' | 'salesforce',
    sourceFieldMapping: Record<string, string>
  ) {
    await this.ensureReady();
    const normalized = this.normalizeAccount(account, sourceFieldMapping);

    const sql = `
      INSERT INTO accounts (
        source, Name, normalizedName,
        Phone, normalizedPhone,
        Website, normalizedWebsite,
        BillingStreet, normalizedBillingStreet,
        BillingCity, BillingState, BillingPostalCode, BillingCountry,
        raw_data
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    const params = [
      source,
      getSourceFieldValue(account, sourceFieldMapping, 'Name'),
      normalized.Name,
      getSourceFieldValue(account, sourceFieldMapping, 'Phone'),
      normalized.Phone,
      getSourceFieldValue(account, sourceFieldMapping, 'Website'),
      normalized.Website,
      getSourceFieldValue(account, sourceFieldMapping, 'BillingStreet'),
      normalized.BillingStreet,
      getSourceFieldValue(account, sourceFieldMapping, 'BillingCity'),
      getSourceFieldValue(account, sourceFieldMapping, 'BillingState'),
      getSourceFieldValue(account, sourceFieldMapping, 'BillingPostalCode'),
      getSourceFieldValue(account, sourceFieldMapping, 'BillingCountry'),
      JSON.stringify(account)
    ];

    try {
        await this.query(sql, params);
    } catch (error) {
        console.error(`Failed to insert account from source '${source}':`, { 
          accountName: getSourceFieldValue(account, sourceFieldMapping, 'Name'), 
          error 
        });
    }
  }

  private normalizeAccount(account: any, sourceFieldMapping: Record<string, string>) {
    return {
      Name: normalizeCompanyName(getSourceFieldValue(account, sourceFieldMapping, 'Name')),
      Phone: normalizePhone(getSourceFieldValue(account, sourceFieldMapping, 'Phone')),
      Website: normalizeWebsite(getSourceFieldValue(account, sourceFieldMapping, 'Website')),
      BillingStreet: normalizeAddress(getSourceFieldValue(account, sourceFieldMapping, 'BillingStreet'))
    };
  }

  public async findPotentialMatches(
    account: any,
    sourceFieldMapping: Record<string, string>,
    targetSource: 'dimensions' | 'salesforce' = 'salesforce',
    country?: string
  ): Promise<AccountDBRow[]> {
    await this.ensureReady();

    const normalizedInputAccount = this.normalizeAccount(account, sourceFieldMapping);
    let foundMatchesMap = new Map<number, AccountDBRow>();
    const queryLimit = 100;

    console.log(`[findPotentialMatches] Searching in '${targetSource}' for:`, account.Name);

    const queries: Promise<void>[] = [];

    // 1. Search by normalized Name
    if (normalizedInputAccount.Name) {
      queries.push(
        this.query<AccountDBRow[]>(
          `SELECT * FROM accounts
           WHERE source = ?
           AND normalizedName LIKE ? 
           LIMIT ?`,
          [targetSource, `%${normalizedInputAccount.Name}%`, queryLimit]
        ).then(matches => {
          console.log(`[findPotentialMatches] Found ${matches?.length || 0} by Name in ${targetSource}`);
          matches?.forEach(match => foundMatchesMap.set(match.id, match));
        })
        .catch(e => console.error(`Query failed (Name in ${targetSource}):`, e))
      );
    }

    // 2. Search by normalized Phone
    if (normalizedInputAccount.Phone) {
      queries.push(
        this.query<AccountDBRow[]>(
          `SELECT * FROM accounts
           WHERE source = ?
           AND normalizedPhone = ? 
           LIMIT ?`,
          [targetSource, normalizedInputAccount.Phone, queryLimit]
        ).then(matches => {
          console.log(`[findPotentialMatches] Found ${matches?.length || 0} by Phone in ${targetSource}`);
          matches?.forEach(match => foundMatchesMap.set(match.id, match));
        })
        .catch(e => console.error(`Query failed (Phone in ${targetSource}):`, e))
      );
    }

    // 3. Search by normalized Website
    if (normalizedInputAccount.Website) {
      queries.push(
        this.query<AccountDBRow[]>(
          `SELECT * FROM accounts
           WHERE source = ?
           AND normalizedWebsite = ? 
           LIMIT ?`,
          [targetSource, normalizedInputAccount.Website, queryLimit]
        ).then(matches => {
          console.log(`[findPotentialMatches] Found ${matches?.length || 0} by Website in ${targetSource}`);
          matches?.forEach(match => foundMatchesMap.set(match.id, match));
        })
        .catch(e => console.error(`Query failed (Website in ${targetSource}):`, e))
      );
    }

    // 4. Search by normalized BillingStreet
    if (normalizedInputAccount.BillingStreet) {
      queries.push(
        this.query<AccountDBRow[]>(
          `SELECT * FROM accounts
           WHERE source = ?
           AND normalizedBillingStreet = ? 
           LIMIT ?`,
          [targetSource, normalizedInputAccount.BillingStreet, queryLimit]
        ).then(matches => {
          console.log(`[findPotentialMatches] Found ${matches?.length || 0} by Street in ${targetSource}`);
          matches?.forEach(match => foundMatchesMap.set(match.id, match));
        })
        .catch(e => console.error(`Query failed (Street in ${targetSource}):`, e))
      );
    }

    await Promise.all(queries);

    console.log(`[findPotentialMatches] Total unique matches in ${targetSource}: ${foundMatchesMap.size}`);
    return Array.from(foundMatchesMap.values());
  }

  public async getAccountCounts(): Promise<{ databricks: number; salesforce: number; total: number }> {
    await this.ensureReady();
    try {
        const counts = await this.query<{ source: string; count: number }[]>(
            `SELECT source, COUNT(*) as count
             FROM accounts
             GROUP BY source`,
            []
        );

        const databricks = counts?.find(c => c.source === 'source')?.count || 0;
        const salesforce = counts?.find(c => c.source === 'salesforce')?.count || 0;

        return {
          databricks,
          salesforce,
          total: databricks + salesforce,
        };
    } catch (error) {
        console.error("Failed to get account counts:", error);
        return { databricks: 0, salesforce: 0, total: 0 };
    }
  }

  public async clearAccounts(source?: 'source' | 'dimensions' | 'salesforce'): Promise<void> {
    await this.ensureReady();
    let sql = "DELETE FROM accounts";
    const params: string[] = [];
    if (source) {
      sql += " WHERE source = ?";
      params.push(source);
    }
    try {
        await this.query(sql, params);
    } catch (error) {
        console.error(`Failed to clear accounts${source ? ` for source: ${source}` : ''}:`, error);
        throw error;
    }
  }

  public async close() {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}

let sqliteClientInstance: SQLiteClient | undefined;

export const getSQLiteClient = async (dbFileName: string = DEFAULT_DB_FILENAME): Promise<SQLiteClient> => {
  if (!sqliteClientInstance) {
    sqliteClientInstance = new SQLiteClient(dbFileName);
    try {
      await sqliteClientInstance.ensureReady();
    } catch (e) {
        console.error(`FATAL: Failed to initialize SQLite client instance for ${dbFileName}:`, e);
        sqliteClientInstance = undefined;
        throw e;
    }
  } else if (sqliteClientInstance.dbName !== dbFileName && dbFileName !== DEFAULT_DB_FILENAME) {
    await sqliteClientInstance.close();
    sqliteClientInstance = new SQLiteClient(dbFileName);
    try {
      await sqliteClientInstance.ensureReady();
    } catch (e) {
        console.error(`FATAL: Failed to re-initialize SQLite client for new DB name ${dbFileName}:`, e);
        sqliteClientInstance = undefined;
        throw e;
    }
  } else {
      try {
          await sqliteClientInstance.ensureReady();
      } catch (e) {
          console.error(`Error ensuring existing SQLite client instance (${sqliteClientInstance.dbName}) is ready:`, e);
           sqliteClientInstance = undefined;
           throw e;
      }
  }

  return sqliteClientInstance;
};