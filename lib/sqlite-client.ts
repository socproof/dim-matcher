import sqlite3InitModule from '@sqlite.org/sqlite-wasm';
import { normalizeCompanyName, normalizePhone, normalizeWebsite, normalizeAddress } from './normalize'; // Assuming normalize functions exist

// Type definition for the structure of rows in the 'accounts' table
export type AccountDBRow = {
  id: number; // Primary Key from the DB
  source: 'databricks' | 'salesforce';
  Name: string | null;
  normalizedName: string | null;
  Company_Registration_No__c: string | null;
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
  raw_data: string; // JSON string of the original account object
  created_at: string; // Timestamp
};

// Helper function to get a field's value from an account object using mapping
// This needs to handle the case where the mapping provides the conceptual name,
// but we need the actual key in the source data object.
function getSourceFieldValue(
  account: any,
  // fieldMapping should be for the specific source, e.g., fieldMapping.databricks
  sourceFieldMapping: Record<string, string>,
  conceptualFieldName: string
): any {
  if (!account || !sourceFieldMapping) {
    return '';
  }

  let sourceSpecificFieldKey = conceptualFieldName; // Default to conceptual name if no specific mapping found
  // Find the actual source field key that maps to the conceptualFieldName
  for (const key in sourceFieldMapping) {
    if (sourceFieldMapping[key] === conceptualFieldName) {
      sourceSpecificFieldKey = key;
      break;
    }
  }

  // Access the value using the determined sourceSpecificFieldKey
  // Handle potential nested keys if sourceSpecificFieldKey uses dot notation (e.g., "Address.Street")
  let currentValue: any = account;
  const keys = sourceSpecificFieldKey.split('.');
  let pathFound = true;

  for (const key of keys) {
    if (currentValue && typeof currentValue === 'object' && key in currentValue) {
      currentValue = currentValue[key];
    } else {
      // Fallback: If the mapped key didn't work, try the conceptual name directly
      // This helps if the mapping isn't perfectly aligned or data uses conceptual names
      if (conceptualFieldName !== sourceSpecificFieldKey && account && typeof account === 'object' && conceptualFieldName in account) {
         currentValue = account[conceptualFieldName];
         pathFound = true; // Found via conceptual name
         break;
      }
      pathFound = false;
      break;
    }
  }

  if (pathFound) {
    // Return the value, ensuring null/undefined becomes an empty string for consistency
    return (currentValue !== undefined && currentValue !== null) ? currentValue : '';
  }

  // Final fallback: try the conceptual name directly one last time if the loop failed
  if (account && typeof account === 'object' && conceptualFieldName in account) {
      const fallbackValue = account[conceptualFieldName];
      return (fallbackValue !== undefined && fallbackValue !== null) ? fallbackValue : '';
  }


  return ''; // Return empty string if the value couldn't be accessed
}


// Interface for the SQLite Database object provided by the WASM module
interface SQLiteDB {
  exec: (options: {
    sql: string;
    bind?: any[];
    rowMode?: 'array' | 'object'; // Use 'object' for easier access
    callback?: (row: any, columnNames?: string[]) => void;
  }) => SQLiteDB;
  close: () => void;
}

// Options for query execution
export type QueryOptions = {
  returnFirst?: boolean; // Return only the first result row
  timeout?: number; // Query timeout in milliseconds
};

// Default database filename
const DEFAULT_DB_FILENAME = 'app_match_database.sqlite3';

// Main SQLite Client Class
export class SQLiteClient {
  private db: SQLiteDB | null = null;
  private sqlite3: any; // Holds the initialized SQLite WASM module
  private initPromise: Promise<void> | null = null; // Tracks initialization state
  public dbName: string; // Name of the database file being used

  constructor(dbName: string = DEFAULT_DB_FILENAME) {
    this.dbName = dbName;
    // Start initialization immediately, but don't block constructor
    this.initPromise = this.initDatabase();
  }

  // Initializes the SQLite WASM module
  private async initDatabase() {
    try {
      console.log('Initializing SQLite WASM module...');
      this.sqlite3 = await sqlite3InitModule({
        print: console.log, // Optional: Log SQLite messages
        printErr: console.error, // Optional: Log SQLite errors
      });
      console.log('SQLite WASM module initialized.');
    } catch (err) {
      console.error('FATAL: Failed to initialize SQLite WASM module:', err);
      // Set initPromise to a rejected state so ensureReady fails clearly
      this.initPromise = Promise.reject(err);
      throw err; // Re-throw to indicate failure
    }
  }

  // Ensures the WASM module is loaded and the database connection is open
  public async ensureReady() {
    // Wait for initialization if it's in progress or hasn't failed
    if (this.initPromise) {
      try {
        await this.initPromise;
        this.initPromise = null; // Initialization succeeded
      } catch (initError) {
        console.error('Initialization failed in ensureReady:', initError);
        throw new Error('SQLiteClient failed to initialize properly.');
      }
    }
    // If initialization succeeded or was already done, check for DB connection
    if (!this.db) {
      // console.debug('Database not connected, attempting connection...');
      await this.connect(); // Attempt to connect using the current dbName
    }
     // Final check
     if (!this.db) {
        throw new Error('Database connection could not be established after ensureReady.');
     }
  }

  // Connects to the SQLite database file
  public async connect(dbName?: string) {
    const targetDbName = dbName || this.dbName;
    // console.debug(`Attempting to connect to database: ${targetDbName}`);

    // Ensure the WASM module is ready before trying to use it
    if (!this.sqlite3) {
      // console.debug('SQLite module not ready, awaiting initialization...');
      if (this.initPromise) await this.initPromise; // Wait if init is ongoing
      if (!this.sqlite3) {
         // This should ideally not happen if initDatabase succeeded/failed correctly
         console.error('SQLite module still not available after waiting for initialization.');
         throw new Error('SQLite module not initialized even after waiting.');
      }
       // console.debug('SQLite module is now ready.');
    }

    try {
      // Close existing connection if switching databases or reconnecting
      if (this.db) {
          // console.debug(`Closing existing connection to ${this.dbName}...`);
          this.db.close();
          this.db = null;
      }
      // Open the database connection (use 'c' flag: create if doesn't exist)
      this.db = new this.sqlite3.oo1.DB(targetDbName, 'c');
      this.dbName = targetDbName; // Update the stored dbName
      // console.info(`Successfully connected to database: "${targetDbName}"`);

      // Check VFS for persistence hints (useful for debugging OPFS setup)
      if (targetDbName !== ':memory:') {
        const vfsInfo = this.sqlite3.capi.sqlite3_vfs_find(null);
        if (!vfsInfo || vfsInfo.name !== 'opfs') {
            console.warn(`Using VFS: ${vfsInfo?.name || 'unknown'}. For data persistence across sessions, 'opfs' (Origin Private File System) is recommended. Ensure your server provides COOP/COEP headers.`);
        } else {
            // console.log("Using 'opfs' VFS, persistence should work if headers are correct.");
        }
      }
    } catch (err) {
      console.error(`Failed to connect to database "${targetDbName}":`, err);
      this.db = null; // Ensure db is null on failure
      throw err; // Re-throw connection error
    }
  }

  // Executes a SQL query
  public async query<T = any>(
    sql: string,
    params: any[] = [],
    options: QueryOptions = {}
  ): Promise<T[] | T | null> {
    // Ensure DB is ready before querying
    await this.ensureReady();
    if (!this.db) {
        // This should not happen if ensureReady works correctly
        console.error("Query attempted but database connection is not available.");
        throw new Error('Database not connected after recovery attempt.');
    }

    return new Promise((resolve, reject) => {
      const results: T[] = [];
      const startTime = performance.now();
      let timeoutId: NodeJS.Timeout | null = null;

      // Set up timeout if requested
      if (options.timeout) {
        timeoutId = setTimeout(() => {
          reject(new Error(`Query timeout after ${options.timeout}ms: ${sql.substring(0, 100)}...`));
        }, options.timeout);
      }

      try {
        // Execute the query using the WASM DB object
        this.db!.exec({
          sql,
          bind: params,
          rowMode: 'object', // Get results as objects { columnName: value }
          callback: (row: T) => { // Callback for each row fetched
            results.push(row);
          }
        });

        // Query finished successfully
        if (timeoutId) clearTimeout(timeoutId); // Clear timeout
        const duration = performance.now() - startTime;

        // Optional: Log slow or impactful queries
        if (duration > 100 || sql.toUpperCase().startsWith("CREATE") || sql.toUpperCase().startsWith("INSERT") || sql.toUpperCase().startsWith("UPDATE") || sql.toUpperCase().startsWith("DELETE")) {
            // console.debug(`Query: ${sql.substring(0,100)}... | Params: ${JSON.stringify(params)} | Rows: ${results.length} | Time: ${duration.toFixed(2)}ms`);
        }

        // Return result(s) based on options
        if (options.returnFirst) {
          resolve(results.length > 0 ? results[0] : null);
        } else {
          resolve(results);
        }
      } catch (err) {
        // Query failed
        if (timeoutId) clearTimeout(timeoutId); // Clear timeout
        console.error('SQLite Query failed:', { sql: sql.substring(0,100), params, error: err });
        reject(err); // Reject the promise with the error
      }
    });
  }

  // Creates the 'accounts' table and necessary indexes if they don't exist
  public async createAccountsTable() {
    await this.ensureReady();
    const sql = `
      PRAGMA journal_mode=WAL; -- Recommended for performance

      CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL CHECK(source IN ('databricks', 'salesforce')),
        Name TEXT,
        normalizedName TEXT, -- For matching company names
        Company_Registration_No__c TEXT, -- Often a good unique identifier
        Phone TEXT,
        normalizedPhone TEXT, -- For matching phone numbers
        Website TEXT,
        normalizedWebsite TEXT, -- For matching websites
        BillingStreet TEXT,
        normalizedBillingStreet TEXT, -- For matching addresses
        BillingCity TEXT,
        BillingState TEXT,
        BillingPostalCode TEXT,
        BillingCountry TEXT,
        raw_data TEXT NOT NULL, -- Store the original JSON data
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );

      -- Indexes for faster lookups
      CREATE INDEX IF NOT EXISTS idx_accounts_source ON accounts(source);
      CREATE INDEX IF NOT EXISTS idx_accounts_normalizedName ON accounts(normalizedName);
      CREATE INDEX IF NOT EXISTS idx_accounts_company_reg_no ON accounts(Company_Registration_No__c);
      CREATE INDEX IF NOT EXISTS idx_accounts_normalizedPhone ON accounts(normalizedPhone);
      CREATE INDEX IF NOT EXISTS idx_accounts_normalizedWebsite ON accounts(normalizedWebsite);
      CREATE INDEX IF NOT EXISTS idx_accounts_normalizedBillingStreet ON accounts(normalizedBillingStreet);
    `;
    try {
        // Use exec directly for multi-statement SQL without expecting results
        await this.ensureReady();
        this.db!.exec({ sql });
        // console.log("'accounts' table checked/created successfully with indexes.");
    } catch (error) {
        console.error("Failed to execute 'accounts' table creation schema:", error);
        throw error;
    }
  }

  // Inserts a single account record into the table
  public async insertAccount(
    account: any, // The raw account object from the source
    source: 'databricks' | 'salesforce',
    // Pass the mapping specific to the source (e.g., fieldMapping.databricks)
    sourceFieldMapping: Record<string, string>
  ) {
    await this.ensureReady();
    // Normalize relevant fields using the source-specific mapping
    const normalized = this.normalizeAccount(account, sourceFieldMapping);

    const sql = `
      INSERT INTO accounts (
        source, Name, normalizedName,
        Company_Registration_No__c, Phone, normalizedPhone,
        Website, normalizedWebsite,
        BillingStreet, normalizedBillingStreet,
        BillingCity, BillingState, BillingPostalCode, BillingCountry,
        raw_data
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO NOTHING; -- Basic conflict handling (adjust if needed)
    `;

    // Get field values using the helper function and source mapping
    const params = [
      source,
      getSourceFieldValue(account, sourceFieldMapping, 'Name'),
      normalized.Name,
      getSourceFieldValue(account, sourceFieldMapping, 'Company_Registration_No__c'),
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
      JSON.stringify(account) // Store the full original object as JSON
    ];

    try {
        await this.query(sql, params);
    } catch (error) {
        console.error(`Failed to insert account from source '${source}':`, { accountName: getSourceFieldValue(account, sourceFieldMapping, 'Name'), error });
        // Decide if you want to throw the error or just log it
    }
  }

  // Helper to normalize specific account fields for matching
  private normalizeAccount(account: any, sourceFieldMapping: Record<string, string>) {
    return {
      Name: normalizeCompanyName(getSourceFieldValue(account, sourceFieldMapping, 'Name')),
      Phone: normalizePhone(getSourceFieldValue(account, sourceFieldMapping, 'Phone')),
      Website: normalizeWebsite(getSourceFieldValue(account, sourceFieldMapping, 'Website')),
      BillingStreet: normalizeAddress(getSourceFieldValue(account, sourceFieldMapping, 'BillingStreet'))
      // Add other fields here if they need normalization for matching queries
    };
  }

  // Finds potential Salesforce matches for a given Databricks account
  public async findPotentialMatches(
    account: any, // The Databricks account object (already parsed)
    // Mapping for the *Databricks* account to extract values for querying SF data
    databricksFieldMapping: Record<string, string>,
    country?: string // Optional country context (currently unused in queries)
  ): Promise<AccountDBRow[]> {
    await this.ensureReady();

    // Normalize fields from the input Databricks account using its mapping
    const normalizedInputAccount = this.normalizeAccount(account, databricksFieldMapping);
    const inputRegNo = getSourceFieldValue(account, databricksFieldMapping, 'Company_Registration_No__c');

    let foundMatchesMap = new Map<number, AccountDBRow>();
    const queryLimit = 50; // Limit results per query type to avoid overwhelming results

    // 1. Prioritize exact match on Company Registration Number (if available and not empty)
    if (inputRegNo && String(inputRegNo).trim() !== '') {
      const exactMatchesByRegNo = await this.query<AccountDBRow[]>(
        `SELECT * FROM accounts
         WHERE source = 'salesforce'
         AND Company_Registration_No__c = ?
         LIMIT ?`,
        [inputRegNo, queryLimit] // Apply limit here too
      );
      if (exactMatchesByRegNo && exactMatchesByRegNo.length > 0) {
        // console.debug(`Found ${exactMatchesByRegNo.length} potential matches by RegNo: ${inputRegNo} for DB ID ${account?.id}`);
        // If we find by RegNo, we can often assume it's the strongest signal
        // You might choose to return *only* these matches:
        // return exactMatchesByRegNo;
        // Or add them to the map and continue searching for corroboration:
         exactMatchesByRegNo.forEach(match => foundMatchesMap.set(match.id, match));
         // If returning only RegNo matches, uncomment the return above and remove the forEach.
      }
    }

    // 2. Search by other normalized fields (add results to the map)
    const queries: Promise<void>[] = [];

    // Query by Normalized Name (LIKE can be slow, consider Full-Text Search if performance is critical)
    if (normalizedInputAccount.Name) {
       queries.push(
         this.query<AccountDBRow[]>(
           `SELECT * FROM accounts
            WHERE source = 'salesforce'
            AND normalizedName LIKE ? LIMIT ?`,
           [`%${normalizedInputAccount.Name}%`, queryLimit]
         ).then(matches => matches?.forEach(match => foundMatchesMap.set(match.id, match)))
         .catch(e => console.error("Query failed (Name):", e)) // Add catch blocks
       );
    }
    // Query by Normalized Phone (Exact match usually better here)
    if (normalizedInputAccount.Phone) {
       queries.push(
         this.query<AccountDBRow[]>(
           `SELECT * FROM accounts
            WHERE source = 'salesforce'
            AND normalizedPhone = ? LIMIT ?`,
           [normalizedInputAccount.Phone, queryLimit]
         ).then(matches => matches?.forEach(match => foundMatchesMap.set(match.id, match)))
         .catch(e => console.error("Query failed (Phone):", e))
       );
    }
     // Query by Normalized Website
    if (normalizedInputAccount.Website) {
       queries.push(
         this.query<AccountDBRow[]>(
           `SELECT * FROM accounts
            WHERE source = 'salesforce'
            AND normalizedWebsite = ? LIMIT ?`,
           [normalizedInputAccount.Website, queryLimit]
         ).then(matches => matches?.forEach(match => foundMatchesMap.set(match.id, match)))
         .catch(e => console.error("Query failed (Website):", e))
       );
    }
     // Query by Normalized Billing Street (Address matching is complex, this is basic)
    if (normalizedInputAccount.BillingStreet) {
       queries.push(
         this.query<AccountDBRow[]>(
           `SELECT * FROM accounts
            WHERE source = 'salesforce'
            AND normalizedBillingStreet = ? LIMIT ?`,
           [normalizedInputAccount.BillingStreet, queryLimit]
         ).then(matches => matches?.forEach(match => foundMatchesMap.set(match.id, match)))
         .catch(e => console.error("Query failed (Street):", e))
       );
    }
    // Add more queries for City, PostalCode etc. if needed, potentially combining them

    // Wait for all search queries to complete
    await Promise.all(queries);

    // console.debug(`Found ${foundMatchesMap.size} potential unique SF matches in total for DB ID ${account?.id || 'N/A'}`);
    return Array.from(foundMatchesMap.values()); // Return unique matches found
  }

  // Gets the count of accounts for each source and the total
  public async getAccountCounts(): Promise<{ databricks: number; salesforce: number; total: number }> {
    await this.ensureReady();
    try {
        const counts = await this.query<{ source: string; count: number }[]>(
            `SELECT source, COUNT(*) as count
             FROM accounts
             GROUP BY source`,
            []
        );

        const databricks = counts?.find(c => c.source === 'databricks')?.count || 0;
        const salesforce = counts?.find(c => c.source === 'salesforce')?.count || 0;

        return {
          databricks,
          salesforce,
          total: databricks + salesforce,
        };
    } catch (error) {
        console.error("Failed to get account counts:", error);
        return { databricks: 0, salesforce: 0, total: 0 }; // Return zero counts on error
    }
  }

  // Clears accounts from the table, optionally filtering by source
  public async clearAccounts(source?: 'databricks' | 'salesforce'): Promise<void> {
    await this.ensureReady();
    let sql = "DELETE FROM accounts";
    const params: string[] = [];
    if (source) {
      sql += " WHERE source = ?";
      params.push(source);
    }
    try {
        await this.query(sql, params);
        // console.log(`Cleared accounts${source ? ` for source: ${source}` : ''}.`);
    } catch (error) {
        console.error(`Failed to clear accounts${source ? ` for source: ${source}` : ''}:`, error);
        throw error;
    }
  }


  // Closes the database connection
  public async close() {
    if (this.db) {
      // console.log(`Closing database connection to ${this.dbName}...`);
      this.db.close();
      this.db = null;
      // console.log("Database connection closed.");
    }
    // Reset sqlite3 module instance? Generally not needed unless re-initializing entirely.
  }
}

// --- Singleton Instance Management ---
let sqliteClientInstance: SQLiteClient | undefined;

// Gets or creates the singleton instance of the SQLiteClient
export const getSQLiteClient = async (dbFileName: string = DEFAULT_DB_FILENAME): Promise<SQLiteClient> => {
  if (!sqliteClientInstance) {
    // console.log(`Creating new SQLiteClient instance for DB: ${dbFileName}`);
    sqliteClientInstance = new SQLiteClient(dbFileName);
    try {
      // Crucial: Ensure the first instance is fully ready before returning
      await sqliteClientInstance.ensureReady();
      // console.log(`SQLiteClient instance for ${dbFileName} is ready.`);
    } catch (e) {
        console.error(`FATAL: Failed to initialize SQLite client instance for ${dbFileName}:`, e);
        sqliteClientInstance = undefined; // Reset instance on failure
        throw e; // Propagate the error
    }
  } else if (sqliteClientInstance.dbName !== dbFileName && dbFileName !== DEFAULT_DB_FILENAME) {
    // Handle case where a *different specific* DB name is requested later
    // console.warn(`Switching SQLiteClient instance from ${sqliteClientInstance.dbName} to ${dbFileName}`);
    await sqliteClientInstance.close(); // Close the old connection
    sqliteClientInstance = new SQLiteClient(dbFileName); // Create new instance
    try {
      await sqliteClientInstance.ensureReady(); // Ensure the new one is ready
      // console.log(`SQLiteClient instance switched and ready for ${dbFileName}.`);
    } catch (e) {
        console.error(`FATAL: Failed to re-initialize SQLite client for new DB name ${dbFileName}:`, e);
        sqliteClientInstance = undefined; // Reset instance on failure
        throw e; // Propagate the error
    }
  } else {
      // Instance exists and dbName matches or is default, ensure it's ready just in case
      try {
          await sqliteClientInstance.ensureReady();
      } catch (e) {
          console.error(`Error ensuring existing SQLite client instance (${sqliteClientInstance.dbName}) is ready:`, e);
           sqliteClientInstance = undefined; // Reset instance on failure
           throw e;
      }
  }

  // Should always return a ready instance or have thrown an error
  return sqliteClientInstance;
};