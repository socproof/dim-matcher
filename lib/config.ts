// lib/config.ts

// Development limits (set to 0 for no limit in production)
export const DEV_LIMITS = {
  source: 1000,       // Source accounts to load
  dimensions: 10000,  // Dimensions accounts to load
  salesforce: 10000,  // Salesforce accounts to load
};

// Processing settings
export const PROCESSING = {
  chunkSize: 10,           // Source accounts per processing chunk
  dbInsertBatchSize: 500,  // Records per INSERT batch
};

// AI Validation settings
export const AI_CONFIG = {
  enabled: true,
  minScore: 20,    // Minimum heuristic score to trigger AI validation
  maxScore: 100,   // Maximum heuristic score (above this = auto-confirm)
  batchSize: 5,    // Pairs per AI validation batch
  parallelBatches: 2,  // Number of parallel AI requests
};

// Matching thresholds
export const MATCHING = {
  threshold: 85,           // Minimum score to consider a match
  maxPossibleScore: 195,   // Maximum possible matching score
  
  // Field weights
  fields: {
    Name: { exact: 85, alike: 50 },
    Phone: 30,
    Website: 25,
    BillingStreet: 20,
    BillingCity: 10,
  },
};

// Generic email domains to ignore when extracting domain from email
export const GENERIC_EMAIL_DOMAINS = [
  'gmail.com', 'yahoo.com', 'yahoo.co.uk', 'hotmail.com', 'hotmail.co.uk',
  'outlook.com', 'live.com', 'msn.com', 'aol.com', 'icloud.com',
  'mail.com', 'protonmail.com', 'zoho.com', 'yandex.com', 'gmx.com',
  'googlemail.com', 'me.com', 'mac.com', 'btinternet.com', 'sky.com',
];

// Helper function to apply dev limits
export const applyLimit = <T>(data: T[], type: 'source' | 'dimensions' | 'salesforce'): T[] => {
  const limit = DEV_LIMITS[type];
  if (limit > 0 && data.length > limit) {
    console.log(`[Config] Applying dev limit for ${type}: ${data.length} -> ${limit}`);
    return data.slice(0, limit);
  }
  return data;
};

// Check if dev limits are active
export const isDevMode = (): boolean => {
  return DEV_LIMITS.source > 0 || DEV_LIMITS.dimensions > 0 || DEV_LIMITS.salesforce > 0;
};