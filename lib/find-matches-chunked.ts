import { AccountDBRow, getPostgresClient } from './postgres-client';
import { calculateMatchScore } from './matching';
import {
  validateBatchWithAI,
  shouldValidateWithAI,
  determineFinalStatus,
  AIValidationResult,
  AccountPairForValidation
} from './ai-validation';

function dbRowToStandardAccount(row: AccountDBRow): any {
  return {
    Name: row.name,
    Phone: row.phone,
    Website: row.website,
    Email: row.email,              // NEW
    EmailDomain: row.email_domain, // NEW
    BillingStreet: row.billing_street,
    BillingCity: row.billing_city,
    BillingPostalCode: row.billing_postal_code,
    BillingCountry: row.billing_country,
    _raw: row.raw_data
  };
}

export interface MatchedAccount {
  sourceAccount: any;
  dimensionsMatch: any | null;
  salesforceMatch: any | null;
  dimensionsScore: number;
  salesforceScore: number;
  dimensionsMatchedFields: string[];
  salesforceMatchedFields: string[];
  dimensionsAI: AIValidationResult | null;
  salesforceAI: AIValidationResult | null;
  dimensionsStatus: 'CONFIRMED' | 'REJECTED' | 'REVIEW' | 'NEW';
  salesforceStatus: 'CONFIRMED' | 'REJECTED' | 'REVIEW' | 'NEW';
  finalStatus: 'BOTH' | 'DIM_ONLY' | 'SF_ONLY' | 'NEW';
}

interface ChunkProcessingResult {
  matches: MatchedAccount[];
  totalSourceAccounts: number;
  processedCount: number;
  hasMore: boolean;
  stats: {
    both: number;
    dimOnly: number;
    sfOnly: number;
    new: number;
    aiValidated: number;
  };
}

interface PendingMatch {
  index: number;
  sourceAccount: any;
  dimensionsMatch: any | null;
  salesforceMatch: any | null;
  dimensionsScore: number;
  salesforceScore: number;
  dimensionsMatchedFields: string[];
  salesforceMatchedFields: string[];
}

export const processSourceChunk = async (
  fieldMapping: Record<string, any>,
  chunkSize: number,
  startIndex: number = 0,
  enableAI: boolean = true
): Promise<ChunkProcessingResult> => {
  const client = getPostgresClient();

  console.log(`[processSourceChunk] Starting: size=${chunkSize}, offset=${startIndex}`);
  const startTime = Date.now();

  const totalSourceAccounts = await client.getTotalSourceCount();

  if (totalSourceAccounts === 0 || startIndex >= totalSourceAccounts) {
    return {
      matches: [], totalSourceAccounts, processedCount: 0, hasMore: false,
      stats: { both: 0, dimOnly: 0, sfOnly: 0, new: 0, aiValidated: 0 }
    };
  }

  const sourceAccountsRows = await client.getSourceAccountsChunk(chunkSize, startIndex);

  if (!sourceAccountsRows || sourceAccountsRows.length === 0) {
    return {
      matches: [], totalSourceAccounts, processedCount: 0,
      hasMore: startIndex < totalSourceAccounts,
      stats: { both: 0, dimOnly: 0, sfOnly: 0, new: 0, aiValidated: 0 }
    };
  }

  // Prepare source accounts with IDs
  const sourceAccounts = sourceAccountsRows.map((row, idx) => ({
    id: idx,
    row,
    account: dbRowToStandardAccount(row)
  }));

  console.log(`[processSourceChunk] Batch searching ${sourceAccounts.length} accounts...`);
  const searchStart = Date.now();

  // BATCH SEARCH - one query for all Dimensions, one for all Salesforce
  const dimMatchesMap = await client.findPotentialMatchesBatch(
    sourceAccounts.map(s => ({ id: s.id, account: s.account })),
    'dimensions',
    30
  );

  const sfMatchesMap = await client.findPotentialMatchesBatch(
    sourceAccounts.map(s => ({ id: s.id, account: s.account })),
    'salesforce',
    30
  );

  console.log(`[processSourceChunk] Batch search completed in ${Date.now() - searchStart}ms`);

  // Process matches and calculate scores
  const pendingMatches: PendingMatch[] = [];

  for (const { id, account } of sourceAccounts) {
    const dimCandidates = dimMatchesMap.get(id) || [];
    const sfCandidates = sfMatchesMap.get(id) || [];

    // Find best Dimensions match
    let bestDimMatch: any = null;
    let bestDimScore = 0;
    let bestDimFields: string[] = [];

    for (const dimRow of dimCandidates) {
      const dimAccount = dbRowToStandardAccount(dimRow);
      const { score, matchedFields } = calculateMatchScore(account, dimAccount);
      if (score > bestDimScore) {
        bestDimScore = score;
        bestDimMatch = dimAccount;
        bestDimFields = matchedFields || [];
      }
    }

    // Find best Salesforce match
    let bestSfMatch: any = null;
    let bestSfScore = 0;
    let bestSfFields: string[] = [];

    for (const sfRow of sfCandidates) {
      const sfAccount = dbRowToStandardAccount(sfRow);
      const { score, matchedFields } = calculateMatchScore(account, sfAccount);
      if (score > bestSfScore) {
        bestSfScore = score;
        bestSfMatch = sfAccount;
        bestSfFields = matchedFields || [];
      }
    }

    pendingMatches.push({
      index: id,
      sourceAccount: account,
      dimensionsMatch: bestDimMatch,
      salesforceMatch: bestSfMatch,
      dimensionsScore: bestDimScore,
      salesforceScore: bestSfScore,
      dimensionsMatchedFields: bestDimFields,
      salesforceMatchedFields: bestSfFields
    });
  }

  console.log(`[processSourceChunk] Scoring completed in ${Date.now() - startTime}ms`);

  // Step 2: Collect pairs for AI validation
  const aiPairs: AccountPairForValidation[] = [];
  let pairId = 1;
  const pairIdMap = new Map<string, number>(); // "index-type" -> pairId

  for (const pm of pendingMatches) {
    if (pm.dimensionsMatch && shouldValidateWithAI(pm.dimensionsScore)) {
      const key = `${pm.index}-dim`;
      pairIdMap.set(key, pairId);
      aiPairs.push({
        id: pairId++,
        source: pm.sourceAccount,
        target: pm.dimensionsMatch,
        score: pm.dimensionsScore,
        matchedFields: pm.dimensionsMatchedFields,
        targetType: 'dimensions'
      });
    }
    if (pm.salesforceMatch && shouldValidateWithAI(pm.salesforceScore)) {
      const key = `${pm.index}-sf`;
      pairIdMap.set(key, pairId);
      aiPairs.push({
        id: pairId++,
        source: pm.sourceAccount,
        target: pm.salesforceMatch,
        score: pm.salesforceScore,
        matchedFields: pm.salesforceMatchedFields,
        targetType: 'salesforce'
      });
    }
  }

  console.log(`[processSourceChunk] Sending ${aiPairs.length} pairs to AI`);

  // Step 3: Batch AI validation
  let aiResults = new Map<number, AIValidationResult>();
  if (enableAI && aiPairs.length > 0) {
    aiResults = await validateBatchWithAI(aiPairs);
  }

  // Step 4: Build final results
  const matches: MatchedAccount[] = [];
  const stats = { both: 0, dimOnly: 0, sfOnly: 0, new: 0, aiValidated: aiPairs.length };

  for (const pm of pendingMatches) {
    const dimPairId = pairIdMap.get(`${pm.index}-dim`);
    const sfPairId = pairIdMap.get(`${pm.index}-sf`);

    const dimAI = dimPairId ? aiResults.get(dimPairId) || null : null;
    const sfAI = sfPairId ? aiResults.get(sfPairId) || null : null;

    const dimStatus = pm.dimensionsMatch
      ? determineFinalStatus(pm.dimensionsScore, dimAI)
      : 'NEW';
    const sfStatus = pm.salesforceMatch
      ? determineFinalStatus(pm.salesforceScore, sfAI)
      : 'NEW';

    // Determine final status
    const hasDim = dimStatus === 'CONFIRMED' || dimStatus === 'REVIEW';
    const hasSf = sfStatus === 'CONFIRMED' || sfStatus === 'REVIEW';

    let finalStatus: 'BOTH' | 'DIM_ONLY' | 'SF_ONLY' | 'NEW';
    if (hasDim && hasSf) {
      finalStatus = 'BOTH';
      stats.both++;
    } else if (hasDim) {
      finalStatus = 'DIM_ONLY';
      stats.dimOnly++;
    } else if (hasSf) {
      finalStatus = 'SF_ONLY';
      stats.sfOnly++;
    } else {
      finalStatus = 'NEW';
      stats.new++;
    }

    matches.push({
      sourceAccount: pm.sourceAccount,
      dimensionsMatch: pm.dimensionsMatch,
      salesforceMatch: pm.salesforceMatch,
      dimensionsScore: pm.dimensionsScore,
      salesforceScore: pm.salesforceScore,
      dimensionsMatchedFields: pm.dimensionsMatchedFields,
      salesforceMatchedFields: pm.salesforceMatchedFields,
      dimensionsAI: dimAI,
      salesforceAI: sfAI,
      dimensionsStatus: dimStatus as any,
      salesforceStatus: sfStatus as any,
      finalStatus
    });
  }

  console.log(`[processSourceChunk] Completed. Stats:`, stats);

  return {
    matches,
    totalSourceAccounts,
    processedCount: sourceAccountsRows.length,
    hasMore: (startIndex + sourceAccountsRows.length) < totalSourceAccounts,
    stats
  };
};