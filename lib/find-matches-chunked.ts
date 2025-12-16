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
    Email: row.email,
    EmailDomain: row.email_domain,
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

  console.log(`[processSourceChunk] ========== START ==========`);
  console.log(`[processSourceChunk] Chunk size: ${chunkSize}, Offset: ${startIndex}, AI enabled: ${enableAI}`);
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

  const sourceAccounts = sourceAccountsRows.map((row, idx) => ({
    id: idx,
    row,
    account: dbRowToStandardAccount(row)
  }));

  console.log(`[processSourceChunk] Loaded ${sourceAccounts.length} source accounts`);
  const searchStart = Date.now();

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

  const pendingMatches: PendingMatch[] = [];

  for (const { id, account } of sourceAccounts) {
    const dimCandidates = dimMatchesMap.get(id) || [];
    const sfCandidates = sfMatchesMap.get(id) || [];

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

  // Build AI validation pairs with detailed logging
  const aiPairs: AccountPairForValidation[] = [];
  let pairId = 1;
  const pairIdMap = new Map<string, number>();

  console.log(`[AI Pair Building] ========== Creating pairs for AI validation ==========`);

  for (const pm of pendingMatches) {
    const sourceName = pm.sourceAccount.Name || 'Unknown';

    // Check Dimensions
    if (pm.dimensionsMatch && shouldValidateWithAI(pm.dimensionsScore)) {
      const key = `${pm.index}-dim`;
      pairIdMap.set(key, pairId);

      console.log(`[AI Pair] ID=${pairId} | SourceIdx=${pm.index} | Type=DIM | Score=${pm.dimensionsScore}`);
      console.log(`  Source: "${sourceName}"`);
      console.log(`  Target: "${pm.dimensionsMatch.Name}"`);
      console.log(`  Key: "${key}" → PairID: ${pairId}`);

      aiPairs.push({
        id: pairId++,
        source: pm.sourceAccount,
        target: pm.dimensionsMatch,
        score: pm.dimensionsScore,
        matchedFields: pm.dimensionsMatchedFields,
        targetType: 'dimensions'
      });
    } else if (pm.dimensionsMatch) {
      console.log(`[Skip DIM] SourceIdx=${pm.index} | Score=${pm.dimensionsScore} | Reason: ${pm.dimensionsScore < 20 ? 'too low (<20)' : 'too high (>100)'}`);
    }

    // Check Salesforce
    if (pm.salesforceMatch && shouldValidateWithAI(pm.salesforceScore)) {
      const key = `${pm.index}-sf`;
      pairIdMap.set(key, pairId);

      console.log(`[AI Pair] ID=${pairId} | SourceIdx=${pm.index} | Type=SF | Score=${pm.salesforceScore}`);
      console.log(`  Source: "${sourceName}"`);
      console.log(`  Target: "${pm.salesforceMatch.Name}"`);
      console.log(`  Key: "${key}" → PairID: ${pairId}`);

      aiPairs.push({
        id: pairId++,
        source: pm.sourceAccount,
        target: pm.salesforceMatch,
        score: pm.salesforceScore,
        matchedFields: pm.salesforceMatchedFields,
        targetType: 'salesforce'
      });
    } else if (pm.salesforceMatch) {
      console.log(`[Skip SF] SourceIdx=${pm.index} | Score=${pm.salesforceScore} | Reason: ${pm.salesforceScore < 20 ? 'too low (<20)' : 'too high (>100)'}`);
    }
  }

  console.log(`[AI Pair Building] Created ${aiPairs.length} pairs for validation`);
  console.log(`[AI Pair Building] PairID Map:`, Object.fromEntries(pairIdMap));

  // AI Validation
  let aiResults = new Map<number, AIValidationResult>();
  if (enableAI && aiPairs.length > 0) {
    console.log(`[AI Validation] Sending ${aiPairs.length} pairs to AI...`);
    aiResults = await validateBatchWithAI(aiPairs);
    console.log(`[AI Validation] Received ${aiResults.size} results from AI`);
    console.log(`[AI Validation] Result IDs:`, Array.from(aiResults.keys()));
  } else {
    console.log(`[AI Validation] Skipped (enabled=${enableAI}, pairs=${aiPairs.length})`);
  }

  // Build final results with detailed mapping verification
  console.log(`[Result Mapping] ========== Mapping AI results back to source accounts ==========`);

  const matches: MatchedAccount[] = [];
  const stats = { both: 0, dimOnly: 0, sfOnly: 0, new: 0, aiValidated: aiPairs.length };

  for (const pm of pendingMatches) {
    const dimKey = `${pm.index}-dim`;
    const sfKey = `${pm.index}-sf`;

    const dimPairId = pairIdMap.get(dimKey);
    const sfPairId = pairIdMap.get(sfKey);

    const dimAI = dimPairId ? aiResults.get(dimPairId) || null : null;
    const sfAI = sfPairId ? aiResults.get(sfPairId) || null : null;

    console.log(`[Mapping] SourceIdx=${pm.index} "${pm.sourceAccount.Name}"`);
    if (dimPairId !== undefined) {
      console.log(`  DIM: Key="${dimKey}" → PairID=${dimPairId} → AI Result: ${dimAI ? `isMatch=${dimAI.isMatch}, conf=${dimAI.confidence}%` : 'NOT FOUND'}`);
      if (dimAI && pm.dimensionsMatch) {
        console.log(`    Expected: "${pm.sourceAccount.Name}" vs "${pm.dimensionsMatch.Name}"`);
      }
    }
    if (sfPairId !== undefined) {
      console.log(`  SF: Key="${sfKey}" → PairID=${sfPairId} → AI Result: ${sfAI ? `isMatch=${sfAI.isMatch}, conf=${sfAI.confidence}%` : 'NOT FOUND'}`);
      if (sfAI && pm.salesforceMatch) {
        console.log(`    Expected: "${pm.sourceAccount.Name}" vs "${pm.salesforceMatch.Name}"`);
      }
    }

    const dimStatus = pm.dimensionsMatch
      ? determineFinalStatus(pm.dimensionsScore, dimAI)
      : 'NEW';
    const sfStatus = pm.salesforceMatch
      ? determineFinalStatus(pm.salesforceScore, sfAI)
      : 'NEW';

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

  console.log(`[processSourceChunk] ========== COMPLETE ==========`);
  console.log(`[processSourceChunk] Stats:`, stats);

  return {
    matches,
    totalSourceAccounts,
    processedCount: sourceAccountsRows.length,
    hasMore: (startIndex + sourceAccountsRows.length) < totalSourceAccounts,
    stats
  };
};