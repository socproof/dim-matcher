// lib/find-matches-chunked.ts

import { AccountDBRow, getPostgresClient } from './postgres-client';
import { calculateMatchScore } from './matching';
import { MATCH_THRESHOLD } from './matching-config';

// Extract standard fields from DB row (already normalized during insert)
function dbRowToStandardAccount(row: AccountDBRow): any {
  return {
    Name: row.name,
    Phone: row.phone,
    Website: row.website,
    BillingStreet: row.billing_street,
    BillingCity: row.billing_city,
    BillingPostalCode: row.billing_postal_code,
    BillingCountry: row.billing_country,
    // Keep raw_data for display purposes
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
  };
}

export const processSourceChunk = async (
  fieldMapping: Record<string, any>,
  chunkSize: number,
  startIndex: number = 0,
): Promise<ChunkProcessingResult> => {
  const client = getPostgresClient();

  console.log(`[processSourceChunk] Starting chunk: size=${chunkSize}, startIndex=${startIndex}`);
  console.log(`[processSourceChunk] Field mapping:`, fieldMapping);

  const totalSourceAccounts = await client.getTotalSourceCount();

  console.log(`[processSourceChunk] Total source accounts: ${totalSourceAccounts}`);

  if (totalSourceAccounts === 0 || startIndex >= totalSourceAccounts) {
    console.log('[processSourceChunk] No more source accounts to process');
    return { 
      matches: [], 
      totalSourceAccounts, 
      processedCount: 0, 
      hasMore: false,
      stats: { both: 0, dimOnly: 0, sfOnly: 0, new: 0 }
    };
  }

  const sourceAccountsRows = await client.getSourceAccountsChunk(chunkSize, startIndex);

  if (!sourceAccountsRows || sourceAccountsRows.length === 0) {
    console.log('[processSourceChunk] No source accounts in this chunk');
    return { 
      matches: [], 
      totalSourceAccounts, 
      processedCount: 0, 
      hasMore: startIndex < totalSourceAccounts,
      stats: { both: 0, dimOnly: 0, sfOnly: 0, new: 0 }
    };
  }

  console.log(`[processSourceChunk] Processing ${sourceAccountsRows.length} source accounts`);

  const matches: MatchedAccount[] = [];
  const stats = { both: 0, dimOnly: 0, sfOnly: 0, new: 0 };

  for (let i = 0; i < sourceAccountsRows.length; i++) {
    const sourceRow = sourceAccountsRows[i];
    const parsedSourceAccount = dbRowToStandardAccount(sourceRow);

    console.log(`\n[processSourceChunk] ===== Processing source account ${i + 1}/${sourceAccountsRows.length} =====`);
    console.log(`[processSourceChunk] Source account name: ${parsedSourceAccount.Name}`);

    // Ищем в Dimensions
    console.log(`[processSourceChunk] Searching in Dimensions...`);
    const dimensionsMatches = await client.findPotentialMatches(
      parsedSourceAccount,
      'dimensions',
      100
    );

    console.log(`[processSourceChunk] Found ${dimensionsMatches?.length || 0} potential Dimensions matches`);

    let bestDimensionsMatch: any = null;
    let bestDimensionsScore = 0;
    let bestDimensionsFields: string[] = [];

    if (dimensionsMatches && dimensionsMatches.length > 0) {
      for (const dimRow of dimensionsMatches) {
        const parsedDimAccount = dbRowToStandardAccount(dimRow);

        const { score, matchedFields } = calculateMatchScore(
          parsedSourceAccount,
          parsedDimAccount,
          fieldMapping
        );

        console.log(`[processSourceChunk] Dimensions match score: ${score}, fields:`, matchedFields);

        if (score > bestDimensionsScore) {
          bestDimensionsScore = score;
          bestDimensionsMatch = parsedDimAccount;
          bestDimensionsFields = matchedFields || [];
        }
      }
    }

    console.log(`[processSourceChunk] Best Dimensions score: ${bestDimensionsScore}`);

    // Ищем в Salesforce
    console.log(`[processSourceChunk] Searching in Salesforce...`);
    const salesforceMatches = await client.findPotentialMatches(
      parsedSourceAccount,
      'salesforce',
      100
    );

    console.log(`[processSourceChunk] Found ${salesforceMatches?.length || 0} potential Salesforce matches`);

    let bestSalesforceMatch: any = null;
    let bestSalesforceScore = 0;
    let bestSalesforceFields: string[] = [];

    if (salesforceMatches && salesforceMatches.length > 0) {
      for (const sfRow of salesforceMatches) {
        const parsedSfAccount = dbRowToStandardAccount(sfRow);

        const { score, matchedFields } = calculateMatchScore(
          parsedSourceAccount,
          parsedSfAccount,
          fieldMapping
        );

        console.log(`[processSourceChunk] Salesforce match score: ${score}, fields:`, matchedFields);

        if (score > bestSalesforceScore) {
          bestSalesforceScore = score;
          bestSalesforceMatch = parsedSfAccount;
          bestSalesforceFields = matchedFields || [];
        }
      }
    }

    console.log(`[processSourceChunk] Best Salesforce score: ${bestSalesforceScore}`);

    const hasDimensionsMatch = bestDimensionsScore >= MATCH_THRESHOLD;
    const hasSalesforceMatch = bestSalesforceScore >= MATCH_THRESHOLD;

    console.log(`[processSourceChunk] Match threshold: ${MATCH_THRESHOLD}`);
    console.log(`[processSourceChunk] Has Dimensions match: ${hasDimensionsMatch}`);
    console.log(`[processSourceChunk] Has Salesforce match: ${hasSalesforceMatch}`);

    let finalStatus: 'BOTH' | 'DIM_ONLY' | 'SF_ONLY' | 'NEW';
    if (hasDimensionsMatch && hasSalesforceMatch) {
      finalStatus = 'BOTH';
      stats.both++;
    } else if (hasDimensionsMatch) {
      finalStatus = 'DIM_ONLY';
      stats.dimOnly++;
    } else if (hasSalesforceMatch) {
      finalStatus = 'SF_ONLY';
      stats.sfOnly++;
    } else {
      finalStatus = 'NEW';
      stats.new++;
    }

    console.log(`[processSourceChunk] Final status: ${finalStatus}`);

    matches.push({
      sourceAccount: parsedSourceAccount,
      dimensionsMatch: bestDimensionsMatch,
      salesforceMatch: bestSalesforceMatch,
      dimensionsScore: bestDimensionsScore,
      salesforceScore: bestSalesforceScore,
      dimensionsMatchedFields: bestDimensionsFields,
      salesforceMatchedFields: bestSalesforceFields,
      finalStatus
    });
  }

  console.log(`\n[processSourceChunk] ===== Chunk completed =====`);
  console.log(`[processSourceChunk] Stats:`, stats);

  return {
    matches,
    totalSourceAccounts,
    processedCount: sourceAccountsRows.length,
    hasMore: (startIndex + sourceAccountsRows.length) < totalSourceAccounts,
    stats
  };
};