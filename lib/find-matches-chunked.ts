// lib/find-matches-chunked.ts

import { AccountDBRow, getSQLiteClient } from './sqlite-client';
import { calculateMatchScore } from './matching';
import { MATCH_THRESHOLD, MAX_POSSIBLE_SCORE } from './matching-config';

interface CountResult {
  count: number;
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

/**
 * Обрабатывает чанк из Source таблицы
 * Для каждой записи ищет совпадения в Dimensions, затем в Salesforce
 */
export const processSourceChunk = async (
  fieldMapping: Record<string, any>,
  chunkSize: number,
  startIndex: number = 0,
): Promise<ChunkProcessingResult> => {
  const client = await getSQLiteClient();

  console.log(`[processSourceChunk] Starting chunk: size=${chunkSize}, startIndex=${startIndex}`);

  // 1. Получаем общее количество Source записей
  const totalResultArray = await client.query<CountResult>(
    "SELECT COUNT(*) as count FROM accounts WHERE source = 'source'"
  );
  const totalSourceAccounts = totalResultArray?.[0]?.count || 0;

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

  // 2. Получаем чанк Source записей
  const sourceAccountsRows = await client.query<AccountDBRow>(
    `SELECT * FROM accounts
     WHERE source = 'source'
     ORDER BY id
     LIMIT ? OFFSET ?`,
    [chunkSize, startIndex]
  );

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

  // 3. Обрабатываем каждую Source запись
  for (const sourceRow of sourceAccountsRows) {
    let parsedSourceAccount: any;
    try {
      parsedSourceAccount = JSON.parse(sourceRow.raw_data);
    } catch (error) {
      console.error(`[processSourceChunk] Failed to parse source account ID ${sourceRow.id}:`, error);
      continue;
    }

    console.log(`[processSourceChunk] Processing source account: ${parsedSourceAccount.Name}`);

    // 4. Ищем совпадения в Dimensions (по ВСЕМ записям)
    const dimensionsMatches = await client.findPotentialMatches(
      parsedSourceAccount,
      fieldMapping.source || fieldMapping.databricks, // Используем source mapping
      'dimensions' // Указываем что ищем в dimensions
    );

    console.log(`[processSourceChunk] Found ${dimensionsMatches?.length || 0} potential Dimensions matches`);

    let bestDimensionsMatch: any = null;
    let bestDimensionsScore = 0;
    let bestDimensionsFields: string[] = [];

    // Скорим все найденные Dimensions совпадения
    if (dimensionsMatches && dimensionsMatches.length > 0) {
      for (const dimRow of dimensionsMatches) {
        let parsedDimAccount: any;
        try {
          parsedDimAccount = JSON.parse(dimRow.raw_data);
        } catch (error) {
          console.error(`[processSourceChunk] Failed to parse dimensions account ID ${dimRow.id}:`, error);
          continue;
        }

        const { score, matchedFields } = calculateMatchScore(
          parsedSourceAccount,
          parsedDimAccount,
          fieldMapping
        );

        if (score > bestDimensionsScore) {
          bestDimensionsScore = score;
          bestDimensionsMatch = parsedDimAccount;
          bestDimensionsFields = matchedFields || [];
        }
      }
    }

    console.log(`[processSourceChunk] Best Dimensions score: ${bestDimensionsScore}`);

    // 5. Ищем совпадения в Salesforce (по ВСЕМ записям)
    const salesforceMatches = await client.findPotentialMatches(
      parsedSourceAccount,
      fieldMapping.source || fieldMapping.databricks,
      'salesforce' // Указываем что ищем в salesforce
    );

    console.log(`[processSourceChunk] Found ${salesforceMatches?.length || 0} potential Salesforce matches`);

    let bestSalesforceMatch: any = null;
    let bestSalesforceScore = 0;
    let bestSalesforceFields: string[] = [];

    if (salesforceMatches && salesforceMatches.length > 0) {
      for (const sfRow of salesforceMatches) {
        let parsedSfAccount: any;
        try {
          parsedSfAccount = JSON.parse(sfRow.raw_data);
        } catch (error) {
          console.error(`[processSourceChunk] Failed to parse salesforce account ID ${sfRow.id}:`, error);
          continue;
        }

        const { score, matchedFields } = calculateMatchScore(
          parsedSourceAccount,
          parsedSfAccount,
          fieldMapping
        );

        if (score > bestSalesforceScore) {
          bestSalesforceScore = score;
          bestSalesforceMatch = parsedSfAccount;
          bestSalesforceFields = matchedFields || [];
        }
      }
    }

    console.log(`[processSourceChunk] Best Salesforce score: ${bestSalesforceScore}`);

    // 6. Определяем финальный статус
    const hasDimensionsMatch = bestDimensionsScore >= MATCH_THRESHOLD;
    const hasSalesforceMatch = bestSalesforceScore >= MATCH_THRESHOLD;

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

  console.log(`[processSourceChunk] Completed. Stats:`, stats);

  return {
    matches,
    totalSourceAccounts,
    processedCount: sourceAccountsRows.length,
    hasMore: (startIndex + sourceAccountsRows.length) < totalSourceAccounts,
    stats
  };
};