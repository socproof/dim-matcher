// /lib/find-matches-chunked.ts - New file or rename/replace find-matches.ts
import { AccountDBRow, getSQLiteClient } from './sqlite-client';
import { calculateMatchScore } from './matching';
import { MATCH_THRESHOLD, MAX_POSSIBLE_SCORE } from './matching-config';

interface CountResult {
  count: number;
}

// Represents a single item processed in the chunk
interface ProcessedItem {
  dbAccountRaw: any; // Parsed raw data from DB
  bestSfMatch: AccountDBRow | null; // Best SF match found (can be null)
  score: number; // Score of the best match (0 if no match or below threshold doesn't matter here)
  matchedFields: string[]; // Fields contributing to the score
  status: 'matched' | 'new'; // Based on threshold comparison
  maxPossibleScore: number;
}

interface ChunkProcessingResult {
  items: ProcessedItem[];
  totalDbAccounts: number;
  processedCount: number;
  hasMore: boolean;
}

export const processChunk = async (
  fieldMapping: Record<string, any>, // Allow nested mapping structure
  chunkSize: number,
  startIndex: number = 0,
): Promise<ChunkProcessingResult> => {
  const client = await getSQLiteClient();

  const totalResultArray = await client.query<CountResult>(
    "SELECT COUNT(*) as count FROM accounts WHERE source = 'databricks' AND Name != ''"
  );

  const totalDbAccounts = (totalResultArray && totalResultArray.length > 0 && totalResultArray[0]) ? totalResultArray[0].count : 0;

  if (totalDbAccounts === 0 || startIndex >= totalDbAccounts) {
    console.log("No more Databricks accounts found to process.");
    return { items: [], totalDbAccounts, processedCount: 0, hasMore: false };
  }

  const dbAccountsRows = await client.query<AccountDBRow>(
    `SELECT * FROM accounts
     WHERE source = 'databricks'
     AND Name != ''
     ORDER BY Name
     LIMIT ? OFFSET ?`,
    [chunkSize, startIndex]
  );

  if (!dbAccountsRows || dbAccountsRows.length === 0) {
    console.log(`No Databricks accounts found for chunk: startIndex ${startIndex}, size ${chunkSize}.`);
    // This condition might indicate the end if startIndex >= totalDbAccounts was not caught
    return { items: [], totalDbAccounts, processedCount: 0, hasMore: startIndex < totalDbAccounts };
  }

  const processedItems: ProcessedItem[] = [];

  // Use Promise.all for parallel processing of each DB account in the chunk
  await Promise.all(
    dbAccountsRows.map(async (dbAccountRow: AccountDBRow) => {
      let parsedDbAccount: any;
      try {
         // Important: Parse the raw_data here
         parsedDbAccount = JSON.parse(dbAccountRow.raw_data);
         // Add the DB row ID to the parsed object if it doesn't exist, for tracking
         if (!parsedDbAccount.id && dbAccountRow.id) {
             parsedDbAccount.id = dbAccountRow.id;
         } else if (!parsedDbAccount.id) {
             console.warn("Databricks record missing an identifiable ID in raw_data and DB row:", dbAccountRow);
             // Assign a temporary unique identifier if absolutely necessary, though relying on DB id is better
             parsedDbAccount.id = `temp_${Math.random()}`;
         }

      } catch (error) {
        console.error('Error parsing Databricks account raw_data:', {
          dbAccountId: dbAccountRow.id,
          error: error instanceof Error ? error.message : String(error),
          rawDataSnippet: dbAccountRow.raw_data?.substring(0, 200)
        });
        // Skip this record if parsing fails fundamentally
        return;
      }

       // Ensure fieldMapping is correctly structured for nested access if needed
       const dbFieldMapping = fieldMapping?.databricks || {};
       const sfFieldMapping = fieldMapping?.salesforce || {}; // Assuming SF mapping might also be nested


      try {
        const potentialSfMatches: AccountDBRow[] = await client.findPotentialMatches(
          parsedDbAccount,
          dbFieldMapping, // Pass the specific DB mapping part
        );

        let bestMatchResult: { sfAccount: AccountDBRow; score: number; matchedFields: string[] } | null = null;

        if (potentialSfMatches && potentialSfMatches.length > 0) {
          for (const sfAccountRow of potentialSfMatches) {
              let parsedSfAccount : any = null;
               try {
                  // Parse SF raw_data for accurate comparison in calculateMatchScore
                  parsedSfAccount = JSON.parse(sfAccountRow.raw_data);
                   if (!parsedSfAccount.id && sfAccountRow.id) {
                      parsedSfAccount.id = sfAccountRow.id; // Add DB row ID if missing
                   }
               } catch (parseError) {
                    console.error("Failed to parse Salesforce raw_data for potential match:", { sfAccountId: sfAccountRow.id, error: parseError });
                    continue; // Skip this potential match if parsing fails
               }

            const { score, matchedFields } = calculateMatchScore(
              parsedDbAccount,
              parsedSfAccount, // Use parsed SF account
              fieldMapping, // Pass the full mapping here as calculateMatchScore might need both sides
              sfAccountRow // Pass the original row too, if needed by calculateMatchScore internals (e.g., for normalized fields)
            );

            if (bestMatchResult === null || score > bestMatchResult.score) {
              bestMatchResult = { sfAccount: sfAccountRow, score, matchedFields }; // Store the original SF row here
            }
          }
        }

        const finalScore = bestMatchResult?.score ?? 0;
        const status: 'matched' | 'new' = finalScore >= MATCH_THRESHOLD ? 'matched' : 'new';

        processedItems.push({
          dbAccountRaw: parsedDbAccount,
          bestSfMatch: bestMatchResult?.sfAccount || null,
          score: finalScore,
          matchedFields: bestMatchResult?.matchedFields || [],
          status: status,
          maxPossibleScore: MAX_POSSIBLE_SCORE
        });

      } catch (error) {
        console.error('Error processing Databricks account for matching:', {
          dbAccountId: parsedDbAccount?.id || dbAccountRow.id,
          error: error instanceof Error ? error.message : String(error),
          // stack: error instanceof Error ? error.stack : undefined,
        });
         // Optionally add a placeholder item indicating an error for this DB account
         processedItems.push({
             dbAccountRaw: parsedDbAccount, // Store what we have
             bestSfMatch: null,
             score: 0,
             matchedFields: [],
             status: 'new', // Treat as new if error occurs during matching
             maxPossibleScore: MAX_POSSIBLE_SCORE,
             // Could add an error flag here: error: true
         });
      }
    })
  );

  // Ensure consistent sorting or maintain original order if needed
  // processedItems.sort((a, b) => (b.score - a.score)); // Or sort by original order if required

  const processedCount = dbAccountsRows.length;
  const nextStartIndex = startIndex + processedCount;

  return {
    items: processedItems,
    totalDbAccounts,
    processedCount,
    hasMore: nextStartIndex < totalDbAccounts
  };
};
