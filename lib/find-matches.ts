// find-matches.ts
import { AccountDBRow, getSQLiteClient } from './sqlite-client';
import { calculateMatchScore } from './matching'; // Assuming this function exists and takes (dbAcc, sfAcc, fieldMapping)
import { MATCH_THRESHOLD, MAX_POSSIBLE_SCORE } from './matching-config'; // Import scoring constants

// Interface for the count result from SQL
interface CountResult {
  count: number;
}

// Interface for the detailed result of matching a single Databricks account
// This structure is returned for *every* DB account in the chunk.
export interface MatchItem {
  dbAccountId: number; // Original DB account ID from the 'accounts' table
  dbAccount: any; // Parsed Databricks account object (from raw_data)
  sfAccount: any | null; // Parsed Salesforce account object (best match found, or null)
  score: number; // The score of the best match found (0 if no potential match or score is 0)
  matchedFields: string[]; // Fields that contributed positively to the score (empty if 'new')
  status: 'matched' | 'new'; // 'matched' if score >= threshold, 'new' otherwise
  maxPossibleScore: number; // Maximum possible score from config
}

// Interface for the paginated result returned by findMatchesChunked
export interface PaginatedMatchResult {
  matches: MatchItem[]; // Array containing results for all DB accounts in the chunk
  total: number; // Total number of Databricks accounts in the DB
  hasMore: boolean; // Indicates if there are more chunks to process
}

/**
 * Processes a chunk of Databricks accounts to find the best potential Salesforce match for each.
 * Returns results for ALL Databricks accounts in the chunk, marking them as 'matched' or 'new'.
 *
 * @param fieldMapping - The complete field mapping object, expected to have keys like 'databricks' and 'salesforce'.
 * @param chunkSize - The number of Databricks accounts to process in this chunk.
 * @param startIndex - The starting offset for fetching Databricks accounts.
 * @returns A Promise resolving to a PaginatedMatchResult object.
 */
export const findMatchesChunked = async (
  // Expecting the full mapping object, e.g., { databricks: {...}, salesforce: {...} }
  fieldMapping: Record<string, Record<string, string>>,
  chunkSize: number,
  startIndex: number = 0,
): Promise<PaginatedMatchResult> => {
  const client = await getSQLiteClient();

  // 1. Get the total count of Databricks accounts for pagination info
  let total = 0;
  try {
      const totalResultArray = await client.query<CountResult>(
        "SELECT COUNT(*) as count FROM accounts WHERE source = 'databricks'"
      );
      total = totalResultArray?.[0]?.count || 0;
  } catch (countError) {
      console.error("Failed to get total Databricks account count:", countError);
      throw new Error("Could not retrieve total account count from database."); // Fail fast if count fails
  }


  if (total === 0) {
    console.log("No Databricks accounts found in the database to process.");
    return { matches: [], total: 0, hasMore: false };
  }

  // 2. Fetch the current chunk of Databricks account rows
  let dbAccountsRows: AccountDBRow[] | null = null;
  try {
      dbAccountsRows = await client.query<AccountDBRow[]>(
        `SELECT * FROM accounts
         WHERE source = 'databricks'
         ORDER BY id -- Ensure consistent ordering for pagination
         LIMIT ? OFFSET ?`,
        [chunkSize, startIndex]
      );
  } catch (fetchError) {
       console.error(`Failed to fetch Databricks accounts chunk (offset: ${startIndex}, size: ${chunkSize}):`, fetchError);
       throw new Error("Could not retrieve account chunk from database."); // Fail fast
  }


  if (!dbAccountsRows || dbAccountsRows.length === 0) {
    // This might happen if startIndex >= total
    // console.log(`No more Databricks accounts found for chunk starting at index ${startIndex}.`);
    return { matches: [], total, hasMore: startIndex < total };
  }

  const results: MatchItem[] = [];
  const databricksMapping = fieldMapping?.databricks || {}; // Extract DB mapping
  const salesforceMapping = fieldMapping?.salesforce || {}; // Extract SF mapping

  // 3. Process each Databricks account in the fetched chunk
  await Promise.all(
    dbAccountsRows.map(async (dbAccountRow: AccountDBRow) => {
      let parsedDbAccount: any;
      try {
        // Parse the raw JSON data for the Databricks account
        parsedDbAccount = JSON.parse(dbAccountRow.raw_data);
      } catch (parseError) {
        console.error(`Failed to parse Databricks account raw_data (ID: ${dbAccountRow.id}):`, parseError, `Raw data snippet: ${dbAccountRow.raw_data.substring(0, 100)}...`);
        // Skip this account if parsing fails, or add an error placeholder to results
        return; // Skip this item
      }

      try {
        // Find potential Salesforce matches using the client's method
        // Pass the *Databricks* mapping so findPotentialMatches knows how to extract query terms
        const potentialSfMatchesRows: AccountDBRow[] = await client.findPotentialMatches(
          parsedDbAccount,
          databricksMapping, // Pass DB mapping for extracting query terms
        );

        let bestMatchScore = -1; // Initialize score below zero
        let bestSfMatchData: any | null = null; // Store the *parsed* best SF match
        let bestMatchedFields: string[] = [];

        // If potential matches were found, score each one against the DB account
        if (potentialSfMatchesRows && potentialSfMatchesRows.length > 0) {
          for (const sfAccountRow of potentialSfMatchesRows) {
             let parsedSfAccount: any;
             try {
                 // Parse the raw JSON data for the Salesforce account
                 parsedSfAccount = JSON.parse(sfAccountRow.raw_data);
             } catch (parseError) {
                 console.error(`Failed to parse Salesforce account raw_data (ID: ${sfAccountRow.id}) during scoring:`, parseError);
                 continue; // Skip this potential match if parsing fails
             }

            // Calculate the match score using the dedicated function
            // Pass both parsed accounts and the full field mapping
            const { score, matchedFields } = calculateMatchScore(
              parsedDbAccount,
              parsedSfAccount, // Use parsed SF account
              fieldMapping // Pass the full mapping object
            );

            // Keep track of the Salesforce account with the highest score
            if (score > bestMatchScore) {
              bestMatchScore = score;
              bestSfMatchData = parsedSfAccount; // Store the parsed data
              bestMatchedFields = matchedFields || []; // Ensure it's an array
            }
          }
        }

        // Determine the final status based on the best score found vs the threshold
        const finalScore = bestMatchScore >= 0 ? bestMatchScore : 0; // Ensure score is not negative
        const status: 'matched' | 'new' = finalScore >= MATCH_THRESHOLD ? 'matched' : 'new';

        // Add the result object for this Databricks account to the results array
        // Every DB account in the chunk gets an entry.
        results.push({
          dbAccountId: dbAccountRow.id, // Use the unique ID from the DB table
          dbAccount: parsedDbAccount,
          // Include the best SF match data found, regardless of status (UI can decide how to display)
          sfAccount: bestSfMatchData,
          score: finalScore,
          // Only include matchedFields details if the status is 'matched'
          matchedFields: status === 'matched' ? bestMatchedFields : [],
          status: status,
          maxPossibleScore: MAX_POSSIBLE_SCORE,
        });

      } catch (matchError) {
        console.error(`Error processing matching logic for Databricks account ID ${dbAccountRow.id}:`, matchError);
        // Optionally add a placeholder error result if needed
        // results.push({ dbAccountId: dbAccountRow.id, status: 'error', ... });
      }
    })
  );

  // 4. Return the results for the chunk
  return {
    matches: results, // Contains results for all processed DB accounts in the chunk
    total, // Total number of DB accounts in the database
    hasMore: (startIndex + dbAccountsRows.length) < total // Check if more chunks exist
  };
};


/**
 * Finds all matches above the threshold across the entire dataset.
 * NOTE: This function contradicts the chunk-based processing requirement
 * and might be very slow or memory-intensive for large datasets.
 * It's kept here for reference but should likely be updated or removed
 * if the primary workflow is chunked processing.
 *
 * @param fieldMapping - The complete field mapping object.
 * @returns A Promise resolving to an array of MatchItem objects (only those above threshold).
 */
export const findAllMatches = async (
  fieldMapping: Record<string, Record<string, string>>,
): Promise<MatchItem[]> => {
   console.warn("findAllMatches is called, which processes the entire dataset at once and may be inefficient. Consider using the chunked approach.");
   const client = await getSQLiteClient();
   const databricksMapping = fieldMapping?.databricks || {};
   const salesforceMapping = fieldMapping?.salesforce || {};

   let dbAccountsRows: AccountDBRow[] | null = null;
   try {
       dbAccountsRows = await client.query<AccountDBRow[]>(
         "SELECT * FROM accounts WHERE source = 'databricks' ORDER BY id"
       );
   } catch (e) {
       console.error("Failed to fetch all Databricks accounts for findAllMatches:", e);
       return [];
   }


   if (!dbAccountsRows || dbAccountsRows.length === 0) {
     console.log("findAllMatches: No Databricks accounts found.");
     return [];
   }

   const allMatchesAboveThreshold: MatchItem[] = [];

   // Process all accounts - potentially very slow!
   await Promise.all(
     dbAccountsRows.map(async (dbAccountRow: AccountDBRow) => {
       let parsedDbAccount: any;
       try {
           parsedDbAccount = JSON.parse(dbAccountRow.raw_data);
       } catch (e) { return; } // Skip if parsing fails

       try {
           const potentialSfMatchesRows: AccountDBRow[] = await client.findPotentialMatches(
             parsedDbAccount,
             databricksMapping,
           );

           let bestMatch: MatchItem | null = null;

           if (potentialSfMatchesRows && potentialSfMatchesRows.length > 0) {
               for (const sfAccountRow of potentialSfMatchesRows) {
                 let parsedSfAccount: any;
                 try {
                     parsedSfAccount = JSON.parse(sfAccountRow.raw_data);
                 } catch(e) { continue; } // Skip if SF parsing fails

                 const { score, matchedFields } = calculateMatchScore(
                   parsedDbAccount,
                   parsedSfAccount,
                   fieldMapping,
                 );

                 // *** Only consider matches AT or ABOVE the threshold ***
                 if (score >= MATCH_THRESHOLD) {
                   const currentMatch: MatchItem = {
                     dbAccountId: dbAccountRow.id,
                     dbAccount: parsedDbAccount,
                     sfAccount: parsedSfAccount,
                     score: score,
                     matchedFields: matchedFields || [],
                     status: 'matched', // Status is always 'matched' here due to threshold check
                     maxPossibleScore: MAX_POSSIBLE_SCORE
                   };

                   // Keep the one with the highest score if multiple exceed threshold
                   if (!bestMatch || currentMatch.score > bestMatch.score) {
                     bestMatch = currentMatch;
                   }
                 }
               }
           }
           // Add the best match *if* one was found above the threshold
           if (bestMatch) {
             // Use a lock or concurrent-safe way to add if needed, but push is generally fine here
             allMatchesAboveThreshold.push(bestMatch);
           }
       } catch (matchError) {
            console.error(`Error processing matching logic (findAllMatches) for Databricks account ID ${dbAccountRow.id}:`, matchError);
       }
     })
   );

   // Sort the final list of above-threshold matches by score descending
   return allMatchesAboveThreshold.sort((a, b) => b.score - a.score);
};