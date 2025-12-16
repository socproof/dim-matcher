import { AI_CONFIG } from './config';

export interface AIValidationResult {
  isMatch: boolean;
  confidence: number;
  reasoning: string;
  error?: string;
  skipped?: boolean;
}

export interface AccountPairForValidation {
  id: number;
  source: any;
  target: any;
  score: number;
  matchedFields: string[];
  targetType: 'dimensions' | 'salesforce';
}

export function shouldValidateWithAI(heuristicScore: number): boolean {
  return heuristicScore >= AI_CONFIG.minScore && heuristicScore <= AI_CONFIG.maxScore;
}

// Split array into chunks
function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

// Single batch request
async function sendBatchRequest(pairs: AccountPairForValidation[]): Promise<Map<number, AIValidationResult>> {
  const resultMap = new Map<number, AIValidationResult>();

  try {
    const response = await fetch('/api/ai/validate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pairs }),
    });

    if (!response.ok) {
      throw new Error('AI request failed');
    }

    const data = await response.json();

    for (const result of data.results) {
      resultMap.set(result.id, {
        isMatch: result.isMatch,
        confidence: result.confidence,
        reasoning: result.reasoning
      });
    }
  } catch (error) {
    // Fill with error results
    for (const pair of pairs) {
      resultMap.set(pair.id, {
        isMatch: false,
        confidence: 0,
        reasoning: 'AI validation failed',
        error: String(error)
      });
    }
  }

  return resultMap;
}

export async function validateBatchWithAI(
  pairs: AccountPairForValidation[]
): Promise<Map<number, AIValidationResult>> {
  const resultMap = new Map<number, AIValidationResult>();

  if (pairs.length === 0) {
    return resultMap;
  }

  // Process in smaller batches of 5 pairs, run 2 in parallel
  const BATCH_SIZE = 5;
  const PARALLEL_BATCHES = 2;

  const batches = chunkArray(pairs, BATCH_SIZE);
  console.log(`[AI Validation] Split ${pairs.length} pairs into ${batches.length} batches`);

  // Process batches in parallel groups
  for (let i = 0; i < batches.length; i += PARALLEL_BATCHES) {
    const parallelBatches = batches.slice(i, i + PARALLEL_BATCHES);

    console.log(`[AI Validation] Processing batches ${i + 1}-${Math.min(i + PARALLEL_BATCHES, batches.length)}/${batches.length}`);

    const results = await Promise.all(
      parallelBatches.map(batch => sendBatchRequest(batch))
    );

    // Merge results
    for (const batchResult of results) {
      for (const [id, result] of batchResult) {
        resultMap.set(id, result);
      }
    }
  }

  return resultMap;
}

export function determineFinalStatus(
  heuristicScore: number,
  aiResult: AIValidationResult | null
): 'CONFIRMED' | 'REJECTED' | 'REVIEW' {
  // No AI result or error
  if (!aiResult || aiResult.error) {
    if (heuristicScore > 100) return 'REVIEW'; // High score but no AI - needs review
    if (heuristicScore < AI_CONFIG.minScore) return 'REJECTED';
    return 'REVIEW';
  }

  // AI result available
  if (aiResult.confidence >= 80) {
    return aiResult.isMatch ? 'CONFIRMED' : 'REJECTED';
  }

  if (aiResult.confidence >= 60) {
    // Medium confidence - consider heuristic score too
    if (aiResult.isMatch && heuristicScore >= 85) return 'CONFIRMED';
    if (!aiResult.isMatch && heuristicScore < 50) return 'REJECTED';
  }

  return 'REVIEW';
}