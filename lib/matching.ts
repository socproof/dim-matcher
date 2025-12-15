import { ALIKE_MATCH_FIELDS, MATCH_THRESHOLD, MAX_POSSIBLE_SCORE } from './matching-config';
import { compareFieldValues } from './normalize';

type FieldMatchResult = {
  status: 'exact' | 'partial' | 'none';
  score: number;
};

export const calculateMatchScore = (
  sourceAccount: any,
  targetAccount: any,
  _fieldMapping?: any, // Keep parameter for compatibility, but not used
  country?: string
) => {
  let totalScore = 0;
  const matchedFields: string[] = [];

  // Both accounts are now in standard format (Name, Phone, BillingStreet, etc.)
  // Compare fields directly by name
  for (const [field, points] of Object.entries(ALIKE_MATCH_FIELDS)) {
    const sourceValue = sourceAccount[field];
    const targetValue = targetAccount[field];

    const { status, similarity } = compareFieldValues(
      field,
      sourceValue,
      targetValue,
      country
    );

    if (status !== 'none') {
      const score = typeof points === 'object' 
        ? (status === 'exact' ? points.exact : points.alike)
        : Math.round((points as number) * similarity);
      
      totalScore += score;
      matchedFields.push(`${field} (${status}: ${Math.round(similarity * 100)}%)`);
    }
  }

  return {
    score: totalScore,
    matchedFields,
    isAboveThreshold: totalScore >= MATCH_THRESHOLD,
    maxPossibleScore: MAX_POSSIBLE_SCORE
  };
};

export const getFieldMatchDetails = (
  field: string,
  value1: any,
  value2: any,
  country?: string
): FieldMatchResult => {
  if (!value1 || !value2) return { status: 'none', score: 0 };

  if (ALIKE_MATCH_FIELDS[field as keyof typeof ALIKE_MATCH_FIELDS]) {
    const { status, similarity } = compareFieldValues(field, value1, value2, country);
    const points = ALIKE_MATCH_FIELDS[field as keyof typeof ALIKE_MATCH_FIELDS];
    
    return {
      status,
      score: typeof points === 'object'
        ? (status === 'exact' ? points.exact : points.alike)
        : Math.round((points as number) * similarity)
    };
  }

  return { status: 'none', score: 0 };
};