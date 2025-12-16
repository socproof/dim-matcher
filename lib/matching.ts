import { MATCHING } from './config';
import { compareFieldValues, extractEmailDomain, normalizeWebsite } from './normalize';

type FieldMatchResult = {
  status: 'exact' | 'partial' | 'none';
  score: number;
};

export const calculateMatchScore = (
  sourceAccount: any,
  targetAccount: any,
  _fieldMapping?: any,
  country?: string
) => {
  let totalScore = 0;
  const matchedFields: string[] = [];

  // Standard field matching
  for (const [field, points] of Object.entries(MATCHING.fields)) {
    const sourceValue = sourceAccount[field];
    const targetValue = targetAccount[field];

    const { status, similarity } = compareFieldValues(field, sourceValue, targetValue, country);

    if (status !== 'none') {
      const score = typeof points === 'object'
        ? (status === 'exact' ? points.exact : points.alike)
        : Math.round((points as number) * similarity);

      totalScore += score;
      matchedFields.push(`${field} (${status}: ${Math.round(similarity * 100)}%)`);
    }
  }

  // CROSS-MATCH: Website ↔ EmailDomain
  const sourceWebsite = normalizeWebsite(sourceAccount.Website || '');
  const sourceEmailDomain = sourceAccount.EmailDomain || extractEmailDomain(sourceAccount.Email || '');
  const targetWebsite = normalizeWebsite(targetAccount.Website || '');
  const targetEmailDomain = targetAccount.EmailDomain || extractEmailDomain(targetAccount.Email || '');

  // Source website matches target email domain
  if (sourceWebsite && targetEmailDomain && sourceWebsite === targetEmailDomain) {
    totalScore += 25;
    matchedFields.push('Website↔EmailDomain (cross-match)');
  }
  // Source email domain matches target website
  else if (sourceEmailDomain && targetWebsite && sourceEmailDomain === targetWebsite) {
    totalScore += 25;
    matchedFields.push('EmailDomain↔Website (cross-match)');
  }

  return {
    score: totalScore,
    matchedFields,
    isAboveThreshold: totalScore >= MATCHING.threshold,
    maxPossibleScore: MATCHING.maxPossibleScore
  };
};

export const getFieldMatchDetails = (
  field: string,
  value1: any,
  value2: any,
  country?: string
): FieldMatchResult => {
  if (!value1 || !value2) return { status: 'none', score: 0 };

  if (MATCHING.fields[field as keyof typeof MATCHING.fields]) {
    const { status, similarity } = compareFieldValues(field, value1, value2, country);
    const points = MATCHING.fields[field as keyof typeof MATCHING.fields];

    return {
      status,
      score: typeof points === 'object'
        ? (status === 'exact' ? points.exact : points.alike)
        : Math.round((points as number) * similarity)
    };
  }

  return { status: 'none', score: 0 };
};