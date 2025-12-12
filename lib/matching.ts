// lib\matching.ts

import { ALIKE_MATCH_FIELDS, MATCH_THRESHOLD, MAX_POSSIBLE_SCORE } from './matching-config';
import { compareFieldValues } from './normalize';

type FieldMatchResult = {
  status: 'exact' | 'partial' | 'none';
  score: number;
};

export const calculateMatchScore = (
  dbAccount: any,
  sfAccount: any,
  fieldMapping: Record<string, string>,
  country?: string
) => {
  let totalScore = 0;
  const matchedFields: string[] = [];

  // Обработка alike match полей
  for (const [sfField, points] of Object.entries(ALIKE_MATCH_FIELDS)) {
    const dbField = Object.keys(fieldMapping).find(k => fieldMapping[k] === sfField);
    if (!dbField) continue;

    const { status, similarity } = compareFieldValues(
      sfField,
      dbAccount[dbField],
      sfAccount[sfField],
      country
    );

    if (status !== 'none') {
      const score = typeof points === 'object' 
        ? (status === 'exact' ? points.exact : points.alike)
        : Math.round(points * similarity);
      
      totalScore += score;
      matchedFields.push(`${sfField} (${status})`);
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
  dbValue: any,
  sfValue: any,
  country?: string
): FieldMatchResult => {
  if (!dbValue || !sfValue) return { status: 'none', score: 0 };

  // Проверка alike match полей
  if (ALIKE_MATCH_FIELDS[field as keyof typeof ALIKE_MATCH_FIELDS]) {
    const { status, similarity } = compareFieldValues(field, dbValue, sfValue, country);
    const points = ALIKE_MATCH_FIELDS[field as keyof typeof ALIKE_MATCH_FIELDS];
    
    return {
      status,
      score: typeof points === 'object'
        ? (status === 'exact' ? points.exact : points.alike)
        : Math.round(points * similarity)
    };
  }

  return { status: 'none', score: 0 };
};