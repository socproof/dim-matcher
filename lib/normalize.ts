import { compareTwoStrings } from 'string-similarity';
import { GENERIC_EMAIL_DOMAINS } from './config';

const COMPANY_SUFFIXES = [
  'ltd', 'limited', 'pty', 'pty ltd', 'pty limited',
  'inc', 'incorporated', 'llc', 'plc', 'llp', 'group',
  'holdings', 'corporation', 'corp', 'sa', 'nv', 'ab',
  'gmbh', 'ag', 'sarl', 'pte', 'lp', 'llp', 'co', 'company'
];

const PHONE_PREFIXES: Record<string, { international: string; local: string; length: number }> = {
  'australia': { international: '61', local: '0', length: 9 },
  'new zealand': { international: '64', local: '0', length: 8 },
  'united kingdom': { international: '44', local: '0', length: 10 },
  'uk': { international: '44', local: '0', length: 10 },
  'england': { international: '44', local: '0', length: 10 },
  'scotland': { international: '44', local: '0', length: 10 },
  'wales': { international: '44', local: '0', length: 10 },
  'northern ireland': { international: '44', local: '0', length: 10 },
  'ireland': { international: '353', local: '0', length: 9 },
  'united states': { international: '1', local: '1', length: 10 },
  'usa': { international: '1', local: '1', length: 10 },
  'germany': { international: '49', local: '0', length: 10 },
  'france': { international: '33', local: '0', length: 9 },
};

// Default prefix for unknown countries
const DEFAULT_PREFIXES = { international: '44', local: '0', length: 10 };

const STREET_ABBREVIATIONS: Record<string, string> = {
  'st': 'street', 'rd': 'road', 'ave': 'avenue',
  'blvd': 'boulevard', 'ln': 'lane', 'dr': 'drive',
  'ct': 'court', 'pl': 'place', 'trl': 'trail',
  'pkwy': 'parkway', 'hwy': 'highway'
};

// Универсальная нормализация строки
export const normalizeString = (str: string): string => {
  if (!str) return '';
  return str.toString()
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s]/g, '')
    .replace(/\s+/g, ' ');
};

// Специализированная нормализация для названий компаний
export const normalizeCompanyName = (name: string): string => {
  if (!name) return '';
  
  let normalized = normalizeString(name);

  COMPANY_SUFFIXES.forEach(suffix => {
    const regex = new RegExp(`\\s*\\b${suffix}\\b\\s*$`);
    normalized = normalized.replace(regex, '');
  });

  return normalized.replace(/\b(and|&)\b/g, '').trim();
};

// Нормализация адресов
export const normalizeAddress = (address: string): string => {
  if (!address) return '';
  
  let normalized = normalizeString(address);

  Object.entries(STREET_ABBREVIATIONS).forEach(([abbr, full]) => {
    normalized = normalized.replace(new RegExp(`\\b${abbr}\\b`, 'g'), full);
  });

  return normalized;
};

// Нормализация телефонов
export const normalizePhone = (phone: string, country?: string): string => {
  if (!phone) return '';
  
  // Find prefixes for the country, fallback to default
  let prefixes = DEFAULT_PREFIXES;
  
  if (country) {
    const countryLower = country.toLowerCase();
    
    // Try exact match first
    if (PHONE_PREFIXES[countryLower]) {
      prefixes = PHONE_PREFIXES[countryLower];
    } else {
      // Try partial match
      for (const [key, value] of Object.entries(PHONE_PREFIXES)) {
        if (countryLower.includes(key) || key.includes(countryLower)) {
          prefixes = value;
          break;
        }
      }
    }
  }

  let digits = phone.replace(/\D/g, '');
  
  if (!digits || digits.length < 5) return '';

  // Remove international prefix if present
  if (digits.startsWith(prefixes.international)) {
    digits = digits.slice(prefixes.international.length);
  } 
  // Remove local prefix (usually 0) if present
  else if (digits.startsWith(prefixes.local) && prefixes.local === '0') {
    digits = digits.slice(1);
  }

  // Return last N digits based on country's expected length
  return digits.slice(-prefixes.length);
};

// Нормализация URL
export const normalizeWebsite = (url: string): string => {
  if (!url) return '';

  return url.toLowerCase()
    .replace(/^https?:\/\//, '')
    .replace(/^www\./, '')
    .replace(/\/$/, '')
    .split('/')[0]
    .split('?')[0];
};

// Извлечение домена из email
export const extractEmailDomain = (email: string): string | null => {
  if (!email) return null;

  const cleaned = email.toLowerCase().trim();
  const match = cleaned.match(/@([a-z0-9.-]+\.[a-z]{2,})$/i);

  if (match) {
    const domain = match[1];

    if (GENERIC_EMAIL_DOMAINS.includes(domain)) {
      return null;
    }

    return domain;
  }

  return null;
};

// Сравнение строк с учетом типа поля
export const compareFieldValues = (
  field: string,
  value1: string,
  value2: string,
  country?: string
): { status: 'exact' | 'partial' | 'none', similarity: number } => {
  if (!value1 || !value2) return { status: 'none', similarity: 0 };

  // Специальная обработка телефонов
  if (field === 'Phone') {
    const norm1 = normalizePhone(value1, country);
    const norm2 = normalizePhone(value2, country);
    return {
      status: norm1 && norm2 && norm1 === norm2 ? 'exact' : 'none',
      similarity: norm1 === norm2 ? 1 : 0
    };
  }

  // Специальная обработка вебсайтов
  if (field === 'Website') {
    const norm1 = normalizeWebsite(value1);
    const norm2 = normalizeWebsite(value2);
    return {
      status: norm1 && norm2 && norm1 === norm2 ? 'exact' : 'none',
      similarity: norm1 === norm2 ? 1 : 0
    };
  }

  // Нормализация в зависимости от типа поля
  let norm1, norm2;
  switch (true) {
    case field === 'Name':
      norm1 = normalizeCompanyName(value1);
      norm2 = normalizeCompanyName(value2);
      break;
    case field.includes('Street') || field.includes('Address'):
      norm1 = normalizeAddress(value1);
      norm2 = normalizeAddress(value2);
      break;
    default:
      norm1 = normalizeString(value1);
      norm2 = normalizeString(value2);
  }

  if (norm1 === norm2) return { status: 'exact', similarity: 1 };

  const similarity = compareTwoStrings(norm1, norm2);
  const threshold = field === 'Name' ? 0.6 : 0.8;

  return {
    status: similarity > threshold ? 'partial' : 'none',
    similarity
  };
};

export const createFieldTypeMap = (fieldMapping: Record<string, string>): Record<string, string> => {
  return Object.fromEntries(
    Object.entries(fieldMapping).map(([dbField, sfField]) => {
      const lowerField = sfField.toLowerCase();

      if (lowerField.includes('phone')) return [dbField, 'phone'];
      if (lowerField.includes('website')) return [dbField, 'website'];
      if (lowerField.includes('street') || lowerField.includes('address')) return [dbField, 'address'];
      if (lowerField.includes('name')) return [dbField, 'name'];
      return [dbField, 'text'];
    })
  );
};