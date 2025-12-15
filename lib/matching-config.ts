// lib\matching-config.ts

export const ALIKE_MATCH_FIELDS = {
  'Name': { exact: 85, alike: 50 },
  'BillingStreet': 20,
  'Phone': 25,
  'Website': 15,
  'Company_Registration_No__c': 100,
  'BillingCity': 25
};

export const MATCH_THRESHOLD = 85;
export const MAX_POSSIBLE_SCORE = 295;