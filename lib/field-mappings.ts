// lib/field-mappings.ts

// Dimensions field mapping to Salesforce format
export const DIMENSIONS_FIELD_MAPPING = {
  'cucode': 'AccountNumber',
  'cuname': 'Name',
  'cuaddress': 'BillingStreet',
  'cupostcode': 'BillingPostalCode',
  'cu_country': 'BillingCountry',
  'cu_address_user1': 'BillingCity',
  'cuphone': 'Phone'
} as const;

// Source table fields (already in Salesforce format)
export const SOURCE_FIELDS = [
  'Name',
  'BillingStreet',
  'BillingCity',
  'BillingPostalCode',
  'BillingCountry',
  'Phone',
  'Website'
] as const;

// Salesforce table fields
export const SALESFORCE_FIELDS = [
  'AccountNumber',
  'Name',
  'BillingStreet',
  'BillingCity',
  'BillingState',
  'BillingPostalCode',
  'BillingCountry',
  'Phone',
  'Website'
] as const;

// Dimensions fields (original names)
export const DIMENSIONS_FIELDS = Object.keys(DIMENSIONS_FIELD_MAPPING);