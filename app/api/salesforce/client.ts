// app/api/salesforce/client.ts
import { getSQLiteClient } from '@/lib/sqlite-client';
import axios from 'axios';

export type SalesforceConfig = {
  username: string;
  password: string;
  securityToken: string;
  loginUrl: string;
};

export type SFField = {
  name: string;
  label: string;
  type: string;
};

export const getSalesforceConfig = (): SalesforceConfig => {
  const config = localStorage.getItem('salesforceConfig');
  return config ? JSON.parse(config) : {
    username: "",
    password: "",
    securityToken: "",
    loginUrl: ""
  };
};

export const fetchSFFields = async (): Promise<SFField[]> => {
  const config = getSalesforceConfig();
  
  const response = await axios.post('/api/salesforce/fields', {
    username: config.username,
    password: config.password,
    securityToken: config.securityToken,
    loginUrl: config.loginUrl
  });

  return response.data.fields;
};

export const fetchSalesforceData = async () => {
  const client = await getSQLiteClient();
  const config = getSalesforceConfig();
  const fieldMapping = JSON.parse(localStorage.getItem('fieldMapping') || '{}');
  const fields = Object.values(fieldMapping).filter(Boolean);

  const response = await fetch('/api/salesforce/accounts', {
    method: 'POST',
    body: JSON.stringify({ 
      ...config,
      fields 
    })
  });
  
  const data = await response.json();

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  // Очищаем старые данные перед загрузкой новых
  await client.query(
    "DELETE FROM accounts WHERE source = 'salesforce'"
  );

  // Сохраняем новые данные в SQLite
  await Promise.all(
    data.map((account: any) => 
      client.insertAccount(account, 'salesforce', fieldMapping)
    )
  );

  return data.length;
};