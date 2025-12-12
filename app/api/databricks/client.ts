// app/api/databricks/client.ts
import { getSQLiteClient } from '@/lib/sqlite-client';
import axios from 'axios';

export type DatabricksConfig = {
  apiUrl: string;
  accessToken: string;
  catalogName: string;
  schemaName: string;
  tableName?: string;
  warehouseId: string;
}

export type TableInfo = {
  name: string;
  columns: {
    name: string;
    type: string;
  }[];
};

export const getDatabricksConfig = (): DatabricksConfig => {
  const config = localStorage.getItem('databricksConfig');
  const fieldMapping = JSON.parse(localStorage.getItem('fieldMapping') || '{}');

  let nameMappedField;
  for (const key in fieldMapping) {
    if(fieldMapping[key] === 'Name')
      nameMappedField = key;
  }

  const res = config ? JSON.parse(config) : { 
    apiUrl: "",
    accessToken: "",
    catalogName: "",
    schemaName: "",
    warehouseId: ""
  };

  return { ...res, nameMappedField };
};

export const fetchTables = async (catalogName?: string, schemaName?: string) => {
  const config = getDatabricksConfig();
  
  const response = await axios.post('/api/databricks/tables', {
    apiUrl: config.apiUrl,
    accessToken: config.accessToken,
    catalogName: catalogName || config.catalogName,
    schemaName: schemaName || config.schemaName
  });

  return response.data.map(({ name, columns }: {name: string, columns: any[]}) => ({ 
    name, 
    columns 
  }));
};

export const fetchDatabricksData = async () => {
  const client = await getSQLiteClient();
  const config = getDatabricksConfig();
  const fieldMapping = JSON.parse(localStorage.getItem('fieldMapping') || '{}');
  const fields = Object.keys(fieldMapping);

  const response = await fetch('/api/databricks/accounts', {
    method: 'POST',
    body: JSON.stringify({ 
      ...config,
      fields 
    })
  });

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }
  
  const data = await response.json();

  // Очищаем старые данные перед загрузкой новых
  await client.query(
    "DELETE FROM accounts WHERE source = 'databricks'"
  );

  // Сохраняем новые данные в SQLite
  await Promise.all(
    data.map((account: any) => 
      client.insertAccount(account, 'databricks', fieldMapping)
    )
  );

  return data.length;
};