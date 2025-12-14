// app/api/databricks/client.ts

export type DatabricksConfig = {
  apiUrl: string;
  accessToken: string;
  catalogName: string;
  schemaName: string;
  warehouseId: string;
  sourceTable: string;
  dimensionsTable: string;
  salesforceTable: string;
}

export const getDatabricksConfig = (): DatabricksConfig | null => {
  if (typeof window === 'undefined') return null;
  
  const config = localStorage.getItem('appConfig');
  return config ? JSON.parse(config) : null;
};

// Fetch data from Databricks table
export async function fetchDatabricksData(
  config: DatabricksConfig,
  tablePath: string,
  fields: string[] | Record<string, string>,
  limit: number = 10000
): Promise<any[]> {
  const response = await fetch('/api/databricks/accounts', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      apiUrl: config.apiUrl,
      accessToken: config.accessToken,
      warehouseId: config.warehouseId,
      tablePath,
      fields,
      limit,
    }),
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch data from Databricks');
  }

  return response.json();
}