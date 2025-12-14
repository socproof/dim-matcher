// app/(main)/databricks/page.tsx

"use client";

import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { SOURCE_FIELDS, SALESFORCE_FIELDS, DIMENSIONS_FIELDS } from '@/lib/field-mappings';
import { useRouter } from 'next/navigation';

type TableValidation = {
  table: string;
  status: 'pending' | 'checking' | 'valid' | 'invalid';
  missingFields: string[];
  availableFields: string[];
};

export default function DatabricksPage() {
  const [config, setConfig] = useState({
    apiUrl: '',
    accessToken: '',
    catalogName: '',
    schemaName: '',
    warehouseId: '',
    sourceTable: '',
    dimensionsTable: '',
    salesforceTable: '',
  });

  const [status, setStatus] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isConnected, setIsConnected] = useState(false);
  const [validations, setValidations] = useState<TableValidation[]>([
    { table: 'source', status: 'pending', missingFields: [], availableFields: [] },
    { table: 'dimensions', status: 'pending', missingFields: [], availableFields: [] },
    { table: 'salesforce', status: 'pending', missingFields: [], availableFields: [] },
  ]);

  const router = useRouter();

  useEffect(() => {
    const saved = localStorage.getItem('appConfig');
    if (saved) {
      const parsed = JSON.parse(saved);
      setConfig(parsed);
      if (parsed.connectionTested) {
        setIsConnected(true);
        setStatus('Configuration loaded from storage');
      }
    }
  }, []);

  const testConnection = async () => {
    setIsLoading(true);
    setStatus('Testing connection to Databricks...');

    try {
      const response = await fetch('/api/databricks/tables', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          apiUrl: config.apiUrl,
          accessToken: config.accessToken,
          catalogName: config.catalogName,
          schemaName: config.schemaName,
          warehouseId: config.warehouseId,
        }),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || 'Connection failed');
      }

      const data = await response.json();
      setIsConnected(true);
      setStatus(`✓ Connected successfully! Found ${data.tables?.length || 0} tables in schema.`);

      const updatedConfig = { ...config, connectionTested: true };
      localStorage.setItem('appConfig', JSON.stringify(updatedConfig));
      setConfig(updatedConfig);

    } catch (error) {
      setIsConnected(false);
      setStatus(`✗ Connection failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
    } finally {
      setIsLoading(false);
    }
  };

  const validateTable = async (tableType: 'source' | 'dimensions' | 'salesforce') => {
    const tablePath =
      tableType === 'source' ? config.sourceTable :
        tableType === 'dimensions' ? config.dimensionsTable :
          config.salesforceTable;

    if (!tablePath) {
      setStatus(`Please enter ${tableType} table path first`);
      return;
    }

    const requiredFields =
      tableType === 'source' ? SOURCE_FIELDS :
        tableType === 'dimensions' ? DIMENSIONS_FIELDS :
          SALESFORCE_FIELDS;

    setValidations(prev => prev.map(v =>
      v.table === tableType ? { ...v, status: 'checking' as const } : v
    ));
    setStatus(`Validating ${tableType} table fields...`);

    try {
      console.log(`[Validation] Checking ${tableType} table:`, tablePath);
      console.log(`[Validation] Required fields:`, requiredFields);

      const response = await fetch('/api/databricks/accounts', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          apiUrl: config.apiUrl,
          accessToken: config.accessToken,
          warehouseId: config.warehouseId,
          tablePath,
          fields: ['*'],
          limit: 1, 
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        console.error(`[Validation] ${tableType} error:`, data);
        throw new Error(data.error || `Failed to query ${tableType} table`);
      }

      console.log(`[Validation] ${tableType} response:`, data);

      if (!data || data.length === 0) {
        throw new Error(`Table ${tablePath} returned no data`);
      }

      const availableFields = Object.keys(data[0]);
      console.log(`[Validation] ${tableType} available fields:`, availableFields);

      const missingFields = requiredFields.filter(f => !availableFields.includes(f));
      console.log(`[Validation] ${tableType} missing fields:`, missingFields);

      setValidations(prev => prev.map(v =>
        v.table === tableType
          ? {
            ...v,
            status: missingFields.length === 0 ? 'valid' : 'invalid',
            missingFields,
            availableFields
          }
          : v
      ));

      if (missingFields.length === 0) {
        setStatus(`✓ ${tableType} table validated successfully`);
      } else {
        setStatus(`✗ ${tableType} table is missing: ${missingFields.join(', ')}`);
        console.warn(`[Validation] Available fields in ${tableType}:`, availableFields);
      }

    } catch (error) {
      console.error(`[Validation] ${tableType} exception:`, error);
      setValidations(prev => prev.map(v =>
        v.table === tableType
          ? { ...v, status: 'invalid', missingFields: requiredFields as any }
          : v
      ));
      setStatus(`✗ Error validating ${tableType}: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  };

  const validateAllTables = async () => {
    if (!config.sourceTable || !config.dimensionsTable || !config.salesforceTable) {
      setStatus('Please fill in all table paths first');
      return;
    }

    setIsLoading(true);
    await validateTable('source');
    await validateTable('dimensions');
    await validateTable('salesforce');
    setIsLoading(false);

    const allValid = validations.every(v => v.status === 'valid');
    if (allValid) {
      localStorage.setItem('appConfig', JSON.stringify(config));
      setStatus('✓ All tables validated and configuration saved!');
    }
  };

  const getValidationBadge = (validation: TableValidation) => {
    switch (validation.status) {
      case 'valid':
        return <Badge className="bg-green-600">Valid</Badge>;
      case 'invalid':
        return <Badge variant="destructive">Invalid</Badge>;
      case 'checking':
        return <Badge variant="secondary">Checking...</Badge>;
      default:
        return <Badge variant="outline">Not checked</Badge>;
    }
  };

  return (
    <div className="space-y-6 p-6">
      <h2 className="text-2xl font-bold">Databricks Configuration</h2>

      {/* Connection Settings */}
      <Card>
        <CardHeader>
          <CardTitle>Connection Settings</CardTitle>
          <CardDescription>Configure your Databricks connection</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <Label>API URL</Label>
              <Input
                value={config.apiUrl}
                onChange={(e) => setConfig({ ...config, apiUrl: e.target.value })}
                placeholder="https://adb-xxx.azuredatabricks.net"
              />
            </div>
            <div>
              <Label>Warehouse ID</Label>
              <Input
                value={config.warehouseId}
                onChange={(e) => setConfig({ ...config, warehouseId: e.target.value })}
                placeholder="5888a6ffca988cd9"
              />
            </div>
            <div>
              <Label>Catalog Name</Label>
              <Input
                value={config.catalogName}
                onChange={(e) => setConfig({ ...config, catalogName: e.target.value })}
                placeholder="mergacq_discovery"
              />
            </div>
            <div>
              <Label>Schema Name</Label>
              <Input
                value={config.schemaName}
                onChange={(e) => setConfig({ ...config, schemaName: e.target.value })}
                placeholder="hireara_sales"
              />
            </div>
          </div>
          <div>
            <Label>Access Token</Label>
            <Input
              type="password"
              value={config.accessToken}
              onChange={(e) => setConfig({ ...config, accessToken: e.target.value })}
              placeholder="dapi..."
            />
          </div>
          <Button onClick={testConnection} disabled={isLoading}>
            {isLoading ? 'Testing...' : 'Test Connection'}
          </Button>
        </CardContent>
      </Card>

      {/* Table Configuration */}
      {isConnected && (
        <Card>
          <CardHeader>
            <CardTitle>Table Paths</CardTitle>
            <CardDescription>Specify your table paths and validate fields</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-3">
              <div>
                <Label>Source Table</Label>
                <Input
                  value={config.sourceTable}
                  onChange={(e) => setConfig({ ...config, sourceTable: e.target.value })}
                  placeholder="catalog.schema.account_vw"
                />
                <div className="mt-2 flex items-center justify-between">
                  <p className="text-xs text-muted-foreground">
                    Required fields: {SOURCE_FIELDS.join(', ')}
                  </p>
                  {getValidationBadge(validations.find(v => v.table === 'source')!)}
                </div>
              </div>

              <div>
                <Label>Dimensions Table</Label>
                <Input
                  value={config.dimensionsTable}
                  onChange={(e) => setConfig({ ...config, dimensionsTable: e.target.value })}
                  placeholder="catalog.schema.dimensions_table"
                />
                <div className="mt-2 flex items-center justify-between">
                  <p className="text-xs text-muted-foreground">
                    Required fields: {DIMENSIONS_FIELDS.join(', ')}
                  </p>
                  {getValidationBadge(validations.find(v => v.table === 'dimensions')!)}
                </div>
              </div>

              <div>
                <Label>Salesforce Table</Label>
                <Input
                  value={config.salesforceTable}
                  onChange={(e) => setConfig({ ...config, salesforceTable: e.target.value })}
                  placeholder="catalog.schema.salesforce_account"
                />
                <div className="mt-2 flex items-center justify-between">
                  <p className="text-xs text-muted-foreground">
                    Required fields: {SALESFORCE_FIELDS.join(', ')}
                  </p>
                  {getValidationBadge(validations.find(v => v.table === 'salesforce')!)}
                </div>
              </div>
            </div>

            <Button onClick={validateAllTables} disabled={isLoading}>
              {isLoading ? 'Validating...' : 'Validate All Tables'}
            </Button>

            {/* Show missing fields if any */}
            {validations.some(v => v.status === 'invalid') && (
              <div className="mt-4 p-4 bg-red-50 border border-red-200 rounded-md">
                <h4 className="font-semibold text-red-800 mb-2">Missing Fields:</h4>
                {validations.filter(v => v.status === 'invalid').map(v => (
                  <div key={v.table} className="text-sm text-red-700">
                    <strong className="capitalize">{v.table}:</strong> {v.missingFields.join(', ')}
                  </div>
                ))}
              </div>
            )}

            {validations.every(v => v.status === 'valid') && (
              <div className="mt-4 p-4 bg-green-50 border border-green-200 rounded-md">
                <div className="flex items-center justify-between">
                  <div>
                    <h4 className="font-semibold text-green-800">✓ All tables validated successfully!</h4>
                    <p className="text-sm text-green-700 mt-1">Configuration saved. You can now proceed to matching.</p>
                  </div>
                  <Button
                    onClick={() => router.push('/matching')}
                    className="bg-green-600 hover:bg-green-700"
                  >
                    Next: Start Matching →
                  </Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      )}

    </div>
  );
}