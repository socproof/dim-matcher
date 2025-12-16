// app/(main)/matching/page.tsx

"use client";
import { useEffect, useState, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Badge } from '@/components/ui/badge';
import { getPostgresClient } from '@/lib/postgres-client';
import { processSourceChunk, MatchedAccount } from '@/lib/find-matches-chunked';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { InfoIcon } from 'lucide-react';
import { Checkbox } from '@/components/ui/checkbox';
import { PROCESSING, DEV_LIMITS, applyLimit, isDevMode } from '@/lib/config';

const CHUNK_SIZE = PROCESSING.chunkSize;

export default function MatchingPage() {
  const [status, setStatus] = useState<string>('Checking database...');
  const [currentChunkData, setCurrentChunkData] = useState<MatchedAccount[]>([]);
  const [processedStatus, setProcessedStatus] = useState<Map<string, 'merged' | 'added'>>(new Map());
  const [isLoading, setIsLoading] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [selectedItem, setSelectedItem] = useState<MatchedAccount | null>(null);
  const [dialogMode, setDialogMode] = useState<'view' | null>(null);
  const [showDetails, setShowDetails] = useState(false);
  const [isDbReady, setIsDbReady] = useState(false);
  const [dbCounts, setDbCounts] = useState<{
    source: number;
    dimensions: number;
    salesforce: number;
    total: number;
  }>({
    source: 0,
    dimensions: 0,
    salesforce: 0,
    total: 0
  });
  const [currentChunkIndex, setCurrentChunkIndex] = useState<number>(0);
  const [hasMoreChunks, setHasMoreChunks] = useState<boolean>(false);
  const [stats, setStats] = useState({ both: 0, dimOnly: 0, sfOnly: 0, new: 0 });
  const [dataLoaded, setDataLoaded] = useState(false);
  const [enableAI, setEnableAI] = useState(true);

  const loadFieldMapping = useCallback(() => {
    const defaults = getDefaultMapping();
    const storedMapping = localStorage.getItem('fieldMapping');

    if (storedMapping) {
      try {
        const parsed = JSON.parse(storedMapping);
        // Merge with defaults to ensure all keys exist
        return {
          source: parsed.source || defaults.source,
          dimensions: parsed.dimensions || defaults.dimensions,
          salesforce: parsed.salesforce || defaults.salesforce
        };
      } catch (e) {
        console.error("Failed to parse fieldMapping from localStorage", e);
        return defaults;
      }
    }
    return defaults;
  }, []);

  const getDefaultMapping = () => ({
    source: {
      'Name': 'Name',
      'BillingStreet': 'BillingStreet',
      'BillingCity': 'BillingCity',
      'BillingPostalCode': 'BillingPostalCode',
      'BillingCountry': 'BillingCountry',
      'Phone': 'Phone',
      'Website': 'Website'
    },
    dimensions: {
      'cucode': 'AccountNumber',
      'cuname': 'Name',
      'cuaddress': 'BillingStreet',
      'cupostcode': 'BillingPostalCode',
      'cu_country': 'BillingCountry',
      'cu_address_user1': 'BillingCity',
      'cuphone': 'Phone',
      'cu_email': 'Email'
    },
    salesforce: {
      'AccountNumber': 'AccountNumber',
      'Name': 'Name',
      'BillingStreet': 'BillingStreet',
      'BillingCity': 'BillingCity',
      'BillingPostalCode': 'BillingPostalCode',
      'BillingCountry': 'BillingCountry',
      'Phone': 'Phone',
      'Website': 'Website'
    }
  });

  useEffect(() => {
    const checkDatabase = async () => {
      setStatus('Checking database connection...');
      try {
        const client = getPostgresClient();
        const counts = await client.getAccountCounts();

        setDbCounts(counts);
        setIsDbReady(true);

        console.log('[Matching] DB Counts:', counts);

        if (counts.total > 0) {
          setDataLoaded(true);
          setStatus(`Database ready. Found ${counts.source} Source, ${counts.dimensions} Dimensions, and ${counts.salesforce} Salesforce accounts.`);
          setHasMoreChunks(counts.source > 0);
        } else {
          setDataLoaded(false);
          setStatus('Database is empty. Click "Load All Data" to populate from Databricks.');
          setHasMoreChunks(false);
        }
      } catch (error) {
        setStatus(`Database connection error: ${error instanceof Error ? error.message : 'Unknown error'}`);
        setIsDbReady(false);
        console.error('[Matching] DB check error:', error);
      }
    };

    checkDatabase();
  }, []);

  const handleLoadAccounts = async () => {
    if (!isDbReady) {
      setStatus('Database is not ready yet. Please wait or refresh.');
      return;
    }

    const devModeWarning = isDevMode()
      ? `\n\nDEV MODE: Limits active (Source: ${DEV_LIMITS.source}, Dim: ${DEV_LIMITS.dimensions}, SF: ${DEV_LIMITS.salesforce})`
      : '';

    const confirmReload = confirm(
      `This will delete all existing data (${dbCounts.total} records) and reload from Databricks. Continue?${devModeWarning}`
    );

    if (!confirmReload) {
      return;
    }

    setIsLoading(true);
    setStatus('Clearing existing data...');
    setCurrentChunkData([]);
    setProcessedStatus(new Map());
    setCurrentChunkIndex(0);
    setHasMoreChunks(false);
    setStats({ both: 0, dimOnly: 0, sfOnly: 0, new: 0 });

    try {
      const client = getPostgresClient();
      await client.clearAllAccounts();

      const config = JSON.parse(localStorage.getItem('appConfig') || '{}');
      const fieldMapping = loadFieldMapping();

      console.log('[LoadAccounts] Config:', config);
      console.log('[LoadAccounts] Dev Limits:', DEV_LIMITS);

      // Load Source
      setStatus('Loading Source accounts...');
      const sourceData = await loadTableData(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.sourceTable,
        DEV_LIMITS.source || 20000  // Limit directly in query
      );
      console.log('[LoadAccounts] Source data loaded:', sourceData.length);

      setStatus(`Saving ${sourceData.length} Source accounts to database...`);
      await client.insertAccountsBatch('source', sourceData);

      // Load Dimensions
      setStatus('Loading Dimensions accounts...');
      const dimensionsData = await loadTableInChunks(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.dimensionsTable,
        5000,  // chunk size
        (loaded) => setStatus(`Loading Dimensions: ${loaded} records loaded...`),
        DEV_LIMITS.dimensions || undefined  // MAX RECORDS LIMIT
      );
      console.log('[LoadAccounts] Dimensions data loaded:', dimensionsData.length);

      setStatus(`Saving ${dimensionsData.length} Dimensions accounts to database...`);
      await client.insertAccountsBatch('dimensions', dimensionsData);

      // Load Salesforce
      setStatus('Loading Salesforce accounts...');
      const salesforceData = await loadTableInChunks(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.salesforceTable,
        5000,  // chunk size
        (loaded) => setStatus(`Loading Salesforce: ${loaded} records loaded...`),
        DEV_LIMITS.salesforce || undefined  // MAX RECORDS LIMIT
      );
      console.log('[LoadAccounts] Salesforce data loaded:', salesforceData.length);

      setStatus(`Saving ${salesforceData.length} Salesforce accounts to database...`);
      await client.insertAccountsBatch('salesforce', salesforceData);

      const newCounts = {
        source: sourceData.length,
        dimensions: dimensionsData.length,
        salesforce: salesforceData.length,
        total: sourceData.length + dimensionsData.length + salesforceData.length
      };

      setDbCounts(newCounts);
      setDataLoaded(true);

      const devNote = isDevMode() ? ' (DEV MODE - limits active)' : '';
      setStatus(`Successfully loaded ${newCounts.source} Source, ${newCounts.dimensions} Dimensions, and ${newCounts.salesforce} Salesforce accounts.${devNote}`);
      setHasMoreChunks(newCounts.source > 0);

    } catch (error) {
      setStatus(`Error loading accounts: ${error instanceof Error ? error.message : 'Failed to load data'}`);
      console.error('[LoadAccounts] Error:', error);
    } finally {
      setIsLoading(false);
    }
  };

  // Вспомогательная функция для загрузки одной таблицы
  const loadTableData = async (
    apiUrl: string,
    accessToken: string,
    warehouseId: string,
    tablePath: string,
    limit: number
  ): Promise<any[]> => {
    const response = await fetch('/api/databricks/accounts', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        apiUrl,
        accessToken,
        warehouseId,
        tablePath,
        limit
      }),
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(`Failed to load ${tablePath}: ${errorData.error || 'Unknown error'}`);
    }

    return await response.json();
  };

  const loadTableInChunks = async (
    apiUrl: string,
    accessToken: string,
    warehouseId: string,
    tablePath: string,
    chunkSize: number,
    onProgress?: (loaded: number) => void,
    maxRecords?: number  // NEW: maximum records to load
  ): Promise<any[]> => {
    const allData: any[] = [];
    let offset = 0;
    let hasMore = true;
    let iteration = 0;

    while (hasMore) {
      iteration++;

      // Check if we've reached the limit
      if (maxRecords && allData.length >= maxRecords) {
        console.log(`[loadTableInChunks] ${tablePath} - Reached limit of ${maxRecords} records, stopping`);
        break;
      }

      // Calculate how many records we still need
      const remainingNeeded = maxRecords ? maxRecords - allData.length : chunkSize;
      const currentChunkSize = Math.min(chunkSize, remainingNeeded);

      console.log(`[loadTableInChunks] ${tablePath} - Iteration ${iteration}, offset: ${offset}, requesting: ${currentChunkSize}`);

      try {
        const response = await fetch('/api/databricks/accounts', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            apiUrl,
            accessToken,
            warehouseId,
            tablePath,
            limit: currentChunkSize,
            offset
          }),
        });

        if (!response.ok) {
          const errorData = await response.json();
          console.error(`[loadTableInChunks] Failed to load chunk at offset ${offset}:`, errorData);

          if (errorData.details?.message?.includes('Inline byte limit exceeded')) {
            console.log('[loadTableInChunks] Reducing chunk size due to byte limit');
            return loadTableInChunks(apiUrl, accessToken, warehouseId, tablePath, Math.floor(chunkSize / 2), onProgress, maxRecords);
          }

          break;
        }

        const chunkData = await response.json();

        console.log(`[loadTableInChunks] ${tablePath} - Chunk ${iteration}: received ${chunkData.length} records`);

        if (!Array.isArray(chunkData) || chunkData.length === 0) {
          console.log(`[loadTableInChunks] ${tablePath} - No more data, stopping`);
          hasMore = false;
          break;
        }

        allData.push(...chunkData);

        if (onProgress) {
          onProgress(allData.length);
        }

        // Check if we've reached the limit after adding data
        if (maxRecords && allData.length >= maxRecords) {
          console.log(`[loadTableInChunks] ${tablePath} - Reached limit of ${maxRecords} records`);
          hasMore = false;
          break;
        }

        if (chunkData.length < currentChunkSize) {
          console.log(`[loadTableInChunks] ${tablePath} - Last chunk (${chunkData.length} < ${currentChunkSize}), stopping`);
          hasMore = false;
        } else {
          offset += chunkData.length;
          console.log(`[loadTableInChunks] ${tablePath} - Moving to next chunk, new offset: ${offset}`);
        }

      } catch (error) {
        console.error(`[loadTableInChunks] ${tablePath} - Error at offset ${offset}:`, error);
        hasMore = false;
      }
    }

    // Trim to exact limit if we got more
    const result = maxRecords && allData.length > maxRecords
      ? allData.slice(0, maxRecords)
      : allData;

    console.log(`[loadTableInChunks] ${tablePath} - Completed. Total records: ${result.length}`);
    return result;
  };


  const handleProcessChunk = async (startIndex: number) => {
    if (!isDbReady) {
      setStatus('Database is not ready.');
      return;
    }
    if (dbCounts.source === 0) {
      setStatus('No Source accounts loaded. Please Load Data first.');
      return;
    }

    setIsProcessing(true);
    setStatus(`Processing Source accounts ${startIndex + 1} to ${startIndex + CHUNK_SIZE}...`);
    setCurrentChunkData([]);
    setProcessedStatus(new Map());

    try {
      const fieldMapping = loadFieldMapping();

      console.log('[ProcessChunk] Starting with AI:', enableAI);

      const result = await processSourceChunk(fieldMapping, CHUNK_SIZE, startIndex, enableAI);

      console.log('[ProcessChunk] Result:', result);

      setCurrentChunkData(result.matches);
      setHasMoreChunks(result.hasMore);
      setCurrentChunkIndex(startIndex);
      setStats(result.stats);

      setStatus(`Processed ${result.processedCount} accounts. Status: ${result.stats.both} BOTH, ${result.stats.dimOnly} DIM_ONLY, ${result.stats.sfOnly} SF_ONLY, ${result.stats.new} NEW`);

    } catch (error) {
      setStatus(`Error processing chunk: ${error instanceof Error ? error.message : 'Failed to process chunk'}`);
      console.error('[ProcessChunk] Error:', error);
    } finally {
      setIsProcessing(false);
    }
  };

  const handleViewDetails = (item: MatchedAccount) => {
    setSelectedItem(item);
    setDialogMode('view');
    setShowDetails(true);
  };

  const handleDiagnose = async () => {
    setStatus('Running diagnostics...');
    try {
      const response = await fetch('/api/postgres/diagnose', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          testName: 'andrews',
          testSource: 'salesforce'
        }),
      });
      const data = await response.json();
      console.log('[Diagnostics]', data);
      setStatus(`Query time: ${data.queryTimeMs}ms, Found: ${data.resultsCount} results. Check console for details.`);
    } catch (error) {
      setStatus(`Diagnostics failed: ${error}`);
    }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'BOTH':
        return <Badge className="bg-green-600">Both Matched</Badge>;
      case 'DIM_ONLY':
        return <Badge className="bg-blue-600">Dimensions Only</Badge>;
      case 'SF_ONLY':
        return <Badge className="bg-purple-600">Salesforce Only</Badge>;
      case 'NEW':
        return <Badge variant="secondary">New</Badge>;
      case 'CONFIRMED':
        return <Badge className="bg-green-600">AI Confirmed</Badge>;
      case 'REJECTED':
        return <Badge className="bg-red-600">AI Rejected</Badge>;
      case 'REVIEW':
        return <Badge className="bg-yellow-600">Needs Review</Badge>;
      default:
        return <Badge variant="outline">{status}</Badge>;
    }
  };

  return (
    <div className="space-y-4 p-4 md:p-6">
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2">
        <h2 className="text-xl font-semibold">Account Matching</h2>
      </div>

      {!dataLoaded && isDbReady && (
        <Alert>
          <InfoIcon className="h-4 w-4" />
          <AlertDescription>
            Database is empty. Click "Load All Data" to populate from Databricks.
            This is a one-time operation - data will persist between sessions.
          </AlertDescription>
        </Alert>
      )}

      {dataLoaded && (
        <Alert className="bg-green-50 border-green-200">
          <InfoIcon className="h-4 w-4 text-green-600" />
          <AlertDescription className="text-green-800">
            Data is loaded and persisted in PostgreSQL. You can start processing or reload data if needed.
          </AlertDescription>
        </Alert>
      )}

      <div className="space-y-2 p-4 border rounded-lg bg-card">
        <p className="text-sm text-muted-foreground">{status}</p>
        {isDbReady && (
          <div className="grid grid-cols-4 gap-4 text-sm">
            <div>
              <span className="font-semibold">Source:</span> {dbCounts.source.toLocaleString()}
            </div>
            <div>
              <span className="font-semibold">Dimensions:</span> {dbCounts.dimensions.toLocaleString()}
            </div>
            <div>
              <span className="font-semibold">Salesforce:</span> {dbCounts.salesforce.toLocaleString()}
            </div>
            <div>
              <span className="font-semibold">Total:</span> {dbCounts.total.toLocaleString()}
            </div>
          </div>
        )}
      </div>

      <div className="flex flex-wrap gap-2">
        <Button
          onClick={handleLoadAccounts}
          disabled={isLoading || isProcessing || !isDbReady}
          variant={dataLoaded ? "outline" : "default"}
        >
          {isLoading ? 'Loading...' : dataLoaded ? 'Reload All Data' : 'Load All Data'}
        </Button>
        <Button
          onClick={() => handleProcessChunk(0)}
          disabled={isLoading || isProcessing || dbCounts.source === 0}
          variant="secondary"
        >
          {isProcessing ? 'Processing...' : 'Process First Chunk'}
        </Button>
        {hasMoreChunks && (
          <Button
            onClick={() => handleProcessChunk(currentChunkIndex + CHUNK_SIZE)}
            disabled={isLoading || isProcessing}
            variant="outline"
          >
            Process Next Chunk
          </Button>
        )}

        <Button
          onClick={handleDiagnose}
          variant="outline"
          size="sm"
          disabled={!isDbReady || isLoading || isProcessing}
        >
          Diagnose DB
        </Button>

        {/* AI Toggle */}
        <div className="flex items-center gap-2 ml-4 border-l pl-4">
          <Checkbox
            id="enableAI"
            checked={enableAI}
            onCheckedChange={(checked) => setEnableAI(checked as boolean)}
          />
          <label htmlFor="enableAI" className="text-sm cursor-pointer">
            Enable AI Validation
          </label>
        </div>
      </div>

      {currentChunkData.length > 0 && (
        <div className="mt-6 space-y-4">
          <h3 className="text-lg font-medium">
            Chunk Results (Items {currentChunkIndex + 1} - {currentChunkIndex + currentChunkData.length})
          </h3>

          <div className="rounded-md border overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Source Account</TableHead>
                  <TableHead>Dimensions Match</TableHead>
                  <TableHead>Salesforce Match</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {currentChunkData.map((item, idx) => (
                  <TableRow key={idx}>
                    <TableCell>
                      <div className="space-y-1">
                        <p className="font-medium">{item.sourceAccount?.Name || 'N/A'}</p>
                        <p className="text-xs text-muted-foreground">{item.sourceAccount?.Phone || ''}</p>
                      </div>
                    </TableCell>
                    <TableCell>
                      {item.dimensionsMatch ? (
                        <div className="space-y-1">
                          <p className="font-medium">{item.dimensionsMatch.Name}</p>
                          <div className="flex gap-1 flex-wrap">
                            <Badge variant="outline" className="text-xs">
                              Score: {item.dimensionsScore}
                            </Badge>
                            {getStatusBadge(item.dimensionsStatus)}
                          </div>
                          {item.dimensionsAI && (
                            <p className="text-xs text-muted-foreground">
                              AI: {item.dimensionsAI.confidence}% - {item.dimensionsAI.reasoning}
                            </p>
                          )}
                        </div>
                      ) : (
                        <span className="text-muted-foreground italic text-sm">No match</span>
                      )}
                    </TableCell>
                    <TableCell>
                      {item.salesforceMatch ? (
                        <div className="space-y-1">
                          <p className="font-medium">{item.salesforceMatch.Name}</p>
                          <div className="flex gap-1 flex-wrap">
                            <Badge variant="outline" className="text-xs">
                              Score: {item.salesforceScore}
                            </Badge>
                            {getStatusBadge(item.salesforceStatus)}
                          </div>
                          {item.salesforceAI && (
                            <p className="text-xs text-muted-foreground">
                              AI: {item.salesforceAI.confidence}% - {item.salesforceAI.reasoning}
                            </p>
                          )}
                        </div>
                      ) : (
                        <span className="text-muted-foreground italic text-sm">No match</span>
                      )}
                    </TableCell>
                    <TableCell>{getStatusBadge(item.finalStatus)}</TableCell>
                    <TableCell>
                      <Button size="sm" variant="outline" onClick={() => handleViewDetails(item)}>
                        View Details
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        </div>
      )}

      <Dialog open={showDetails} onOpenChange={setShowDetails}>
        <DialogContent className="max-w-4xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Match Details</DialogTitle>
          </DialogHeader>
          {selectedItem && (
            <div className="space-y-4">
              <div className="grid grid-cols-3 gap-4">
                <div className="border rounded p-3">
                  <h4 className="font-semibold mb-2">Source Account</h4>
                  <pre className="text-xs overflow-auto">
                    {JSON.stringify(selectedItem.sourceAccount, null, 2)}
                  </pre>
                </div>
                <div className="border rounded p-3">
                  <h4 className="font-semibold mb-2">Dimensions Match</h4>
                  {selectedItem.dimensionsMatch ? (
                    <pre className="text-xs overflow-auto">
                      {JSON.stringify(selectedItem.dimensionsMatch, null, 2)}
                    </pre>
                  ) : (
                    <p className="text-muted-foreground italic">No match</p>
                  )}
                  {selectedItem.dimensionsScore > 0 && (
                    <Badge className="mt-2">Score: {selectedItem.dimensionsScore}</Badge>
                  )}
                </div>
                <div className="border rounded p-3">
                  <h4 className="font-semibold mb-2">Salesforce Match</h4>
                  {selectedItem.salesforceMatch ? (
                    <pre className="text-xs overflow-auto">
                      {JSON.stringify(selectedItem.salesforceMatch, null, 2)}
                    </pre>
                  ) : (
                    <p className="text-muted-foreground italic">No match</p>
                  )}
                  {selectedItem.salesforceScore > 0 && (
                    <Badge className="mt-2">Score: {selectedItem.salesforceScore}</Badge>
                  )}
                </div>
              </div>
              <div className="flex justify-center">
                {getStatusBadge(selectedItem.finalStatus)}
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}