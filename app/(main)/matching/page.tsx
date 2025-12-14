// app/(main)/matching/page.tsx

"use client";
import { useEffect, useState, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Progress } from '@/components/ui/progress';
import { Badge } from '@/components/ui/badge';
import { MAX_POSSIBLE_SCORE, MATCH_THRESHOLD } from '@/lib/matching-config';
import { getSQLiteClient } from '@/lib/sqlite-client';
import { processSourceChunk, MatchedAccount } from '@/lib/find-matches-chunked';
import { fetchDatabricksData } from '@/app/api/databricks/client';

const CHUNK_SIZE = 10;

export default function MatchingPage() {
  const [status, setStatus] = useState<string>('Initializing database...');
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

  const loadFieldMapping = useCallback(() => {
    const storedMapping = localStorage.getItem('fieldMapping');
    if (storedMapping) {
      try {
        return JSON.parse(storedMapping);
      } catch (e) {
        console.error("Failed to parse fieldMapping from localStorage", e);
        return getDefaultMapping();
      }
    }
    return getDefaultMapping();
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
      'cuphone': 'Phone'
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

  // Инициализация БД
  useEffect(() => {
    const initDbAndCheckData = async () => {
      setStatus('Initializing database...');
      try {
        const client = await getSQLiteClient();
        await client.createAccountsTable();
        setIsDbReady(true);

        const counts = await client.getAccountCounts();

        // Обновляем структуру counts для трех источников
        const updatedCounts = {
          source: counts.databricks || 0, // Временно используем databricks как source
          dimensions: 0,
          salesforce: counts.salesforce || 0,
          total: counts.total || 0
        };

        // Получаем count для dimensions отдельно
        const dimCountResult = await client.query<{ count: number }>(
          "SELECT COUNT(*) as count FROM accounts WHERE source = 'dimensions'"
        );
        updatedCounts.dimensions = dimCountResult?.[0]?.count || 0;

        // Получаем count для source отдельно
        const sourceCountResult = await client.query<{ count: number }>(
          "SELECT COUNT(*) as count FROM accounts WHERE source = 'source'"
        );
        updatedCounts.source = sourceCountResult?.[0]?.count || 0;

        updatedCounts.total = updatedCounts.source + updatedCounts.dimensions + updatedCounts.salesforce;

        setDbCounts(updatedCounts);

        console.log('[Matching] DB Counts:', updatedCounts);

        if (updatedCounts.total > 0) {
          setStatus(`Found ${updatedCounts.source} Source, ${updatedCounts.dimensions} Dimensions, and ${updatedCounts.salesforce} Salesforce accounts. Ready to process.`);
          setHasMoreChunks(updatedCounts.source > 0);
        } else {
          setStatus('Database ready. Load accounts from sources to begin.');
          setHasMoreChunks(false);
        }
      } catch (error) {
        setStatus(`Database initialization error: ${error instanceof Error ? error.message : 'Unknown error'}`);
        setIsDbReady(false);
        console.error('[Matching] Init error:', error);
      }
    };

    initDbAndCheckData();
  }, []);

  const handleLoadAccounts = async () => {
    if (!isDbReady) {
      setStatus('Database is not ready yet. Please wait or refresh.');
      return;
    }

    setIsLoading(true);
    setStatus('Loading accounts from all three sources...');
    setCurrentChunkData([]);
    setProcessedStatus(new Map());
    setCurrentChunkIndex(0);
    setHasMoreChunks(false);
    setStats({ both: 0, dimOnly: 0, sfOnly: 0, new: 0 });

    try {
      const client = await getSQLiteClient();
      await client.clearAccounts();

      const config = JSON.parse(localStorage.getItem('appConfig') || '{}');
      const fieldMapping = loadFieldMapping();

      console.log('[LoadAccounts] Config:', config);
      console.log('[LoadAccounts] Field Mapping:', fieldMapping);

      // Загружаем Source (маленькая таблица - можно за раз)
      setStatus('Loading Source accounts...');
      const sourceData = await loadTableData(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.sourceTable,
        20000
      );

      console.log('[LoadAccounts] Source data loaded:', sourceData.length);

      for (const account of sourceData) {
        await client.insertAccount(account, 'source', fieldMapping.source);
      }

      // Загружаем Dimensions чанками (206k записей)
      setStatus('Loading Dimensions accounts (0/?)...');
      const dimensionsData = await loadTableInChunks(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.dimensionsTable,
        30000, // Уменьшаем чанк до 30k для безопасности
        (loaded) => {
          setStatus(`Loading Dimensions: ${loaded} records loaded...`);
        }
      );

      console.log('[LoadAccounts] Dimensions data loaded:', dimensionsData.length);

      setStatus(`Saving ${dimensionsData.length} Dimensions accounts to database...`);
      for (let i = 0; i < dimensionsData.length; i++) {
        await client.insertAccount(dimensionsData[i], 'dimensions', fieldMapping.dimensions);
        if (i % 1000 === 0) {
          setStatus(`Saving Dimensions: ${i}/${dimensionsData.length}...`);
        }
      }

      // Загружаем Salesforce чанками (744k записей)
      setStatus('Loading Salesforce accounts (0/?)...');
      const salesforceData = await loadTableInChunks(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.salesforceTable,
        30000, // Уменьшаем чанк до 30k для безопасности
        (loaded) => {
          setStatus(`Loading Salesforce: ${loaded} records loaded...`);
        }
      );

      console.log('[LoadAccounts] Salesforce data loaded:', salesforceData.length);

      setStatus(`Saving ${salesforceData.length} Salesforce accounts to database...`);
      for (let i = 0; i < salesforceData.length; i++) {
        await client.insertAccount(salesforceData[i], 'salesforce', fieldMapping.salesforce);
        if (i % 1000 === 0) {
          setStatus(`Saving Salesforce: ${i}/${salesforceData.length}...`);
        }
      }

      const newCounts = {
        source: sourceData.length,
        dimensions: dimensionsData.length,
        salesforce: salesforceData.length,
        total: sourceData.length + dimensionsData.length + salesforceData.length
      };

      setDbCounts(newCounts);
      setStatus(`Successfully loaded ${newCounts.source} Source, ${newCounts.dimensions} Dimensions, and ${newCounts.salesforce} Salesforce accounts.`);
      setHasMoreChunks(newCounts.source > 0);

    } catch (error) {
      setStatus(`Error loading accounts: ${error instanceof Error ? error.message : 'Failed to load data'}`);
      console.error('[LoadAccounts] Error:', error);
    } finally {
      setIsLoading(false);
    }
  };

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
    onProgress?: (loaded: number, total: number) => void
  ): Promise<any[]> => {
    const allData: any[] = [];
    let offset = 0;
    let hasMore = true;
    let iteration = 0;

    while (hasMore) {
      iteration++;
      console.log(`[loadTableInChunks] ${tablePath} - Iteration ${iteration}, offset: ${offset}`);

      try {
        const response = await fetch('/api/databricks/accounts', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            apiUrl,
            accessToken,
            warehouseId,
            tablePath,
            limit: chunkSize,
            offset
          }),
        });

        if (!response.ok) {
          const errorData = await response.json();
          console.error(`[loadTableInChunks] Failed to load chunk at offset ${offset}:`, errorData);

          // Если ошибка связана с лимитом размера, уменьшаем chunkSize
          if (errorData.details?.message?.includes('Inline byte limit exceeded')) {
            console.log('[loadTableInChunks] Reducing chunk size due to byte limit');
            return loadTableInChunks(apiUrl, accessToken, warehouseId, tablePath, Math.floor(chunkSize / 2), onProgress);
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
          onProgress(allData.length, allData.length);
        }

        // Если получили меньше чем chunkSize, значит это последний чанк
        if (chunkData.length < chunkSize) {
          console.log(`[loadTableInChunks] ${tablePath} - Last chunk (${chunkData.length} < ${chunkSize}), stopping`);
          hasMore = false;
        } else {
          // Увеличиваем offset для следующей итерации
          offset += chunkData.length;
          console.log(`[loadTableInChunks] ${tablePath} - Moving to next chunk, new offset: ${offset}`);
        }

      } catch (error) {
        console.error(`[loadTableInChunks] ${tablePath} - Error at offset ${offset}:`, error);
        hasMore = false;
      }
    }

    console.log(`[loadTableInChunks] ${tablePath} - Completed. Total records: ${allData.length}`);
    return allData;
  };


  // Обработка чанка
  const handleProcessChunk = async (startIndex: number) => {
    if (!isDbReady) {
      setStatus('Database is not ready.');
      return;
    }
    if (dbCounts.source === 0) {
      setStatus('No Source accounts loaded. Please Load Accounts first.');
      return;
    }

    setIsProcessing(true);
    setStatus(`Processing Source accounts ${startIndex + 1} to ${startIndex + CHUNK_SIZE}...`);
    setCurrentChunkData([]);
    setProcessedStatus(new Map());

    try {
      const fieldMapping = loadFieldMapping();

      console.log('[ProcessChunk] Starting with mapping:', fieldMapping);

      const result = await processSourceChunk(fieldMapping, CHUNK_SIZE, startIndex);

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
      default:
        return <Badge variant="outline">{status}</Badge>;
    }
  };

  return (
    <div className="space-y-4 p-4 md:p-6">
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2">
        <h2 className="text-xl font-semibold">Account Matching</h2>
      </div>

      <div className="space-y-2 p-4 border rounded-lg bg-card">
        <p className="text-sm text-muted-foreground">{status}</p>
        {isDbReady && (
          <div className="grid grid-cols-4 gap-4 text-sm">
            <div>
              <span className="font-semibold">Source:</span> {dbCounts.source}
            </div>
            <div>
              <span className="font-semibold">Dimensions:</span> {dbCounts.dimensions}
            </div>
            <div>
              <span className="font-semibold">Salesforce:</span> {dbCounts.salesforce}
            </div>
            <div>
              <span className="font-semibold">Total:</span> {dbCounts.total}
            </div>
          </div>
        )}
      </div>

      <div className="flex flex-wrap gap-2">
        <Button
          onClick={handleLoadAccounts}
          disabled={isLoading || isProcessing || !isDbReady}
        >
          {isLoading ? 'Loading...' : 'Reload All'}
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
                          <Badge variant="outline" className="text-xs">
                            Score: {item.dimensionsScore}
                          </Badge>
                        </div>
                      ) : (
                        <span className="text-muted-foreground italic text-sm">No match</span>
                      )}
                    </TableCell>
                    <TableCell>
                      {item.salesforceMatch ? (
                        <div className="space-y-1">
                          <p className="font-medium">{item.salesforceMatch.Name}</p>
                          <Badge variant="outline" className="text-xs">
                            Score: {item.salesforceScore}
                          </Badge>
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

      {/* Dialog для просмотра деталей */}
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