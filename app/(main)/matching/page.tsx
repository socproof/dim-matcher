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
import { InfoIcon, Download } from 'lucide-react';
import { Checkbox } from '@/components/ui/checkbox';
import { PROCESSING, DEV_LIMITS, applyLimit, isDevMode } from '@/lib/config';

const CHUNK_SIZE = PROCESSING.chunkSize;
const MATCH_THRESHOLD = 100;

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
  const [hasProcessedAnyChunk, setHasProcessedAnyChunk] = useState(false);

  const loadFieldMapping = useCallback(() => {
    const defaults = getDefaultMapping();
    const storedMapping = localStorage.getItem('fieldMapping');

    if (storedMapping) {
      try {
        const parsed = JSON.parse(storedMapping);
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
    setHasProcessedAnyChunk(false);

    try {
      const client = getPostgresClient();
      await client.clearAllAccounts();

      const config = JSON.parse(localStorage.getItem('appConfig') || '{}');
      const fieldMapping = loadFieldMapping();

      console.log('[LoadAccounts] Config:', config);
      console.log('[LoadAccounts] Dev Limits:', DEV_LIMITS);

      setStatus('Loading Source accounts...');
      const sourceData = await loadTableData(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.sourceTable,
        DEV_LIMITS.source || 20000
      );
      console.log('[LoadAccounts] Source data loaded:', sourceData.length);

      setStatus(`Saving ${sourceData.length} Source accounts to database...`);
      await client.insertAccountsBatch('source', sourceData);

      setStatus('Loading Dimensions accounts...');
      const dimensionsData = await loadTableInChunks(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.dimensionsTable,
        5000,
        (loaded) => setStatus(`Loading Dimensions: ${loaded} records loaded...`),
        DEV_LIMITS.dimensions || undefined
      );
      console.log('[LoadAccounts] Dimensions data loaded:', dimensionsData.length);

      setStatus(`Saving ${dimensionsData.length} Dimensions accounts to database...`);
      await client.insertAccountsBatch('dimensions', dimensionsData);

      setStatus('Loading Salesforce accounts...');
      const salesforceData = await loadTableInChunks(
        config.apiUrl,
        config.accessToken,
        config.warehouseId,
        config.salesforceTable,
        5000,
        (loaded) => setStatus(`Loading Salesforce: ${loaded} records loaded...`),
        DEV_LIMITS.salesforce || undefined
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
    maxRecords?: number
  ): Promise<any[]> => {
    const allData: any[] = [];
    let offset = 0;
    let hasMore = true;
    let iteration = 0;

    while (hasMore) {
      iteration++;

      if (maxRecords && allData.length >= maxRecords) {
        console.log(`[loadTableInChunks] ${tablePath} - Reached limit of ${maxRecords} records, stopping`);
        break;
      }

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
      setHasProcessedAnyChunk(true);

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

  const handleExportResults = () => {
    console.log('[ExportResults] Exporting results...');
    // TODO: Implement export logic
  };

  const handleMergeBoth = (item: MatchedAccount) => {
    console.log('[MergeBoth]', item);
    // TODO: Implement merge both logic
  };

  const handleMergeDimensions = (item: MatchedAccount) => {
    console.log('[MergeDimensions]', item);
    // TODO: Implement merge dimensions logic
  };

  const handleMergeSalesforce = (item: MatchedAccount) => {
    console.log('[MergeSalesforce]', item);
    // TODO: Implement merge salesforce logic
  };

  const handleCreateNew = (item: MatchedAccount) => {
    console.log('[CreateNew]', item);
    // TODO: Implement create new logic
  };

  const handleNeedsReview = (item: MatchedAccount) => {
    console.log('[NeedsReview]', item);
    // TODO: Implement needs review logic
  };

  const getMatchStatus = (item: MatchedAccount): string => {
    const dimMatched = item.dimensionsScore >= MATCH_THRESHOLD;
    const sfMatched = item.salesforceScore >= MATCH_THRESHOLD;

    if (dimMatched && sfMatched) return 'BOTH';
    if (dimMatched) return 'DIM_ONLY';
    if (sfMatched) return 'SF_ONLY';
    return 'NEW';
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

  const renderMatchInfo = (
    match: any | null,
    score: number,
    status: string,
    aiResult: { confidence: number; reasoning: string; decision: string } | undefined,
    isMatched: boolean
  ) => {
    if (!match && score === 0) {
      return <span className="text-muted-foreground italic text-sm">No potential match</span>;
    }

    return (
      <div className="space-y-1">
        {isMatched && match ? (
          <p className="font-medium text-green-700">{match.Name}</p>
        ) : match ? (
          <p className="font-medium text-gray-600">{match.Name}</p>
        ) : null}

        {score > 0 && (
          <Badge
            variant="outline"
            className={`text-xs ${isMatched ? 'bg-green-50 border-green-300' : 'bg-gray-50'}`}
          >
            Score: {score}
          </Badge>
        )}

        {!isMatched && score > 0 && score < 50 && (
          <p className="text-xs text-muted-foreground italic">
            Below threshold ({MATCH_THRESHOLD})
          </p>
        )}

        {!isMatched && score >= 50 && score < MATCH_THRESHOLD && (
          <p className="text-xs text-yellow-600 italic font-medium">
            Needs Review (50-{MATCH_THRESHOLD - 1})
          </p>
        )}

        {aiResult && (
          <div className="text-xs space-y-1">
            <Badge
              variant="outline"
              className={
                aiResult.decision === 'CONFIRMED' ? 'bg-green-100 border-green-400' :
                  aiResult.decision === 'REJECTED' ? 'bg-red-100 border-red-400' :
                    'bg-yellow-100 border-yellow-400'
              }
            >
              AI: {aiResult.confidence}% {aiResult.decision}
            </Badge>
            <p className="text-muted-foreground">{aiResult.reasoning}</p>
          </div>
        )}


      </div>
    );
  };

  const renderActionButtons = (item: MatchedAccount) => {
    const dimScore = item.dimensionsScore || 0;
    const sfScore = item.salesforceScore || 0;
    const dimMatched = dimScore >= MATCH_THRESHOLD;
    const sfMatched = sfScore >= MATCH_THRESHOLD;

    return (
      <div className="flex flex-col gap-1">
        <Button
          size="sm"
          variant="outline"
          onClick={() => handleViewDetails(item)}
          className="w-full"
        >
          View Details
        </Button>

        {dimMatched && sfMatched && (
          <Button
            size="sm"
            className="bg-green-600 hover:bg-green-700 w-full"
            onClick={() => handleMergeBoth(item)}
          >
            Merge Both
          </Button>
        )}

        {dimMatched && !sfMatched && (
          <Button
            size="sm"
            className="bg-blue-600 hover:bg-blue-700 w-full"
            onClick={() => handleMergeDimensions(item)}
          >
            Merge Dimensions
          </Button>
        )}

        {!dimMatched && sfMatched && (
          <Button
            size="sm"
            className="bg-purple-600 hover:bg-purple-700 w-full"
            onClick={() => handleMergeSalesforce(item)}
          >
            Merge Salesforce
          </Button>
        )}

        {dimScore <= 50 && sfScore <= 50 && (
          <Button
            size="sm"
            variant="secondary"
            className="w-full"
            onClick={() => handleCreateNew(item)}
          >
            Create New
          </Button>
        )}

        {((dimScore > 50 && dimScore < MATCH_THRESHOLD) || (sfScore > 50 && sfScore < MATCH_THRESHOLD)) && (
          <Button
            size="sm"
            className="bg-yellow-600 hover:bg-yellow-700 w-full"
            onClick={() => handleNeedsReview(item)}
          >
            Needs Review
          </Button>
        )}
      </div>
    );
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
          disabled={isLoading || isProcessing || dbCounts.source === 0 || hasProcessedAnyChunk}
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
          onClick={handleExportResults}
          variant="outline"
          disabled={true}
          className="gap-2"
        >
          <Download className="h-4 w-4" />
          Export Results
        </Button>

        <Button
          onClick={handleDiagnose}
          variant="outline"
          size="sm"
          disabled={!isDbReady || isLoading || isProcessing}
        >
          Diagnose DB
        </Button>

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
                {currentChunkData.map((item, idx) => {
                  const matchStatus = getMatchStatus(item);
                  const dimMatched = item.dimensionsScore >= MATCH_THRESHOLD;
                  const sfMatched = item.salesforceScore >= MATCH_THRESHOLD;

                  return (
                    <TableRow key={idx}>
                      <TableCell>
                        <div className="space-y-1">
                          <p className="font-medium">{item.sourceAccount?.Name || 'N/A'}</p>
                          <p className="text-xs text-muted-foreground">{item.sourceAccount?.Phone || ''}</p>
                        </div>
                      </TableCell>
                      <TableCell>
                        {renderMatchInfo(
                          item.dimensionsMatch,
                          item.dimensionsScore,
                          item.dimensionsStatus,
                          item.dimensionsAI,
                          dimMatched
                        )}
                      </TableCell>
                      <TableCell>
                        {renderMatchInfo(
                          item.salesforceMatch,
                          item.salesforceScore,
                          item.salesforceStatus,
                          item.salesforceAI,
                          sfMatched
                        )}
                      </TableCell>
                      <TableCell>{getStatusBadge(matchStatus)}</TableCell>
                      <TableCell>
                        {renderActionButtons(item)}
                      </TableCell>
                    </TableRow>
                  );
                })}
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
                  <h4 className="font-semibold mb-2">
                    Dimensions Match
                    {selectedItem.dimensionsScore >= MATCH_THRESHOLD && ' ✓'}
                  </h4>
                  {selectedItem.dimensionsMatch ? (
                    <>
                      <pre className="text-xs overflow-auto">
                        {JSON.stringify(selectedItem.dimensionsMatch, null, 2)}
                      </pre>
                      <div className="mt-2 space-y-1">
                        <Badge>Score: {selectedItem.dimensionsScore}</Badge>
                        {selectedItem.dimensionsAI && (
                          <Badge variant="outline">
                            AI: {selectedItem.dimensionsAI.confidence}% {selectedItem.dimensionsAI.decision}
                          </Badge>
                        )}
                      </div>
                    </>
                  ) : (
                    <p className="text-muted-foreground italic">No match found</p>
                  )}
                </div>
                <div className="border rounded p-3">
                  <h4 className="font-semibold mb-2">
                    Salesforce Match
                    {selectedItem.salesforceScore >= MATCH_THRESHOLD && ' ✓'}
                  </h4>
                  {selectedItem.salesforceMatch ? (
                    <>
                      <pre className="text-xs overflow-auto">
                        {JSON.stringify(selectedItem.salesforceMatch, null, 2)}
                      </pre>
                      <div className="mt-2 space-y-1">
                        <Badge>Score: {selectedItem.salesforceScore}</Badge>
                        {selectedItem.salesforceAI && (
                          <Badge variant="outline">
                            AI: {selectedItem.salesforceAI.confidence}% {selectedItem.salesforceAI.decision}
                          </Badge>
                        )}
                      </div>
                    </>
                  ) : (
                    <p className="text-muted-foreground italic">No match found</p>
                  )}
                </div>
              </div>
              <div className="flex justify-center">
                {getStatusBadge(getMatchStatus(selectedItem))}
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}