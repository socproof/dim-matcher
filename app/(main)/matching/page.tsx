// app/(main)/matching/page.tsx
"use client";
import { useEffect, useState, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { fetchDatabricksData } from '@/app/api/databricks/client';
import { fetchSalesforceData } from '@/app/api/salesforce/client';

import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { getFieldMatchDetails } from '@/lib/matching';
import { Progress } from '@/components/ui/progress';
import { Badge } from '@/components/ui/badge';
import { MAX_POSSIBLE_SCORE, MATCH_THRESHOLD } from '@/lib/matching-config';
import { getSQLiteClient } from '@/lib/sqlite-client';
import { processChunk } from '@/lib/find-matches-chunked';

const CHUNK_SIZE = 5;

export default function MatchingPage() {
  const [status, setStatus] = useState<string>('Initializing database...');
  const [currentChunkData, setCurrentChunkData] = useState<any[]>([]); // Holds the current 10 items
  const [processedStatus, setProcessedStatus] = useState<Map<string, 'merged' | 'added'>>(new Map()); // Tracks processed items in the chunk <dbAccountId, status>
  const [progress, setProgress] = useState<{ loaded: number; total: number }>({ loaded: 0, total: 0 });
  const [isLoading, setIsLoading] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false); // Renamed from isSearching
  const [selectedItem, setSelectedItem] = useState<any>(null); // For dialogs
  const [dialogMode, setDialogMode] = useState<'merge' | 'create' | null>(null);
  const [showDetails, setShowDetails] = useState(false);
  const [isDbReady, setIsDbReady] = useState(false);
  const [dbCounts, setDbCounts] = useState<{ databricks: number, salesforce: number, total: number }>({ databricks: 0, salesforce: 0, total: 0 });
  const [currentChunkIndex, setCurrentChunkIndex] = useState<number>(0); // Tracks the start index of the current chunk
  const [hasMoreChunks, setHasMoreChunks] = useState<boolean>(false);
  const [showDownloadReport, setShowDownloadReport] = useState<boolean>(false);
  const [lastActionMessage, setLastActionMessage] = useState<string | null>(null);

  const loadFieldMapping = useCallback(() => {
    const storedMapping = localStorage.getItem('fieldMapping');
    if (storedMapping) {
      try {
        return JSON.parse(storedMapping);
      } catch (e) {
        console.error("Failed to parse fieldMapping from localStorage", e);
        setStatus("Error: Invalid field mapping in localStorage. Please check configuration.");
        return {};
      }
    }
    return {};
  }, []);


  useEffect(() => {
    const initDbAndCheckData = async () => {
      setStatus('Initializing database...');
      try {
        const client = await getSQLiteClient();
        await client.createAccountsTable();
        setIsDbReady(true);

        const counts = await client.getAccountCounts();
        setDbCounts(counts);

        if (counts.total > 0) {
          setStatus(`Found ${counts.databricks} Databricks and ${counts.salesforce} Salesforce accounts in local DB. Ready to process or reload.`);
          setProgress({ loaded: counts.total, total: counts.total });
          setHasMoreChunks(counts.databricks > 0); // Initially true if there are DB accounts
        } else {
          setStatus('Database ready. Load accounts from sources to begin.');
          setProgress({ loaded: 0, total: 0 });
          setHasMoreChunks(false);
        }
      } catch (error) {
        setStatus(`Database initialization error: ${error instanceof Error ? error.message : 'Unknown error'}`);
        setIsDbReady(false);
      }
    };

    initDbAndCheckData();
  }, []);

  useEffect(() => {
    if (currentChunkData.length > 0 && processedStatus.size === currentChunkData.length) {
      setShowDownloadReport(true);
      setStatus(`Chunk processed (${processedStatus.size}/${currentChunkData.length}). Ready for next chunk or download report.`);
    } else if (currentChunkData.length > 0) {
       setShowDownloadReport(false);
       const remaining = currentChunkData.length - processedStatus.size;
       setStatus(`Processing chunk... ${remaining} item(s) remaining.`);
    } else {
      setShowDownloadReport(false);
    }
  }, [processedStatus, currentChunkData]);


  const handleLoadAccounts = async () => {
    if (!isDbReady) {
      setStatus('Database is not ready yet. Please wait or refresh.');
      return;
    }

    setIsLoading(true);
    setStatus('Loading accounts from sources... This will overwrite existing data in the local DB.');
    setProgress({ loaded: 0, total: 0 });
    setCurrentChunkData([]);
    setProcessedStatus(new Map());
    setCurrentChunkIndex(0);
    setHasMoreChunks(false);
    setShowDownloadReport(false);
    setLastActionMessage(null);

    try {
      const client = await getSQLiteClient();
      await client.clearAccounts(); // Clear existing data before loading

      const [dbLoadedCount, sfLoadedCount] = await Promise.all([
        fetchDatabricksData(),
        fetchSalesforceData()
      ]);

      const totalLoaded = dbLoadedCount + sfLoadedCount;
      setDbCounts({ databricks: dbLoadedCount, salesforce: sfLoadedCount, total: totalLoaded });
      setStatus(`Successfully loaded ${dbLoadedCount} Databricks and ${sfLoadedCount} Salesforce accounts into local DB.`);
      setProgress({ loaded: totalLoaded, total: totalLoaded });
      setHasMoreChunks(dbLoadedCount > 0); // Set based on loaded count

    } catch (error) {
      setStatus(`Error loading accounts: ${error instanceof Error ? error.message : 'Failed to load data'}`);
      try {
        const client = await getSQLiteClient();
        const counts = await client.getAccountCounts();
        setDbCounts(counts);
        setProgress({ loaded: counts.total, total: counts.total });
        setHasMoreChunks(counts.databricks > 0);
      } catch (dbError) {
        console.error("Failed to get DB counts after loading error:", dbError);
      }
    } finally {
      setIsLoading(false);
    }
  };

 const handleProcessChunk = async (startIndex: number) => {
    if (!isDbReady) {
      setStatus('Database is not ready.');
      return;
    }
    if (dbCounts.databricks === 0) {
      setStatus('No Databricks accounts loaded in the database. Please Load Accounts first.');
      return;
    }

    setIsProcessing(true);
    setStatus(`Processing Databricks accounts starting from index ${startIndex}...`);
    setCurrentChunkData([]);
    setProcessedStatus(new Map());
    setShowDownloadReport(false);
    setLastActionMessage(null);

    try {
      const fieldMapping = loadFieldMapping();
      if (Object.keys(fieldMapping).length === 0 && !localStorage.getItem('fieldMapping')) {
        setStatus("Field mapping not found. Please configure it first.");
        setIsProcessing(false);
        return;
      }

      const result = await processChunk(fieldMapping, CHUNK_SIZE, startIndex);

      setCurrentChunkData(result.items);
      setHasMoreChunks(result.hasMore);
      setCurrentChunkIndex(startIndex); // Store the start index of the *current* chunk

      if (result.items.length === 0 && startIndex < dbCounts.databricks) {
         setStatus(`No more Databricks accounts found from index ${startIndex}. Total processed: ${startIndex}.`);
         setHasMoreChunks(false);
      } else if (result.items.length > 0) {
         setStatus(`Loaded chunk of ${result.items.length} accounts (Index ${startIndex} to ${startIndex + result.items.length - 1}). ${result.hasMore ? 'More available.' : 'This is the last chunk.'}`);
      } else {
         setStatus(`All ${dbCounts.databricks} Databricks accounts processed.`);
         setHasMoreChunks(false);
      }


    } catch (error) {
      setStatus(`Error processing chunk: ${error instanceof Error ? error.message : 'Failed to process chunk'}`);
      console.error("Error processing chunk:", error);
    } finally {
      setIsProcessing(false);
    }
  };

  const handleMergeClick = (item: any) => {
    const fieldMapping = loadFieldMapping();
    const dbAccountData = item.dbAccountRaw || item.dbAccount;
    const sfAccountData = item.bestSfMatch?.raw_data ? JSON.parse(item.bestSfMatch.raw_data) : null;

    const detailedFields = Object.entries(dbAccountData).map(([key, dbValue]) => {
      const conceptualFieldName = fieldMapping.databricks?.[key] || key;
      let sfFieldName = null;
      if (fieldMapping.salesforce && sfAccountData) {
        for (const sfKey in fieldMapping.salesforce) {
          if (fieldMapping.salesforce[sfKey] === conceptualFieldName) {
            sfFieldName = sfKey;
            break;
          }
        }
      }
      const sfValue = sfFieldName && sfAccountData ? sfAccountData[sfFieldName] : (item.bestSfMatch?.[conceptualFieldName] ?? '');

      const matchDetails = getFieldMatchDetails(
        conceptualFieldName,
        String(dbValue ?? ''),
        String(sfValue ?? ''),
        dbAccountData.BillingCountry || dbAccountData.Country || 'australia'
      );
      return {
        field: conceptualFieldName,
        dbValue: dbValue ?? 'N/A',
        sfValue: sfValue ?? 'N/A',
        status: matchDetails.status,
        score: matchDetails.score,
      };
    }).filter(df => df.dbValue !== 'N/A' || df.sfValue !== 'N/A' || df.score > 0);

    setSelectedItem({
      ...item,
      dbAccountRaw: dbAccountData, // Ensure raw data is passed if available
      sfAccountRaw: sfAccountData, // Pass parsed raw SF data
      detailedFields: detailedFields.sort((a, b) => b.score - a.score)
    });
    setDialogMode('merge');
    setShowDetails(true);
    setLastActionMessage(null);
  };

  const handleCreateClick = (item: any) => {
    setSelectedItem({
        ...item,
        dbAccountRaw: item.dbAccountRaw || item.dbAccount // Ensure raw data is passed
    });
    setDialogMode('create');
    setShowDetails(true);
    setLastActionMessage(null);
  };

  const handleConfirmMerge = () => {
      const dbAccountId = selectedItem.dbAccountRaw?.id || selectedItem.dbAccount?.id || Date.now(); // Use a fallback ID if needed
      console.log(`Simulating merge for DB Account ID: ${dbAccountId}`);
      setProcessedStatus(prev => new Map(prev).set(String(dbAccountId), 'merged'));
      setLastActionMessage(`Account ${selectedItem.dbAccountRaw?.Name || 'N/A'} (ID: ${dbAccountId}) marked as Merged.`);
      setShowDetails(false);
      setSelectedItem(null);
      setDialogMode(null);
  };

  const handleConfirmCreate = () => {
      const dbAccountId = selectedItem.dbAccountRaw?.id || selectedItem.dbAccount?.id || Date.now(); // Use a fallback ID if needed
      const assignedCode = `NEW-${Math.random().toString(36).substring(2, 8).toUpperCase()}`; // Simulate new code
      console.log(`Simulating creation for DB Account ID: ${dbAccountId}, Assigned Code: ${assignedCode}`);
      setProcessedStatus(prev => new Map(prev).set(String(dbAccountId), 'added'));
      setLastActionMessage(`Account ${selectedItem.dbAccountRaw?.Name || 'N/A'} (ID: ${dbAccountId}) marked as Added with code ${assignedCode}.`);
      setShowDetails(false);
      setSelectedItem(null);
      setDialogMode(null);
  };

  const handleDownloadReport = () => {
    if (!showDownloadReport || currentChunkData.length === 0) return;

    const fieldMapping = loadFieldMapping(); // Load mapping to get conceptual names if needed

    const getFieldValue = (accountData: any, conceptualName: string) => {
        if (!accountData) return '';
        // Find the source-specific key for the conceptual name
        let sourceKey = conceptualName; // Default if not found in mapping
        if (fieldMapping.databricks) {
            for (const key in fieldMapping.databricks) {
                if (fieldMapping.databricks[key] === conceptualName) {
                    sourceKey = key;
                    break;
                }
            }
        }
        return accountData[sourceKey] ?? '';
    };


    const headers = ['Processed Status', 'Databricks Account ID', 'Account Name', 'Phone', 'Website', 'Billing Address'];
    const csvRows = [headers.join(',')];

    currentChunkData.forEach(item => {
        const dbAccountData = item.dbAccountRaw || item.dbAccount;
        const dbAccountId = String(dbAccountData?.id || 'N/A');
        const status = processedStatus.get(dbAccountId) || 'Unknown'; // Should always be 'merged' or 'added' here

        const name = getFieldValue(dbAccountData, 'Name');
        const phone = getFieldValue(dbAccountData, 'Phone');
        const website = getFieldValue(dbAccountData, 'Website');
        const street = getFieldValue(dbAccountData, 'BillingStreet');
        const city = getFieldValue(dbAccountData, 'BillingCity');
        const state = getFieldValue(dbAccountData, 'BillingState');
        const postcode = getFieldValue(dbAccountData, 'BillingPostalCode');
        const country = getFieldValue(dbAccountData, 'BillingCountry');
        const address = [street, city, state, postcode, country].filter(Boolean).join(', ');

        const row = [
            status.toUpperCase(),
            `"${dbAccountId}"`, // Ensure ID is treated as string
            `"${name.replace(/"/g, '""')}"`, // Escape double quotes
            `"${phone.replace(/"/g, '""')}"`,
            `"${website.replace(/"/g, '""')}"`,
            `"${address.replace(/"/g, '""')}"`
        ];
        csvRows.push(row.join(','));
    });

    const csvContent = csvRows.join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement("a");
    if (link.download !== undefined) {
        const url = URL.createObjectURL(blob);
        link.setAttribute("href", url);
        link.setAttribute("download", `processing_report_chunk_${currentChunkIndex}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
     setLastActionMessage(`Report for chunk ${currentChunkIndex} downloaded.`);
  };


  const calculateScorePercentage = (score: number) => {
    return Math.max(0, Math.min(100, Math.round((score / MAX_POSSIBLE_SCORE) * 100)));
  };


  const canProcess = isDbReady && dbCounts.databricks > 0;

  return (
    <div className="space-y-4 p-4 md:p-6">
      <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2">
        <h2 className="text-xl font-semibold">Account Matching & Processing (Chunked)</h2>
         {/* Removed search mode buttons */}
      </div>

      <div className="space-y-2 p-4 border rounded-lg bg-card">
        <p className="text-sm text-muted-foreground min-h-[20px]">{status}</p>
        {isDbReady && (
          <div className="space-y-1">
            <div className="flex justify-between text-sm text-muted-foreground">
              <span>Data in local DB: {dbCounts.total} accounts</span>
               {dbCounts.total > 0 && <span>{Math.round((progress.loaded / (progress.total || 1)) * 100)}% Loaded</span>}
            </div>
            {dbCounts.total > 0 && <Progress value={(progress.loaded / (progress.total || 1)) * 100} className="w-full" />}
            <p className="text-xs text-muted-foreground">
              Databricks: {dbCounts.databricks}, Salesforce: {dbCounts.salesforce}
            </p>
          </div>
        )}
         {lastActionMessage && (
           <p className="text-sm text-green-600 dark:text-green-400 mt-2">{lastActionMessage}</p>
         )}
      </div>

      <div className="flex flex-wrap gap-2">
        <Button
          onClick={handleLoadAccounts}
          disabled={isLoading || isProcessing || !isDbReady}
          className="min-w-[160px]"
        >
          {isLoading ? 'Loading...' : (dbCounts.total > 0 ? 'Reload All Accounts' : 'Load Accounts to DB')}
        </Button>
        <Button
          onClick={() => handleProcessChunk(0)} // Start from the beginning
          disabled={isLoading || isProcessing || !canProcess || showDownloadReport} // Disable if chunk is complete
          variant="secondary"
          className="min-w-[180px]"
        >
          {isProcessing ? 'Processing...' : 'Process First Chunk'}
        </Button>
         {hasMoreChunks && !showDownloadReport && currentChunkData.length > 0 && ( // Show only if there are more and current chunk is *not* fully processed
             <Button
                 onClick={() => handleProcessChunk(currentChunkIndex + CHUNK_SIZE)}
                 disabled={isLoading || isProcessing}
                 variant="outline"
                 className="min-w-[180px]"
             >
                {isProcessing ? 'Processing...' : 'Process Next Chunk'}
             </Button>
         )}
        {showDownloadReport && (
          <Button
            onClick={handleDownloadReport}
            disabled={isLoading || isProcessing}
            variant="default"
            className="bg-blue-600 hover:bg-blue-700 min-w-[180px]"
          >
            Download Report
          </Button>
        )}
         {showDownloadReport && hasMoreChunks && ( // Show next chunk button after report download if more exist
             <Button
                 onClick={() => handleProcessChunk(currentChunkIndex + CHUNK_SIZE)}
                 disabled={isLoading || isProcessing}
                 variant="outline"
                 className="min-w-[180px]"
             >
                {isProcessing ? 'Processing...' : 'Process Next Chunk'}
             </Button>
         )}
      </div>

      {!isDbReady && (
        <div className="p-4 bg-yellow-100 text-yellow-700 rounded-md border border-yellow-300">
          Initializing database, please wait... If this persists, check console for errors or try refreshing.
        </div>
      )}

      {currentChunkData.length > 0 && (
        <div className="mt-6 space-y-4">
          <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-2">
             <h3 className="text-lg font-medium">
                Processing Chunk (Items {currentChunkIndex + 1} - {Math.min(currentChunkIndex + CHUNK_SIZE, dbCounts.databricks)})
                 <span className="ml-2 text-sm font-normal text-muted-foreground">
                    (Match Threshold: {MATCH_THRESHOLD}/{MAX_POSSIBLE_SCORE})
                </span>
            </h3>
           {/* Removed Load More Button, replaced by Process Next Chunk */}
          </div>

          <div className="rounded-md border overflow-x-auto">
            <Table>
              <TableHeader className="bg-muted">
                <TableRow>
                  <TableHead className="min-w-[200px] px-2 py-3">Databricks Account</TableHead>
                  <TableHead className="min-w-[200px] px-2 py-3">Best Salesforce Match</TableHead>
                  <TableHead className="min-w-[150px] px-2 py-3">Match Score</TableHead>
                  <TableHead className="min-w-[100px] px-2 py-3">Status</TableHead>
                  <TableHead className="min-w-[120px] px-2 py-3 text-center">Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {currentChunkData.map((item) => {
                  const dbAccountData = item.dbAccountRaw || item.dbAccount;
                  const sfAccountData = item.bestSfMatch?.raw_data ? JSON.parse(item.bestSfMatch.raw_data) : null;
                  const dbAccountId = String(dbAccountData?.id || Date.now()); // Use a consistent ID
                  const itemStatus = item.status; // 'matched' or 'new' based on threshold
                  const processedState = processedStatus.get(dbAccountId); // 'merged', 'added', or undefined
                  const scorePercentage = calculateScorePercentage(item.score);
                   const isProcessed = !!processedState;
                  const key = `item-${dbAccountId}`;


                  return (
                    <TableRow
                      key={key}
                      className={`${isProcessed ? 'opacity-60 bg-muted/30' : ''}`}
                    >
                      <TableCell className="px-2 py-2">
                        <div className="space-y-1">
                          <p className="font-medium text-sm">{dbAccountData?.Name || <span className="italic text-muted-foreground">No Name</span>}</p>
                          <p className="text-xs text-muted-foreground">ID: {dbAccountId}</p>
                        </div>
                      </TableCell>
                      <TableCell className="px-2 py-2">
                        {sfAccountData ? (
                          <div className="space-y-1">
                            <p className="font-medium text-sm">{sfAccountData.Name || <span className="italic text-muted-foreground">No Name</span>}</p>
                             <p className="text-xs text-muted-foreground">ID: {item.bestSfMatch?.id || 'N/A'}</p>
                          </div>
                        ) : (
                          <span className="text-xs text-muted-foreground italic">No potential SF match found</span>
                        )}
                      </TableCell>
                      <TableCell className="px-2 py-2">
                         {item.score > 0 || sfAccountData ? ( // Show score only if there was a potential match evaluated
                            <div className="flex items-center gap-2 sm:gap-3">
                            <div className="w-full max-w-[80px] sm:max-w-[100px]">
                                <div className="flex justify-between text-xs text-muted-foreground mb-0.5 sm:mb-1">
                                <span>{item.score}</span>
                                <span>{MAX_POSSIBLE_SCORE}</span>
                                </div>
                                <div className="relative h-1.5 sm:h-2 w-full bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                                <div
                                    className={`absolute top-0 left-0 h-full ${item.score >= MATCH_THRESHOLD
                                    ? 'bg-green-500'
                                    : item.score > MATCH_THRESHOLD * 0.6
                                        ? 'bg-yellow-500'
                                        : 'bg-red-500'
                                    }`}
                                    style={{ width: `${scorePercentage}%` }}
                                />
                                </div>
                            </div>
                            <Badge
                                variant={
                                item.score >= MATCH_THRESHOLD
                                    ? 'default'
                                    : item.score > MATCH_THRESHOLD * 0.6
                                    ? 'secondary'
                                    : 'destructive'
                                }
                                className="text-xs"
                            >
                                {scorePercentage}%
                            </Badge>
                            </div>
                         ) : (
                             <span className="text-xs text-muted-foreground italic">-</span>
                         )}
                      </TableCell>
                       <TableCell className="px-2 py-2">
                          {processedState ? (
                             <Badge variant={processedState === 'merged' ? 'default' : 'secondary'} className={`capitalize ${processedState === 'merged' ? 'bg-green-600 hover:bg-green-600' : 'bg-blue-600 hover:bg-blue-600'} text-white`}>
                                {processedState}
                             </Badge>
                          ) : (
                             <Badge variant={itemStatus === 'matched' ? 'outline' : 'secondary'} className={`capitalize ${itemStatus === 'matched' ? 'border-green-500 text-green-700' : 'border-orange-500 text-orange-700'}`}>
                                {itemStatus}
                             </Badge>
                          )}
                       </TableCell>
                      <TableCell className="px-2 py-2 text-center">
                         {!isProcessed && itemStatus === 'matched' && (
                            <Button size="sm" variant="default" className="bg-green-600 hover:bg-green-700" onClick={() => handleMergeClick(item)}>Merge</Button>
                         )}
                         {!isProcessed && itemStatus === 'new' && (
                            <Button size="sm" variant="secondary" onClick={() => handleCreateClick(item)}>Create</Button>
                         )}
                          {isProcessed && (
                             <span className="text-xs text-muted-foreground italic">-</span>
                          )}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </div>
      )}

      {/* Dialog for Merge and Create */}
      <Dialog open={showDetails} onOpenChange={setShowDetails}>
        <DialogContent className={`max-w-4xl max-h-[90vh] flex flex-col ${dialogMode === 'create' ? 'max-w-lg' : ''}`}>
          <DialogHeader>
            <DialogTitle className="capitalize">{dialogMode} Account</DialogTitle>
          </DialogHeader>

          {selectedItem && dialogMode === 'merge' && (
            // Existing Merge Dialog Content
            <div className="mt-2 space-y-4 overflow-y-auto flex-grow pr-1">
              <div className="flex justify-between items-start mb-4 border-b pb-4">
                <div className="space-y-1">
                  <div className="flex items-center gap-2">
                    <div className="text-2xl font-bold">
                      {selectedItem.score}/{MAX_POSSIBLE_SCORE}
                    </div>
                    <Badge
                      variant={
                        selectedItem.score >= MATCH_THRESHOLD
                          ? 'default'
                          : selectedItem.score > MATCH_THRESHOLD * 0.6
                            ? 'secondary'
                            : 'destructive'
                      }
                      className="text-base"
                    >
                      {calculateScorePercentage(selectedItem.score)}%
                    </Badge>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Threshold: {MATCH_THRESHOLD} ({Math.round((MATCH_THRESHOLD / MAX_POSSIBLE_SCORE) * 100)}%)
                  </p>
                </div>
                <Button variant="default" className="bg-green-600 hover:bg-green-700 text-white" onClick={handleConfirmMerge}>
                  Confirm Merge
                </Button>
              </div>

              <div className="grid grid-cols-[1fr_auto_1fr] items-start gap-x-3 mb-2">
                <h4 className="font-semibold text-base text-left">Databricks Account</h4>
                <div className="text-center font-semibold text-base">Match</div>
                <h4 className="font-semibold text-base text-right">Salesforce Account</h4>
              </div>

              <div className="space-y-3">
                {(selectedItem.detailedFields?.length > 0 ? selectedItem.detailedFields : [
                   { field: 'Name', dbValue: selectedItem.dbAccountRaw?.Name, sfValue: selectedItem.sfAccountRaw?.Name, status: 'N/A_PLACEHOLDER' },
                   { field: 'ID (Source)', dbValue: selectedItem.dbAccountRaw?.id, sfValue: selectedItem.bestSfMatch?.id, status: 'N/A_PLACEHOLDER' },
                   { field: 'Company Registration No', dbValue: selectedItem.dbAccountRaw?.Company_Registration_No__c, sfValue: selectedItem.sfAccountRaw?.Company_Registration_No__c, status: 'N/A_PLACEHOLDER' },
                   { field: 'Phone', dbValue: selectedItem.dbAccountRaw?.Phone, sfValue: selectedItem.sfAccountRaw?.Phone, status: 'N/A_PLACEHOLDER' },
                   { field: 'Website', dbValue: selectedItem.dbAccountRaw?.Website, sfValue: selectedItem.sfAccountRaw?.Website, status: 'N/A_PLACEHOLDER' },
                 ].filter(f => f.dbValue !== undefined || f.sfValue !== undefined)
                ).map(({ field, dbValue, sfValue, status: initialStatus }: { field: string, dbValue: any, sfValue: any, status: string }) => {

                  const dbDisplayValue = dbValue !== undefined && dbValue !== null && String(dbValue).trim() !== "" ? String(dbValue) : 'N/A';
                  const sfDisplayValue = sfValue !== undefined && sfValue !== null && String(sfValue).trim() !== "" ? String(sfValue) : 'N/A';

                  let displayStatus = initialStatus;

                  if (initialStatus !== 'exact' && initialStatus !== 'partial') {
                    if (dbDisplayValue === 'N/A' && sfDisplayValue === 'N/A') {
                      displayStatus = 'N/A';
                    } else if (dbDisplayValue === 'N/A' || sfDisplayValue === 'N/A') {
                      displayStatus = 'differs';
                    } else {
                      displayStatus = String(dbValue).trim().toLowerCase() === String(sfValue).trim().toLowerCase() ? 'exact' : 'differs';
                    }
                  }

                  return (
                    <div key={field} className="grid grid-cols-[1fr_auto_1fr] items-center gap-x-3 gap-y-1 py-2 border-b last:border-b-0">
                      <div className="text-sm">
                        <p className="font-medium text-muted-foreground">{field}</p>
                        <p className="truncate" title={dbDisplayValue}>{dbDisplayValue}</p>
                      </div>
                      <div className="flex justify-center">
                        <Badge
                          variant={
                            displayStatus === 'exact' ? 'default' :
                              displayStatus === 'partial' ? 'secondary' :
                                displayStatus === 'differs' ? 'destructive' :
                                  'outline'
                          }
                          className={`w-20 justify-center text-xs
                                ${displayStatus === 'exact' ? 'bg-green-100 border-green-400 text-green-700 dark:bg-green-800 dark:border-green-600 dark:text-green-200'
                              : displayStatus === 'partial' ? 'bg-yellow-100 border-yellow-400 text-yellow-700 dark:bg-yellow-700 dark:border-yellow-600 dark:text-yellow-100'
                                : displayStatus === 'differs' ? 'bg-red-100 border-red-400 text-red-700 dark:bg-red-800 dark:border-red-600 dark:text-red-200'
                                  : displayStatus === 'N/A' ? 'bg-gray-100 border-gray-300 text-gray-500 dark:bg-gray-700 dark:border-gray-500 dark:text-gray-400'
                                    : ''}`}
                        >
                          {displayStatus === 'exact' ? 'Exact' :
                            displayStatus === 'partial' ? 'Partial' :
                              displayStatus === 'differs' ? 'Differs' : 'N/A'}
                        </Badge>
                      </div>

                      <div className="text-sm text-right">
                        <p className="font-medium text-muted-foreground">{field}</p>
                        <p className="truncate" title={sfDisplayValue}>{sfDisplayValue}</p>
                      </div>
                    </div>
                  );
                })}
                 {
                   !(selectedItem.detailedFields?.length > 0 ?
                    selectedItem.detailedFields :
                     [
                       { dbValue: selectedItem.dbAccountRaw?.Name, sfValue: selectedItem.sfAccountRaw?.Name },
                       { dbValue: selectedItem.dbAccountRaw?.id, sfValue: selectedItem.bestSfMatch?.id },
                       { dbValue: selectedItem.dbAccountRaw?.Company_Registration_No__c, sfValue: selectedItem.sfAccountRaw?.Company_Registration_No__c },
                       { dbValue: selectedItem.dbAccountRaw?.Phone, sfValue: selectedItem.sfAccountRaw?.Phone },
                       { dbValue: selectedItem.dbAccountRaw?.Website, sfValue: selectedItem.sfAccountRaw?.Website },
                    ].filter(f => f.dbValue !== undefined || f.sfValue !== undefined)
                   ).some((f: { dbValue: any; sfValue: any; }) => (f.dbValue !== undefined && f.dbValue !== null && String(f.dbValue).trim() !== "") ||
                    (f.sfValue !== undefined && f.sfValue !== null && String(f.sfValue).trim() !== "")) &&
                   (
                    <p className="text-center text-sm text-muted-foreground py-4">
                      No detailed field data to compare for this match.
                    </p>
                   )
                 }
              </div>
            </div>
          )}

           {selectedItem && dialogMode === 'create' && (
             <div className="mt-2 space-y-4 overflow-y-auto flex-grow pr-1">
                <p className="text-sm text-muted-foreground">
                    This account did not have a strong match in Salesforce. Review the details below before creating a new record.
                </p>
                <div className="space-y-3 border rounded-md p-4 bg-card">
                 <h4 className="font-semibold text-base mb-2">Databricks Account Details</h4>
                 {Object.entries(selectedItem.dbAccountRaw || selectedItem.dbAccount || {}).map(([key, value]) => {
                     const displayValue = (value !== null && value !== undefined && String(value).trim() !== '') ? String(value) : 'N/A';
                     if (key === 'raw_data' || key === 'created_at' || key.startsWith('normalized')) return null; // Skip internal fields
                     return (
                        <div key={key} className="grid grid-cols-[auto_1fr] gap-x-4 items-center text-sm py-1 border-b last:border-b-0">
                            <p className="font-medium text-muted-foreground">{key}:</p>
                            <p className="truncate" title={displayValue}>{displayValue}</p>
                        </div>
                     )
                    })}
                 {Object.keys(selectedItem.dbAccountRaw || selectedItem.dbAccount || {}).length === 0 && (
                    <p className="text-sm text-muted-foreground italic">No details available.</p>
                 )}
                </div>
                 <div className="flex justify-end mt-4">
                    <Button variant="default" className="bg-blue-600 hover:bg-blue-700 text-white" onClick={handleConfirmCreate}>
                    Confirm Create
                    </Button>
                 </div>
             </div>
            )}

        </DialogContent>
      </Dialog>
    </div>
  );
}