// app/pick-db-fields/page.tsx
"use client";
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import { useRouter } from 'next/navigation';
import { ColumnsTable } from '@/components/columns-table';
import { DatabricksConfig, fetchTables } from '@/app/api/databricks/client';
import { useLocalStorage } from '@/hooks/use-local-storage';

export default function PickDBFieldsPage() {
  const [tables, setTables] = useState<{name: string, columns: {name: string, type_text: string}[]}[]>([]);
  const [selectedTable, setSelectedTable] = useState('');
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const router = useRouter();

  useEffect(() => {
    fetchTables().then(setTables);
  }, []);

  const handleTableSelect = useCallback((tableName: string) => {
    const table = tables.find(t => t.name === tableName);
    setSelectedTable(tableName);
    setSelectedColumns([]);
  }, [tables]);

  const handleColumnToggle = useCallback((columnName: string) => {
    setSelectedColumns(prev => 
      prev.includes(columnName) 
        ? prev.filter(c => c !== columnName) 
        : [...prev, columnName]
    );
  }, []);

  const saveMapping = useCallback(() => {
    const config = JSON.parse(localStorage.getItem('databricksConfig') || '');
    localStorage.setItem('databricksConfig', JSON.stringify({ ...config, tableName: selectedTable }))

    localStorage.setItem('databricksColumns', JSON.stringify(selectedColumns));
    router.push('/salesforce');
  }, [selectedTable, selectedColumns, router]);

  const selectedTableData = useMemo(() => {
    return tables.find(t => t.name === selectedTable);
  }, [tables, selectedTable]);

  const tableButtons = useMemo(() => (
    tables.map(table => (
      <Button
        key={table.name}
        variant={selectedTable === table.name ? 'default' : 'outline'}
        onClick={() => handleTableSelect(table.name)}
      >
        {table.name}
      </Button>
    ))
  ), [tables, selectedTable, handleTableSelect]);

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold">2. Databricks columns picking</h2>
      
      <div className="grid gap-4">
        <h3>Select Table</h3>
        <div className="flex flex-wrap gap-2">
          {tableButtons}
        </div>

        {selectedTableData && (
          <>
            <h3>Select Columns</h3>
            <ColumnsTable 
              columns={selectedTableData.columns}
              selectedColumns={selectedColumns}
              onToggle={handleColumnToggle}
            />
          </>
        )}
      </div>

      <Button 
        onClick={saveMapping} 
        disabled={!selectedColumns.length}
      >
        Save and Continue
      </Button>
    </div>
  );
}