"use client";

import { useEffect, useState, useCallback, useMemo } from 'react';
import { Button } from '@/components/ui/button';
import { useRouter } from 'next/navigation';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Check, ChevronsUpDown, Loader2 } from 'lucide-react';
import { cn } from '@/lib/utils';
import { fetchTables as apiFetchDatabricksTables, DatabricksConfig } from '@/app/api/databricks/client';
import { useLocalStorage } from '@/hooks/use-local-storage'; // Assuming useLocalStorage is compatible
import { toast } from 'sonner';

interface SalesforceField {
  name: string;
  label: string;
  type: string;
}

interface DatabricksTable {
  name: string;
  columns: { name: string; type_text: string }[];
}

interface MappingRow extends SalesforceField {
  id: string;
  selectedDbTable: string | null;
  selectedDbColumn: string | null;
}

const PREDEFINED_SF_FIELDS: SalesforceField[] = [
  { name: 'Company_Registration_No__c', label: 'Company Registration Number', type: 'string' },
  { name: 'Name', label: 'Account Name', type: 'string' },
  { name: 'BillingStreet', label: 'Billing Street', type: 'string' },
  { name: 'BillingCity', label: 'Billing City', type: 'string' },
  { name: 'BillingState', label: 'Billing State', type: 'string' },
  { name: 'Phone', label: 'Phone', type: 'phone' },
  { name: 'Website', label: 'Website', type: 'url' }
];

const initialDatabricksConfig: DatabricksConfig = {
    apiUrl: "",
    accessToken: "",
    catalogName: "",
    schemaName: "",
    warehouseId: "",
    tableName: "",
};


export default function ConfigureFieldMappingPage() {
  const router = useRouter();
  const [databricksTables, setDatabricksTables] = useState<DatabricksTable[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [fieldMappings, setFieldMappings] = useState<MappingRow[]>([]);
  const [databricksConfig, setDatabricksConfig] = useLocalStorage<DatabricksConfig>('databricksConfig', initialDatabricksConfig);

  useEffect(() => {
    const initialMappings = PREDEFINED_SF_FIELDS.map(sfField => ({
      ...sfField,
      id: sfField.name,
      selectedDbTable: null,
      selectedDbColumn: null,
    }));
    setFieldMappings(initialMappings);

    async function loadDatabricksData() {
      setIsLoading(true);
      try {
        const tables = await apiFetchDatabricksTables();
        if (tables && tables.length > 0) {
          setDatabricksTables(tables);
        } else {
          setDatabricksTables([]);
          toast.warning("No Databricks tables found or failed to load.", { description: "Please check your Databricks connection and configuration."});
        }
      } catch (error) {
        console.error("Failed to fetch Databricks tables:", error);
        toast.error("Failed to load Databricks tables.", { description: error instanceof Error ? error.message : "Unknown error" });
        setDatabricksTables([]);
      } finally {
        setIsLoading(false);
      }
    }
    loadDatabricksData();
  }, []);

  const handleDbTableSelect = useCallback((sfFieldId: string, dbTableName: string | null) => {
    setFieldMappings(prevMappings =>
      prevMappings.map(m =>
        m.id === sfFieldId
          ? { ...m, selectedDbTable: dbTableName, selectedDbColumn: null }
          : m
      )
    );
  }, []);

  const handleDbColumnSelect = useCallback((sfFieldId: string, dbColumnName: string | null) => {
    setFieldMappings(prevMappings =>
      prevMappings.map(m =>
        m.id === sfFieldId ? { ...m, selectedDbColumn: dbColumnName } : m
      )
    );
  }, []);

  const getColumnsForTable = useCallback((tableName: string | null): { name: string; type_text: string }[] => {
    if (!tableName) return [];
    const table = databricksTables.find(t => t.name === tableName);
    return table ? table.columns : [];
  }, [databricksTables]);

  const handleSaveMappings = useCallback(() => {
    const dbToSfMap: Record<string, string> = {};
    let lastSelectedTable: string | null = null;

    fieldMappings.forEach(m => {
      if (m.selectedDbTable && m.selectedDbColumn) {
        dbToSfMap[m.selectedDbColumn] = m.name;
        lastSelectedTable = m.selectedDbTable;
      }
    });

    if (Object.keys(dbToSfMap).length === 0) {
      toast.info("No fields mapped.", { description: "Please map at least one field to continue." });
      return;
    }

    localStorage.setItem('fieldMapping', JSON.stringify(dbToSfMap));

    if (lastSelectedTable) {
        const currentDbConfigString = localStorage.getItem('databricksConfig');
        let currentDbConfig = {};
        if (currentDbConfigString) {
            try {
                currentDbConfig = JSON.parse(currentDbConfigString);
            } catch (e) {
                console.error("Error parsing databricksConfig from localStorage", e);
            }
        }
        setDatabricksConfig({
             ...(currentDbConfig as DatabricksConfig), // Cast to ensure type compatibility
             tableName: lastSelectedTable
        });
    }

    toast.success("Field mappings saved successfully!");
    router.push('/matching');
  }, [fieldMappings, router, setDatabricksConfig]);

  const canSave = useMemo(() => {
    return fieldMappings.some(m => m.selectedDbTable && m.selectedDbColumn);
  }, [fieldMappings]);

  if (isLoading && databricksTables.length === 0) {
    return (
      <div className="flex justify-center items-center h-screen">
        <Loader2 className="h-8 w-8 animate-spin" />
        <p className="ml-2">Loading Databricks schema...</p>
      </div>
    );
  }

  return (
    <div className="container mx-auto p-4 space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-semibold">Configure Field Mapping</h1>
        <Button onClick={() => router.back()} variant="outline">
          Back
        </Button>
      </div>
      
      <p className="text-sm text-muted-foreground">
        Map predefined Salesforce fields to your Databricks tables and columns. The last selected table will be saved as the primary Databricks table.
      </p>

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[30%]">Salesforce Field</TableHead>
            <TableHead className="w-[35%]">Databricks Source Table</TableHead>
            <TableHead className="w-[35%]">Databricks Source Column</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {fieldMappings.map((mappingRow) => {
            const availableColumns = getColumnsForTable(mappingRow.selectedDbTable);
            return (
              <TableRow key={mappingRow.id}>
                <TableCell>
                  <div className="font-medium">{mappingRow.label}</div>
                  <div className="text-xs text-muted-foreground">{mappingRow.name} ({mappingRow.type})</div>
                </TableCell>
                <TableCell>
                  <DbTableSelector
                    value={mappingRow.selectedDbTable}
                    onSelect={(tableName) => handleDbTableSelect(mappingRow.id, tableName)}
                    tables={databricksTables}
                    isLoading={isLoading && databricksTables.length === 0}
                  />
                </TableCell>
                <TableCell>
                  <DbColumnSelector
                    value={mappingRow.selectedDbColumn}
                    onSelect={(columnName) => handleDbColumnSelect(mappingRow.id, columnName)}
                    columns={availableColumns}
                    disabled={!mappingRow.selectedDbTable || availableColumns.length === 0}
                    isLoadingTable={isLoading && !mappingRow.selectedDbTable && databricksTables.length > 0}
                  />
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>

      <div className="flex justify-end pt-4">
        <Button onClick={handleSaveMappings} disabled={!canSave || (isLoading && databricksTables.length === 0)}>
          {(isLoading && databricksTables.length === 0) ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Save Mappings & Continue
        </Button>
      </div>
    </div>
  );
}

interface DbSelectorProps {
  value: string | null;
  onSelect: (value: string | null) => void;
  disabled?: boolean;
}

interface DbTableSelectorProps extends DbSelectorProps {
  tables: DatabricksTable[];
  isLoading?: boolean;
}

function DbTableSelector({ value, onSelect, tables, disabled, isLoading }: DbTableSelectorProps) {
  const [open, setOpen] = useState(false);
  
  let buttonText = "Select table...";
  if (isLoading) {
    buttonText = "Loading tables...";
  } else if (tables.length === 0) {
    buttonText = "No tables found";
  } else if (value) {
    buttonText = tables.find(t => t.name === value)?.name || "Select table...";
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between"
          disabled={disabled || isLoading || tables.length === 0}
        >
          {buttonText}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[--radix-popover-trigger-width] p-0">
        <Command>
          <CommandInput placeholder="Search table..." />
          <CommandList>
            <CommandEmpty>No table found.</CommandEmpty>
            <CommandGroup>
              {tables.map((table) => (
                <CommandItem
                  key={table.name}
                  value={table.name}
                  onSelect={(currentValue) => {
                    onSelect(currentValue === value ? null : currentValue);
                    setOpen(false);
                  }}
                >
                  <Check
                    className={cn("mr-2 h-4 w-4", value === table.name ? "opacity-100" : "opacity-0")}
                  />
                  {table.name}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

interface DbColumnSelectorProps extends DbSelectorProps {
  columns: { name: string; type_text: string }[];
  isLoadingTable?: boolean;
}

function DbColumnSelector({ value, onSelect, columns, disabled, isLoadingTable }: DbColumnSelectorProps) {
  const [open, setOpen] = useState(false);

  let buttonText = "Select column...";
  if (disabled && isLoadingTable) {
      buttonText = "Select a table first";
  } else if (disabled && columns.length === 0 && !isLoadingTable) {
      buttonText = "No columns available";
  } else if (value) {
      buttonText = columns.find(c => c.name === value)?.name || "Select column...";
  }


  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          role="combobox"
          aria-expanded={open}
          className="w-full justify-between"
          disabled={disabled}
        >
          {buttonText}
          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-[--radix-popover-trigger-width] p-0">
        <Command>
          <CommandInput placeholder="Search column..." />
          <CommandList>
            <CommandEmpty>No column found.</CommandEmpty>
            <CommandGroup>
              {columns.map((column) => (
                <CommandItem
                  key={column.name}
                  value={column.name}
                  onSelect={(currentValue) => {
                    onSelect(currentValue === value ? null : currentValue);
                    setOpen(false);
                  }}
                >
                  <Check
                    className={cn("mr-2 h-4 w-4", value === column.name ? "opacity-100" : "opacity-0")}
                  />
                  {column.name} ({column.type_text})
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}