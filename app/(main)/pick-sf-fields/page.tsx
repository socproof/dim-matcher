"use client";
import { useEffect, useMemo, useState, useCallback } from 'react';
import { Button } from '@/components/ui/button';
import { useRouter } from 'next/navigation';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from '@/components/ui/command';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Check, ChevronsUpDown } from 'lucide-react';
import { cn } from '@/lib/utils';

type SFField = {
  name: string;
  label: string;
  type: string;
};

type Mapping = Record<string, string>;

// Predefined list of Salesforce fields
const PREDEFINED_SF_FIELDS: SFField[] = [
  { name: 'Company_Registration_No__c', label: 'Company Registration Number', type: 'string' },
  { name: 'Name', label: 'Account Name', type: 'string' },
  { name: 'BillingStreet', label: 'Billing Street', type: 'string' },
  { name: 'BillingCity', label: 'Billing City', type: 'string' },
  { name: 'BillingState', label: 'Billing State', type: 'string' },
  { name: 'Phone', label: 'Phone', type: 'phone' },
  { name: 'Website', label: 'Website', type: 'url' }
];

export default function PickSFFieldsPage() {
  const [dbColumns, setDbColumns] = useState<string[]>([]);
  const [mapping, setMapping] = useState<Mapping>({});
  const router = useRouter();

  const memoizedDbColumns = useMemo(() => dbColumns, [dbColumns]);

  const handleMappingChange = useCallback((dbColumn: string, sfField: string) => {
    setMapping(prev => ({ ...prev, [dbColumn]: sfField }));
  }, []);

  const saveMapping = useCallback(() => {
    localStorage.setItem('fieldMapping', JSON.stringify(mapping));
    router.push('/matching');
  }, [mapping, router]);

  const FieldSelector = useCallback(({ column }: { column: string }) => {
    const [open, setOpen] = useState(false);

    return (
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button
            variant="outline"
            className="w-full justify-between"
          >
            {mapping[column] || "Select field"}
            <ChevronsUpDown className="ml-2 h-4 w-4" />
          </Button>
        </PopoverTrigger>
        <PopoverContent className="p-0">
          <Command>
            <CommandInput placeholder="Search fields..." />
            <CommandList>
              <CommandEmpty>No fields found.</CommandEmpty>
              <CommandGroup>
                {PREDEFINED_SF_FIELDS.map(field => (
                  <CommandItem
                    key={field.name}
                    value={field.name}
                    onSelect={() => {
                      handleMappingChange(column, field.name);
                      setOpen(false);
                    }}
                  >
                    <Check
                      className={cn(
                        "mr-2 h-4 w-4",
                        mapping[column] === field.name ? "opacity-100" : "opacity-0"
                      )}
                    />
                    {field.label} ({field.type})
                  </CommandItem>
                ))}
              </CommandGroup>
            </CommandList>
          </Command>
        </PopoverContent>
      </Popover>
    );
  }, [mapping, handleMappingChange]);

  useEffect(() => {
    const columns = JSON.parse(localStorage.getItem('databricksColumns') || '[]');
    setDbColumns(columns);
  }, []);

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-semibold">Field Mapping</h2>
      
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Databricks Column</TableHead>
            <TableHead>Salesforce Field</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {memoizedDbColumns.map(column => (
            <TableRow key={column}>
              <TableCell>{column}</TableCell>
              <TableCell>
                <FieldSelector column={column} />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>

      <Button onClick={saveMapping}>
        Continue to Matching
      </Button>
    </div>
  );
}