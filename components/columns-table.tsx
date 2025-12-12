"use client";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Checkbox } from '@/components/ui/checkbox';
import { memo } from 'react';

interface ColumnsTableProps {
  columns: { name: string, type_text: string }[];
  selectedColumns: string[];
  onToggle: (name: string) => void;
}

export const ColumnsTable = memo(({ columns, selectedColumns, onToggle }: ColumnsTableProps) => (
  <Table>
    <TableHeader>
      <TableRow>
        <TableHead>Select</TableHead>
        <TableHead>Column Name</TableHead>
        <TableHead>Type</TableHead>
      </TableRow>
    </TableHeader>
    <TableBody>
      {columns.map(column => (
        <TableRow key={column.name}>
          <TableCell>
            <Checkbox
              checked={selectedColumns.includes(column.name)}
              onCheckedChange={() => onToggle(column.name)}
            />
          </TableCell>
          <TableCell>{column.name}</TableCell>
          <TableCell>{column.type_text}</TableCell>
        </TableRow>
      ))}
    </TableBody>
  </Table>
));