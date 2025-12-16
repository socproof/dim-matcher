// app/api/postgres/route.ts

import { NextRequest, NextResponse } from 'next/server';
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// PostgreSQL has a limit of ~65535 parameters per query
const MAX_PARAMS_PER_QUERY = 60000;
const PARAMS_PER_ROW = 13; // Based on your INSERT statement

// Sanitize string by removing null bytes and other invalid UTF-8 sequences
function sanitizeString(value: any): any {
  if (value === null || value === undefined) {
    return null;
  }
  
  if (typeof value === 'string') {
    // Remove null bytes and other control characters except newlines and tabs
    return value
      .replace(/\u0000/g, '') // Remove null bytes
      .replace(/[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]/g, ''); // Remove other control chars
  }
  
  return value;
}

// Recursively sanitize all string values in params array
function sanitizeParams(params: any[]): any[] {
  return params.map(param => sanitizeString(param));
}

export async function POST(request: NextRequest) {
  try {
    const { sql, params } = await request.json();

    if (!sql) {
      return NextResponse.json(
        { error: 'SQL query is required' },
        { status: 400 }
      );
    }

    console.log('[Postgres] Executing query:', sql.substring(0, 100));
    console.log('[Postgres] Params count:', params?.length || 0);

    const client = await pool.connect();
    
    try {
      await client.query("SET pg_trgm.similarity_threshold = 0.3");

      // If no params or params fit in one query
      if (!params || params.length === 0) {
        const result = await client.query(sql);
        return NextResponse.json({
          rows: result.rows,
          rowCount: result.rowCount,
        });
      }

      // Sanitize all params before processing
      const sanitizedParams = sanitizeParams(params);
      console.log('[Postgres] Sanitized params');

      if (sanitizedParams.length <= MAX_PARAMS_PER_QUERY) {
        const result = await client.query(sql, sanitizedParams);
        return NextResponse.json({
          rows: result.rows,
          rowCount: result.rowCount,
        });
      }

      // Need to split into batches
      console.log('[Postgres] Splitting into batches due to parameter limit');

      const totalRows = sanitizedParams.length / PARAMS_PER_ROW;
      const maxRowsPerBatch = Math.floor(MAX_PARAMS_PER_QUERY / PARAMS_PER_ROW);
      const batches = Math.ceil(totalRows / maxRowsPerBatch);

      console.log('[Postgres] Total rows:', totalRows);
      console.log('[Postgres] Max rows per batch:', maxRowsPerBatch);
      console.log('[Postgres] Number of batches:', batches);

      let totalInserted = 0;

      for (let i = 0; i < batches; i++) {
        const startRow = i * maxRowsPerBatch;
        const endRow = Math.min((i + 1) * maxRowsPerBatch, totalRows);
        const rowsInBatch = endRow - startRow;
        
        const startParam = startRow * PARAMS_PER_ROW;
        const endParam = endRow * PARAMS_PER_ROW;
        const batchParams = sanitizedParams.slice(startParam, endParam);

        // Build VALUES clause for this batch
        const valueClauses: string[] = [];
        for (let row = 0; row < rowsInBatch; row++) {
          const placeholders: string[] = [];
          for (let col = 0; col < PARAMS_PER_ROW; col++) {
            placeholders.push(`$${row * PARAMS_PER_ROW + col + 1}`);
          }
          valueClauses.push(`(${placeholders.join(', ')})`);
        }

        // Extract the INSERT part and ON CONFLICT part from original SQL
        const valuesKeyword = 'VALUES';
        const valuesIndex = sql.toUpperCase().indexOf(valuesKeyword);
        const beforeValues = sql.substring(0, valuesIndex + valuesKeyword.length);
        
        // Find everything after the first VALUES clause (like ON CONFLICT)
        const afterValuesMatch = sql.match(/VALUES\s*\([^)]*\)(.*)/is);
        const afterValues = afterValuesMatch ? afterValuesMatch[1].replace(/,\s*\([^)]*\)/g, '') : '';

        const batchSql = `${beforeValues} ${valueClauses.join(', ')} ${afterValues}`;

        console.log(`[Postgres] Batch ${i + 1}/${batches}: rows ${startRow}-${endRow}, params ${batchParams.length}`);
        
        try {
          const result = await client.query(batchSql, batchParams);
          totalInserted += result.rowCount || 0;
        } catch (batchError) {
          console.error(`[Postgres] Batch ${i + 1} error:`, batchError);
          
          // Log problematic params for debugging
          console.error('[Postgres] Problematic batch params sample:', 
            batchParams.slice(0, 20).map((p, idx) => ({
              index: idx,
              value: p,
              type: typeof p,
              hasNullByte: typeof p === 'string' && p.includes('\u0000')
            }))
          );
          
          throw batchError;
        }
      }

      console.log(`[Postgres] Total inserted: ${totalInserted} rows`);

      return NextResponse.json({
        rows: [],
        rowCount: totalInserted,
        batches: batches,
      });

    } finally {
      client.release();
    }

  } catch (error) {
    console.error('[Postgres] Query error:', error);
    return NextResponse.json(
      { 
        error: 'Database query failed',
        details: error instanceof Error ? error.message : 'Unknown error',
        code: (error as any)?.code,
        position: (error as any)?.position,
        where: (error as any)?.where
      },
      { status: 500 }
    );
  }
}