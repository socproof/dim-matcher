// app/api/postgres/route.ts

import { NextRequest, NextResponse } from 'next/server';
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

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

    const client = await pool.connect();
    
    try {
      const result = await client.query(sql, params || []);
      
      return NextResponse.json({
        rows: result.rows,
        rowCount: result.rowCount,
      });
    } finally {
      client.release();
    }

  } catch (error) {
    console.error('[Postgres] Query error:', error);
    return NextResponse.json(
      { 
        error: 'Database query failed',
        details: error instanceof Error ? error.message : 'Unknown error'
      },
      { status: 500 }
    );
  }
}