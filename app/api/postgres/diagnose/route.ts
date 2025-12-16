import { NextRequest, NextResponse } from 'next/server';
import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

export async function POST(request: NextRequest) {
  const client = await pool.connect();
  
  try {
    const { testName, testSource } = await request.json();
    
    // Set threshold
    await client.query("SET pg_trgm.similarity_threshold = 0.3");
    
    const diagnostics: any = {};
    
    // Count by source
    const counts = await client.query(`
      SELECT source, COUNT(*) as count FROM accounts GROUP BY source
    `);
    diagnostics.counts = counts.rows;
    
    // Test trigram search with EXPLAIN ANALYZE
    if (testName) {
      const explainResult = await client.query(`
        EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
        SELECT id, name, normalized_name
        FROM accounts 
        WHERE source = $1 AND normalized_name % $2
        ORDER BY normalized_name <-> $2
        LIMIT 20
      `, [testSource || 'salesforce', testName.toLowerCase()]);
      
      diagnostics.explain = explainResult.rows[0];
      
      // Actual query time
      const start = Date.now();
      const searchResult = await client.query(`
        SELECT id, name, normalized_name, similarity(normalized_name, $2) as sim
        FROM accounts 
        WHERE source = $1 AND normalized_name % $2
        ORDER BY normalized_name <-> $2
        LIMIT 20
      `, [testSource || 'salesforce', testName.toLowerCase()]);
      
      diagnostics.queryTimeMs = Date.now() - start;
      diagnostics.resultsCount = searchResult.rows.length;
      diagnostics.sampleResults = searchResult.rows.slice(0, 5);
    }
    
    // Check indexes
    const indexes = await client.query(`
      SELECT indexname, indexdef 
      FROM pg_indexes 
      WHERE tablename = 'accounts'
    `);
    diagnostics.indexes = indexes.rows;
    
    return NextResponse.json(diagnostics);
  } catch (error) {
    return NextResponse.json({ error: String(error) }, { status: 500 });
  } finally {
    client.release();
  }
}