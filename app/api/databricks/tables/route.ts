// app/api/databricks/tables/route.ts

import { NextResponse } from 'next/server';

export async function HEAD(request: Request) {
  const { searchParams } = new URL(request.url);
  
  try {
    const apiUrl = searchParams.get('apiUrl');
    const accessToken = searchParams.get('accessToken');
    const catalogName = searchParams.get('catalogName') || 'main';
    const schemaName = searchParams.get('schemaName') || 'default';

    if (!apiUrl || !accessToken) {
      return new NextResponse(null, { status: 400 });
    }

    const testUrl = new URL(`${apiUrl.replace(/\/$/, '')}/api/2.1/unity-catalog/tables`);
    testUrl.searchParams.set('catalog_name', catalogName);
    testUrl.searchParams.set('schema_name', schemaName);
    testUrl.searchParams.set('max_results', '1');

    const res = await fetch(testUrl.toString(), {
      method: 'GET',
      headers: {
        'Authorization': `Bearer ${accessToken}`
      }
    });

    return new NextResponse(null, {
      status: res.ok ? 200 : 401,
    });

  } catch (error: any) {
    return new NextResponse(null, { 
      status: 500,
      headers: { 'X-Error': error.message } 
    });
  }
}

export async function POST(request: Request) {
  const { apiUrl, accessToken, catalogName, schemaName } = await request.json();

  try {
    const url = new URL(`${apiUrl.replace(/\/$/, '')}/api/2.1/unity-catalog/tables`);
    url.searchParams.set('catalog_name', catalogName || 'main');
    url.searchParams.set('schema_name', schemaName || 'default');

    const res = await fetch(url.toString(), {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json'
      }
    });

    if (!res.ok) {
      const error = await res.text();
      throw new Error(error || 'Failed to fetch tables');
    }

    const data = await res.json();
    return NextResponse.json(data.tables || []);

  } catch (error: any) {
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    );
  }
}