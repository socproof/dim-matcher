import { NextRequest, NextResponse } from 'next/server';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { apiUrl, accessToken, warehouseId, tablePath, limit, offset = 0 } = body;

    if (!apiUrl || !accessToken || !warehouseId || !tablePath) {
      return NextResponse.json(
        { error: 'Missing required parameters' },
        { status: 400 }
      );
    }

    // Определяем список полей в зависимости от таблицы
    let fieldsList: string;

    if (tablePath.includes('account_vw')) {
      // Source table
      fieldsList = 'Name, BillingStreet, BillingCity, BillingPostalCode, BillingCountry, Phone, Website';
    } else if (tablePath.includes('sl_accounts')) {
      // Dimensions table - добавили cu_email!
      fieldsList = 'cucode, cuname, cuaddress, cupostcode, cu_country, cu_address_user1, cuphone, cu_email';
    } else if (tablePath.includes('salesforce.account')) {
      // Salesforce table
      fieldsList = 'AccountNumber, Name, BillingStreet, BillingCity, BillingPostalCode, BillingCountry, Phone, Website';
    } else {
      fieldsList = '*';
    }

    console.log('[Databricks API] Table:', tablePath);
    console.log('[Databricks API] Field list:', fieldsList);

    const query = `SELECT ${fieldsList} FROM ${tablePath}${limit ? ` LIMIT ${limit}` : ''}${offset ? ` OFFSET ${offset}` : ''}`;

    console.log('[Databricks API] Executing query:', query);

    const response = await fetch(`${apiUrl}/api/2.0/sql/statements/`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        warehouse_id: warehouseId,
        statement: query,
        wait_timeout: '50s',
      }),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('[Databricks API] Error response:', errorText);
      return NextResponse.json(
        { error: `Databricks API error: ${response.status}`, details: errorText },
        { status: response.status }
      );
    }

    const data = await response.json();

    if (data.status?.state === 'FAILED') {
      console.error('[Databricks API] Query failed:', data.status?.error);
      return NextResponse.json(
        { error: 'Query failed', details: data.status?.error, query },
        { status: 400 }
      );
    }

    if (!data.result?.data_array) {
      console.warn('[Databricks API] No data returned');
      return NextResponse.json([]);
    }

    const columns = data.manifest?.schema?.columns || [];
    const rows = data.result.data_array;

    const accounts = rows.map((row: any[]) => {
      const account: any = {};
      columns.forEach((col: any, index: number) => {
        account[col.name] = row[index];
      });
      return account;
    });

    console.log('[Databricks API] Successfully fetched', accounts.length, 'accounts');

    return NextResponse.json(accounts);

  } catch (error) {
    console.error('[Databricks API] Exception:', error);
    return NextResponse.json(
      { error: 'Internal server error', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}