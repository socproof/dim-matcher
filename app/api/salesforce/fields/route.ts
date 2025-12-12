// app/api/salesforce/fields/route.ts
import { NextResponse } from 'next/server';
import jsforce from 'jsforce';

export async function POST(request: Request) {
  try {
    const { username, password, securityToken, loginUrl } = await request.json();
    
    if (!username || !password || !loginUrl) {
      return NextResponse.json(
        { error: 'Missing required parameters' },
        { status: 400 }
      );
    }

    const conn = new jsforce.Connection({ loginUrl });
    await conn.login(username, password + securityToken);

    const result = await conn.describe('Account');
    const fields = result.fields.map((f: any) => ({
      name: f.name,
      label: f.label,
      type: f.type
    }));

    return NextResponse.json({ fields });
    
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Failed to fetch fields' },
      { status: 500 }
    );
  }
}