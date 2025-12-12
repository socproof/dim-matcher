import { NextResponse } from 'next/server';
import jsforce from 'jsforce';

export async function POST(request: Request) {
  const { username, password, securityToken, loginUrl } = await request.json();

  try {
    const conn = new jsforce.Connection({
      loginUrl,
    });

    await conn.login(username, password + securityToken);
    
    const result = await conn.query('SELECT Id FROM Account LIMIT 1');
    
    return NextResponse.json({
      success: true,
      result
    });

  } catch (error: any) {
    return NextResponse.json(
      { error: error.message },
      { status: 401 }
    );
  }
}