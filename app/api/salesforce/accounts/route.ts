import { NextResponse } from 'next/server';
import jsforce from 'jsforce';

export async function POST(request: Request) {
  const { username, password, securityToken, loginUrl, fields } = await request.json();
  
  try {
    const conn = new jsforce.Connection({ loginUrl });
    await conn.login(username, password + securityToken);

    const query = `SELECT ${fields.join(', ')} FROM Account ORDER BY Name`;
    const result = await conn.query(query);
    
    let allRecords = [...result.records];
    let nextRecordsUrl = result.nextRecordsUrl;
    let iterationCount = 1;
    const maxIterations = 5;

    while (nextRecordsUrl && iterationCount < maxIterations) {
      const moreResult = await conn.queryMore(nextRecordsUrl);
      allRecords = [...allRecords, ...moreResult.records];
      nextRecordsUrl = moreResult.nextRecordsUrl;
      iterationCount++;
      
      if (moreResult.done) break;
    }

    return NextResponse.json(allRecords);

  } catch (error: any) {
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    );
  }
}