import { NextResponse } from 'next/server';

export async function POST(request: Request) {
    const { apiUrl, accessToken, catalogName, schemaName, tableName, fields, warehouseId, nameMappedField } = await request.json();
    
    try {
      let sql = `SELECT ${fields.join(', ')} FROM ${catalogName}.${schemaName}.${tableName}`;
      
      if(nameMappedField) {
        sql += ` ORDER BY ${nameMappedField}`;
      }

      const response = await fetch(`${apiUrl}/api/2.0/sql/statements`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          statement: sql,
          disposition: 'INLINE',
          wait_timeout: '50s',
          warehouse_id: warehouseId,
        })
      });
  
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Databricks API error');
      }
  
      const result = await response.json();

      if (result.manifest?.schema?.columns && result.result?.data_array) {
        const columns = result.manifest.schema.columns;
        const data = result.result.data_array.map((row: any[]) => {
          return columns.reduce((obj: any, col: any, index: number) => {
            obj[col.name] = row[index];
            return obj;
          }, {});
        });
        
        return NextResponse.json(data);
      }

      return NextResponse.json([]);
  
    } catch (error: any) {
      return NextResponse.json(
        { error: error.message },
        { status: 500 }
      );
    }
  }