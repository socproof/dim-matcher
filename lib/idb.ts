// lib\idb.ts

const DB_NAME = "AccountMatcher";
const DB_VERSION = 4;

export const initDB = async () => {
  return new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    
    request.onupgradeneeded = (event) => {
      const db = (event.target as IDBOpenDBRequest).result;
      
      if (db.objectStoreNames.contains('databricksAccounts')) {
        db.deleteObjectStore('databricksAccounts');
      }
      if (db.objectStoreNames.contains('salesforceAccounts')) {
        db.deleteObjectStore('salesforceAccounts');
      }
      
      db.createObjectStore('databricksAccounts', { autoIncrement: true });
      db.createObjectStore('salesforceAccounts', { autoIncrement: true });
    };

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
};

export const saveAccounts = async (storeName: 'databricksAccounts' | 'salesforceAccounts', data: any[]) => {
  const db = await initDB();
  const tx = db.transaction(storeName, 'readwrite');
  const store = tx.objectStore(storeName);
  
  await new Promise((resolve) => {
    const clearRequest = store.clear();
    clearRequest.onsuccess = resolve;
  });
  
  const promises = data.map(item => 
    new Promise((resolve) => {
      const request = store.add(item);
      request.onsuccess = resolve;
    })
  );
  
  await Promise.all(promises);
  return new Promise((resolve) => tx.oncomplete = () => resolve(null));
};

export const getAccounts = async (storeName: 'databricksAccounts' | 'salesforceAccounts') => {
  const db = await initDB();
  const tx = db.transaction(storeName, 'readonly');
  const store = tx.objectStore(storeName);
  
  return new Promise<any[]>((resolve) => {
    const request = store.getAll();
    request.onsuccess = () => resolve(request.result || []);
  });
};

export const getAccountsPaginated = async (
  storeName: string,
  page: number,
  pageSize: number
) => {
  const db = await initDB();
  return new Promise<any[]>((resolve, reject) => {
    const tx = db.transaction(storeName, 'readonly');
    const store = tx.objectStore(storeName);
    const request = store.openCursor();
    const results: any[] = [];
    let advanced = false;
    let counter = 0;
    const startIndex = (page - 1) * pageSize;

    request.onsuccess = (event) => {
      const cursor = (event.target as IDBRequest<IDBCursorWithValue>).result;
      
      if (!cursor) {
        resolve(results);
        return;
      }

      if (!advanced && startIndex > 0) {
        advanced = true;
        cursor.advance(startIndex);
        return;
      }

      if (counter < pageSize) {
        results.push(cursor.value);
        counter++;
        cursor.continue();
      } else {
        resolve(results);
      }
    };

    request.onerror = () => {
      reject(request.error);
    };
  });
};

export const getTotalCount = async (storeName: string) => {
  const db = await initDB();
  return new Promise<number>((resolve) => {
    const tx = db.transaction(storeName, 'readonly');
    const store = tx.objectStore(storeName);
    const request = store.count();
    
    request.onsuccess = () => resolve(request.result);
  });
};