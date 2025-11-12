import { useState, useEffect } from 'react';

const STORAGE_KEY = 'clickhouse-server-url';
const DEFAULT_URL = 'https://node01-8123.containerman.me/';

export function useClickhouseUrl() {
  const [url, setUrl] = useState<string>(() => {
    const saved = localStorage.getItem(STORAGE_KEY);
    return saved || DEFAULT_URL;
  });

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, url);
  }, [url]);

  return { url, setUrl };
}

