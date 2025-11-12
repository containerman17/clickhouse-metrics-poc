import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import { useMemo } from 'react';
import { useClickhouseUrl } from './useClickhouseUrl';

export interface ChainSyncData {
  chain_id: number;
  name: string;
  last_updated: number;
  last_block_on_chain: number;
  watermark_block: number | null;
  syncPercentage: number;
  blocksBehind: number | null;
}

interface ChainSyncRawData {
  chain_id: number;
  name: string;
  last_updated: number;
  last_block_on_chain: number;
  watermark_block: number | null;
}

export function useSyncStatus() {
  const { url } = useClickhouseUrl();

  const clickhouse = useMemo(() => createClient({
    url,
    username: "anonymous",
  }), [url]);

  const { data, isLoading, error, refetch, isFetching } = useQuery<ChainSyncData[]>({
    queryKey: ['syncStatus', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT 
            chain_status.chain_id,
            chain_status.name,
            toUnixTimestamp(chain_status.last_updated) as last_updated,
            chain_status.last_block_on_chain,
            sw.block_number as watermark_block
          FROM chain_status FINAL
          LEFT JOIN sync_watermark sw ON chain_status.chain_id = sw.chain_id
          ORDER BY chain_status.chain_id
        `,
        format: 'JSONEachRow',
      });
      const rawData = await result.json<ChainSyncRawData>();
      
      // Calculate sync percentage and blocks behind
      return (rawData as ChainSyncRawData[]).map(chain => {
        const blocksBehind = chain.watermark_block !== null
          ? chain.last_block_on_chain - chain.watermark_block
          : null;
        const syncPercentage = chain.watermark_block !== null
          ? (chain.watermark_block / chain.last_block_on_chain) * 100
          : 0;
        
        return {
          ...chain,
          syncPercentage,
          blocksBehind,
        };
      });
    },
    refetchInterval: 60000,
  });

  return {
    chains: data,
    isLoading,
    error,
    refetch,
    isFetching,
  };
}

