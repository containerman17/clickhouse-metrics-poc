- [Done] List all chains associated with a given address
    Incremental into a table
- List latest blocks
    Just a simple max over all
- Get block
    oooookay, but kv would be faster
- List latest blocks across all supported EVM chains
    Just a simple max over all
- Get deployment transaction
    Has to be incremental KV
- List deployed contracts (by chainId and address)
    Separate table ordered by chainid, address, contractid (questionable) 
- List ERC transfers (by chainid and address)
    Table ordered by chainid, address, blocknumber, 
- Lists native transactions for an address. Filterable by block range.
- Lists ERC-20 transfers for an address. Filterable by block range.
- Lists ERC-721 transfers for an address. Filterable by block range.
- Lists ERC-1155 transfers for an address. Filterable by block range.
- Returns a list of internal transactions for an address and chain. Filterable by block range.
- Gets the details of a single transaction.
- Lists the transactions that occured in a given block.
- Lists the latest transactions. Filterable by status.
- Lists the most recent transactions from all supported EVM-compatible chains. The results can be filtered based on transaction status.

Balances
- Get native token balance at block number (currency would be a nice bonus)
- Lists ERC-20 token balances of a wallet address at block paginated
- Lists ERC-721 token balances of a wallet address (only latest, can be filtered by contractAddress)
- List ERC-1155 balances. Balance at a given block can be retrieved with the blockNumber parameter.
- List collectible (ERC-721/ERC-1155) balances. Balance for a specific contract can be retrieved with the contractAddress parameter.


NFTs
- Lists tokens for an NFT contract.
