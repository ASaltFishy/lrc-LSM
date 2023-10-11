# lrc-LSM
KV store based on LSM Tree ,course project of SJTU SE2322 *Advanced Data Structures*, 2021 Spring.

## features
- MemTable in DRAM
  Implemented using SKipTable
- SSTable on disk
  ![image](https://github.com/ASaltFishy/lrc-LSM/assets/100567593/daa323c4-22d1-4e78-92c5-7e16ae4311f9)
  Header is used for metadata including timestamp, number of KV, min key and max key.
  Bloom Filter is used for quickly determine whether a certain key value exists in SSTable.
- Compaction
