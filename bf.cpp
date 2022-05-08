#include <iostream>
#include <string>
#include <vector>
#include "bf.h"
#include "MurmurHash3.h"

BloomFilter::BloomFilter()
{
    table.reset();
}

BloomFilter::~BloomFilter()
{
    table.reset();
}

bool BloomFilter::is_inbf(const uint64_t &key)
{
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    bool retval = table[hash[0]%bf_length]&&table[hash[1]%bf_length]&&table[hash[2]%bf_length]&&table[hash[3]%bf_length];
    return retval;
}

void BloomFilter::add(const uint64_t &key){
    uint32_t hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
    table.set(hash[0]%bf_length);
    table.set(hash[1]%bf_length);
    table.set(hash[2]%bf_length);
    table.set(hash[3]%bf_length);
}
