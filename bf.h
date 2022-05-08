#ifndef BF_H
#define BF_H

#include <iostream>
#include <bitset>
#define bf_length 81920

using namespace std;

class BloomFilter{
private:
    bitset<bf_length> table;

public:
    BloomFilter();
    ~BloomFilter();
    bool is_inbf(const uint64_t &key);
    void add(const uint64_t &key);
};

#endif