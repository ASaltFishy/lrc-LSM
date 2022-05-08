//保存在内存中的缓冲子类

#ifndef SSTABLE_H
#define SSTABLE_H

#include <iostream>
#include <string>
#include <utility>
#include <list>
#include <vector>
#include <fstream>

#include "bf.h"
#include "utils.h"

typedef pair<uint64_t, uint32_t> Index;
typedef pair<uint64_t, uint64_t> Range;
// typedef std::pair<uint64_t,string> KV;
struct KV{
	uint64_t key;
	string val;
	int ts;

    KV(uint64_t _key,string _val,int _ts):key(_key),val(_val),ts(_ts){}
    KV(){}
};

using namespace std;

struct HEADER
    {
    public:
        uint64_t timeStamp;
        uint64_t kvnumber;
        uint64_t minkey;
        uint64_t maxkey;
    };

class SSTable
{
private:
    BloomFilter bf;
    vector<Index> index;
    // vector<string> data; 
    //ssatble类相当于buffer的原子组成部分 不需要再保存data，data直接由memtable写到磁盘里去了

public:
    HEADER header{};
    SSTable(list<pair<uint64_t, string>> &list);
    SSTable(string &filepath,int &stamp);
    ~SSTable();
    void mkFile(string &path,string &dirpath,list<pair<uint64_t, std::string>> &list);

    //工具函数
    string getval(uint64_t key,int level,uint64_t &t);
    bool is_in(uint64_t key);
    void getscale(Range &scale);
    void toKV(vector<KV> *tobecompact,int level,int stamp);
};

#endif