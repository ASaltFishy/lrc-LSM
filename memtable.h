#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <iostream>
#include <vector>
#include <climits>
#include <time.h>
#include <string>
#include <list>

#include "ssTable.h"

using namespace std;

#define MAX_LEVEL 13

enum SKNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct SKNode
{
    uint64_t length;
    uint64_t key;
    string val;
    SKNodeType type;
    vector<SKNode *> forwards;
    SKNode(uint64_t _key, string _val, SKNodeType _type)
        : key(_key), val(_val), type(_type)
    {
        for (int i = 0; i < MAX_LEVEL; i++)
        {
            forwards.push_back(nullptr);
        }
        length = 12 + val.size();
    }
    ~SKNode(){
        vector<SKNode *>().swap(forwards);
        //清空vector并释放内存
    }
};

class memtable{

    private:
    SKNode *head;
    SKNode *NIL;
    unsigned long long s = 1;
    double my_rand();
    int randomLevel();


    public:
    uint64_t totallength;
    memtable();
    void Insert(uint64_t key, const string &value);
    string Search(uint64_t key);
    bool Delete(uint64_t key);
    uint64_t Scan(uint64_t key_start, uint64_t key_end);
    void Display();
    void toSSTable(list<pair<uint64_t, std::string>> &list);
    void reset();
    ~memtable();

    
};

#endif