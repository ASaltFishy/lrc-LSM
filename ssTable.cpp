#include <iostream>
#include "ssTable.h"

using namespace std;

// sstable生成方式一：从满了或者系统关闭时的memtable转化 或者从合并操作中得来
SSTable::SSTable(list<pair<uint64_t, string>> &list)
{
    level = 0;
    DATA = NULL;
    uint64_t size = list.size();
    header.minkey = list.front().first;
    header.maxkey = list.back().first;
    header.kvnumber = size;
    extern int time_stamp;
    time_stamp++;
    header.timeStamp = time_stamp;

    uint32_t temp = 10272 + size * 12; //第一个offset保存的位置 10272 = 10KB+32B
    for (auto i = list.begin();i!=list.end(); i++)
    {
        uint64_t key = i->first;
        string val = i->second;
        bf.add(key);
        Index tempindex(key, temp);
        temp += val.length();
        index.push_back(tempindex);
    }
}

// sstable生成方式二：从data路径中二进制文件读出(mkFile)的逆向操作
SSTable::SSTable(string &path, int &stamp,int _level)
{
    level = _level;
    DATA = NULL;
    ifstream file;
    file.open(path, ios::in | ios::binary);
    file.read((char *)&header, 32);
    file.read((char *)&bf, 10240);
    int size = header.kvnumber;
    stamp = header.timeStamp;
    for (int i = 0; i < size; i++)
    {
        Index temp;
        file.read((char *)&temp.first, 8);
        file.read((char *)&temp.second, 4);
        index.push_back(temp);
    }
    file.close();
}

SSTable::SSTable(){
    DATA = NULL;
    header.timeStamp = 0;
    header.kvnumber = 0;
    header.minkey = 0;
    header.maxkey = 0;
}

SSTable::~SSTable()
{
    if(!DATA)
        delete DATA;
    DATA = NULL;
}

//将sstable类型转换为二进制文件写入
void SSTable::mkFile(string &path, string &dirpath,list<pair<uint64_t, std::string>> &list)
{
    mkdir(dirpath.c_str());
    path = dirpath + "/" + path;
    ofstream file;
    file.open(path, ios::out | ios::binary);
    file.write((const char *)&header, 32);
    file.write((const char *)&bf, 10240);
    for (int i = 0; i < index.size(); i++)
    {
        file.write((const char *)&index[i].first, 8);
        file.write((const char *)&index[i].second, 4);
    }
    for (auto i = list.begin();i!=list.end(); i++)
    {
        string val = i->second;
        file.write(val.data(), val.length());
    }
    file.close();
}

//得到硬盘中值的同时返回最晚时间戳
string SSTable::getval(uint64_t key,int level,uint64_t &t)
{
    string emptys = "";
    if (key < header.minkey || key > header.maxkey)
        return emptys;
    if (!bf.is_inbf(key))
        return emptys;
    int size = index.size();
    int low=0,high=size-1,mid=0;
    uint32_t offset = 0;
    uint32_t endoffset = 0;
    while (low <= high)
    {
        mid = (low + high) / 2;
        if (index[mid].first == key)
        {
            offset = index[mid].second;
            if (mid != size-1)
                endoffset = index[mid + 1].second;
            break;
        }
        if (index[mid].first < key)
        {
            low = mid + 1;
        }
        else
            high = mid - 1;
    }
    if (offset == 0)
        return emptys; //没有找到
    string path = "./data/level-"+to_string(level)+"/" + to_string(header.timeStamp) +"-"+to_string(header.kvnumber)+ ".sst";
    ifstream file;
    file.open(path, ios::in | ios::binary);
    if (endoffset==0)
    {
        file.seekg(0, ios::end);
        endoffset = file.tellg();
    }
    size = endoffset - offset;
    file.seekg(offset, ios::beg);
    char val[size+1];
    memset(val,'\0',size+1);
    file.read(val, size);
    file.close();
    t = header.timeStamp;
    return val;
}

bool SSTable::is_in(uint64_t key)
{
    return bf.is_inbf(key);
}

void SSTable::getscale(Range &scale)
{
    scale.first = header.minkey;
    scale.second = header.maxkey;
}

//将ssTable中对应的KV值全部找出并返回vector，用于合并重新划分
void SSTable::addData()
{
    if(DATA==NULL)
    DATA = new list<KV>;
    uint32_t offset = 0;
    uint32_t endoffset = 0;
    uint32_t size = 0;
    vector<Index>::iterator it;

    string path = "./data/level-" + to_string(level) + "/" + to_string(header.timeStamp)+"-"+to_string(header.kvnumber) + ".sst";
    ifstream file;
    file.open(path, ios::in | ios::binary);
    //依次取出kv对
    for (it = index.begin(); it != index.end() - 1; it++)
    {
        offset = it->second;
        endoffset = (it + 1)->second;
        file.seekg(offset, ios::beg);
        size = endoffset - offset;
        char _val[size];
        memset(_val,'\0',size+1);
        file.read(_val, size);
        KV kvtemp(it->first, _val);
        DATA->push_back(kvtemp);
    }
    //单独处理最后一个值
    file.seekg(0, ios::end);
    endoffset = file.tellg();
    offset = it->second;
    size = endoffset - offset;
//    string val(size,'\0');
    char _val[size];
    memset(_val,'\0',size+1);
    file.seekg(offset, ios::beg);
    file.read(_val, size);
    KV temp(it->first, _val);
    DATA->push_back(temp);

    file.close();
}

bool SSTable::addScanData(uint64_t key1, uint64_t key2)
{
    if(header.maxkey<key1||header.minkey>key2)return false;

    if(DATA == NULL)
    DATA = new std::list<KV>;
    string path = "./data/level-" + to_string(level) + "/" + to_string(header.timeStamp)+"-"+to_string(header.kvnumber) + ".sst";
    ifstream file;
    file.open(path, ios::in | ios::binary);
    uint32_t offset = 0;
    uint32_t endoffset = 0;
    uint32_t size = 0;

    for(auto it=index.begin();it!=index.end();it++){
        if(it->first>key2)break;
        if(it->first>=key1 && (it+1)!=index.end()){
            offset = it->second;
            endoffset = (it + 1)->second;
            file.seekg(offset, ios::beg);
            size = endoffset - offset;
            char _val[size];
            memset(_val,'\0',size+1);
            file.read(_val, size);
            KV temp(it->first, _val);
            DATA->push_back(temp);
        }
        if((it+1)==index.end()){
            file.seekg(0, ios::end);
            endoffset = file.tellg();
            offset = it->second;
            size = endoffset - offset;
            char _val[size];
            memset(_val,'\0',size+1);
            file.seekg(offset, ios::beg);
            file.read(_val, size);
            KV temp(it->first, _val);
            DATA->push_back(temp);
        }
    }
    file.close();
    return true;
}
