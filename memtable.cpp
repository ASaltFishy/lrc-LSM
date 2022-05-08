#include "memtable.h"

using namespace std;

memtable::memtable()
    {
        totallength = 0;
        head = new SKNode(0, "\0", SKNodeType::HEAD);
        NIL = new SKNode(UINT64_MAX, "\0", SKNodeType::NIL);
        for (int i = 0; i < MAX_LEVEL; ++i) {
            head->forwards[i] = NIL;
        }
    }

memtable::~memtable()
    {
        extern int time_stamp;
        list<pair<uint64_t, std::string>> list;
        toSSTable(list);
		SSTable newSSTable(list);
        string sspath = to_string(time_stamp) + ".sst";
        string dirpath = "./data/level-0";
		newSSTable.mkFile(sspath,dirpath,list);
        //这里暂时不合并了，下次加载之后随便插入一个数据便可发现问题然后自动合并
        SKNode *n1 = head;
        SKNode *n2;
        while (n1)
        {
            n2 = n1->forwards[0];
            delete n1;
            n1 = n2;
        }
        head = NULL;
        n1 = NULL;
        n2 = NULL;
        NIL = NULL;
    }
    //析构函数将整个mentable删除———系统关闭调用，要把剩余memtale写入磁盘
    //reset函数则是清空（还保留head和nil)——主动调用清空


double memtable::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int memtable::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

void memtable::Insert(uint64_t key, const string &value)
{
    // TODO
    //到最低层 find the previous node temp
    vector<SKNode *> update;
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        update.push_back(nullptr);
    }
    SKNode *temp = head;
    int l = 0;

    for (int i = MAX_LEVEL - 1; i >= 0; i--)
    {
        while (temp->forwards[i]->key < key)
        {
            temp = temp->forwards[i];
        }
        update[i] = temp;
    }
    if (temp->forwards[0]->key == key)
    {
        temp->forwards[0]->val = value;
        return;
    }
    else
    {
        l = randomLevel();
        SKNode *newNode = new SKNode(key, value, SKNodeType::NORMAL);
        for (int i = l - 1; i >= 0; i--)
        {
            newNode->forwards[i] = update[i]->forwards[i];
            update[i]->forwards[i] = newNode;
        }
        totallength += newNode->length;
    }
}

string memtable::Search(uint64_t key)
{
    // TODO
    SKNode *temp = head;
    for (int i = MAX_LEVEL - 1; i >= 0; i--)
    {

        while (temp->forwards[i]->key < key)
        {
            temp = temp->forwards[i];
        }
    }
    temp = temp->forwards[0];

    // if the key is the same,return the string
    if (temp->key == key && temp->val!="~DELETED~")
    {
        return temp->val;
    }
    // Not Found return the empty string
    else
    {
        // string empty="";
        return "";
    }
}

bool memtable::Delete(uint64_t key)
{
    SKNode *temp = head;
    SKNode *maybedel = nullptr;
    // vector<SKNode *> update;
    // for (int i = 0; i < MAX_LEVEL; ++i)
    // {
    //     update.push_back(nullptr);
    // }
    for (int i = MAX_LEVEL - 1; i >= 0; i--)
    {
        while (temp->forwards[i]->key < key)
        {
            temp = temp->forwards[i];
        }
        // update[i] = temp;
    }
    maybedel = temp->forwards[0];
    if (maybedel->key == key)
    {
        //无需删去真正节点只要用deleted覆盖即可
        // totallength -= maybedel->length;
        // for (int i = 0; i < MAX_LEVEL; i++)
        // {
        //     if (maybedel->forwards[i])
        //     {
        //         update[i]->forwards[i] = maybedel->forwards[i];
        //         maybedel->forwards[i] = nullptr;
        //     }
        // }

        // delete maybedel;
        // maybedel = nullptr;
        if(!maybedel->val.compare("~DELETED~"))return false;//表示已经删除
        maybedel->val = "~DELETED~";
        return true;
    }
    else return false;
}

// void memtable::Display()
// {
//     for (int i = MAX_LEVEL - 1; i >= 0; --i)
//     {
//         std::cout << "Level " << i + 1 << ":h";
//         SKNode *node = head->forwards[i];
//         while (node->type != SKNodeType::NIL)
//         {
//             std::cout << "-->(" << node->key << "," << node->val << ")";
//             node = node->forwards[i];
//         }

//         std::cout << "-->N" << std::endl;
//     }
// }

// void memtable::Scan(uint64_t key_start, uint64_t key_end, list<pair<uint64_t, std::string>> &list){
//     SKNode *temp = head;
//     for (int i = MAX_LEVEL - 1; i >= 0; i--)
//     {

//         while (temp->forwards[i]->key < key_start)
//         {
//             temp = temp->forwards[i];
//         }
//     }
//     temp = temp->forwards[0];
//     while(temp->key<=key_end){
//         pair<uint64_t, std::string> kv(temp->key,temp->val);
//         list.push_back(kv);
//         temp = temp->forwards[0];
//     }

// }

//return the minimum key of the scan
uint64_t memtable::Scan(uint64_t key_start, uint64_t key_end){
    SKNode *temp = head;
    for (int i = MAX_LEVEL - 1; i >= 0; i--)
    {

        while (temp->forwards[i]->key < key_start)
        {
            temp = temp->forwards[i];
        }
    }
    temp = temp->forwards[0];
    if(temp->key > key_end)return 0;
    else return temp->key;
}


void memtable::toSSTable( list<pair<uint64_t, std::string>> &list){
    SKNode *temp = head;
    temp = temp->forwards[0];
    while(temp->type!=SKNodeType::NIL){
        pair<uint64_t, std::string> kv(temp->key,temp->val);
        list.push_back(kv);
        temp = temp->forwards[0];
    }

}

void memtable::reset(){
    totallength = 0;
    SKNode *n1 = head->forwards[0];
        SKNode *n2;
        while (n1->type!=SKNodeType::NIL)
        {
            n2 = n1->forwards[0];
            delete n1;
            n1 = n2;
        }
        n1 = NULL;
        n2 = NULL;
        for (int i = 0; i < MAX_LEVEL; ++i) {
            head->forwards[i] = NIL;
        }
}
