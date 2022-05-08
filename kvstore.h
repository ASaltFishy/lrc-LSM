#ifndef KVSTROE
#define KVSTORE

#include "kvstore_api.h"
#include "memtable.h"
#include "SSTable.h"

typedef std::pair<uint64_t, uint64_t> Range;
// // typedef std::pair<uint64_t,string> KV;
// #ifndef STRUCT_KV
// #define STRUCT_KV 
// typedef struct KV{
// 	uint64_t key;
// 	string val;
// 	int ts;

// 	KV(uint64_t _key,string _val,int _ts):key(_key),val(_val),ts(_ts){}
//     KV(){}
// };
// #endif

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
	memtable mtable;
	vector<vector<SSTable>> buffer;
	int maxlevel;
	void compactLevel(int level);

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override; // override指明了该函数是父类方法的重写

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;
};
//merge sort
void mergeSort(vector<KV> &array,int l,int r);
void merge(vector<KV> &arr,int l,int mid,int r);

#endif