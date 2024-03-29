#ifndef KVSTROE
#define KVSTORE

#include "kvstore_api.h"
#include "memtable.h"
#include "SSTable.h"

typedef std::pair<uint64_t, uint64_t> Range;
//typedef pair<list<KV>,uint64_t> SCAN_UNIT;
void scanMergeSort(vector<SSTable*> &array);
SSTable* scanMerge(SSTable &table1, SSTable &table2);
void mergeSort(vector<SSTable> &array);
SSTable merge(SSTable &table1, SSTable &table2);


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

#endif