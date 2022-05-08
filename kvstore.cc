#include "kvstore.h"
#include "MinHeap.h"
#include <algorithm>
#include <string>
#include <vector>

#include "utils.h"
using namespace utils;
int time_stamp; // kvstore类中记录时间的全局变量

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
	maxlevel = 0;
	vector<string> ret;
	int temptime = 0;
	int latesttime = 0;
	int curlevel = 0;
	string dirpath = "./data/level-0";
	while (dirExists(dirpath)) {
        string filepath = "";
        scanDir(dirpath, ret);
        vector<SSTable> levelbuffer;
        for (int i = 0; i < ret.size(); i++) {
            filepath = dirpath + "/" + ret[i];
            SSTable temp(filepath, temptime);
            levelbuffer.push_back(temp);
            if (temptime > latesttime)
                latesttime = temptime;
        }
        buffer.push_back(levelbuffer);
        vector<SSTable>().swap(levelbuffer);
        curlevel++;
        dirpath = "./data/level-" + to_string(curlevel);
    }
    vector<SSTable> empty;
    if(buffer.empty())buffer.push_back(empty);
    else {
        time_stamp = latesttime;
        maxlevel = curlevel - 1;
    }
}

//关闭时的操作都写在memtable.h和各类的reset函数当中了
KVStore::~KVStore()
{
	//缓存清空 内存写入文件
	mtable.~memtable();
	vector<vector<SSTable>>().swap(buffer);
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if (mtable.totallength >= 2086800) //除去头部和BF的最大长度
	{
		list<pair<uint64_t, std::string>> list;
		mtable.toSSTable(list);
		SSTable newSSTable(list);
		buffer[0].push_back(newSSTable); //存入缓存
		string sspath = to_string(time_stamp) + ".sst";
		string dirpath = "./data/level-0";
		newSSTable.mkFile(sspath, dirpath, list);
		mtable.reset();
	}
	KVStore::mtable.Insert(key, s);
	//判断需要合并的情况
	for (int i = 0; i <= maxlevel; i++)
	{
		int maxsize = 1 << (i + 1);
		if (buffer[i].size() > maxsize)
			compactLevel(i);
		else
			break;
	}
}

// compactLevel函数：
//将level层的sstable与下层重叠部分放入缓存归并排序后合并
void KVStore::compactLevel(int level)
{
	vector<Range> scale;
    vector<SSTable> table2compact;
	vector<KV> tobecompact;
	Range tempscale;
	int size = buffer[level].size();
	int maxStamp = 0; //记录参与合并的是stable的最大时间戳

	//若为第零层，全部放入tobecompact中
//	if (level == 0)
//	{
//		for (int i = 0; i < size; i++)
//		{
//			buffer[level][i].getscale(tempscale);
//			scale.push_back(tempscale);
//			int tempStamp = buffer[level][i].header.timeStamp;
//            table2compact.push_back(buffer[level][i]);
//			buffer[level][i].toKV(&tobecompact, level, tempStamp);
//			if (tempStamp > maxStamp)
//				maxStamp = tempStamp;
//		}
//		buffer[level].clear();
//	}
	//其他层则排序后寻找时间戳最小层与下层合并
		int num = size - (1 << (level + 1)); //超过的table个数
        if(level==0)num = size;
		vector<int> timetable;
		for (int i = 0; i < size; i++)
		{
			timetable.push_back(buffer[level][i].header.timeStamp);
		}

		// sort函数将时间戳从小到大排序，然后选取num个最小的去参与排序
		sort(timetable.begin(), timetable.end());
		for (int i = 0; i < num; i++)
		{
			int temptime = timetable[i];
			for (int j = 0; j < size - i; j++)
			{
				if (buffer[level][j].header.timeStamp == temptime)
				{
					int tempStamp = buffer[level][j].header.timeStamp;
					if (tempStamp > maxStamp)
						maxStamp = tempStamp;
                    table2compact.push_back(buffer[level][j]);//按照时间戳从小到大排序
					buffer[level][j].toKV(&tobecompact, level, tempStamp);
					buffer[level][j].getscale(tempscale);
					scale.push_back(tempscale);
					buffer[level].erase(buffer[level].begin() + j);
					break;
				}
			}
		}
        vector<int>().swap(timetable);

	//找重叠(不是最大一层)
	if (level != maxlevel)
	{
		int nextlevel = level + 1;
		int scalesize = scale.size();
		for (auto it = buffer[nextlevel].begin(); it != buffer[nextlevel].end();)
		{
			int max = it->header.maxkey;
			int min = it->header.minkey;
			bool iserased = false;
			for (int j = 0; j < scalesize; j++)
			{
				if (!(max < scale[j].first && min > scale[j].second))
				{
					int tempStamp = it->header.timeStamp;
					if (tempStamp > maxStamp)
						maxStamp = tempStamp;

					it->toKV(&tobecompact, nextlevel, tempStamp);
					it = buffer[level].erase(it);
					iserased = true;
					break;
				}
			}
			if (iserased)
				it++;
		}
		vector<Range>().swap(scale);
	}
	else
	{
		maxlevel++;
		vector<SSTable> newOne;
		buffer.push_back(newOne);
        vector<SSTable>().swap(newOne);
	}

	//对tobecompact保存的SStable中的key值进行归并排序
	mergeSort(tobecompact, 0, tobecompact.size());

	//按照2MB划分
	for (auto it = tobecompact.begin(); it != tobecompact.end(); it++)
	{
		int length = 0;
		list<pair<uint64_t, std::string>> table;
		while (length <= 2086800 && it != tobecompact.end())
		{
			auto next = it + 1;
			while ((next->key == it->key) && (next != tobecompact.end()))
			{
				if (next->ts < it->ts)
				{
					swap(*it, *next);
				}
				it++;
				next++;
			}
			//考虑最大层删去结点
			if (level == maxlevel)
			{
				if (it->val == "~DELETED~")
					it++;
				continue;
			}
			//循环结束时it保存的即为去除了重复并要保存到table当中的KV
			length += 12 + sizeof(it->val);
			pair<uint64_t, std::string> temp(it->key, it->val);
			table.push_back(temp);
			it++;
		}
		SSTable newSSTable(table);
		newSSTable.header.timeStamp = maxStamp;
		//不用判断直接一股脑放了下次再取了来合并
		buffer[level + 1].push_back(table);
		string dirpath = "./data/level-" + to_string(level + 1);
		string sspath = to_string(maxStamp) + ".sst";
		newSSTable.mkFile(sspath, dirpath, table);
	}
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{

	string memval = KVStore::mtable.Search(key);
	if (memval == "~DELETED~")
		return "";
	else if (memval != "")
		return memval;
	else
	{
		string val = "";
		string latest_val = "";
		uint64_t t = 0; //记录所遇最晚时间戳
		uint64_t latest = 0;
		for (int i = 0; i <= maxlevel; i++)
		{
			for (int j = 0; j < buffer[i].size(); j++)
			{
				val = buffer[i][j].getval(key, i, t); //没找到时t原样返回
				if (t > latest)
				{
					latest_val = val;
					latest = t;
				}
			}
		}
		if (latest_val == "~DELETED~" || latest == 0)
			return "";
		return latest_val;
	}
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false if the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	if (KVStore::mtable.Delete(key) == true)
		return true;
	int size = buffer.size();
	for (int i = 0; i <= maxlevel; i++)
	{
		for (int j = 0; j < buffer[i].size(); j++)
		{
			if (buffer[i][j].is_in(key))
			{
				mtable.Insert(key, "~DELETED~");
				return true;
			}
		}
	}
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	KVStore::mtable.reset();
	vector<vector<SSTable>>().swap(buffer);
	vector<string> ret;
	for (int j = 0; j <= maxlevel; j++)
	{
		string dirpath = "./data/level-" + to_string(j);
		string filepath = "";
		utils::scanDir(dirpath, ret);
		for (int i = 0; i < ret.size(); i++)
		{
			filepath = dirpath + "/" + ret[i];
			utils::rmfile(filepath.c_str());
		}
		utils::rmdir(dirpath.c_str());
	}
    vector<string>().swap(ret);
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	//使用堆排序和同时管理多个指针的方式
	MinHeap heap;
	uint64_t tempkey;
	tempkey = mtable.Scan(key1, key2);
	if (tempkey)
		heap.insert(tempkey);
}

void mergeSort(vector<KV> &array, int l, int r)
{
	if (l >= r)
		return;
	int mid = (l + r) / 2;
	mergeSort(array, l, mid);
	mergeSort(array, mid + 1, r);
	merge(array, l, mid, r);
    cout<<l<<" "<<r<<endl;
}

void merge(vector<KV> &arr, int l, int mid, int r)
{
	vector<KV> temp = arr;
	int left = l;
	int right = mid + 1;
	int index = l;
	while (left <= mid && right <= r)
	{
		if (temp[left].key < temp[right].key)
		{
			arr[index] = temp[left];
			left++;
			index++;
		}
		else
		{
			arr[index] = temp[right];
			right++;
			index++;
		}
	}
	while (left <= mid)
	{
		arr[index] = temp[left];
		left++;
		index++;
	}
	while (right <= r)
	{
		arr[index] = temp[right];
		right++;
		index++;
	}
}