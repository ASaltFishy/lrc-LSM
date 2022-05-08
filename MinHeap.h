#ifndef MINHEAPS_MINHEAP_H
#define MINHEAPS_MINHEAP_H
#include <iostream>
#include <vector>
class MinHeap {
private:
    std::vector<uint64_t> myArray;
public:
    MinHeap();
    void heapify();
    void insert(uint64_t );
    void print();
    uint64_t getMinimum();
    uint64_t pop();
    unsigned long size();
    bool isEmpty();
};


#endif //MINHEAPS_MINHEAP_H
