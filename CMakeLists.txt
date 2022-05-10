cmake_minimum_required(VERSION 3.21)
project(LSM)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(LSM
        bf.cpp
        bf.h
        persistence.cc
        kvstore.cc
        kvstore.h
        kvstore_api.h
        memtable.cpp
        memtable.h
        MinHeap.cpp
        MinHeap.h
        MurmurHash3.h
        ssTable.cpp
        ssTable.h
        test.h
        utils.h)
