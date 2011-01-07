// Copyright (c) 2010, Roman Khmelichek
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//  1. Redistributions of source code must retain the above copyright notice,
//     this list of conditions and the following disclaimer.
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//  3. Neither the name of Roman Khmelichek nor the names of its contributors
//     may be used to endorse or promote products derived from this software
//     without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR "AS IS" AND ANY EXPRESS OR IMPLIED
// WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
// EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
// OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
// OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
// ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

//==============================================================================================================================================================
// Author(s): Roman Khmelichek
//
// TODO: Might be useful to implement a "fragmented cache policy", where the memory is allocated in chunks instead of one contiguous piece. Could be useful if
//       the memory is fragmented and the system can't allocate such a large contiguous chunk of memory. Would require keeping an array with pointers to each
//       allocated chunk of memory (assuming each chunk fits the same number of blocks).
//==============================================================================================================================================================

#ifndef CACHE_MANAGER_H_
#define CACHE_MANAGER_H_

#include <cassert>
#include <cstring>
#include <stdint.h>

#include <algorithm>
#include <limits>
#include <list>
#include <map>
#include <string>
#include <utility>
//#include <ext/hash_map>

#include <aio.h>
#include <pthread.h>

#include "index_layout_parameters.h"

/**************************************************************************************************************************************************************
 * CacheBlockInfo
 *
 * Implements a bit array that holds some information for each cache block.
 * Also holds the control blocks for the asynchronous transfer status for each cache block.
 **************************************************************************************************************************************************************/
class CacheBlockInfo {
public:
  enum BitAttributes {
    kReady
  };

  CacheBlockInfo(uint64_t cache_size) :
    kNumAttributes(1), info_size_(NumIntsRequired(cache_size)), bit_info_(new uint32_t[info_size_]), aiocb_(new struct aiocb[cache_size]),
        block_pin_count_(new int[cache_size]) {

    assert(kNumAttributes <= static_cast<int>(sizeof(*bit_info_) * 8));

    // Initially all cache blocks are ready.
    for (int i = 0; i < info_size_; ++i) {
      bit_info_[i] = 0;
    }

    memset(aiocb_, 0, sizeof(struct aiocb) * cache_size);
    for (size_t i = 0; i < cache_size; ++i) {
      aiocb_[i].aio_fildes = -1;
      block_pin_count_[i] = 0;
    }
  }

  ~CacheBlockInfo() {
    delete[] bit_info_;
    delete[] aiocb_;
    delete[] block_pin_count_;
  }

  int NumIntsRequired(uint64_t cache_size) const {
    int num_bits_required = cache_size * kNumAttributes;
    int int_size_bits = sizeof(*bit_info_) * 8;
    return (num_bits_required / int_size_bits) + (num_bits_required % int_size_bits == 0 ? 0 : 1);
  }

  void PinBlock(uint64_t cache_block) {
    ++block_pin_count_[cache_block];
  }

  void UnpinBlock(uint64_t cache_block) {
    if (block_pin_count_[cache_block] > 0)
      --block_pin_count_[cache_block];
  }

  // Set bit to indicate cache block is loading.
  void LoadingBlock(uint64_t cache_block) {
    set_block_bit(cache_block, kReady);
  }

  // Unset bit to indicate cache block is ready.
  void ReadyBlock(uint64_t cache_block) {
    unset_block_bit(cache_block, kReady);
  }

  // Returns true when the cache block is pinned.
  bool IsBlockPinned(uint64_t cache_block) const {
    return block_pin_count_[cache_block];
  }

  // Returns true when the cache block is ready.
  bool IsBlockReady(uint64_t cache_block) const {
    return !read_block_bit(cache_block, kReady);
  }

  struct aiocb* aiocb(uint64_t cache_block) {
    return &aiocb_[cache_block];
  }

private:
  int info_index(uint64_t cache_block) const {
    return (cache_block * kNumAttributes) / (sizeof(*bit_info_) * 8);
  }

  int bit_offset(uint64_t cache_block) const {
    int bit_offset = (cache_block % (8 * sizeof(*bit_info_))) * kNumAttributes;
    return bit_offset;
  }

  void set_block_bit(uint64_t cache_block, int bit_idx) {
    int info_idx = info_index(cache_block);
    uint32_t data = bit_info_[info_idx];

    int mask = 1 << (bit_offset(cache_block) + bit_idx);
    bit_info_[info_idx] = data | mask;
  }

  void unset_block_bit(uint64_t cache_block, int bit_idx) {
    int info_idx = info_index(cache_block);
    uint32_t data = bit_info_[info_idx];

    int mask = 1 << (bit_offset(cache_block) + bit_idx);
    bit_info_[info_idx] = data & ~mask;
  }

  bool read_block_bit(uint64_t cache_block, int bit_idx) const {
    int info_idx = info_index(cache_block);
    uint32_t data = bit_info_[info_idx];

    return (data >> (bit_offset(cache_block) + bit_idx)) & 1;
  }

  const int kNumAttributes;
  int info_size_;
  uint32_t* bit_info_;
  struct aiocb* aiocb_;
  int* block_pin_count_;
};

/**************************************************************************************************************************************************************
 * CacheManager
 *
 * Abstract base class that provides an interface to build new caching policies.
 **************************************************************************************************************************************************************/
class CacheManager {
public:
  CacheManager(const char* index_filename);
  virtual ~CacheManager();

  virtual int QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num) = 0;

  virtual uint32_t* GetBlock(uint64_t block_num) = 0;

  virtual void FreeBlock(uint64_t block_num) = 0;

  uint64_t total_index_blocks() {
    return kTotalIndexBlocks;
  }

  static const uint64_t kBlockSize = BLOCK_SIZE;  // The block size in number of bytes.

protected:
  const int kIndexFd;                // File descriptor for the inverted index file.
  const uint64_t kTotalIndexBlocks;  // The total number of blocks in this inverted index file.
private:
  uint64_t CalculateTotalIndexBlocks() const;
};

/**************************************************************************************************************************************************************
 * AllocatedCacheManager
 *
 * Abstract base class that provides an interface to build new caching policies based on memory allocation of blocks.
 **************************************************************************************************************************************************************/
class AllocatedCacheManager : public CacheManager {
public:
  AllocatedCacheManager(const char* index_filename, uint64_t cache_size);
  virtual ~AllocatedCacheManager();

protected:
  static const uint64_t kIndexSizedCache;  // Sentinel value that when passed into the constructor as the 'cache_size' parameter, will cause the cache manager
                                           // to allocate as much space for the cache as there are blocks in the whole index.
  const uint64_t kCacheSize;               // Size of this cache in number of blocks.
  uint32_t* block_cache_;                  // Pointer to the memory allocated for the block cache.
};

/**************************************************************************************************************************************************************
 * LruCachePolicy
 *
 * Implements a caching policy for blocks. It is concurrent safe for multiple queries running simultaneously.
 **************************************************************************************************************************************************************/
class LruCachePolicy : public AllocatedCacheManager {
public:
  LruCachePolicy(const char* index_filename);
  ~LruCachePolicy();

  int QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num);

  uint32_t* GetBlock(uint64_t block_num);

  void FreeBlock(uint64_t block_num);

private:
  typedef std::list<std::pair<int, uint64_t> > LruList;

  // TODO: Consider using a hash table for more speed.
  //typedef __gnu_cxx::hash_map<uint64_t, LruList::iterator> CacheMap;
  typedef std::map<uint64_t, LruList::iterator> CacheMap;

  // Moves the cache block referenced by 'lru_list_itr' to the end of the list
  // and updates the cache map for 'block_num' to point to the new iterator.
  // Returns the new iterator to the last element in the list.
  LruList::iterator MoveToBack(LruList::iterator lru_list_itr, uint64_t block_num);

  CacheBlockInfo cache_block_info_;

  // Access to the block cache must be concurrent safe.
  pthread_mutex_t query_mutex_;

  // Stores indexes into the block cache, in LRU order.
  LruList lru_list_;

  // Maps a block number to an array slot in the block cache.
  CacheMap cache_map_;
};

/**************************************************************************************************************************************************************
 * MergingCachePolicy
 *
 * Cache policy for when we're merging multiple indices. Main point is that it doesn't consume much memory.
 * We also know when merging lists, we'll be reading the index sequentially, so whenever we try to access a block not in the cache, we might as well flush the
 * cache and fill it with the next several blocks.
 **************************************************************************************************************************************************************/
class MergingCachePolicy : public AllocatedCacheManager {
public:
  MergingCachePolicy(const char* index_filename);
  ~MergingCachePolicy();

  int QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num);

  uint32_t* GetBlock(uint64_t block_num);

  void FreeBlock(uint64_t block_num);

private:
  void FillCache(uint64_t block_num);

  uint64_t initial_cache_block_num_;
};

/**************************************************************************************************************************************************************
 * FullContiguousCachePolicy
 *
 * Loads the full index into a contiguous block of main memory. Eliminates the overheads associated with various caching policies
 * (assuming you have enough main memory).
 **************************************************************************************************************************************************************/
class FullContiguousCachePolicy : public AllocatedCacheManager {
public:
  FullContiguousCachePolicy(const char* index_filename);
  ~FullContiguousCachePolicy();

  int QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num);

  uint32_t* GetBlock(uint64_t block_num);

  void FreeBlock(uint64_t block_num);

private:
  void FillCache();
};

/**************************************************************************************************************************************************************
 * MemoryMappedCachePolicy
 *
 * Memory maps the entire index into our address space. Note that this will not work on 32-bit systems when the index file is very large because it won't be
 * able to fit into the address space.
 **************************************************************************************************************************************************************/
class MemoryMappedCachePolicy : public CacheManager {
public:
  MemoryMappedCachePolicy(const char* index_filename);
  ~MemoryMappedCachePolicy();

  int QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num);

  uint32_t* GetBlock(uint64_t block_num);

  void FreeBlock(uint64_t block_num);

private:
  off_t index_size_;  // Size of the index in bytes.
  uint32_t* index_;   // Pointer into the index file that's memory mapped.
};

#endif /* CACHE_MANAGER_H_ */
