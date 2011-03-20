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
//==============================================================================================================================================================

#include "cache_manager.h"

#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "config_file_properties.h"
#include "configuration.h"
#include "globals.h"
#include "index_layout_parameters.h"
#include "logger.h"
using namespace std;

/**************************************************************************************************************************************************************
 * CacheManager
 *
 **************************************************************************************************************************************************************/
const uint64_t CacheManager::kBlockSize;  // Initialized in the class definition.

CacheManager::CacheManager(const char* index_filename) :
  kIndexFd(open(index_filename, O_RDONLY)), kTotalIndexBlocks(CalculateTotalIndexBlocks()) {
  if (kIndexFd < 0) {
    GetErrorLogger().LogErrno("open() in CacheManager::CacheManager(), trying to open index file", errno, true);
  }
}

CacheManager::~CacheManager() {
  int close_ret = close(kIndexFd);
  if (close_ret < 0) {
    GetErrorLogger().LogErrno("close() in CacheManager::~CacheManager(), trying to close index file", errno, false);
  }
}

uint64_t CacheManager::CalculateTotalIndexBlocks() const {
  struct stat stat_buf;
  if (fstat(kIndexFd, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in CacheManager::CalculateCacheSize()", errno, true);
  }
  assert(stat_buf.st_size % kBlockSize == 0);
  return stat_buf.st_size / kBlockSize;
}

/**************************************************************************************************************************************************************
 * AllocatedCacheManager
 *
 **************************************************************************************************************************************************************/
const uint64_t AllocatedCacheManager::kIndexSizedCache = numeric_limits<uint64_t>::max();

AllocatedCacheManager::AllocatedCacheManager(const char* index_filename, uint64_t cache_size) :
  CacheManager(index_filename),
  kCacheSize(cache_size == kIndexSizedCache ? kTotalIndexBlocks : cache_size),
  block_cache_(new uint32_t[kBlockSize * kCacheSize / sizeof(*block_cache_)]) {
  assert(kCacheSize != 0);
}

AllocatedCacheManager::~AllocatedCacheManager() {
  delete[] block_cache_;
}

/**************************************************************************************************************************************************************
 * LruCachePolicy
 *
 * The cache size is assumed to be big enough to hold this many blocks: (# of unique words in query) * (# of blocks of read ahead per list).
 * This is because we don't want to evict any read ahead blocks before they have been processed.
 * Thus all queued blocks are pinned.  They will all have to be freed by the caller.
 **************************************************************************************************************************************************************/
LruCachePolicy::LruCachePolicy(const char* index_filename) :
  AllocatedCacheManager(index_filename, atol(Configuration::GetConfiguration().GetValue(config_properties::kBlockCacheSize).c_str())),
  cache_block_info_(kCacheSize) {
  pthread_mutex_init(&query_mutex_, NULL);

  for (uint64_t i = 0; i < kCacheSize; ++i) {
    lru_list_.push_back(make_pair(i, 0));
  }
}

LruCachePolicy::~LruCachePolicy() {
}

// Queues the requested range to be loaded into the cache: ['starting_block_num', 'ending_block_num')
// If a block is in the cache already, updates the LRU linked list, so that the block location is now in the back, meaning it was just used.
// If the block isn't in the cache, evicts the LRU block from the cache, at the front of the LRU list (if the cache is full),
// and sets up an asynchronous request to load a block from disk into the cache. This block is added to the back of the LRU list, since it was just used.
// If the block num we're looking for is not in the cache, we know that it is in a ready state because it either had its aio request previously canceled,
// or it has completed its aio request, or it never had any aio requests associated with it.
// If the block is already in the cache, some previous request must have queued it, and we'll deal with it when we actually get the block.
// Returns the number of blocks that had to be read in from the disk.
int LruCachePolicy::QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num) {
  pthread_mutex_lock(&query_mutex_);  // Lock the mutex because we don't want 'cache_map_' to be read while it's being modified.

  int num_disk_blocks = 0;  // Tracks the number of blocks requested to be loaded from disk.

  struct aiocb* aiocb_list[ending_block_num - starting_block_num];  // Variable length array here.
  int curr_aiocb_list_item = 0;

  for (uint64_t block_num = starting_block_num; block_num < ending_block_num; ++block_num) {
    // Our block is not in the cache, need to bring it in, and evict someone (unless we have't filled the cache yet).
    if (cache_map_.find(block_num) == cache_map_.end()) {
      // First check whether the cache block is not used and thus can be safely invalidated.
      // Otherwise, we need to find a different block to evict, in order of least recently used.
      LruList::iterator lru_list_itr = lru_list_.begin();
      while (lru_list_itr != lru_list_.end() && cache_block_info_.IsBlockPinned(lru_list_itr->first)) {
        ++lru_list_itr;
      }
      assert(lru_list_itr != lru_list_.end());  // Most likely need to increase the block cache size, and/or decrease the number of read ahead blocks.
                                                // Alternatively, put a limit on the number of unique words in a single query.

      // Evict someone from the cache.
      // We will also cancel any pending aio requests associated with it.
      if (cache_map_.size() == static_cast<size_t> (kCacheSize)) {
        uint64_t evicted_block = lru_list_itr->second;
        int invalidated_cache_block = lru_list_itr->first;

        // If there is still a request in progress for the cache block we're invalidating, need to cancel it.
        int aio_status_ret = aio_error(cache_block_info_.aiocb(invalidated_cache_block));
        assert(aio_status_ret != -1);
        if (aio_status_ret == EINPROGRESS) {
          // Canceling a request is just a hint to the system, not guaranteed to be canceled.
          int aio_ret = aio_cancel(cache_block_info_.aiocb(invalidated_cache_block)->aio_fildes, cache_block_info_.aiocb(invalidated_cache_block));

          // If we couldn't cancel the request, wait for it to complete.
          if (aio_ret != AIO_CANCELED) {
            struct aiocb* cblist[1];
            cblist[0] = cache_block_info_.aiocb(invalidated_cache_block);
            int aio_suspend_ret = aio_suspend(cblist, 1, NULL);
            if (aio_suspend_ret < 0) {
              GetErrorLogger().LogErrno("aio_suspend() in LruCachePolicy::QueueBlocks()", errno, true);
            }
          }
        }

        cache_block_info_.ReadyBlock(invalidated_cache_block);
        cache_map_.erase(evicted_block);
      }

      int cache_block = MoveToBack(lru_list_itr, block_num)->first;
      cache_block_info_.PinBlock(cache_block);
      cache_block_info_.LoadingBlock(cache_block);

      uint32_t* buffer = block_cache_ + (cache_block * kBlockSize / sizeof(*block_cache_));

      struct aiocb* curr_aiocb = cache_block_info_.aiocb(cache_block);

      // Initialize the necessary fields in the current aiocb.
      curr_aiocb->aio_fildes = kIndexFd;
      curr_aiocb->aio_buf = buffer;
      curr_aiocb->aio_lio_opcode = LIO_READ;
      curr_aiocb->aio_nbytes = kBlockSize;
      curr_aiocb->aio_offset = block_num * kBlockSize;
      curr_aiocb->aio_sigevent.sigev_notify = SIGEV_NONE;

      aiocb_list[curr_aiocb_list_item++] = curr_aiocb;

      ++num_disk_blocks;
    } else {
      // Since we're accessing a block, need to move it to the back of the LRU list.
      int cache_block = MoveToBack(cache_map_[block_num], block_num)->first;

      // Block is already in the cache, pin it so it doesn't get evicted.
      cache_block_info_.PinBlock(cache_block);
    }
  }

  pthread_mutex_unlock(&query_mutex_);  // Safe to unlock the mutex because from this point on, not modifying any global or classwise structures.

  int lio_listio_ret = lio_listio(LIO_NOWAIT, aiocb_list, curr_aiocb_list_item, NULL);
  if (lio_listio_ret < 0) {
    GetErrorLogger().LogErrno("lio_listio() in LruCachePolicy::QueueBlocks()", errno, true);
  }

  return num_disk_blocks;
}

// Assumes that 'block_num' has been previously queued by a call to QueueBlocks().
// If the 'block_num' is in the cache map and it's ready, then we can just return it.
// If the 'block_num' is in the cache map, but is not marked as ready, then it must be transferring from disk. Wait for completion.
uint32_t* LruCachePolicy::GetBlock(uint64_t block_num) {
  pthread_mutex_lock(&query_mutex_);  // Lock the mutex because we don't want 'cache_map_' to be read while it's being modified.

  // The block should be in the cache already.
  assert(cache_map_.find(block_num) != cache_map_.end());

  int cache_block = MoveToBack(cache_map_[block_num], block_num)->first;

  pthread_mutex_unlock(&query_mutex_);  // Safe to unlock mutex.

  // Only do this the first time we load the block from disk.
  if (!cache_block_info_.IsBlockReady(cache_block)) {
    struct aiocb* cblist[1];
    cblist[0] = cache_block_info_.aiocb(cache_block);
    int ret = aio_suspend(cblist, 1, NULL);
    assert(ret == 0);

    // Check whether the request completed successfully.
    ret = aio_error(cache_block_info_.aiocb(cache_block));
    assert(ret == 0);

    // Get the return status. Should only be called once, otherwise, result is undefined.
    ret = aio_return(cache_block_info_.aiocb(cache_block));
    assert(ret != -1 && ret == static_cast<int>(kBlockSize));

    cache_block_info_.ReadyBlock(cache_block);
  }

  uint32_t* buffer = block_cache_ + (cache_block * kBlockSize / sizeof(*block_cache_));
  return buffer;
}

// Unpins the block. Note that for a block to be unpinned, every list sharing this block must unpin it.
// This handles the case when a block is shared by several lists, which occurs in adjacent lists.
void LruCachePolicy::FreeBlock(uint64_t block_num) {
  assert(cache_map_.find(block_num) != cache_map_.end());

  int cache_block = cache_map_[block_num]->first;
  cache_block_info_.UnpinBlock(cache_block);
}

LruCachePolicy::LruList::iterator LruCachePolicy::MoveToBack(LruList::iterator lru_list_itr, uint64_t block_num) {
  int cache_block = lru_list_itr->first;
  lru_list_.erase(lru_list_itr);
  lru_list_.push_back(make_pair(cache_block, block_num));

  LruList::iterator new_lru_list_itr = --lru_list_.end();  // Cache map should update its' iterator so that it points to the last element that we just inserted.
  cache_map_[block_num] = new_lru_list_itr;
  return new_lru_list_itr;
}

/**************************************************************************************************************************************************************
 * MergingCachePolicy
 *
 **************************************************************************************************************************************************************/
MergingCachePolicy::MergingCachePolicy(const char* index_filename) :
  AllocatedCacheManager(index_filename, 32), initial_cache_block_num_(0) {
  // Fill the cache initially.
  FillCache(0);
}

MergingCachePolicy::~MergingCachePolicy() {
}

uint32_t* MergingCachePolicy::GetBlock(uint64_t block_num) {
  // Assuming that all accesses will be forward index accesses.
  assert((block_num - initial_cache_block_num_) >= 0);
  if ((block_num - initial_cache_block_num_) < kCacheSize) {
    // Cache hit.
    return block_cache_ + ((block_num - initial_cache_block_num_) * (kBlockSize / sizeof(*block_cache_)));
  } else {
    // Cache miss.
    FillCache(block_num);
    return block_cache_;
  }
}

void MergingCachePolicy::FillCache(uint64_t block_num) {
  // Fill cache with the next 'kCacheSize' blocks starting from 'block_num'.
  lseek(kIndexFd, block_num * kBlockSize, SEEK_SET);
  int read_ret = read(kIndexFd, block_cache_, kBlockSize * kCacheSize);
  if (read_ret < 0) {
    GetErrorLogger().LogErrno("read() in MergingCachePolicy::FillCache()", errno, true);
  }
  // We're reading in at most 'kBlockSize * kCacheSize' bytes, since an index might have less blocks left than we want to read, so it could be lower.
  assert(read_ret % kBlockSize == 0);
  initial_cache_block_num_ = block_num;
}

// Returns the number of blocks read in from the disk (all of them).
// For the last time QueueBlocks() is called, it will not necessarily return the correct number of blocks read from disk,
// because there might have been more blocks requested than there are left in the index. NOTE: This is no longer the case
// because we now store the total number of blocks in a list and don't request more blocks than there are in the list.
int MergingCachePolicy::QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num) {
  // Nothing to be done when merging.
  return ending_block_num - starting_block_num;
}

void MergingCachePolicy::FreeBlock(uint64_t block_num) {
  // Nothing to be done when merging.
}

/**************************************************************************************************************************************************************
 * FullContiguousCachePolicy
 *
 **************************************************************************************************************************************************************/
FullContiguousCachePolicy::FullContiguousCachePolicy(const char* index_filename) :
  AllocatedCacheManager(index_filename, AllocatedCacheManager::kIndexSizedCache) {
  FillCache();
}

FullContiguousCachePolicy::~FullContiguousCachePolicy() {
}

uint32_t* FullContiguousCachePolicy::GetBlock(uint64_t block_num) {
  return block_cache_ + (block_num * (kBlockSize / sizeof(*block_cache_)));
}

void FullContiguousCachePolicy::FillCache() {
  struct stat stat_buf;
  if (fstat(kIndexFd, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in FullContiguousCachePolicy::FillCache()", errno, true);
  }

  uint64_t total_data_read = 0;
  const int kReadSize = stat_buf.st_blksize;  // Preferred block size for I/O for the file.
  char* curr_read_location = reinterpret_cast<char*> (block_cache_);

  ssize_t read_ret;
  while ((read_ret = read(kIndexFd, curr_read_location, kReadSize)) > 0) {
    total_data_read += read_ret;
    curr_read_location += read_ret;
  }

  if (read_ret < 0) {
    GetErrorLogger().LogErrno("read() in FullContiguousCachePolicy::FillCache()", errno, true);
  }
  assert(static_cast<uint64_t> (total_data_read) == kBlockSize * kCacheSize);
}

// Returns the number of blocks read in from the disk (none, since the index was fully loaded into memory).
// For the last time QueueBlocks() is called, it will not necessarily return the correct number of blocks read from disk,
// because there might have been more blocks requested than there are left in the index. NOTE: This is no longer the case
// because we now store the total number of blocks in a list and don't request more blocks than there are in the list.
int FullContiguousCachePolicy::QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num) {
  return 0;
}

void FullContiguousCachePolicy::FreeBlock(uint64_t block_num) {
}

/**************************************************************************************************************************************************************
 * MemoryMappedCachePolicy
 *
 **************************************************************************************************************************************************************/
MemoryMappedCachePolicy::MemoryMappedCachePolicy(const char* index_filename) :
  CacheManager(index_filename),
  index_size_(0),
  index_(NULL) {
  struct stat stat_buf;
  if (fstat(kIndexFd, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in MemoryMappedCachePolicy::MemoryMappedCachePolicy()", errno, true);
  }

  index_size_ = stat_buf.st_size;

  void* src;
  if ((src = mmap(0, index_size_, PROT_READ, MAP_SHARED, kIndexFd, 0)) == MAP_FAILED) {
    GetErrorLogger().LogErrno("mmap() in MemoryMappedCachePolicy::MemoryMappedCachePolicy()", errno, true);
  }

  index_ = static_cast<uint32_t*> (src);

  // Loop over the index to make sure it's been read into memory.
  /*uint32_t index_data;
  assert(index_size_ % sizeof(*index_) == 0);
  uint64_t index_size_ints = index_size_ / sizeof(*index_);
  for (uint64_t i = 0; i < index_size_ints; ++i) {
    index_data = index_[i];
  }*/
}

MemoryMappedCachePolicy::~MemoryMappedCachePolicy() {
  if (munmap(index_, index_size_) < 0) {
    GetErrorLogger().LogErrno("munmap() in MemoryMappedCachePolicy::~MemoryMappedCachePolicy()", errno, true);
  }
}

uint32_t* MemoryMappedCachePolicy::GetBlock(uint64_t block_num) {
  return index_ + (block_num * (kBlockSize / sizeof(*index_)));
}

// Returns the number of blocks read in from the disk (since the index is memory mapped and we don't know, just return none).
int MemoryMappedCachePolicy::QueueBlocks(uint64_t starting_block_num, uint64_t ending_block_num) {
  return 0;
}

void MemoryMappedCachePolicy::FreeBlock(uint64_t block_num) {
}
