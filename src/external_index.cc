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

#include "external_index.h"

#include <cassert>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>

#include <limits>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "globals.h"
#include "logger.h"
#include "index_build.h"
using namespace std;
using namespace logger;

/**************************************************************************************************************************************************************
 * ExternalFrequencyData
 *
 **************************************************************************************************************************************************************/


/**************************************************************************************************************************************************************
 * ExternalDocumentData
 *
 **************************************************************************************************************************************************************/


/**************************************************************************************************************************************************************
 * ExternalChunk
 *
 **************************************************************************************************************************************************************/


/**************************************************************************************************************************************************************
 * ExternalBlock
 *
 **************************************************************************************************************************************************************/
ExternalBlock::ExternalBlock() :
  block_max_score_(-numeric_limits<float>::max()) {
}

ExternalBlock::~ExternalBlock() {
}

/**************************************************************************************************************************************************************
 * ExternalIndexBuilder
 *
 **************************************************************************************************************************************************************/
ExternalIndexBuilder::ExternalIndexBuilder(const char* external_index_filename) :
  external_index_fd_(open(external_index_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)),
  curr_offset_(0) {
}

ExternalIndexBuilder::~ExternalIndexBuilder() {
  NewList();  // Flush anything we have left.
  close(external_index_fd_);
}

void ExternalIndexBuilder::AddChunk(const ChunkEncoder& chunk) {
  if (external_blocks_.empty()) {
    NewBlock();
  }
  ExternalBlock& curr_external_block = external_blocks_.back();
  ExternalChunk external_chunk;
  external_chunk.set_chunk_max_score(chunk.max_score());
  curr_external_block.AddChunk(external_chunk);
}

// Start a new current block.
void ExternalIndexBuilder::NewBlock() {
  ExternalBlock new_external_block;
  external_blocks_.push_back(new_external_block);
}

// Used for both a new layer within a term or for a new term list.
// Means we also need to start a new block.
void ExternalIndexBuilder::NewList() {
  ssize_t write_ret;
//  cout << "Writing out New List!" << endl;

  for (size_t i = 0; i < external_blocks_.size(); ++i) {
    const ExternalBlock& external_block = external_blocks_[i];
    int block_num_chunks = external_block.num_external_chunks();
    float block_max_score = external_block.block_max_score();

//    cout << "Writing block with " << block_num_chunks << " chunks and max score of " << block_max_score << endl;

    write_ret = write(external_index_fd_, &block_num_chunks, sizeof(block_num_chunks));
    curr_offset_ += 1;
    assert(write_ret == sizeof(block_num_chunks));
    write_ret = write(external_index_fd_, &block_max_score, sizeof(block_max_score));
    curr_offset_ += 1;
    assert(write_ret == sizeof(block_max_score));

    for (int j = 0; j < block_num_chunks; ++j) {
      const ExternalChunk& external_chunk = external_block.external_chunk_num(j);
      float chunk_max_score = external_chunk.chunk_max_score();

      write_ret = write(external_index_fd_, &chunk_max_score, sizeof(chunk_max_score));
      curr_offset_ += 1;
      assert(write_ret == sizeof(chunk_max_score));
    }
  }

  external_blocks_.clear();
}

/**************************************************************************************************************************************************************
 * ExternalIndexReader
 *
 **************************************************************************************************************************************************************/
ExternalIndexReader::ExternalIndexReader(const char* external_index_filename) :
  external_index_fd_(open(external_index_filename, O_RDONLY)),
  external_index_size_(0),
  external_index_(NULL) {
  struct stat stat_buf;
  if (fstat(external_index_fd_, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in ExternalIndexReader::ExternalIndexReader()", errno, true);
  }

  external_index_size_ = stat_buf.st_size;
  void* src;
  if ((src = mmap(0, external_index_size_, PROT_READ, MAP_SHARED, external_index_fd_, 0)) == MAP_FAILED) {
    GetErrorLogger().LogErrno("mmap() in MemoryMappedCachePolicy::MemoryMappedCachePolicy()", errno, true);
  }

  external_index_ = static_cast<uint32_t*> (src);

  // Loop over the index to make sure it's been read into memory.
  uint32_t external_index_data;
  assert(external_index_size_ % sizeof(*external_index_) == 0);
  uint64_t external_index_size_ints = external_index_size_ / sizeof(*external_index_);
  for (uint64_t i = 0; i < external_index_size_ints; ++i) {
    external_index_data = external_index_[i];
  }
}

ExternalIndexReader::~ExternalIndexReader() {
  if (munmap(external_index_, external_index_size_) < 0) {
    GetErrorLogger().LogErrno("munmap() in ExternalIndexReader::~ExternalIndexReader()", errno, true);
  }
}

void ExternalIndexReader::AdvanceToBlock(uint32_t block_num, ExternalIndexPointer* external_index_pointer) const {
  while (external_index_pointer->block_num_curr <= block_num) {
    DecodeBlock(external_index_pointer);
  }
}

void ExternalIndexReader::AdvanceChunk(ExternalIndexPointer* external_index_pointer) const {
  // We always advance through every chunk in a block (though we don't decode every chunk in the primary index).
  memcpy(&external_index_pointer->chunk_max_score, external_index_ + external_index_pointer->offset_curr, sizeof(external_index_pointer->chunk_max_score));
  external_index_pointer->offset_curr += 1;
}

// Decodes current block being pointed to by the 'external_index_pointer' and advances the pointer to the next block to be decoded.
void ExternalIndexReader::DecodeBlock(ExternalIndexPointer* external_index_pointer) const {
  assert(sizeof(external_index_pointer->offset_curr) == sizeof(external_index_pointer->block_num_chunks));

  memcpy(&external_index_pointer->block_num_chunks, external_index_ + external_index_pointer->offset_curr, sizeof(external_index_pointer->block_num_chunks));
//  external_index_pointer->block_num_chunks = external_index_[external_index_pointer->offset_curr];

  external_index_pointer->offset_curr += 1;
  memcpy(&external_index_pointer->block_max_score, external_index_ + external_index_pointer->offset_curr, sizeof(external_index_pointer->block_max_score));
//  external_index_pointer->block_max_score = external_index_[external_index_pointer->offset_curr];

  external_index_pointer->offset_curr += 1;
//  external_index_pointer->offset_curr += external_index_pointer->block_num_chunks; // TODO: Only if we won't be decoding chunks.

  external_index_pointer->block_num_curr += 1;
}

/**************************************************************************************************************************************************************
 * ExternalIndexReader::ExternalIndexListData
 *
 **************************************************************************************************************************************************************/
ExternalIndexReader::ExternalIndexPointer::ExternalIndexPointer(uint32_t block_num, uint32_t offset) :
  offset_start(offset),
  offset_curr(offset_start),
  block_num_start(block_num),
  block_num_curr(block_num_start),
  block_num_chunks(0),
  block_max_score(numeric_limits<float>::max()),
  chunk_max_score(numeric_limits<float>::max()) {
}

void ExternalIndexReader::ExternalIndexPointer::Reset() {
  offset_curr = offset_start;
  block_num_curr = block_num_start;
  block_num_chunks = 0;
  block_max_score = numeric_limits<float>::max();
  chunk_max_score = numeric_limits<float>::max();
}
