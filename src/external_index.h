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
// Some future goals for this class:
// We should be able to store any data here in a generic way. This data could be fixed size or variable size. We cannot use bitmaps to store
// which data is in the index and whether it's fixed size or variable sized (more info on that coming up).
// Fix size data should implicitly store the width of all the elements, while variable sized data should store the length of each element.
// The data we store could be compressed or not.
// Certain data could always be derived from an already existing index (such as block max scores), while certain data (such as positions, contexts, etc)
// cannot be derived, thus it should be mergeable; most likely, this will be per document or per frequency data.
//
// It's probably a good idea to reuse the existing IndexReader and IndexBuilder for per document and per frequency data, since
// we'll already support caching and efficient traversal as well as block compression. The only modification would be to write this data separately
// from the main index (in case some algorithms or the user doesn't need this data).
// This index should really contain summary statistics about the index that will help with traversal. Things like per block statistics, since these are not
// easy to support in our main index (because blocks are shared across lists). These shouldn't consume much space, so they
// could be easily loaded into main memory or provide some basic caching. Furthermore, these statistics should be easily generated from an existing index,
// and so they don't need to be merged.
//==============================================================================================================================================================

#ifndef EXTERNAL_INDEX_H_
#define EXTERNAL_INDEX_H_

// Enables debugging output for this module.
#define EXTERNAL_INDEX_DEBUG

#include <cassert>
#include <cstddef>
#include <cstring>
#include <stdint.h>

#ifdef EXTERNAL_INDEX_DEBUG
#include <iostream>
#endif
#include <vector>

#include <sys/types.h>

/**************************************************************************************************************************************************************
 * ExternalFrequencyData
 *
 * Data stored for every occurrence of a term.
 **************************************************************************************************************************************************************/
class ExternalFrequencyData {
public:
private:
  int dummy_data_;
};

/**************************************************************************************************************************************************************
 * ExternalDocumentData
 *
 * Data stored for every document in the list.
 **************************************************************************************************************************************************************/
class ExternalDocumentData {
public:
private:
  int dummy_data_;

  std::vector<ExternalFrequencyData> frequency_data;
};

/**************************************************************************************************************************************************************
 * ExternalChunk
 *
 **************************************************************************************************************************************************************/
class ExternalChunk {
public:
  void set_chunk_max_score(float chunk_max_score) {
    chunk_max_score_ = chunk_max_score;
  }

  float chunk_max_score() const {
    return chunk_max_score_;
  }

private:
  float chunk_max_score_;

  std::vector<ExternalDocumentData> document_data_;
};

/**************************************************************************************************************************************************************
 * ExternalBlock
 *
 **************************************************************************************************************************************************************/
class ExternalBlock {
public:
  ExternalBlock();
  ~ExternalBlock();

  void AddChunk(const ExternalChunk& chunk) {
    if (chunk.chunk_max_score() > block_max_score_) {
      block_max_score_ = chunk.chunk_max_score();
    }

    external_chunks_.push_back(chunk);
  }

  float block_max_score() const {
    return block_max_score_;
  }

  void set_block_max_score(float block_max_score) {
    block_max_score_ = block_max_score;
  }

  int num_external_chunks() const {
    return external_chunks_.size();
  }

  const ExternalChunk& external_chunk_num(int chunk_num) const {
    return external_chunks_[chunk_num];
  }

private:
  float block_max_score_;

  std::vector<ExternalChunk> external_chunks_;
};

/**************************************************************************************************************************************************************
 * ExternalIndexBuilder
 *
 * We must make sure that we're always integer (4-byte) aligned.
 **************************************************************************************************************************************************************/
class ChunkEncoder;
class ExternalIndexBuilder {
public:
  ExternalIndexBuilder(const char* external_index_filename);
  ~ExternalIndexBuilder();

  // Add a chunk to the current block.
  void AddChunk(const ChunkEncoder& chunk);

  // Start a new current block.
  void NewBlock();

  // Used for both a new layer within a term or for a new term list.
  // Means we also need to start a new block.
  void NewList();

  uint32_t curr_offset() const {
    return curr_offset_;
  }

private:
  int external_index_fd_;

  uint32_t curr_offset_;

  std::vector<ExternalBlock> external_blocks_;
};

/**************************************************************************************************************************************************************
 * ExternalIndexReader
 *
 * The ExternalIndexReader object itself can be shared across threads. ExternalIndexPointer is used to maintain state.
 **************************************************************************************************************************************************************/
class ExternalIndexReader {
public:
  struct ExternalIndexPointer {
    ExternalIndexPointer(uint32_t block_num, uint32_t offset);

    void Reset();

    uint32_t offset_start;  // The integer offset for the list.
    uint32_t offset_curr;   // The integer offset of the next block that needs to be decoded.

    // These are for synchronization with our main index traversal.
    uint32_t block_num_start;  // The starting block number (from the main index) of the list.
    uint32_t block_num_curr;   // The number (from the main index) of the next block that needs to be decoded.

    // The data for the current block.
    int block_num_chunks;
    float block_max_score;
    float chunk_max_score;
  };

  ExternalIndexReader(const char* external_index_filename);
  ~ExternalIndexReader();

  void AdvanceToBlock(uint32_t block_num, ExternalIndexPointer* external_index_pointer) const;

  void AdvanceChunk(ExternalIndexPointer* external_index_pointer) const;

private:
  void DecodeBlock(ExternalIndexPointer* external_index_pointer) const;

  int external_index_fd_;      // File descriptor of the external index.
  off_t external_index_size_;  // Size of the external index in bytes.
  uint32_t* external_index_;   // Pointer to the start of external index file that's memory mapped.
};

#endif /* EXTERNAL_INDEX_H_ */
