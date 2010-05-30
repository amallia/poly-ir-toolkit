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
// TODO: Compress the integer entries in lexicon and front code the terms. Should do this in some sort of block size.
// TODO: Can compress contexts with stuff like pfor delta, since they're all really small ints, but try to do magic bytes with contexts for testing this
//       feature.
//==============================================================================================================================================================

#include "index_build.h"

#include <cerrno>
#include <cstring>

#include <limits>

#include <fcntl.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include "compression_toolkit/pfor_coding.h"
#include "compression_toolkit/rice_coding.h"
#include "compression_toolkit/rice_coding2.h"
#include "compression_toolkit/s9_coding.h"
#include "compression_toolkit/s16_coding.h"
#include "compression_toolkit/vbyte_coding.h"
#include "globals.h"
#include "index_layout_parameters.h"
#include "logger.h"
using namespace std;

/**************************************************************************************************************************************************************
 * InvertedListMetaData
 *
 **************************************************************************************************************************************************************/
InvertedListMetaData::InvertedListMetaData() :
  term_(NULL), term_len_(0), block_number_(0), chunk_number_(0), num_docs_(0) {
}

InvertedListMetaData::InvertedListMetaData(const char* term, int term_len) :
  term_(new char[term_len]), term_len_(term_len), block_number_(0), chunk_number_(0), num_docs_(0) {
  memcpy(term_, term, term_len);
}

InvertedListMetaData::InvertedListMetaData(const char* term, int term_len, int block_number, int chunk_number, int num_docs) :
  term_(new char[term_len]), term_len_(term_len), block_number_(block_number), chunk_number_(chunk_number), num_docs_(num_docs) {
  memcpy(term_, term, term_len);
}

InvertedListMetaData::~InvertedListMetaData() {
  delete[] term_;
}

/**************************************************************************************************************************************************************
 * Chunk
 *
 **************************************************************************************************************************************************************/
const int Chunk::kChunkSize;  // Initialized in the class definition.
const int Chunk::kMaxProperties;  // Initialized in the class definition.

Chunk::Chunk(uint32_t* doc_ids, uint32_t* frequencies, uint32_t* positions, unsigned char* contexts, int num_docs, int num_properties,
             uint32_t prev_chunk_last_doc_id, bool ordered_doc_ids) :
  num_docs_(num_docs), size_(0), last_doc_id_(prev_chunk_last_doc_id), compressed_doc_ids_(NULL), compressed_doc_ids_len_(0), compressed_frequencies_(NULL),
      compressed_frequencies_len_(0), compressed_positions_(NULL), compressed_positions_len_(0), compressed_contexts_(NULL), compressed_contexts_len_(0) {
  // 'ordered_doc_ids' indicates that the doc ids are ordered and we need to use the last doc id from the previous chunk.
  if (ordered_doc_ids) {
    for (int i = 0; i < num_docs; ++i) {
      last_doc_id_ += doc_ids[i];
    }
  } else {
    last_doc_id_ = doc_ids[num_docs - 1];
  }

  //TODO: Here we have to pick the right compress functions to call, or do it by inheritance.
  CompressDocIds(doc_ids, num_docs);
  CompressFrequencies(frequencies, num_docs);
  CompressPositions(positions, num_properties);

  // Calculate total compressed size of this chunk in bytes.
  size_ = compressed_doc_ids_len_ * sizeof(*compressed_doc_ids_) + compressed_frequencies_len_ * sizeof(*compressed_frequencies_) + compressed_positions_len_
      * sizeof(*compressed_positions_) + compressed_contexts_len_ * sizeof(*compressed_contexts_);
}

Chunk::~Chunk() {
  delete[] compressed_doc_ids_;
  delete[] compressed_frequencies_;
  delete[] compressed_positions_;
  delete[] compressed_contexts_;
}

void Chunk::CompressDocIds(uint32_t* doc_ids, int doc_ids_len) {
  assert(doc_ids != NULL && doc_ids_len > 0);

  compressed_doc_ids_ = new uint32_t[doc_ids_len];
  s16_coding compressor;
  compressed_doc_ids_len_ = compressor.Compression(doc_ids, compressed_doc_ids_, doc_ids_len);

  // PForDelta coding with total padding, results in bad compression.
  // Assumption that 'doc_ids []' is of size 'kChunkSize'.
  //for (int i = doc_ids_len; i < kChunkSize; ++i) {
  //  doc_ids[i] = 0;
  //}
  //compressed_doc_ids_ = new uint32_t[kChunkSize + 1];  // +1 for PForDelta metainfo.
  //pfor_coding compressor;
  //compressor.set_size(kChunkSize);
  //compressed_doc_ids_len_ = compressor.Compression(doc_ids, compressed_doc_ids_);
}

void Chunk::CompressFrequencies(uint32_t* frequencies, int frequencies_len) {
  assert(frequencies != NULL && frequencies_len > 0);

  compressed_frequencies_ = new uint32_t[frequencies_len];
  s9_coding compressor;
  compressed_frequencies_len_ = compressor.Compression(frequencies, compressed_frequencies_, frequencies_len);
}

void Chunk::CompressPositions(uint32_t* positions, int positions_len) {
  assert(positions != NULL && positions_len > 0);

  pfor_coding compressor;
  compressor.set_size(kChunkSize);

  int num_whole_chunklets = positions_len / kChunkSize;
  compressed_positions_ = new uint32_t[(num_whole_chunklets + 1) * (kChunkSize + 1)];  // +1 for PForDelta metainfo.

  int compressed_positions_offset = 0;
  int uncompressed_positions_offset = 0;
  while (num_whole_chunklets-- > 0) {
    compressed_positions_len_ += compressor.Compression(positions + uncompressed_positions_offset, compressed_positions_ + compressed_positions_offset, kChunkSize);
    compressed_positions_offset = compressed_positions_len_;
    uncompressed_positions_offset += kChunkSize;
  }

  //TODO: Make sure parameter matches in encoding code (make global setting).
  const int kPadUnless = 100;  // Pad unless we have < kPadUntil documents left to code into a block.

  int positions_left = positions_len % kChunkSize;
  if (positions_left == 0) {
    // Nothing to do here.
  } else if (positions_left < kPadUnless) {
    // Encode leftover portion with a non-blockwise compressor.
    s16_coding leftover_compressor;
    compressed_positions_len_ += leftover_compressor.Compression(positions + uncompressed_positions_offset,
                                                                 compressed_positions_ + compressed_positions_offset, positions_left);
  } else {
    // Encode leftover portion with a blockwise compressor, and pad it to the blocksize.
    // Assumption that 'positions []' is of size ('kChunkSize * kMaxProperties').
    int pad_until = kChunkSize * ((positions_len / kChunkSize) + 1);
    for (int i = positions_len; i < pad_until; ++i) {
      positions[i] = 0;
    }
    compressed_positions_len_ += compressor.Compression(positions + uncompressed_positions_offset, compressed_positions_ + compressed_positions_offset, kChunkSize);
  }
}

/**************************************************************************************************************************************************************
 * Block
 *
 **************************************************************************************************************************************************************/
const int Block::kBlockSize;  // Initialized in the class definition.

Block::Block() :
  block_data_size_(0), chunk_properties_compressed_(NULL), chunk_properties_compressed_len_(0), num_block_header_bytes_(0), num_doc_ids_bytes_(0),
      num_frequency_bytes_(0), num_positions_bytes_(0), num_wasted_space_bytes_(0) {
}

Block::~Block() {
  delete[] chunk_properties_compressed_;
  for (size_t i = 0; i < chunks_.size(); ++i) {
    delete chunks_[i];
  }
}

// Returns true when 'chunk' fits into this block, false otherwise.
// First we calculate a crude upper bound on the size of this block if 'chunk' was included.
// If the upper bound indicates it might not fit, we have to compress the header info with 'chunk' included as well,
// in order to see if it actually fits. The upper bound is done as an optimization, so we don't have to compress the block header
// when adding each additional chunk, but only the last few chunks.
// TODO: Make an assert for when a single chunk cannot fit into a block and output a message that 'Chunk::kMaxProperties'
//       needs to be decreased because it's causing a single chunk to not fit into a block of 'BLOCK_SIZE' bytes (or alternatively increase the block size).
bool Block::AddChunk(const Chunk& chunk) {
  uint32_t num_chunks = chunks_.size() + 1;

  int num_chunks_size = sizeof(num_chunks);
  //TODO: 'curr_block_header_size' changes with PForDelta, because of padding and it uses an additional byte for meta info.
  //TODO: Also would need to change upper bound size of compressed array.
  //TODO: Can improve upper bound by considering the last 'chunk_properties_compressed_len_' and
  //      adding the additional # of chunk properties needed to compress in full 4 byte int representation.
  int curr_block_header_size = num_chunks_size + (2 * num_chunks * sizeof(uint32_t));  // Upper bound with 'chunk' included.
  int curr_block_data_size = block_data_size_ + chunk.size();
  int upper_bound_block_size = curr_block_data_size + curr_block_header_size;
  if (upper_bound_block_size > kBlockSize) {
    uint32_t* curr_chunk_properties_compressed = new uint32_t[2 * num_chunks];  // Upper bound on space for compressed last doc_ids and chunk sizes.
    uint32_t* chunk_properties_uncompressed = new uint32_t[2 * num_chunks];  // Store all pairs of (last doc_id, chunk size).

    for (size_t i = 0, j = 0; i < chunks_.size(); ++i, j += 2) {
      chunk_properties_uncompressed[j] = ConvertChunkLastDocId(chunks_[i]->last_doc_id());
      chunk_properties_uncompressed[j + 1] = ConvertChunkSize(chunks_[i]->size());
    }
    chunk_properties_uncompressed[2 * num_chunks - 2] = ConvertChunkLastDocId(chunk.last_doc_id());
    chunk_properties_uncompressed[2 * num_chunks - 1] = ConvertChunkSize(chunk.size());

    int curr_chunk_properties_compressed_len = CompressHeader(chunk_properties_uncompressed, curr_chunk_properties_compressed, 2 * num_chunks);

    delete[] chunk_properties_uncompressed;

    if ((curr_block_data_size + num_chunks_size + curr_chunk_properties_compressed_len * static_cast<int>(sizeof(*curr_chunk_properties_compressed))) <= kBlockSize) {
      delete[] chunk_properties_compressed_;
      chunk_properties_compressed_ = curr_chunk_properties_compressed;
      chunk_properties_compressed_len_ = curr_chunk_properties_compressed_len;
    } else {
      delete [] curr_chunk_properties_compressed;
      return false;
    }
  }

  chunks_.push_back(&chunk);
  block_data_size_ += chunk.size();
  return true;
}

void Block::Finalize() {
  delete[] chunk_properties_compressed_;
  chunk_properties_compressed_ = new uint32_t[2 * chunks_.size()];  // Upper bound on space for compressed last doc_ids and chunk sizes.
  uint32_t* chunk_properties_uncompressed = new uint32_t[2 * chunks_.size()];  // Store all pairs of (last doc_id, chunk size).

  for (size_t i = 0, j = 0; i < chunks_.size(); ++i, j += 2) {
    chunk_properties_uncompressed[j] = ConvertChunkLastDocId(chunks_[i]->last_doc_id());
    chunk_properties_uncompressed[j + 1] = ConvertChunkSize(chunks_[i]->size());
  }
  chunk_properties_compressed_len_ = CompressHeader(chunk_properties_uncompressed, chunk_properties_compressed_, 2 * chunks_.size());
  delete[] chunk_properties_uncompressed;
}

// TODO: Allow to choose type of coder and padding policy (need to make sure 'in' is big enough to be padded until the chosen block size).
int Block::CompressHeader(uint32_t* in, uint32_t* out, int n) {
  s16_coding compressor;
  return compressor.Compression(in, out, n);
}

// 'block_bytes_len' must be block sized.
void Block::GetBlockBytes(unsigned char* block_bytes, int block_bytes_len) {
  if (chunk_properties_compressed_ == NULL) {
    Finalize();
  }
  assert(chunk_properties_compressed_ != NULL && chunk_properties_compressed_len_ > 0);
  assert(block_bytes_len == kBlockSize);

  int block_bytes_offset = 0;
  int num_bytes;

  // Number of chunks.
  uint32_t num_chunks = chunks_.size();
  num_bytes = sizeof(num_chunks);
  assert(block_bytes_offset + num_bytes <= block_bytes_len);
  memcpy(block_bytes + block_bytes_offset, &num_chunks, num_bytes);
  block_bytes_offset += num_bytes;
  num_block_header_bytes_ += num_bytes;

  // Block header.
  num_bytes = chunk_properties_compressed_len_ * sizeof(*chunk_properties_compressed_);
  assert(block_bytes_offset + num_bytes <= block_bytes_len);
  memcpy(block_bytes + block_bytes_offset, chunk_properties_compressed_, num_bytes);
  block_bytes_offset += num_bytes;
  num_block_header_bytes_ += num_bytes;

  for (size_t i = 0; i < chunks_.size(); ++i) {
    // DocIDs.
    const uint32_t* doc_ids = chunks_[i]->compressed_doc_ids();
    int doc_ids_len = chunks_[i]->compressed_doc_ids_len();
    num_bytes = doc_ids_len * sizeof(*doc_ids);
    assert(block_bytes_offset + num_bytes <= block_bytes_len);
    memcpy(block_bytes + block_bytes_offset, doc_ids, num_bytes);
    block_bytes_offset += num_bytes;
    num_doc_ids_bytes_ += num_bytes;

    // Frequencies.
    const uint32_t* frequencies = chunks_[i]->compressed_frequencies();
    int frequencies_len = chunks_[i]->compressed_frequencies_len();
    num_bytes = frequencies_len * sizeof(*frequencies);
    assert(block_bytes_offset + num_bytes <= block_bytes_len);
    memcpy(block_bytes + block_bytes_offset, frequencies, num_bytes);
    block_bytes_offset += num_bytes;
    num_frequency_bytes_ += num_bytes;

    // Positions.
    const uint32_t* positions = chunks_[i]->compressed_positions();
    int positions_len = chunks_[i]->compressed_positions_len();
    num_bytes = positions_len * sizeof(*positions);
    assert(block_bytes_offset + num_bytes <= block_bytes_len);
    memcpy(block_bytes + block_bytes_offset, positions, num_bytes);
    block_bytes_offset += num_bytes;
    num_positions_bytes_ += num_bytes;
  }

  // Fill remaining wasted space with 0s.
  num_bytes = block_bytes_len - block_bytes_offset;
  memset(block_bytes + block_bytes_offset, 0, num_bytes);
  num_wasted_space_bytes_ += num_bytes;
}

/**************************************************************************************************************************************************************
 * IndexBuilder
 *
 **************************************************************************************************************************************************************/
IndexBuilder::IndexBuilder(const char* lexicon_filename, const char* index_filename) :
  kBlocksBufferSize(64), blocks_buffer_(new Block*[kBlocksBufferSize]), blocks_buffer_offset_(0), curr_block_(new Block()), curr_block_number_(0),
      curr_chunk_number_(0), kIndexFilename(index_filename),
      index_fd_(open(kIndexFilename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)), kLexiconBufferSize(65536),
      lexicon_(new InvertedListMetaData*[kLexiconBufferSize]), lexicon_offset_(0), lexicon_fd_(open(lexicon_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR
          | S_IWUSR | S_IRGRP | S_IROTH)), total_num_block_header_bytes_(0), total_num_doc_ids_bytes_(0), total_num_frequency_bytes_(0),
      total_num_positions_bytes_(0), total_num_wasted_space_bytes_(0) {
  if (index_fd_ == -1) {
    GetErrorLogger().LogErrno("open(), trying to open index for writing", errno, true);
  }

  if (lexicon_fd_ == -1) {
    GetErrorLogger().LogErrno("open(), trying to open lexicon for writing", errno, true);
  }
}

IndexBuilder::~IndexBuilder() {
  delete[] blocks_buffer_;
  delete[] lexicon_;

  int ret;
  ret = close(index_fd_);
  assert(ret != -1);

  ret = close(lexicon_fd_);
  assert(ret != -1);
}

void IndexBuilder::Add(const Chunk& chunk, const char* term, int term_len) {
  assert(term != NULL && term_len > 0);

  if (!curr_block_->AddChunk(chunk)) {
    // Reset chunk counter and increase block counter.
    ++curr_block_number_;
    curr_chunk_number_ = 0;

    blocks_buffer_[blocks_buffer_offset_++] = curr_block_;
    curr_block_ = new Block();
    bool added_chunk = curr_block_->AddChunk(chunk);
    assert(added_chunk == true);
    if (blocks_buffer_offset_ == kBlocksBufferSize) {
      WriteBlocks();
      blocks_buffer_offset_ = 0;
    }
  }

  InvertedListMetaData* curr_lexicon_entry = lexicon_offset_ == 0 ? NULL : lexicon_[lexicon_offset_ - 1];
  if (curr_lexicon_entry == NULL || (curr_lexicon_entry->term_len() != term_len
      || strncasecmp(curr_lexicon_entry->term(), term, curr_lexicon_entry->term_len()) != 0)) {
    // If our lexicon buffer is full, dump it to disk.
    if (lexicon_offset_ == kLexiconBufferSize) {
      WriteLexicon();
    }

    // This chunk belongs to a new term, so we have to insert it into the lexicon.
    lexicon_[lexicon_offset_++] = new InvertedListMetaData(term, term_len, curr_block_number_, curr_chunk_number_, chunk.num_docs());
  } else {
    // Update the existing term in the lexicon with more docs.
    curr_lexicon_entry->UpdateNumDocs(chunk.num_docs());
  }

  ++curr_chunk_number_;
}

void IndexBuilder::WriteBlocks() {
  unsigned char block_bytes[Block::kBlockSize];

  for (int i = 0; i < blocks_buffer_offset_; ++i) {
    if (blocks_buffer_[i]->num_chunks() > 0) {
      blocks_buffer_[i]->GetBlockBytes(block_bytes, Block::kBlockSize);

      // Update statistics on byte breakdown of index.
      total_num_block_header_bytes_ += blocks_buffer_[i]->num_block_header_bytes();
      total_num_doc_ids_bytes_ += blocks_buffer_[i]->num_doc_ids_bytes();
      total_num_frequency_bytes_ += blocks_buffer_[i]->num_frequency_bytes();
      total_num_positions_bytes_ += blocks_buffer_[i]->num_positions_bytes();
      total_num_wasted_space_bytes_ += blocks_buffer_[i]->num_wasted_space_bytes();

      int ret = write(index_fd_, block_bytes, Block::kBlockSize);
      assert(ret != -1);
      delete blocks_buffer_[i];
    }
  }
}

void IndexBuilder::Finalize() {
  assert(blocks_buffer_offset_ < kBlocksBufferSize);
  blocks_buffer_[blocks_buffer_offset_++] = curr_block_;
  curr_block_ = NULL;

  WriteBlocks();
  WriteLexicon();
}

void IndexBuilder::WriteLexicon() {
  assert(lexicon_fd_ != -1);

  const int kLexiconBufferSize = 16384;
  unsigned char lexicon_data[kLexiconBufferSize];
  int lexicon_data_offset = 0;

  for (int i = 0; i < lexicon_offset_; ++i) {
    InvertedListMetaData* lexicon_entry = lexicon_[i];

    // term_len
    int term_len = lexicon_entry->term_len();
    int term_len_bytes = sizeof(term_len);

    // term
    const char* term = lexicon_entry->term();
    int term_bytes = term_len;

    // block_number
    int block_number = lexicon_entry->block_number();
    int block_number_bytes = sizeof(block_number);

    // chunk_number
    int chunk_number = lexicon_entry->chunk_number();
    int chunk_number_bytes = sizeof(chunk_number);

    // num_docs
    int num_docs = lexicon_entry->num_docs();
    int num_docs_bytes = sizeof(num_docs);

    int total_bytes = term_len_bytes + term_bytes + block_number_bytes + chunk_number_bytes + num_docs_bytes;

    if(total_bytes > kLexiconBufferSize) {
      // Indicates the term is very long.
      // Probably should prune such long terms in the first place, but...
      // Not very efficient, but this is a very rare occurrence.
      int write_ret;
      // Need to first flush the buffer here so things stay in order.
      write_ret = write(lexicon_fd_, lexicon_data, lexicon_data_offset);
      assert(write_ret == lexicon_data_offset);
      lexicon_data_offset = 0;

      write_ret = write(lexicon_fd_, &term_len, term_len_bytes);
      assert(write_ret == term_len_bytes);

      write_ret = write(lexicon_fd_, term, term_bytes);
      assert(write_ret == term_bytes);

      write_ret = write(lexicon_fd_, &block_number, block_number_bytes);
      assert(write_ret == block_number_bytes);

      write_ret = write(lexicon_fd_, &chunk_number, chunk_number_bytes);
      assert(write_ret == chunk_number_bytes);

      write_ret = write(lexicon_fd_, &num_docs, num_docs_bytes);
      assert(write_ret == num_docs_bytes);

      continue;
    }

    if (lexicon_data_offset + total_bytes > kLexiconBufferSize) {
      int write_ret = write(lexicon_fd_, lexicon_data, lexicon_data_offset);
      assert(write_ret == lexicon_data_offset);
      lexicon_data_offset = 0;
      --i;  // Need to rerun this iteration, since now we made room in our buffer.
      continue;
    }

    // term_len
    memcpy(lexicon_data + lexicon_data_offset, &term_len, term_len_bytes);
    lexicon_data_offset += term_len_bytes;

    // term
    memcpy(lexicon_data + lexicon_data_offset, term, term_bytes);
    lexicon_data_offset += term_bytes;

    // block_number
    memcpy(lexicon_data + lexicon_data_offset, &block_number, block_number_bytes);
    lexicon_data_offset += block_number_bytes;

    // chunk_number
    memcpy(lexicon_data + lexicon_data_offset, &chunk_number, chunk_number_bytes);
    lexicon_data_offset += chunk_number_bytes;

    // num_docs
    memcpy(lexicon_data + lexicon_data_offset, &num_docs, num_docs_bytes);
    lexicon_data_offset += num_docs_bytes;

    delete lexicon_entry;
  }

  int write_ret = write(lexicon_fd_, lexicon_data, lexicon_data_offset);
  assert(write_ret == lexicon_data_offset);

  lexicon_offset_ = 0;
}
