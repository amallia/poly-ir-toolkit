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
// TODO: Can compress contexts with stuff like PForDelta, since they're all really small integers, but try to do magic bytes with contexts for testing this
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

#include "config_file_properties.h"
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
 * ChunkEncoder
 *
 **************************************************************************************************************************************************************/
const int ChunkEncoder::kChunkSize;      // Initialized in the class definition.
const int ChunkEncoder::kMaxProperties;  // Initialized in the class definition.

ChunkEncoder::ChunkEncoder(uint32_t* doc_ids, uint32_t* frequencies, uint32_t* positions, unsigned char* contexts, int num_docs, int num_properties,
                       uint32_t prev_chunk_last_doc_id, const CodingPolicy& doc_id_compressor, const CodingPolicy& frequency_compressor,
                       const CodingPolicy& position_compressor) :
  num_docs_(num_docs), num_properties_(num_properties), size_(0), first_doc_id_(prev_chunk_last_doc_id), last_doc_id_(first_doc_id_),
      compressed_doc_ids_len_(0), compressed_frequencies_len_(0), compressed_positions_len_(0) {
  for (int i = 0; i < num_docs; ++i) {
    // The docIDs are d-gap coded. We need to decode them (and add them to the last docID from the previous chunk) to find the last docID in this chunk.
    last_doc_id_ += doc_ids[i];
  }

  CompressDocIds(doc_ids, num_docs, doc_id_compressor);
  CompressFrequencies(frequencies, num_docs, frequency_compressor);
  if (positions != NULL)
    CompressPositions(positions, num_properties_, position_compressor);

  // Calculate total compressed size of this chunk in words.
  size_ = compressed_doc_ids_len_ + compressed_frequencies_len_ + compressed_positions_len_;
}

void ChunkEncoder::CompressDocIds(uint32_t* doc_ids, int doc_ids_len, const CodingPolicy& doc_id_compressor) {
  assert(doc_ids != NULL && doc_ids_len > 0);
  assert(doc_ids_len <= ChunkEncoder::kChunkSize);

  if (doc_id_compressor.primary_coder_is_blockwise())
    assert(doc_id_compressor.block_size() == kChunkSize);

  compressed_doc_ids_len_ = doc_id_compressor.Compress(doc_ids, compressed_doc_ids_, doc_ids_len);
}

void ChunkEncoder::CompressFrequencies(uint32_t* frequencies, int frequencies_len, const CodingPolicy& frequency_compressor) {
  assert(frequencies != NULL && frequencies_len > 0);
  assert(frequencies_len <= ChunkEncoder::kChunkSize);

  if (frequency_compressor.primary_coder_is_blockwise())
    assert(frequency_compressor.block_size() == kChunkSize);

  compressed_frequencies_len_ = frequency_compressor.Compress(frequencies, compressed_frequencies_, frequencies_len);
}

void ChunkEncoder::CompressPositions(uint32_t* positions, int positions_len, const CodingPolicy& position_compressor) {
  assert(positions != NULL && positions_len > 0);
  assert(positions_len <= ChunkEncoder::kChunkSize * ChunkEncoder::kMaxProperties);

  if (position_compressor.primary_coder_is_blockwise())
    assert(position_compressor.block_size() >= kChunkSize);

  compressed_positions_len_ = position_compressor.Compress(positions, compressed_positions_, positions_len);
}

/**************************************************************************************************************************************************************
 * BlockEncoder
 *
 **************************************************************************************************************************************************************/
const int BlockEncoder::kBlockSize;                            // Initialized in the class definition.
const int BlockEncoder::kChunkSizeLowerBound;                  // Initialized in the class definition.
const int BlockEncoder::kChunkPropertiesUpperbound;            // Initialized in the class definition.
const int BlockEncoder::kChunkPropertiesCompressedUpperbound;  // Initialized in the class definition.

BlockEncoder::BlockEncoder(const CodingPolicy& block_header_compressor) :
  block_header_compressor_(block_header_compressor), num_chunks_(0), block_data_offset_(0),
      chunk_properties_uncompressed_size_(UncompressedInBufferUpperbound(kChunkPropertiesUpperbound, block_header_compressor_.block_size())),
      chunk_properties_uncompressed_(new uint32_t[chunk_properties_uncompressed_size_]), chunk_properties_uncompressed_offset_(0),
      chunk_properties_compressed_len_(0), num_block_header_bytes_(0), num_doc_ids_bytes_(0), num_frequency_bytes_(0), num_positions_bytes_(0),
      num_wasted_space_bytes_(0) {
}

BlockEncoder::~BlockEncoder() {
  delete[] chunk_properties_uncompressed_;
}

// Returns true when 'chunk' fits into this block, false otherwise.
// First we calculate a crude upper bound on the size of this block if 'chunk' was included.
// If the upper bound indicates it might not fit, we have to compress the header info with 'chunk' included as well,
// in order to see if it actually fits. The upper bound is done as an optimization, so we don't have to compress the block header
// when adding each additional chunk, but only the last few chunks.
bool BlockEncoder::AddChunk(const ChunkEncoder& chunk) {
  uint32_t num_chunks = num_chunks_ + 1;

  // Store the next pair of (last doc_id, chunk size).
  int chunk_last_doc_id_idx = 2 * num_chunks - 2;
  int chunk_size_idx = 2 * num_chunks - 1;
  assert(chunk_last_doc_id_idx < chunk_properties_uncompressed_size_);
  assert(chunk_size_idx < chunk_properties_uncompressed_size_);
  chunk_properties_uncompressed_[chunk_last_doc_id_idx] = chunk.last_doc_id();
  chunk_properties_uncompressed_[chunk_size_idx] = chunk.size();

  const int kNumHeaderInts = 2 * num_chunks;

  int num_chunks_size = sizeof(num_chunks);
  int curr_block_header_size = num_chunks_size + CompressedOutBufferUpperbound((kNumHeaderInts * sizeof(uint32_t)));  // Upper bound with 'chunk' included.
  int curr_block_data_size = (block_data_offset_ + chunk.size()) * sizeof(uint32_t);
  int upper_bound_block_size = curr_block_data_size + curr_block_header_size;

  if (upper_bound_block_size > kBlockSize) {
    int curr_chunk_properties_compressed_len = CompressHeader(chunk_properties_uncompressed_, chunk_properties_compressed_, kNumHeaderInts);

    if ((curr_block_data_size + num_chunks_size + (curr_chunk_properties_compressed_len * static_cast<int> (sizeof(*chunk_properties_compressed_)))) <= kBlockSize) {
      chunk_properties_compressed_len_ = curr_chunk_properties_compressed_len;
    } else {
      // TODO: Test this condition.
      // When a single chunk cannot fit into a block we have a problem.
      if (num_chunks == 1) {
        GetErrorLogger().Log(string("A single chunk cannot fit into a block. There are two possible solutions.\n")
            + string("1) Decrease the maximum number of properties per document.\n") + string("2) Increase the block size."), true);
      }

      // Need to recompress the header (without the new chunk properties, since it doesn't fit).
      chunk_properties_compressed_len_ = CompressHeader(chunk_properties_uncompressed_, chunk_properties_compressed_, num_chunks_ * 2);
      return false;
    }
  }

  // Chunk fits into this block, so copy chunk data to this block.
  CopyChunkData(chunk);
  ++num_chunks_;
  return true;
}

void BlockEncoder::CopyChunkData(const ChunkEncoder& chunk) {
  const int kWordSize = sizeof(uint32_t);
  const int kBlockSizeWords = kBlockSize / kWordSize;
  assert(block_data_offset_ + chunk.size() <= kBlockSizeWords);

  int num_bytes;
  int num_words;

  // DocIDs.
  const uint32_t* doc_ids = chunk.compressed_doc_ids();
  int doc_ids_len = chunk.compressed_doc_ids_len();
  num_bytes = doc_ids_len * sizeof(*doc_ids);
  assert(num_bytes % kWordSize == 0);
  num_words = num_bytes / kWordSize;
  assert(block_data_offset_ + num_words <= kBlockSizeWords);
  if (doc_ids_len != 0) {
    memcpy(block_data_ + block_data_offset_, doc_ids, num_bytes);
    block_data_offset_ += num_words;
    num_doc_ids_bytes_ += num_bytes;
  } else {
    assert(false);  // DocIDs should always be present.
  }

  // Frequencies.
  const uint32_t* frequencies = chunk.compressed_frequencies();
  int frequencies_len = chunk.compressed_frequencies_len();
  num_bytes = frequencies_len * sizeof(*frequencies);
  assert(num_bytes % kWordSize == 0);
  num_words = num_bytes / kWordSize;
  assert(block_data_offset_ + num_words <= kBlockSizeWords);
  if (frequencies_len != 0) {
    memcpy(block_data_ + block_data_offset_, frequencies, num_bytes);
    block_data_offset_ += num_words;
    num_frequency_bytes_ += num_bytes;
  } else {
    assert(false);  // Frequencies should always be present.
  }

  // Positions.
  const uint32_t* positions = chunk.compressed_positions();
  int positions_len = chunk.compressed_positions_len();
  num_bytes = positions_len * sizeof(*positions);
  assert(num_bytes % kWordSize == 0);
  num_words = num_bytes / kWordSize;
  assert(block_data_offset_ + num_words <= kBlockSizeWords);
  if (positions_len != 0) {
    memcpy(block_data_ + block_data_offset_, positions, num_bytes);
    block_data_offset_ += num_words;
    num_positions_bytes_ += num_bytes;
  } else {
    // If we don't have positions, then we're building an index without them.
  }
}

void BlockEncoder::Finalize() {
  const int kNumHeaderInts = 2 * num_chunks_;
  chunk_properties_compressed_len_ = CompressHeader(chunk_properties_uncompressed_, chunk_properties_compressed_, kNumHeaderInts);
}

// The 'header' array size needs to be a multiple of the block size used by the block header compressor.
int BlockEncoder::CompressHeader(uint32_t* header, uint32_t* output, int header_len) {
  return block_header_compressor_.Compress(header, output, header_len);
}

// 'block_bytes_len' must be block sized.
void BlockEncoder::GetBlockBytes(unsigned char* block_bytes, int block_bytes_len) {
  if (chunk_properties_compressed_len_ == 0) {
    Finalize();
  }
  assert(chunk_properties_compressed_len_ > 0);
  assert(block_bytes_len == kBlockSize);

  int block_bytes_offset = 0;
  int num_bytes;

  // Number of chunks.
  num_bytes = sizeof(num_chunks_);
  assert(block_bytes_offset + num_bytes <= block_bytes_len);
  memcpy(block_bytes + block_bytes_offset, &num_chunks_, num_bytes);
  block_bytes_offset += num_bytes;
  num_block_header_bytes_ += num_bytes;

  // Block header.
  num_bytes = chunk_properties_compressed_len_ * sizeof(*chunk_properties_compressed_);
  assert(block_bytes_offset + num_bytes <= block_bytes_len);
  memcpy(block_bytes + block_bytes_offset, chunk_properties_compressed_, num_bytes);
  block_bytes_offset += num_bytes;
  num_block_header_bytes_ += num_bytes;

  // Compressed chunk data.
  num_bytes = block_data_offset_ * sizeof(*block_data_);
  assert(block_bytes_offset + num_bytes <= block_bytes_len);
  memcpy(block_bytes + block_bytes_offset, block_data_, num_bytes);
  block_bytes_offset += num_bytes;

  // Fill remaining wasted space with 0s.
  num_bytes = block_bytes_len - block_bytes_offset;
  memset(block_bytes + block_bytes_offset, 0, num_bytes);
  num_wasted_space_bytes_ += num_bytes;
}

/**************************************************************************************************************************************************************
 * IndexBuilder
 *
 * TODO: Don't need to create new BlockEncoders every time. Just allocate array of BlockEncoders that you can then reset.
 **************************************************************************************************************************************************************/
IndexBuilder::IndexBuilder(const char* lexicon_filename, const char* index_filename, const CodingPolicy& block_header_compressor) :
  kBlocksBufferSize(64), blocks_buffer_(new BlockEncoder*[kBlocksBufferSize]), blocks_buffer_offset_(0), curr_block_(new BlockEncoder(block_header_compressor)),
      curr_block_number_(0), curr_chunk_number_(0), kIndexFilename(index_filename),
      index_fd_(open(kIndexFilename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)), kLexiconBufferSize(65536),
      lexicon_(new InvertedListMetaData*[kLexiconBufferSize]), lexicon_offset_(0),
      lexicon_fd_(open(lexicon_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)),
      block_header_compressor_(block_header_compressor), num_unique_terms_(0), posting_count_(0),
      total_num_block_header_bytes_(0), total_num_doc_ids_bytes_(0), total_num_frequency_bytes_(0), total_num_positions_bytes_(0),
      total_num_wasted_space_bytes_(0) {
  if (index_fd_ < 0) {
    GetErrorLogger().LogErrno("open() in IndexBuilder::IndexBuilder(), trying to open index for writing", errno, true);
  }

  if (lexicon_fd_ < 0) {
    GetErrorLogger().LogErrno("open() in IndexBuilder::IndexBuilder(), trying to open lexicon for writing", errno, true);
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

void IndexBuilder::Add(const ChunkEncoder& chunk, const char* term, int term_len) {
  assert(term != NULL && term_len > 0);

  // Update index statistics.
  posting_count_ += chunk.num_properties();

  if (!curr_block_->AddChunk(chunk)) {
    // Reset chunk counter and increase block counter.
    ++curr_block_number_;
    curr_chunk_number_ = 0;

    blocks_buffer_[blocks_buffer_offset_++] = curr_block_;
    curr_block_ = new BlockEncoder(block_header_compressor_);
    bool added_chunk = curr_block_->AddChunk(chunk);
    if (!added_chunk)
      assert(false);
    if (blocks_buffer_offset_ == kBlocksBufferSize) {
      WriteBlocks();
      blocks_buffer_offset_ = 0;
    }
  }

  InvertedListMetaData* curr_lexicon_entry = ((lexicon_offset_ == 0) ? NULL : lexicon_[lexicon_offset_ - 1]);
  if (curr_lexicon_entry == NULL || (curr_lexicon_entry->term_len() != term_len
      || strncasecmp(curr_lexicon_entry->term(), term, curr_lexicon_entry->term_len()) != 0)) {
    // If our lexicon buffer is full, dump it to disk.
    if (lexicon_offset_ == kLexiconBufferSize) {
      WriteLexicon();
    }

    // This chunk belongs to a new term, so we have to insert it into the lexicon.
    lexicon_[lexicon_offset_++] = new InvertedListMetaData(term, term_len, curr_block_number_, curr_chunk_number_, chunk.num_docs());
    ++num_unique_terms_;  // This really just follows the 'lexicon_offset_'.
  } else {
    // Update the existing term in the lexicon with more docs.
    curr_lexicon_entry->UpdateNumDocs(chunk.num_docs());
  }

  ++curr_chunk_number_;
}

void IndexBuilder::WriteBlocks() {
  unsigned char block_bytes[BlockEncoder::kBlockSize];

  for (int i = 0; i < blocks_buffer_offset_; ++i) {
    if (blocks_buffer_[i]->num_chunks() > 0) {
      blocks_buffer_[i]->GetBlockBytes(block_bytes, BlockEncoder::kBlockSize);

      // Update statistics on byte breakdown of index.
      total_num_block_header_bytes_ += blocks_buffer_[i]->num_block_header_bytes();
      total_num_doc_ids_bytes_ += blocks_buffer_[i]->num_doc_ids_bytes();
      total_num_frequency_bytes_ += blocks_buffer_[i]->num_frequency_bytes();
      total_num_positions_bytes_ += blocks_buffer_[i]->num_positions_bytes();
      total_num_wasted_space_bytes_ += blocks_buffer_[i]->num_wasted_space_bytes();

      int write_ret = write(index_fd_, block_bytes, BlockEncoder::kBlockSize);
      if (write_ret < 0) {
        GetErrorLogger().LogErrno("write() in IndexBuilder::WriteBlocks()", errno, true);
      }
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
      if (write_ret < 0) {
        GetErrorLogger().LogErrno("write() in IndexBuilder::WriteLexicon()", errno, true);
      }
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
  if (write_ret < 0) {
    GetErrorLogger().LogErrno("write() in IndexBuilder::WriteLexicon()", errno, true);
  }
  assert(write_ret == lexicon_data_offset);

  lexicon_offset_ = 0;
}
