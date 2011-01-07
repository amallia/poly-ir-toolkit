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
// TODO: IndexBuilder should handle the index meta file routines since most of the parameters come directly from the index builder. Then routines that currently
//       write the index meta file should just call the index builder and pass in any other parameters that they need to write.
//==============================================================================================================================================================

#include "index_build.h"

#include <cerrno>
#include <cstring>

#include <iostream>//TODO
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
const int InvertedListMetaData::kMaxLayers;  // Initialized in the class definition.

InvertedListMetaData::InvertedListMetaData() :
  term_(NULL), term_len_(0), num_layers_(0) {
  Init();
}

InvertedListMetaData::InvertedListMetaData(const char* term, int term_len) :
  term_(new char[term_len]), term_len_(term_len), num_layers_(0) {
  memcpy(term_, term, term_len);
  Init();
}

InvertedListMetaData::~InvertedListMetaData() {
  delete[] term_;
}

void InvertedListMetaData::Init() {
  for (int i = 0; i < kMaxLayers; ++i) {
    num_docs_[i] = 0;
    num_chunks_[i] = 0;
    num_chunks_last_block_[i] = 0;
    num_blocks_[i] = 0;
    block_numbers_[i] = 0;
    chunk_numbers_[i] = 0;
    score_thresholds_[i] = 0.0f;
  }
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
  num_docs_(num_docs),
  num_properties_(num_properties),
  size_(0),
  first_doc_id_(prev_chunk_last_doc_id),
  last_doc_id_(first_doc_id_),
  max_score_(numeric_limits<float>::max()),
  compressed_doc_ids_len_(0),
  compressed_frequencies_len_(0),
  compressed_positions_len_(0) {
  for (int i = 0; i < num_docs_; ++i) {
    // The docIDs are d-gap coded. We need to decode them (and add them to the last docID from the previous chunk) to find the last docID in this chunk.
    last_doc_id_ += doc_ids[i];
  }

  CompressDocIds(doc_ids, num_docs_, doc_id_compressor);
  CompressFrequencies(frequencies, num_docs_, frequency_compressor);
  if (positions != NULL) {
    CompressPositions(positions, num_properties_, position_compressor);
  } else {
    // If the positions weren't included, we must calculate the number of properties based on the frequency values.
    num_properties_ = 0;
    for (int i = 0; i < num_docs_; ++i) {
      num_properties_ += frequencies[i];
    }
  }

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
  block_header_compressor_(block_header_compressor),
  block_header_size_(0),
  num_chunks_(0),
  block_max_score_(-numeric_limits<float>::max()),
  block_data_offset_(0),
  chunk_properties_uncompressed_size_(UncompressedInBufferUpperbound(kChunkPropertiesUpperbound, block_header_compressor_.block_size())),
  chunk_properties_uncompressed_(new uint32_t[chunk_properties_uncompressed_size_]),
  chunk_properties_uncompressed_offset_(0),
  chunk_properties_compressed_len_(0),
  num_block_header_bytes_(0),
  num_doc_ids_bytes_(0),
  num_frequency_bytes_(0),
  num_positions_bytes_(0),
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

  int block_header_size = sizeof(block_header_size_);
  int num_chunks_size = sizeof(num_chunks);
  int block_max_score_size = sizeof(block_max_score_);

  int curr_block_header_size = block_header_size + num_chunks_size + block_max_score_size + CompressedOutBufferUpperbound((kNumHeaderInts * sizeof(uint32_t)));  // Upper bound with 'chunk' included.
  int curr_block_data_size = (block_data_offset_ + chunk.size()) * sizeof(uint32_t);
  int upper_bound_block_size = curr_block_data_size + curr_block_header_size;

  if (upper_bound_block_size > kBlockSize) {
    int curr_chunk_properties_compressed_len = CompressHeader(chunk_properties_uncompressed_, chunk_properties_compressed_, kNumHeaderInts);
    if ((curr_block_data_size + block_header_size + num_chunks_size + block_max_score_size + (curr_chunk_properties_compressed_len * static_cast<int> (sizeof(*chunk_properties_compressed_)))) <= kBlockSize) {
      chunk_properties_compressed_len_ = curr_chunk_properties_compressed_len;
      block_header_size_ = sizeof(num_chunks_) + chunk_properties_compressed_len_ * sizeof(*chunk_properties_compressed_);
    } else {
      // When a single chunk cannot fit into a block we have a problem.
      if (num_chunks == 1) {
        GetErrorLogger().Log(string("A single chunk cannot fit into a block. There are two possible solutions.\n")
            + string("1) Decrease the maximum number of properties per document.\n") + string("2) Increase the block size."), true);
      }

      // Need to recompress the header (without the new chunk properties, since it doesn't fit).
      chunk_properties_compressed_len_ = CompressHeader(chunk_properties_uncompressed_, chunk_properties_compressed_, num_chunks_ * 2);
      block_header_size_ = sizeof(num_chunks_) + chunk_properties_compressed_len_ * sizeof(*chunk_properties_compressed_);
      return false;
    }
  }

  // Chunk fits into this block, so copy chunk data to this block.
  CopyChunkData(chunk);

  // Update the max score in this block.
  if (chunk.max_score() > block_max_score_) {
    block_max_score_ = chunk.max_score();
  }

  ++num_chunks_;
  return true;
}

void BlockEncoder::CopyChunkData(const ChunkEncoder& chunk) {
  const int kWordSize = sizeof(uint32_t);
  const int kBlockSizeWords = kBlockSize / kWordSize;
  if (block_data_offset_ + chunk.size() > kBlockSizeWords)
    assert(false);

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
  block_header_size_ = sizeof(num_chunks_) + sizeof(block_max_score_) + chunk_properties_compressed_len_ * sizeof(*chunk_properties_compressed_);
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

  // Block header size.
  num_bytes = sizeof(block_header_size_);
  assert(block_bytes_offset + num_bytes <= block_bytes_len);
  memcpy(block_bytes + block_bytes_offset, &block_header_size_, num_bytes);
  block_bytes_offset += num_bytes;
  num_block_header_bytes_ += num_bytes;

  // Number of chunks.
  num_bytes = sizeof(num_chunks_);
  assert(block_bytes_offset + num_bytes <= block_bytes_len);
  memcpy(block_bytes + block_bytes_offset, &num_chunks_, num_bytes);
  block_bytes_offset += num_bytes;
  num_block_header_bytes_ += num_bytes;

  // Maximum docID score in the block.
  num_bytes = sizeof(block_max_score_);
  assert(block_bytes_offset + num_bytes <= block_bytes_len);
  memcpy(block_bytes + block_bytes_offset, &block_max_score_, num_bytes);
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
  kBlocksBufferSize(64),
  blocks_buffer_(new BlockEncoder*[kBlocksBufferSize]),
  blocks_buffer_offset_(0),
  curr_block_(new BlockEncoder(block_header_compressor)),
  curr_block_number_(0),
  curr_chunk_number_(0),
  insert_layer_offset_(false),
  index_fd_(open(index_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)),
  kLexiconBufferSize(65536),
  lexicon_(new InvertedListMetaData*[kLexiconBufferSize]),
  lexicon_offset_(0),
  lexicon_fd_(open(lexicon_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)),
  block_header_compressor_(block_header_compressor),
  total_num_chunks_(0),
  total_num_per_term_blocks_(0),
  num_unique_terms_(0),
  posting_count_(0),
  total_num_block_header_bytes_(0),
  total_num_doc_ids_bytes_(0),
  total_num_frequency_bytes_(0),
  total_num_positions_bytes_(0),
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
  ++total_num_chunks_;
  posting_count_ += chunk.num_properties();

  bool block_added = false;
  if (!curr_block_->AddChunk(chunk)) {
    block_added = true;

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

    if (curr_lexicon_entry && curr_lexicon_entry->num_layers() == 0) {
      // Must do this in case we're dealing with a non-layered index.
      // We have to set the number of layers to one before we're done with this lexicon entry.
      // If this was a layered index, the num layers would already be at least one.
      curr_lexicon_entry->increase_curr_layer();
    }

    // If our lexicon buffer is full, dump it to disk.
    if (lexicon_offset_ == kLexiconBufferSize) {
      WriteLexicon();
    }

    // This chunk belongs to a new term, so we have to insert it into the lexicon.
    InvertedListMetaData* new_lexicon_entry = new InvertedListMetaData(term, term_len);
    new_lexicon_entry->set_curr_layer_num_docs(chunk.num_docs());
    new_lexicon_entry->set_curr_layer_offset(curr_block_number_, curr_chunk_number_);
    new_lexicon_entry->increase_curr_layer_num_chunks(1);
    new_lexicon_entry->set_curr_layer_num_chunks_last_block(1);
    new_lexicon_entry->increase_curr_layer_num_blocks(1);
    lexicon_[lexicon_offset_++] = new_lexicon_entry;

    insert_layer_offset_ = false;  // This covers the case when we are making a layered index, but we turned on the flag for the last layer.
                                   // In that case we don't want the 'insert_layer_offset_' flag to stay true for the next term, so we turn it off here.

    ++total_num_per_term_blocks_;  // Every time we add a new term we count it as if a block was added.

    ++num_unique_terms_;  // This really just follows the 'lexicon_offset_'.
  } else {
    if (block_added) {
      ++total_num_per_term_blocks_;  // Increment when we add a new block to an existing inverted list.
      curr_lexicon_entry->set_curr_layer_num_chunks_last_block(0);
      curr_lexicon_entry->increase_curr_layer_num_blocks(1);
    }

    curr_lexicon_entry->increase_curr_layer_num_chunks_last_block(1);

    // Update the existing term in the lexicon with more docs.
    curr_lexicon_entry->increase_curr_layer_num_docs(chunk.num_docs());
    curr_lexicon_entry->increase_curr_layer_num_chunks(1);

    // This means that the current chunk is actually meant to go into a new layer.
    // We now need to set the offsets for it in the lexicon.
    // It's curr layer has already been increased by the call to 'FinalizeLayer()'.
    if (insert_layer_offset_) {
      if (!block_added) {
        ++total_num_per_term_blocks_;  // Every time we add a new layer we count it as if a block was added (but we don't double count if this was also a new block).
        curr_lexicon_entry->increase_curr_layer_num_blocks(1);
      }

      curr_lexicon_entry->set_curr_layer_offset(curr_block_number_, curr_chunk_number_);
      insert_layer_offset_ = false;
    }
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

  // If the last lexicon entry has 0 layers (we're building a standard single layered index), we need to increment it.
  // This is because we always increment lexicon entry for the previous term (if it's still 0) when adding chunks,
  // but we can't do that for the last term.
  assert(lexicon_offset_ > 0);
  InvertedListMetaData* last_lexicon_entry = lexicon_[lexicon_offset_ - 1];
  if (last_lexicon_entry->num_layers() == 0) {
    last_lexicon_entry->increase_curr_layer();
  }

  WriteBlocks();
  WriteLexicon();
}

// Sets the score threshold (the max partial docID score for the current layer) and sets the new current layer.
void IndexBuilder::FinalizeLayer(float score_threshold) {
  assert(lexicon_offset_ > 0);
  InvertedListMetaData* curr_lexicon_entry = lexicon_[lexicon_offset_ - 1];
  curr_lexicon_entry->set_curr_layer_threshold(score_threshold);  // Set the threshold for the current layer.

  curr_lexicon_entry->increase_curr_layer();  // Move on to the next layer now.
  insert_layer_offset_ = true;
}

void IndexBuilder::WriteLexicon() {
  assert(lexicon_fd_ != -1);

  const int kLexiconBufferSize = 16384;
  unsigned char lexicon_data[kLexiconBufferSize];
  int lexicon_data_offset = 0;

  for (int i = 0; i < lexicon_offset_; ++i) {
    InvertedListMetaData* lexicon_entry = lexicon_[i];
    assert(lexicon_entry != NULL);

    // num_layers
    int num_layers = lexicon_entry->num_layers();
    int num_layers_bytes = sizeof(num_layers);

    // term_len
    int term_len = lexicon_entry->term_len();
    int term_len_bytes = sizeof(term_len);

    // term
    const char* term = lexicon_entry->term();
    int term_bytes = term_len;

    // num_docs
    const int* num_docs = lexicon_entry->num_docs();
    int num_docs_bytes = num_layers * sizeof(*num_docs);

    // num_chunks
    const int* num_chunks = lexicon_entry->num_chunks();
    int num_chunks_bytes = num_layers * sizeof(*num_chunks);

    // num_chunks_last_block
    const int* num_chunks_last_block = lexicon_entry->num_chunks_last_block();
    int num_chunks_last_block_bytes = num_layers * sizeof(*num_chunks_last_block);

    // num_blocks
    const int* num_blocks = lexicon_entry->num_blocks();
    int num_blocks_bytes = num_layers * sizeof(*num_blocks);

    // block_numbers
    const int* block_numbers = lexicon_entry->block_numbers();
    int block_numbers_bytes = num_layers * sizeof(*block_numbers);

    // chunk_numbers
    const int* chunk_numbers = lexicon_entry->chunk_numbers();
    int chunk_numbers_bytes = num_layers * sizeof(*chunk_numbers);

    // score_thresholds
    const float* score_thresholds = lexicon_entry->score_thresholds();
    int score_thresholds_bytes = num_layers * sizeof(*score_thresholds);

    int total_bytes = term_len_bytes + term_bytes + num_layers_bytes + num_docs_bytes + num_chunks_bytes + num_chunks_last_block_bytes + num_blocks_bytes
        + block_numbers_bytes + chunk_numbers_bytes + score_thresholds_bytes;
    if(total_bytes > kLexiconBufferSize) {
      // Indicates the term is very long.
      // Probably should prune such long terms in the first place, but...
      // Not very efficient, but this is a very rare occurrence.
      int write_ret;
      // Need to first flush the buffer here so things stay in order.
      write_ret = write(lexicon_fd_, lexicon_data, lexicon_data_offset);
      assert(write_ret == lexicon_data_offset);
      lexicon_data_offset = 0;

      write_ret = write(lexicon_fd_, &num_layers, num_layers_bytes);
      assert(write_ret == num_layers_bytes);

      write_ret = write(lexicon_fd_, &term_len, term_len_bytes);
      assert(write_ret == term_len_bytes);

      write_ret = write(lexicon_fd_, term, term_bytes);
      assert(write_ret == term_bytes);

      write_ret = write(lexicon_fd_, num_docs, num_docs_bytes);
      assert(write_ret == num_docs_bytes);

      write_ret = write(lexicon_fd_, num_chunks, num_chunks_bytes);
      assert(write_ret == num_chunks_bytes);

      write_ret = write(lexicon_fd_, num_chunks_last_block, num_chunks_last_block_bytes);
      assert(write_ret == num_chunks_last_block_bytes);

      write_ret = write(lexicon_fd_, num_blocks, num_blocks_bytes);
      assert(write_ret == num_blocks_bytes);

      write_ret = write(lexicon_fd_, block_numbers, block_numbers_bytes);
      assert(write_ret == block_numbers_bytes);

      write_ret = write(lexicon_fd_, chunk_numbers, chunk_numbers_bytes);
      assert(write_ret == chunk_numbers_bytes);

      write_ret = write(lexicon_fd_, score_thresholds, score_thresholds_bytes);
      assert(write_ret == score_thresholds_bytes);

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

    // num_layers
    memcpy(lexicon_data + lexicon_data_offset, &num_layers, num_layers_bytes);
    lexicon_data_offset += num_layers_bytes;

    // term_len
    memcpy(lexicon_data + lexicon_data_offset, &term_len, term_len_bytes);
    lexicon_data_offset += term_len_bytes;

    // term
    memcpy(lexicon_data + lexicon_data_offset, term, term_bytes);
    lexicon_data_offset += term_bytes;

    // num_docs
    memcpy(lexicon_data + lexicon_data_offset, num_docs, num_docs_bytes);
    lexicon_data_offset += num_docs_bytes;

    // num_chunks
    memcpy(lexicon_data + lexicon_data_offset, num_chunks, num_chunks_bytes);
    lexicon_data_offset += num_chunks_bytes;

    // num_chunks_last_block
    memcpy(lexicon_data + lexicon_data_offset, num_chunks_last_block, num_chunks_last_block_bytes);
    lexicon_data_offset += num_chunks_last_block_bytes;

    // num_blocks
    memcpy(lexicon_data + lexicon_data_offset, num_blocks, num_blocks_bytes);
    lexicon_data_offset += num_blocks_bytes;

    // block_numbers
    memcpy(lexicon_data + lexicon_data_offset, block_numbers, block_numbers_bytes);
    lexicon_data_offset += block_numbers_bytes;

    // chunk_numbers
    memcpy(lexicon_data + lexicon_data_offset, chunk_numbers, chunk_numbers_bytes);
    lexicon_data_offset += chunk_numbers_bytes;

    // score_thresholds
    memcpy(lexicon_data + lexicon_data_offset, score_thresholds, score_thresholds_bytes);
    lexicon_data_offset += score_thresholds_bytes;

    delete lexicon_entry;
  }

  int write_ret = write(lexicon_fd_, lexicon_data, lexicon_data_offset);
  if (write_ret < 0) {
    GetErrorLogger().LogErrno("write() in IndexBuilder::WriteLexicon()", errno, true);
  }
  assert(write_ret == lexicon_data_offset);

  lexicon_offset_ = 0;
}
