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

#include "index_reader.h"

#include <cerrno>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <iostream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "config_file_properties.h"
#include "configuration.h"
#include "globals.h"
#include "logger.h"
#include "meta_file_properties.h"
#include "timer.h"
using namespace std;

/**************************************************************************************************************************************************************
 * ChunkDecoder
 *
 **************************************************************************************************************************************************************/
const int ChunkDecoder::kChunkSize;      // Initialized in the class definition.
const int ChunkDecoder::kMaxProperties;  // Initialized in the class definition.

ChunkDecoder::ChunkDecoder(const CodingPolicy& doc_id_decompressor, const CodingPolicy& frequency_decompressor,
                           const CodingPolicy& position_decompressor) :
  num_docs_(0),
  curr_document_offset_(0),
  prev_document_offset_(0),
  curr_position_offset_(0),
  prev_decoded_doc_id_(0),
  num_positions_(0),
  decoded_properties_(false),
  curr_buffer_position_(NULL),
  decoded_(false),
  doc_id_decompressor_(doc_id_decompressor),
  frequency_decompressor_(frequency_decompressor),
  position_decompressor_(position_decompressor) {
}

void ChunkDecoder::InitChunk(const ChunkProperties& properties, const uint32_t* buffer, int num_docs) {
  properties_ = properties;
  num_docs_ = num_docs;
  curr_document_offset_ = 0;
  prev_document_offset_ = 0;
  curr_position_offset_ = 0;
  prev_decoded_doc_id_ = 0;
  decoded_properties_ = false;
  curr_buffer_position_ = buffer;
  decoded_ = false;
}

void ChunkDecoder::DecodeDocIds() {
  assert(curr_buffer_position_ != NULL);
  if (doc_id_decompressor_.primary_coder_is_blockwise())
    assert(doc_id_decompressor_.block_size() == kChunkSize);

  // A word is meant to be sizeof(uint32_t) bytes in this context.
  int num_words_consumed = doc_id_decompressor_.Decompress(const_cast<uint32_t*> (curr_buffer_position_), doc_ids_, num_docs_);
  curr_buffer_position_ += num_words_consumed;
  decoded_ = true;
}

void ChunkDecoder::DecodeProperties() {
  curr_buffer_position_ += DecodeFrequencies(curr_buffer_position_);
  if (properties_.includes_positions)
    curr_buffer_position_ += DecodePositions(curr_buffer_position_);

  curr_buffer_position_ = NULL;  // This pointer is no longer valid.
  decoded_properties_ = true;
}

int ChunkDecoder::DecodeFrequencies(const uint32_t* compressed_frequencies) {
  assert(compressed_frequencies != NULL);
  // A word is meant to be sizeof(uint32_t) bytes in this context.
  if (frequency_decompressor_.primary_coder_is_blockwise())
    assert(frequency_decompressor_.block_size() == kChunkSize);

  int num_words_consumed = frequency_decompressor_.Decompress(const_cast<uint32_t*> (compressed_frequencies), frequencies_, num_docs_);
  return num_words_consumed;
}

int ChunkDecoder::DecodePositions(const uint32_t* compressed_positions) {
  assert(compressed_positions != NULL);

  num_positions_ = 0;
  // Count the number of positions we have in total by summing up all the frequencies.
  for (int i = 0; i < num_docs_; ++i) {
    assert(frequencies_[i] != 0);  // This condition indicates a bug in the program.
    num_positions_ += frequencies_[i];
  }

  assert(num_positions_ <= (ChunkDecoder::kChunkSize * ChunkDecoder::kMaxProperties));
  if (position_decompressor_.primary_coder_is_blockwise())
    assert(position_decompressor_.block_size() >= kChunkSize);

  // A word is meant to be sizeof(uint32_t) bytes in this context.
  int num_words_consumed = position_decompressor_.Decompress(const_cast<uint32_t*> (compressed_positions), positions_, num_positions_);
  return num_words_consumed;
}

void ChunkDecoder::UpdatePropertiesOffset() {
  assert(decoded_properties_ == true);
  for (int i = prev_document_offset_; i < curr_document_offset_; ++i) {
    assert(frequencies_[i] != 0);  // This indicates a bug in the program.
    curr_position_offset_ += frequencies_[i];
  }
  prev_document_offset_ = curr_document_offset_;
}

/**************************************************************************************************************************************************************
 * BlockDecoder
 *
 **************************************************************************************************************************************************************/
const int BlockDecoder::kBlockSize;                              // Initialized in the class definition.
const int BlockDecoder::kChunkSizeLowerBound;                    // Initialized in the class definition.
const int BlockDecoder::kChunkPropertiesDecompressedUpperbound;  // Initialized in the class definition.

BlockDecoder::BlockDecoder(const CodingPolicy& doc_id_decompressor, const CodingPolicy& frequency_decompressor, const CodingPolicy& position_decompressor,
                           const CodingPolicy& block_header_decompressor) :
  curr_block_num_(0),
  block_header_size_(0),
  num_chunks_(0),
  block_max_score_(numeric_limits<float>::max()),
  starting_chunk_(0),
  curr_chunk_(0),
  curr_block_data_(NULL),
  curr_chunk_decoder_(doc_id_decompressor, frequency_decompressor, position_decompressor),
  block_header_decompressor_(block_header_decompressor) {
}

void BlockDecoder::InitBlock(uint64_t block_num, int starting_chunk, uint32_t* block_data) {
  curr_block_num_ = block_num;
  starting_chunk_ = starting_chunk;
  curr_chunk_ = starting_chunk_;
  curr_block_data_ = block_data;
  block_max_score_ = numeric_limits<float>::max();

  uint32_t block_header_size;
  memcpy(&block_header_size, curr_block_data_, sizeof(block_header_size));
  block_header_size_ = block_header_size;
  assert(block_header_size_ > 0);
  curr_block_data_ += 1;

  uint32_t num_chunks;
  memcpy(&num_chunks, curr_block_data_, sizeof(num_chunks));
  num_chunks_ = num_chunks;
  assert(num_chunks_ > 0);
  curr_block_data_ += 1;

  curr_block_data_ += DecodeHeader(curr_block_data_);

  // Adjust our block data pointer to point to the starting chunk of this particular list.
  for (int i = 0; i < starting_chunk_; ++i) {
    curr_block_data_ += chunk_size(i);
  }

  curr_chunk_decoder_.set_decoded(false);
}

int BlockDecoder::DecodeHeader(uint32_t* compressed_header) {
  int header_len = num_chunks_ * 2;  // Number of integer entries we have to decompress (two integers per chunk).
  if (header_len > kChunkPropertiesDecompressedUpperbound)
    assert(false);

  // A word is meant to be sizeof(uint32_t) bytes in this context.
  int num_words_consumed = block_header_decompressor_.Decompress(compressed_header, chunk_properties_, header_len);
  return num_words_consumed;
}

/**************************************************************************************************************************************************************
 * ListData
 *
 **************************************************************************************************************************************************************/
ListData::ListData(int layer_num, uint32_t initial_block_num, uint32_t initial_chunk_num, int num_docs, int num_docs_complete_list, int num_chunks_last_block,
                   int num_blocks, const uint32_t* last_doc_ids, float score_threshold, CacheManager& cache_manager, const CodingPolicy& doc_id_decompressor,
                   const CodingPolicy& frequency_decompressor, const CodingPolicy& position_decompressor, const CodingPolicy& block_header_decompressor,
                   uint32_t external_index_offset, const ExternalIndexReader& external_index_reader, bool single_term_query) :
  kNumLeftoverDocs(num_docs % ChunkDecoder::kChunkSize),
  kReadAheadBlocks(Configuration::GetResultValue<long int>(Configuration::GetConfiguration().GetNumericalValue(config_properties::kReadAheadBlocks))),
  cache_manager_(cache_manager),
  curr_block_decoder_(doc_id_decompressor, frequency_decompressor, position_decompressor, block_header_decompressor),
  layer_num_(layer_num),
  initial_chunk_num_(initial_chunk_num),
  initial_block_num_(initial_block_num),
  curr_block_num_(initial_block_num_),
  curr_block_idx_(0),
  num_docs_(num_docs),
  num_docs_complete_list_(num_docs_complete_list),
  num_docs_last_chunk_(kNumLeftoverDocs == 0 ? ChunkDecoder::kChunkSize : kNumLeftoverDocs),
  num_chunks_((num_docs / ChunkDecoder::kChunkSize) + ((kNumLeftoverDocs == 0) ? 0 : 1)),
  num_chunks_last_block_(num_chunks_last_block),
  num_blocks_(num_blocks),
  num_docs_left_(num_docs_),
  num_chunks_last_block_left_(num_chunks_last_block_),
  num_blocks_left_(num_blocks_),
  first_block_loaded_(false),
  last_doc_ids_(last_doc_ids),
  score_threshold_(score_threshold),
  prev_block_last_doc_id_(0),
  last_queued_block_num_(curr_block_num_),
  term_num_(-1),
  external_index_pointer_(initial_block_num, external_index_offset),
  external_index_reader_(external_index_reader),
  single_term_query_(single_term_query),
  cached_bytes_read_(0),
  disk_bytes_read_(0) {
  if (kReadAheadBlocks <= 0) {
    Configuration::ErroneousValue(config_properties::kReadAheadBlocks, Configuration::GetConfiguration().GetValue(config_properties::kReadAheadBlocks));
  }

  Init();
}

ListData::~ListData() {
  FreeQueuedBlocks();
}

void ListData::Init() {
  // If we know that we'll not be doing any block skipping, it's slightly more efficient to turn it off.
  if (single_term_query_) {
    last_doc_ids_ = NULL;
  }

  // We can only skip this if we have an in memory block level index.
  if (last_doc_ids_ == NULL) {
    // The first block has a starting chunk defined in the lexicon.
    // Any subsequent blocks in the list will have the starting chunk at 0.
    SkipBlocks(0, initial_chunk_num_);
  }
}

void ListData::FreeQueuedBlocks() {
  // Free the current block plus any blocks we read ahead.
  for (uint64_t i = curr_block_num_; i < last_queued_block_num_; ++i) {
    // If using the LruCachePolicy, disk I/O is performed asynchronously.
    // If we exit the program before the all the requested disk I/O completes, a segmentation fault could occur.
    // Thus, we need to get the block, which will cause the I/O to block until it completes, if the block has not been read into our buffer yet.
    // This is especially important for algorithms that can early terminate, as they are likely to queue blocks, but not process them.
    cache_manager_.GetBlock(i);

    // Free the block, so it can be evicted from the cache.
    cache_manager_.FreeBlock(i);
  }
}

void ListData::ResetList(bool single_term_query) {
  FreeQueuedBlocks();

  curr_block_num_ = initial_block_num_;
  curr_block_idx_ = 0;
  num_docs_left_ = num_docs_;
  num_chunks_last_block_left_ = num_chunks_last_block_;
  num_blocks_left_ = num_blocks_;
  first_block_loaded_ = false;
  prev_block_last_doc_id_ = 0;
  last_queued_block_num_ = curr_block_num_;
  single_term_query_ = single_term_query;
  cached_bytes_read_ = 0;
  disk_bytes_read_ = 0;

  external_index_pointer_.Reset();

  Init();
}

void ListData::SkipBlocks(int num_blocks, uint32_t initial_chunk_num) {
  // We need to free up any blocks we might have queued up for loading, depending on how many blocks we skipped.
  for (uint32_t i = curr_block_num_; i < min((curr_block_num_ + num_blocks), last_queued_block_num_); ++i) {
    cache_manager_.FreeBlock(i);
  }

  curr_block_num_ += num_blocks;
  curr_block_idx_ += num_blocks;
  num_blocks_left_ -= num_blocks;

  // Load the block we need plus the next few blocks in advance.
  if (num_blocks_left_ > 0) {
    // Read ahead the next several MBs worth of blocks, but not past the length of the list.
    if (curr_block_num_ == last_queued_block_num_) {
      last_queued_block_num_ += min(kReadAheadBlocks, num_blocks_left_);
      int disk_blocks_read = cache_manager_.QueueBlocks(curr_block_num_, last_queued_block_num_);
      int cached_blocks_read = (last_queued_block_num_ - curr_block_num_) - disk_blocks_read;
      disk_bytes_read_ += disk_blocks_read * CacheManager::kBlockSize;
      cached_bytes_read_ += cached_blocks_read * CacheManager::kBlockSize;
    }

    curr_block_decoder_.InitBlock(curr_block_num_, initial_chunk_num, cache_manager_.GetBlock(curr_block_num_));

    // Advance the external index pointer up to the block we need and decode it.
    external_index_reader_.AdvanceToBlock(curr_block_num_, &external_index_pointer_);

    // Set the max score of the block, which we get from the external index.
    curr_block_decoder_.set_block_max_score(external_index_pointer_.block_max_score);
  }
}

void ListData::AdvanceBlock() {
  // If list is across blocks, need to get offset from previous block, to be used when decoding docID gaps.
  if (last_doc_ids_ != NULL) {
    set_prev_block_last_doc_id(curr_block_last_doc_id());
  } else {
    set_prev_block_last_doc_id(curr_block_decoder_.chunk_last_doc_id(curr_block_decoder_.num_chunks() - 1));
  }

  SkipBlocks(1, 0);
}

void ListData::AdvanceBlock(uint32_t doc_id) {
  // Determines whether we use binary search or sequential search to find the block we want to skip to.
  const bool kUseBinarySearch = false;

  if (last_doc_ids_ != NULL) {
    if (kUseBinarySearch)
      BlockBinarySearch(doc_id);
    else
      BlockSequentialSearch(doc_id);
  }
}

// TODO: We can also bias this binary search towards the lower half of the array, since that's where we expect our search to end (you can divide by 4, for example).
//       We always expect to go forward into the inverted list, so this seems like a reasonable optimization.
void ListData::BlockBinarySearch(uint32_t doc_id) {
  assert(last_doc_ids_ != NULL);

  uint32_t start = curr_block_idx_;
  uint32_t end = num_blocks_ - 1;
  uint32_t middle;

  while (start != end) {
    middle = start + ((end - start) / 2);
    if (doc_id <= last_doc_ids_[middle]) {  // Go to the lower half.
      end = middle;
    } else {  // Go to the upper half.
      start = middle + 1;
    }
  }

  SkipToBlock(start);
}

void ListData::BlockSequentialSearch(uint32_t doc_id) {
  assert(last_doc_ids_ != NULL);

  for (uint32_t i = curr_block_idx_; i < static_cast<uint32_t> (num_blocks_); ++i) {
    // We do this only if the docID we're looking for could potentially be in the block.
    if (doc_id <= last_doc_ids_[i]) {
      SkipToBlock(i);
      break;
    }
  }
}

// Helper method for the BlockSequentialSearch() and BlockBinarySearch() methods.
void ListData::SkipToBlock(uint32_t skip_to_block_idx) {
  // Only load this block if we haven't already loaded this block or if we haven't loaded the first block yet.
  // Loading the first block is a special case because on every other call to AdvanceBlock() besides the first, there will already be a block loaded.
  if ((skip_to_block_idx != curr_block_idx_ || !first_block_loaded_)) {
    int num_blocks = skip_to_block_idx - curr_block_idx_;
    int initial_chunk_num;
    if (skip_to_block_idx > 0) {
      initial_chunk_num = 0;
      set_prev_block_last_doc_id(last_doc_ids_[skip_to_block_idx - 1]);
    } else {
      initial_chunk_num = initial_chunk_num_;
      first_block_loaded_ = true;
    }

    SkipBlocks(num_blocks, initial_chunk_num);
  }
}

void ListData::AdvanceChunk() {
  // Set the current chunk in the block so the pointer to the next chunk is correctly offset in the block data.
  curr_block_decoder_.advance_curr_chunk();
  curr_block_decoder_.curr_chunk_decoder()->set_decoded(false);

  // Can update the number of documents left to process after processing the complete chunk.
  update_num_docs_left();

  // Adjust the number of chunks in the last block (only if we're in the last block).
  if (final_block()) {
    decrease_num_chunks_last_block_left(1);
  }
}

/**************************************************************************************************************************************************************
 * LexiconData
 *
 **************************************************************************************************************************************************************/
LexiconData::LexiconData(const char* term, int term_len) :
  term_len_(term_len),
  term_(new char[term_len_]),
  num_layers_(0),
  additional_layers_(NULL),
  next_(NULL) {
  memcpy(term_, term, term_len);
}

LexiconData::~LexiconData() {
  delete[] term_;
}

void LexiconData::InitLayers(int num_layers, const int* num_docs, const int* num_chunks, const int* num_chunks_last_block, const int* num_blocks,
                             const int* block_numbers, const int* chunk_numbers, const float* score_thresholds, const uint32_t* external_index_offsets) {
  assert(num_layers > 0);

  num_layers_ = num_layers;

  // Initialize the data for the first layer.
  first_layer_.num_docs = num_docs[0];
  first_layer_.num_chunks = num_chunks[0];
  first_layer_.num_chunks_last_block = num_chunks_last_block[0];
  first_layer_.num_blocks = num_blocks[0];
  first_layer_.block_number = block_numbers[0];
  first_layer_.chunk_number = chunk_numbers[0];
  first_layer_.score_threshold = score_thresholds[0];
  first_layer_.external_index_offset = external_index_offsets[0];
  first_layer_.last_doc_ids = NULL;

  if (num_layers_ > 1) {
    additional_layers_ = new LayerInfo[num_layers_ - 1];
    for (int i = 1; i < num_layers_; ++i) {
      additional_layers_[i].num_docs = num_docs[i];
      additional_layers_[i].num_chunks = num_chunks[i];
      additional_layers_[i].num_chunks_last_block = num_chunks_last_block[i];
      additional_layers_[i].num_blocks = num_blocks[i];
      additional_layers_[i].block_number = block_numbers[i];
      additional_layers_[i].chunk_number = chunk_numbers[i];
      additional_layers_[i].score_threshold = score_thresholds[i];
      additional_layers_[i].external_index_offset = external_index_offsets[i];
      additional_layers_[i].last_doc_ids = NULL;
    }
  }
}

/**************************************************************************************************************************************************************
 * Lexicon
 *
 * Reads the lexicon file in smallish chunks and inserts entries into an in-memory hash table (when querying) or returns the next sequential entry (when
 * merging).
 **************************************************************************************************************************************************************/
Lexicon::Lexicon(int hash_table_size, const char* lexicon_filename, bool random_access) :
  lexicon_(random_access ? new MoveToFrontHashTable<LexiconData> (hash_table_size) : NULL),
  kLexiconBufferSize(1 << 20),
  lexicon_buffer_(new char[kLexiconBufferSize]),
  lexicon_buffer_ptr_(lexicon_buffer_),
  lexicon_fd_(-1),
  lexicon_file_size_(0),
  num_bytes_read_(0) {
  Open(lexicon_filename, random_access);
}

Lexicon::~Lexicon() {
  delete[] lexicon_buffer_;
  delete lexicon_;

  int close_ret = close(lexicon_fd_);
  if (close_ret < 0) {
    GetErrorLogger().LogErrno("close() in Lexicon::~Lexicon(), trying to close lexicon", errno, false);
  }
}

void Lexicon::Open(const char* lexicon_filename, bool random_access) {
  lexicon_fd_ = open(lexicon_filename, O_RDONLY);
  if (lexicon_fd_ < 0) {
    GetErrorLogger().LogErrno("open() in Lexicon::Open()", errno, true);
  }

  struct stat stat_buf;
  if (fstat(lexicon_fd_, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in Lexicon::Open()", errno, true);
  }
  lexicon_file_size_ = stat_buf.st_size;

  int read_ret = read(lexicon_fd_, lexicon_buffer_, kLexiconBufferSize);
  if (read_ret < 0) {
    GetErrorLogger().LogErrno("read() in Lexicon::Open(), trying to read lexicon", errno, true);
  }

  if (random_access) {
    int num_terms = 0;
    LexiconEntry lexicon_entry;
    while (num_bytes_read_ < lexicon_file_size_) {
      GetNext(&lexicon_entry);

      LexiconData* lex_data = lexicon_->Insert(lexicon_entry.term, lexicon_entry.term_len);
      lex_data->InitLayers(lexicon_entry.num_layers, lexicon_entry.num_docs, lexicon_entry.num_chunks, lexicon_entry.num_chunks_last_block,
                           lexicon_entry.num_blocks, lexicon_entry.block_numbers, lexicon_entry.chunk_numbers, lexicon_entry.score_thresholds,
                           lexicon_entry.external_index_offsets);

      ++num_terms;
    }

    cout << "Lexicon size (number of unique terms): " << num_terms << endl;

    delete[] lexicon_buffer_;
    lexicon_buffer_ = NULL;
  }
}

LexiconData* Lexicon::GetEntry(const char* term, int term_len) {
  return lexicon_->Find(term, term_len);
}

// Returns a pointer to the next lexicon entry lexicographically, or NULL if no more.
// Should be deleted by the caller when done.
// This is used only when IndexReader is in 'kMerge' mode.
LexiconData* Lexicon::GetNextEntry() {
  LexiconEntry lexicon_entry;
  if (num_bytes_read_ < lexicon_file_size_) {
    GetNext(&lexicon_entry);
    LexiconData* lex_data = new LexiconData(lexicon_entry.term, lexicon_entry.term_len);
    lex_data->InitLayers(lexicon_entry.num_layers, lexicon_entry.num_docs, lexicon_entry.num_chunks, lexicon_entry.num_chunks_last_block,
                         lexicon_entry.num_blocks, lexicon_entry.block_numbers, lexicon_entry.chunk_numbers, lexicon_entry.score_thresholds,
                         lexicon_entry.external_index_offsets);
    return lex_data;
  } else {
    delete [] lexicon_buffer_;
    lexicon_buffer_ = NULL;
    return NULL;
  }
}

// TODO: The reinterpret casts used here are not particularly portable/safe. It is only considered safe to reinterpret cast to char* types.
//       Can fix by making the types char, and then later using memcpy() to load the data into the type we desire
//       (down side would be having to know the size of the data type).
void Lexicon::GetNext(LexiconEntry* lexicon_entry) {
  const int kLexiconEntryFixedLengthFieldsBytes = sizeof(lexicon_entry->term_len) + sizeof(lexicon_entry->num_layers);

  // This just counts a lower bound, as if the number of layers was only one.
  const int kLexiconEntryLayerDependentFieldsBytes = (sizeof(*lexicon_entry->num_docs) + sizeof(*lexicon_entry->num_chunks)
      + sizeof(*lexicon_entry->num_chunks_last_block) + sizeof(*lexicon_entry->num_blocks) + sizeof(*lexicon_entry->block_numbers)
      + sizeof(*lexicon_entry->chunk_numbers) + sizeof(*lexicon_entry->score_thresholds) + sizeof(*lexicon_entry->external_index_offsets));

  // We definitely need to load more data in this case.
  if (lexicon_buffer_ptr_ + (kLexiconEntryFixedLengthFieldsBytes + kLexiconEntryLayerDependentFieldsBytes) > lexicon_buffer_ + kLexiconBufferSize) {
    lseek(lexicon_fd_, num_bytes_read_, SEEK_SET);  // Seek just past where we last read data.
    int read_ret = read(lexicon_fd_, lexicon_buffer_, kLexiconBufferSize);
    if (read_ret < 0) {
      GetErrorLogger().LogErrno("read() in Lexicon::GetNext(), trying to read lexicon", errno, true);
    }
    lexicon_buffer_ptr_ = lexicon_buffer_;
  }

  // num_layers
  int num_layers;
  int num_layers_bytes = sizeof(num_layers);
  assert((lexicon_buffer_ptr_ + num_layers_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  memcpy(&num_layers, lexicon_buffer_ptr_, num_layers_bytes);
  lexicon_buffer_ptr_ += num_layers_bytes;
  num_bytes_read_ += num_layers_bytes;

  // term_len
  int term_len;
  int term_len_bytes = sizeof(term_len);
  assert((lexicon_buffer_ptr_ + term_len_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  memcpy(&term_len, lexicon_buffer_ptr_, term_len_bytes);
  lexicon_buffer_ptr_ += term_len_bytes;
  num_bytes_read_ += term_len_bytes;

  // The term plus the rest of the fields do not fit into the buffer, so we need to increase it.
  int lexicon_entry_size = term_len + kLexiconEntryFixedLengthFieldsBytes + kLexiconEntryLayerDependentFieldsBytes * num_layers;
  if (lexicon_entry_size > kLexiconBufferSize) {
    kLexiconBufferSize = lexicon_entry_size;
    delete[] lexicon_buffer_;
    lexicon_buffer_ = new char[kLexiconBufferSize];
    lseek(lexicon_fd_, num_bytes_read_, SEEK_SET);  // Seek just past where we last read data.
    int read_ret = read(lexicon_fd_, lexicon_buffer_, kLexiconBufferSize);
    if (read_ret < 0) {
      GetErrorLogger().LogErrno("read() in Lexicon::GetNext(), trying to read lexicon", errno, true);
    }
    lexicon_buffer_ptr_ = lexicon_buffer_;
  }

  // We couldn't read the whole term plus the rest of the integer fields of a lexicon entry into the buffer.
  if (lexicon_buffer_ptr_ + (kLexiconEntryLayerDependentFieldsBytes * num_layers) + term_len > lexicon_buffer_ + kLexiconBufferSize) {
    lseek(lexicon_fd_, num_bytes_read_, SEEK_SET);  // Seek just past where we last read data.
    int read_ret = read(lexicon_fd_, lexicon_buffer_, kLexiconBufferSize);
    if (read_ret < 0) {
      GetErrorLogger().LogErrno("read() in Lexicon::GetNext(), trying to read lexicon", errno, true);
    }
    lexicon_buffer_ptr_ = lexicon_buffer_;
  }

  // term
  char* term = lexicon_buffer_ptr_;
  int term_bytes = term_len;
  assert((lexicon_buffer_ptr_+term_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += term_bytes;
  num_bytes_read_ += term_bytes;

  // num_docs
  int* num_docs = reinterpret_cast<int*> (lexicon_buffer_ptr_);
  int num_docs_bytes = num_layers * sizeof(*num_docs);
  assert((lexicon_buffer_ptr_+num_docs_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += num_docs_bytes;
  num_bytes_read_ += num_docs_bytes;

  // num_chunks
  int* num_chunks = reinterpret_cast<int*> (lexicon_buffer_ptr_);
  int num_chunks_bytes = num_layers * sizeof(*num_chunks);
  assert((lexicon_buffer_ptr_+num_chunks_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += num_chunks_bytes;
  num_bytes_read_ += num_chunks_bytes;

  // num_chunks_last_block
  int* num_chunks_last_block = reinterpret_cast<int*> (lexicon_buffer_ptr_);
  int num_chunks_last_block_bytes = num_layers * sizeof(*num_chunks_last_block);
  assert((lexicon_buffer_ptr_+num_chunks_last_block_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += num_chunks_last_block_bytes;
  num_bytes_read_ += num_chunks_last_block_bytes;

  // num_blocks
  int* num_blocks = reinterpret_cast<int*> (lexicon_buffer_ptr_);
  int num_blocks_bytes = num_layers * sizeof(*num_blocks);
  assert((lexicon_buffer_ptr_+num_blocks_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += num_blocks_bytes;
  num_bytes_read_ += num_blocks_bytes;

  // block_number
  int* block_numbers = reinterpret_cast<int*> (lexicon_buffer_ptr_);
  int block_numbers_bytes = num_layers * sizeof(*block_numbers);
  assert((lexicon_buffer_ptr_+block_numbers_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += block_numbers_bytes;
  num_bytes_read_ += block_numbers_bytes;

  // chunk_number
  int* chunk_numbers = reinterpret_cast<int*> (lexicon_buffer_ptr_);
  int chunk_numbers_bytes = num_layers * sizeof(*chunk_numbers);
  assert((lexicon_buffer_ptr_+chunk_numbers_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += chunk_numbers_bytes;
  num_bytes_read_ += chunk_numbers_bytes;

  // score_thresholds
  float* score_thresholds = reinterpret_cast<float*> (lexicon_buffer_ptr_);
  int score_thresholds_bytes = num_layers * sizeof(*score_thresholds);
  assert((lexicon_buffer_ptr_+score_thresholds_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += score_thresholds_bytes;
  num_bytes_read_ += score_thresholds_bytes;

  // external_index_offsets
  uint32_t* external_index_offsets = reinterpret_cast<uint32_t*> (lexicon_buffer_ptr_);
  int external_index_offsets_bytes = num_layers * sizeof(*external_index_offsets);
  assert((lexicon_buffer_ptr_+external_index_offsets_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  lexicon_buffer_ptr_ += external_index_offsets_bytes;
  num_bytes_read_ += external_index_offsets_bytes;

  lexicon_entry->term = term;
  lexicon_entry->term_len = term_len;
  lexicon_entry->num_layers = num_layers;
  lexicon_entry->num_docs = num_docs;
  lexicon_entry->num_chunks = num_chunks;  // TODO: This is actually not necessary to store since it can be derived from the number of documents.
  lexicon_entry->num_chunks_last_block = num_chunks_last_block;
  lexicon_entry->num_blocks = num_blocks;
  lexicon_entry->block_numbers = block_numbers;
  lexicon_entry->chunk_numbers = chunk_numbers;
  lexicon_entry->score_thresholds = score_thresholds;
  lexicon_entry->external_index_offsets = external_index_offsets;
}

/**************************************************************************************************************************************************************
 * IndexReader
 *
 * Initiates loading of several MBs worth of blocks ahead since we don't know exactly how many blocks are in the list. Each block is processed as soon as it's
 * needed and we know it has been loaded into memory.
 **************************************************************************************************************************************************************/
IndexReader::IndexReader(Purpose purpose, DocumentOrder document_order, CacheManager& cache_manager, const char* lexicon_filename,
                         const char* doc_map_filename, const char* meta_info_filename, bool use_positions) :
  purpose_(purpose),
  document_order_(document_order),
  kLexiconSize(Configuration::GetResultValue<long int>(Configuration::GetConfiguration().GetNumericalValue(config_properties::kLexiconSize))),
  lexicon_(kLexiconSize, lexicon_filename, (purpose_ == kRandomQuery)),
  document_map_("index.dmap"/*doc_map_filename*/),  // TODO: Use passed in filename.
  cache_manager_(cache_manager),
  meta_info_(meta_info_filename),
  includes_contexts_(false),
  includes_positions_(false),  // TODO: Get from index meta file.
  use_positions_(use_positions && includes_positions_),
  external_index_reader_("index.ext"),
  doc_id_decompressor_(CodingPolicy::kDocId),
  frequency_decompressor_(CodingPolicy::kFrequency),
  position_decompressor_(CodingPolicy::kPosition),
  block_header_decompressor_(CodingPolicy::kBlockHeader),
  total_cached_bytes_read_(0),
  total_disk_bytes_read_(0),
  total_num_lists_accessed_(0) {
  if (kLexiconSize <= 0) {
    Configuration::ErroneousValue(config_properties::kLexiconSize, Configuration::GetConfiguration().GetValue(config_properties::kLexiconSize));
  }

  LoadMetaInfo();

  coding_policy_helper::LoadPolicyAndCheck(doc_id_decompressor_, meta_info_.GetValue(meta_properties::kIndexDocIdCoding), "docID");
  coding_policy_helper::LoadPolicyAndCheck(frequency_decompressor_, meta_info_.GetValue(meta_properties::kIndexFrequencyCoding), "frequency");
  coding_policy_helper::LoadPolicyAndCheck(position_decompressor_, meta_info_.GetValue(meta_properties::kIndexPositionCoding), "position");
  coding_policy_helper::LoadPolicyAndCheck(block_header_decompressor_, meta_info_.GetValue(meta_properties::kIndexBlockHeaderCoding), "block header");
}

ListData* IndexReader::OpenList(const LexiconData& lex_data, int layer_num, bool single_term_query) {
  assert(lex_data.layer_block_number(layer_num) >= 0 && lex_data.layer_chunk_number(layer_num) >= 0 && lex_data.layer_num_docs(layer_num) >= 0);

  // TODO: If there are errors reading the values for these keys (most likely missing value), we assume they're false
  // (because that would require updating the index meta file generation in some places, which should be done eventually).
  KeyValueStore::KeyValueResult<long int> overlapping_layers_res = meta_info_.GetNumericalValue(meta_properties::kOverlappingLayers);
  bool overlapping_layers = overlapping_layers_res.error() ? false : overlapping_layers_res.value_t();

  int num_docs_complete_list = 0; // Need to find the total number of documents for the whole list, to be used in the BM25 score calculation.
  if (overlapping_layers) {
    num_docs_complete_list = lex_data.layer_num_docs(lex_data.num_layers() - 1);
  } else {
    for (int i = 0; i < lex_data.num_layers(); ++i) {
      num_docs_complete_list += lex_data.layer_num_docs(i);
    }
  }

  ListData* list_data = new ListData(layer_num,
                                     lex_data.layer_block_number(layer_num),
                                     lex_data.layer_chunk_number(layer_num),
                                     lex_data.layer_num_docs(layer_num),
                                     num_docs_complete_list,
                                     lex_data.layer_num_chunks_last_block(layer_num),
                                     lex_data.layer_num_blocks(layer_num),
                                     lex_data.layer_last_doc_ids(layer_num),
                                     lex_data.layer_score_threshold(layer_num),
                                     cache_manager_,
                                     doc_id_decompressor_,
                                     frequency_decompressor_,
                                     position_decompressor_,
                                     block_header_decompressor_,
                                     lex_data.layer_external_index_offset(layer_num),
                                     external_index_reader_,
                                     single_term_query);
  return list_data;
}

ListData* IndexReader::OpenList(const LexiconData& lex_data, int layer_num, bool single_term_query, int term_num) {
  ListData* list = OpenList(lex_data, layer_num, single_term_query);
  list->set_term_num(term_num);
  return list;
}

// TODO: Some potential improvements:
// * For single word and OR type queries, we don't need to decode the block header, since we won't be doing any chunk skipping. This requires storing the size of
//   the block header (in number of ints) so we can skip past it. We'd also need to decompress the entire chunk (even positions if there are any)
//   in order to get the complete chunk size before moving on to the next chunk, or otherwise we wouldn't know where the next chunk would start.
// * Can calculate the inclusive prefix sum of all the chunk sizes while building the block header; then won't have to add them while doing chunk skipping.
// * Keep in mind cache line size: 64 bytes (16 4-byte integers) on Core 2 and later (32 bytes is more portable though).
//inline uint32_t __attribute__((always_inline)) IndexReader::NextGEQ(ListData* list_data, uint32_t doc_id) {
uint32_t IndexReader::NextGEQ(ListData* list_data, uint32_t doc_id) {
  assert(list_data != NULL);

  // For standard DAAT OR mode processing, setting this parameter to 'false' resulted in an improvement of a few milliseconds in terms of average query latency,
  // which was a little surprising.
  const bool kDecodeAllChunkDGaps = false;

  // Since we now have the capability to skip over multiple blocks without even decoding the block header,
  // we can't tell how many documents or chunks are left in the list. For this reason, we now count the number of blocks remaining to be processed
  // in the list as well as how many chunks are present in the last remaining block of the list.
  // Additionally, we have to determine the number of chunks present in the last chunk of a list through the total number
  // of documents present in the list.

  // TODO: Can you mix the binary search and sequential search for skipping blocks to optimize performance?
  list_data->AdvanceBlock(doc_id);

  ChunkDecoder::ChunkProperties chunk_properties;
  chunk_properties.includes_contexts = includes_contexts_;
  chunk_properties.includes_positions = includes_positions_;

  BlockDecoder* block;
  ChunkDecoder* chunk;
  int i;
  int num_chunk_docs;
  int curr_chunk_num;
  int curr_document_offset;
  uint32_t curr_doc_id;

  while (list_data->has_more()) {
    block = list_data->curr_block_decoder();

    curr_chunk_num = block->curr_chunk();
    if (curr_chunk_num < block->num_chunks()) {
      // Decide whether or not we can skip past this chunk by checking the current docID we're looking for against the last docID of this chunk.
      if (doc_id <= block->chunk_last_doc_id(curr_chunk_num)) {  // We cannot skip past this chunk.
        chunk = block->curr_chunk_decoder();

        // Check if we previously decoded this chunk.
        if (block->curr_chunk_decoded() == false) {  // We have to decode this chunk.
          // TODO: If we know there are no block skips possible (it's a single term query, unoptimized OR query, etc)
          //       We can keep track of the number of documents left instead to save on some branchy code.
          //       Note that when doing block skips, we cannot keep track of the number of documents left since we don't know
          //       the number of documents for a particular list in a block.
          //       This information can now be easily stored in the external index.
          //       Would be interesting to make use of it to try to make NextGEQ() less branchy.
          // num_chunk_docs = min(ChunkDecoder::kChunkSize, list_data->num_docs_left());
          num_chunk_docs = ((list_data->final_block() && list_data->final_chunk()) ? list_data->num_docs_last_chunk() : ChunkDecoder::kChunkSize);
          chunk->InitChunk(chunk_properties, block->curr_block_data(), num_chunk_docs);
          chunk->DecodeDocIds();

          // Need to do these offsets only if we haven't done them before for this chunk. This is to handle d-gap coding.
          // List is across blocks and we need offset from previous block.
          if (!list_data->initial_block() && curr_chunk_num == 0) {
            chunk->update_prev_decoded_doc_id(list_data->prev_block_last_doc_id());
          }

          // We need offset from previous chunk if this is not the first chunk in the list.
          if (curr_chunk_num > block->starting_chunk()) {
            chunk->update_prev_decoded_doc_id(block->chunk_last_doc_id(curr_chunk_num - 1));
          }

          // We always decode the first d-gap in the chunk.
          chunk->update_prev_decoded_doc_id(chunk->doc_id(0));

          if (kDecodeAllChunkDGaps) {
            // Can decode all d-gaps in one swoop.
            uint32_t prev_doc_id = chunk->prev_decoded_doc_id();
            chunk->set_doc_id(0, prev_doc_id);
            for (i = 1; i < chunk->num_docs(); ++i) {
              prev_doc_id += chunk->doc_id(i);
              chunk->set_doc_id(i, prev_doc_id);
            }
          }
        }

        if (!kDecodeAllChunkDGaps) {
          curr_doc_id = chunk->prev_decoded_doc_id();
        }

        // We always start the chunk offset from the last returned (or first, if this is a newly decoded chunk) document.
        curr_document_offset = chunk->curr_document_offset();
        for (i = curr_document_offset; i < chunk->num_docs(); ++i) {
          if (kDecodeAllChunkDGaps) {
            curr_doc_id = chunk->doc_id(i);
          } else {
            if (i != curr_document_offset)
              curr_doc_id += chunk->doc_id(i);  // Necessary for d-gap coding.
          }

          // Found the docID we're looking for.
          if (curr_doc_id >= doc_id) {
            chunk->set_curr_document_offset(i);  // Offset for the frequency.
            if (!kDecodeAllChunkDGaps) {
              chunk->set_prev_decoded_doc_id(curr_doc_id);
            }
            return curr_doc_id;
          }
        }
      }

      list_data->AdvanceChunk();
    } else {
      // We're moving on to process the next block. This block is of no use to us anymore.
      list_data->AdvanceBlock();
    }
  }

  // No other chunks in this list have a docID >= 'doc_id', so return this sentinel value to indicate this.
  return std::numeric_limits<uint32_t>::max();
}

// Returns the upperbound score of the block on the current docID in the list.
float IndexReader::GetBlockScoreBound(ListData* list_data) {
  assert(list_data != NULL);
  BlockDecoder* block = list_data->curr_block_decoder();
  assert(block != NULL);
  return block->block_max_score();
}

// Returns the next docID that has a score of at least 'min_score'.
// TODO: Should this be strictly greater --- having an equal score won't put us into the top-k.
uint32_t IndexReader::NextGEQScore(ListData* list_data, float min_score) {
  while (list_data->has_more()) {
    if (list_data->curr_block_decoder()->block_max_score() < min_score) {
//      return NextGEQ(list_data, 0);

      list_data->AdvanceBlock();
      cout << "Skipping block!" << endl;//TODO
    } else {
      cout << "No block skipping!" << endl;//TODO
      return NextGEQ(list_data, 0);
    }
  }

  // No other blocks in this list have a score >= 'min_score', so return this sentinel value to indicate this.
  return std::numeric_limits<uint32_t>::max();
}

uint32_t IndexReader::GetFreq(ListData* list_data, uint32_t doc_id) {
  assert(list_data != NULL);
  ChunkDecoder* chunk = list_data->curr_block_decoder()->curr_chunk_decoder();
  assert(chunk != NULL);
  // Decode the frequencies and positions if we haven't already.
  if (!chunk->decoded_properties()) {
    chunk->DecodeProperties();
  }

  if (use_positions_) {
    // Need to set the correct offset for the positions and other frequency dependent properties.
    chunk->UpdatePropertiesOffset();
  }

  return chunk->current_frequency();
}

int IndexReader::GetList(ListData* list_data, IndexDataType data_type, uint32_t* index_data, int index_data_size) {
  assert(list_data != NULL);

  int curr_index_data_idx = 0;
  uint32_t next_doc_id = 0;

  uint32_t curr_doc_id;
  uint32_t curr_frequency;
  const uint32_t* curr_positions;

  // Necessary for positions. If we have started decoding the list (on the previous call to this function)
  // but stopped because we couldn't copy all the positions, copy them now.
  if (data_type == kPosition && list_data->curr_block_decoder()->curr_chunk_decoder()->decoded()) {
    curr_frequency = GetFreq(list_data, 0);  // Getting the frequency for docID 0 will actually return the frequency for the last decoded docID in the chunk.
    curr_positions = list_data->curr_block_decoder()->curr_chunk_decoder()->current_positions();
    if (static_cast<int> (curr_frequency) <= index_data_size) {
      // Copy the positions.
      for (size_t i = 0; i < curr_frequency; ++i) {
        index_data[curr_index_data_idx++] = curr_positions[i];
      }
    } else {
      return -1;  // Indicates the array needs to be larger to retrieve the positions.
    }
  }

  while (curr_index_data_idx < index_data_size && (curr_doc_id = NextGEQ(list_data, next_doc_id)) < numeric_limits<uint32_t>::max()) {
    next_doc_id = curr_doc_id + 1;

    bool continue_next = false;

    switch (data_type) {
      case kDocId:
        // Copy the docID.
        index_data[curr_index_data_idx++] = curr_doc_id;
        break;

      case kFrequency:
        // Copy the frequency.
        index_data[curr_index_data_idx++] = GetFreq(list_data, curr_doc_id);
        break;

      case kPosition:
        curr_frequency = GetFreq(list_data, curr_doc_id);
        curr_positions = list_data->curr_block_decoder()->curr_chunk_decoder()->current_positions();

        // We need to make sure we don't consume positions unless *all* of them fit into the supplied array.
        if (static_cast<int> (curr_index_data_idx + curr_frequency) <= index_data_size) {
          // Copy the positions.
          for (size_t i = 0; i < curr_frequency; ++i) {
            index_data[curr_index_data_idx++] = curr_positions[i];
          }
        } else {
          // If this is the position data for the first document that doesn't fit into the supplied array,
          // need to indicate for the user that the supplied array needs to be larger (instead of misleadingly returning 0).
          if (curr_index_data_idx == 0) {
            return -1;
          }
          continue_next = true;
        }
        break;

      default:
        assert(false);
    }

    if (continue_next) {
      break;
    }
  }

  // Returns the number of data items decoded and stored into the supplied array.
  // No more data left to consume after we return 0.
  return curr_index_data_idx;
}

// TODO: CHECK WHETHER THIS IS OK WITH THE RECENT UPDATES TO THE LIST_DATA!!!
// TODO: Would be useful to store the total number of chunks contained in a block and the size of the block header
//       (Note: this is implemented now --- can make use of it).
int IndexReader::LoopOverList(ListData* list_data, IndexDataType data_type) {
  assert(list_data != NULL);

  int count = 0;

  uint32_t curr_frequency;
  while (list_data->num_docs_left() > 0) {
    BlockDecoder* block = list_data->curr_block_decoder();

    int curr_chunk_num = block->curr_chunk();
    if (curr_chunk_num < block->num_chunks()) {
      ChunkDecoder* chunk = block->curr_chunk_decoder();
      // Create a new chunk and add it to the block.
      ChunkDecoder::ChunkProperties chunk_properties;
      chunk_properties.includes_contexts = includes_contexts_;
      chunk_properties.includes_positions = includes_positions_;
      chunk->InitChunk(chunk_properties, block->curr_block_data(), std::min(ChunkDecoder::kChunkSize, list_data->num_docs_left()));
      chunk->DecodeDocIds();

      for (int k = 0; k < chunk->num_docs(); ++k) {
        switch (data_type) {
          case kDocId:
            chunk->doc_id(k);
            ++count;
            break;

          case kFrequency:
            chunk->set_curr_document_offset(k);  // Offset for the frequency.
            curr_frequency = GetFreq(list_data, 0);
            ++count;
            break;

          case kPosition:
            chunk->set_curr_document_offset(k);  // Offset for the frequency.
            curr_frequency = GetFreq(list_data, 0);
            list_data->curr_block_decoder()->curr_chunk_decoder()->current_positions();
            count += curr_frequency;
            break;

          default:
            assert(false);
        }
      }

      // Moving on to the next chunk.
      block->advance_curr_chunk();
      // Can update the number of documents left to process after processing the complete chunk.
      list_data->update_num_docs_left();
    } else {
      // We're moving on to process the next block. This block is of no use to us anymore.
      list_data->AdvanceBlock();
    }
  }

  return count;
}

void IndexReader::CloseList(ListData* list_data) {
  total_cached_bytes_read_ += list_data->cached_bytes_read();
  total_disk_bytes_read_ += list_data->disk_bytes_read();
  ++total_num_lists_accessed_;

  delete list_data;
}

int IndexReader::GetDocLen(uint32_t doc_id) {
  return document_map_.GetDocumentLen(doc_id);
}

const char* IndexReader::GetDocUrl(uint32_t doc_id) {
  // TODO: Get value from doc map.
  return "url";
}

void IndexReader::LoadMetaInfo() {
  includes_contexts_ = IndexConfiguration::GetResultValue(meta_info_.GetNumericalValue(meta_properties::kIncludesContexts), false);
  includes_positions_ = IndexConfiguration::GetResultValue(meta_info_.GetNumericalValue(meta_properties::kIncludesPositions), false);
}
