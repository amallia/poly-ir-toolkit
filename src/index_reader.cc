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
  num_docs_(0), curr_document_offset_(-1), prev_document_offset_(0), curr_position_offset_(0), prev_decoded_doc_id_(0), num_positions_(0),
      decoded_properties_(false), curr_buffer_position_(NULL), decoded_(false), doc_id_decompressor_(doc_id_decompressor),
      frequency_decompressor_(frequency_decompressor), position_decompressor_(position_decompressor) {
}

void ChunkDecoder::InitChunk(const ChunkProperties& properties, const uint32_t* buffer, int num_docs) {
  properties_ = properties;
  num_docs_ = num_docs;
  curr_document_offset_ = -1;  // -1 signals that we haven't had an intersection yet.
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
  curr_block_num_(0), num_chunks_(0), starting_chunk_(0), curr_chunk_(0), curr_block_data_(NULL),
      curr_chunk_decoder_(doc_id_decompressor, frequency_decompressor, position_decompressor),
      block_header_decompressor_(block_header_decompressor) {
}

void BlockDecoder::InitBlock(uint64_t block_num, int starting_chunk, uint32_t* block_data) {
  curr_block_num_ = block_num;
  starting_chunk_ = starting_chunk;
  curr_chunk_ = starting_chunk_;
  curr_block_data_ = block_data;

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
ListData::ListData(uint64_t initial_block_num, int starting_chunk, int num_docs, CacheManager& cache_manager, const CodingPolicy& doc_id_decompressor,
                   const CodingPolicy& frequency_decompressor, const CodingPolicy& position_decompressor,
                   const CodingPolicy& block_header_decompressor) :
  kReadAheadBlocks(atol(Configuration::GetConfiguration().GetValue(config_properties::kReadAheadBlocks).c_str())), cache_manager_(cache_manager),
      initial_block_num_(initial_block_num), last_queued_block_num_(initial_block_num + kReadAheadBlocks), curr_block_num_(initial_block_num_),
      curr_block_decoder_(doc_id_decompressor, frequency_decompressor, position_decompressor, block_header_decompressor), num_docs_(num_docs),
      num_docs_left_(num_docs_), num_chunks_((num_docs / ChunkDecoder::kChunkSize) + ((num_docs % ChunkDecoder::kChunkSize == 0) ? 0 : 1)),
      prev_block_last_doc_id_(0), cached_bytes_read_(0), disk_bytes_read_(0) {
  if (kReadAheadBlocks <= 0) {
    GetErrorLogger().Log("Incorrect configuration value for '" + string(config_properties::kReadAheadBlocks) + "'", true);
  }

  int disk_blocks_read = cache_manager_.QueueBlocks(initial_block_num_, last_queued_block_num_);
  int cached_blocks_read = (last_queued_block_num_ - initial_block_num_) - disk_blocks_read;
  disk_bytes_read_ += disk_blocks_read * CacheManager::kBlockSize;
  cached_bytes_read_ += cached_blocks_read * CacheManager::kBlockSize;

  curr_block_decoder_.InitBlock(curr_block_num_, starting_chunk, cache_manager_.GetBlock(curr_block_num_));
}

void ListData::AdvanceToNextBlock() {
  cache_manager_.FreeBlock(curr_block_num_);
  ++curr_block_num_;

  if (num_docs_left_ > 0) {
    // Read ahead the next several MBs worth of blocks.
    if (curr_block_num_ == last_queued_block_num_) {
      last_queued_block_num_ += kReadAheadBlocks;
      int disk_blocks_read = cache_manager_.QueueBlocks(curr_block_num_, last_queued_block_num_);
      int cached_blocks_read = (last_queued_block_num_ - curr_block_num_) - disk_blocks_read;
      disk_bytes_read_ += disk_blocks_read * CacheManager::kBlockSize;
      cached_bytes_read_ += cached_blocks_read * CacheManager::kBlockSize;
    }

    // Any subsequent blocks in the list (after the first block, will have the list start at chunk 0).
    curr_block_decoder_.InitBlock(curr_block_num_, 0, cache_manager_.GetBlock(curr_block_num_));
  }
}

ListData::~ListData() {
  // Free the current block plus any blocks we read ahead.
  for (uint64_t i = curr_block_num_; i < last_queued_block_num_; ++i) {
    cache_manager_.FreeBlock(i);
  }
}

/**************************************************************************************************************************************************************
 * Lexicon
 *
 * Reads the lexicon file in smallish chunks and inserts entries into an in-memory hash table (when querying) or returns the next sequential entry (when
 * merging).
 **************************************************************************************************************************************************************/
Lexicon::Lexicon(int hash_table_size, const char* lexicon_filename, bool random_access) :
  lexicon_(random_access ? new MoveToFrontHashTable<LexiconData> (hash_table_size) : NULL), kLexiconBufferSize(1 << 20),
      lexicon_buffer_(new char[kLexiconBufferSize]), lexicon_buffer_ptr_(lexicon_buffer_), lexicon_fd_(-1), lexicon_file_size_(0), num_bytes_read_(0) {
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
      lex_data->set_block_number(lexicon_entry.block_number);
      lex_data->set_chunk_number(lexicon_entry.chunk_number);
      lex_data->set_num_docs(lexicon_entry.num_docs);

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
    return new LexiconData(lexicon_entry.term, lexicon_entry.term_len, lexicon_entry.block_number, lexicon_entry.chunk_number, lexicon_entry.num_docs);
  } else {
    delete [] lexicon_buffer_;
    lexicon_buffer_ = NULL;
    return NULL;
  }
}

void Lexicon::GetNext(LexiconEntry* lexicon_entry) {
  const int kNumLexiconIntEntries = 4;  // Not counting the term entry which is variable length.

  // We definitely need to load more data in this case.
  if (lexicon_buffer_ptr_ + (kNumLexiconIntEntries * sizeof(int)) > lexicon_buffer_ + kLexiconBufferSize) {
    lseek(lexicon_fd_, num_bytes_read_, SEEK_SET); // Seek just past where we last read data.
    int read_ret = read(lexicon_fd_, lexicon_buffer_, kLexiconBufferSize);
    if (read_ret < 0) {
      GetErrorLogger().LogErrno("read() in Lexicon::GetNext(), trying to read lexicon", errno, true);
    }
    lexicon_buffer_ptr_ = lexicon_buffer_;
  }

  // term_len
  int term_len;
  int term_len_bytes = sizeof(term_len);
  assert((lexicon_buffer_ptr_+term_len_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  memcpy(&term_len, lexicon_buffer_ptr_, term_len_bytes);
  lexicon_buffer_ptr_ += term_len_bytes;
  num_bytes_read_ += term_len_bytes;

  // The term plus the rest of the integer fields do not fit into the buffer, so we need to increase it.
  int lexicon_entry_size = (kNumLexiconIntEntries * sizeof(int)) + term_len;
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
  if (lexicon_buffer_ptr_ + ((kNumLexiconIntEntries - 1) * sizeof(int)) + term_len > lexicon_buffer_ + kLexiconBufferSize) {
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

  // block_number
  int block_number;
  int block_number_bytes = sizeof(block_number);
  assert((lexicon_buffer_ptr_+block_number_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  memcpy(&block_number, lexicon_buffer_ptr_, block_number_bytes);
  lexicon_buffer_ptr_ += block_number_bytes;
  num_bytes_read_ += block_number_bytes;

  // chunk_number
  int chunk_number;
  int chunk_number_bytes = sizeof(chunk_number);
  assert((lexicon_buffer_ptr_+chunk_number_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  memcpy(&chunk_number, lexicon_buffer_ptr_, chunk_number_bytes);
  lexicon_buffer_ptr_ += chunk_number_bytes;
  num_bytes_read_ += chunk_number_bytes;

  // num_docs
  int num_docs;
  int num_docs_bytes = sizeof(num_docs);
  assert((lexicon_buffer_ptr_+num_docs_bytes) <= (lexicon_buffer_ + kLexiconBufferSize));
  memcpy(&num_docs, lexicon_buffer_ptr_, num_docs_bytes);
  lexicon_buffer_ptr_ += num_docs_bytes;
  num_bytes_read_ += num_docs_bytes;

  lexicon_entry->term = term;
  lexicon_entry->term_len = term_len;
  lexicon_entry->block_number = block_number;
  lexicon_entry->chunk_number = chunk_number;
  lexicon_entry->num_docs = num_docs;
}

/**************************************************************************************************************************************************************
 * IndexReader
 *
 * Initiates loading of several MBs worth of blocks ahead since we don't know exactly how many blocks are in the list. Each block is processed as soon as it's
 * needed and we know it has been loaded into memory.
 **************************************************************************************************************************************************************/
IndexReader::IndexReader(Purpose purpose, DocumentOrder document_order, CacheManager& cache_manager, const char* lexicon_filename,
                         const char* doc_map_filename, const char* meta_info_filename) :
  purpose_(purpose), document_order_(document_order), kLexiconSize(atol(Configuration::GetConfiguration().GetValue(config_properties::kLexiconSize).c_str())),
      lexicon_(kLexiconSize, lexicon_filename, (purpose_ == kRandomQuery)), cache_manager_(cache_manager), meta_info_(meta_info_filename),
      includes_contexts_(false), includes_positions_(false), doc_id_decompressor_(CodingPolicy::kDocId), frequency_decompressor_(CodingPolicy::kFrequency),
      position_decompressor_(CodingPolicy::kPosition), block_header_decompressor_(CodingPolicy::kBlockHeader), total_cached_bytes_read_(0),
      total_disk_bytes_read_(0), total_num_lists_accessed_(0) {
  if (kLexiconSize <= 0) {
    GetErrorLogger().Log("Incorrect configuration value for '" + string(config_properties::kLexiconSize) + "'", true);
  }

  LoadMetaInfo();

  coding_policy_helper::LoadPolicyAndCheck(doc_id_decompressor_, meta_info_.GetValue(meta_properties::kIndexDocIdCoding), "docID");
  coding_policy_helper::LoadPolicyAndCheck(frequency_decompressor_, meta_info_.GetValue(meta_properties::kIndexFrequencyCoding), "frequency");
  coding_policy_helper::LoadPolicyAndCheck(position_decompressor_, meta_info_.GetValue(meta_properties::kIndexPositionCoding), "position");
  coding_policy_helper::LoadPolicyAndCheck(block_header_decompressor_, meta_info_.GetValue(meta_properties::kIndexBlockHeaderCoding), "block header");

  LoadDocMap(doc_map_filename);
}

ListData* IndexReader::OpenList(const LexiconData& lex_data) {
  assert(lex_data.block_number() >= 0 && lex_data.chunk_number() >= 0 && lex_data.num_docs() >= 0);
  ListData* list_data = new ListData(lex_data.block_number(), lex_data.chunk_number(), lex_data.num_docs(), cache_manager_,
                                     doc_id_decompressor_, frequency_decompressor_, position_decompressor_, block_header_decompressor_);
  return list_data;
}

// TODO: Some potential improvements:
// * For single word and OR type queries, we don't need to decode the block header, since we won't be doing any chunk skipping. This requires storing the size of
//   the block header (in number of ints) so we can skip past it.
// * Can calculate the inclusive prefix sum of all the chunk sizes while building the block header; then won't have to add them while doing chunk skipping.
// * Keep in mind cache line size: 64 bytes (16 4-byte integers) on Core 2 and later (32 bytes is more portable though).
//inline uint32_t __attribute__((always_inline)) IndexReader::NextGEQ(ListData* list_data, uint32_t doc_id) {
uint32_t IndexReader::NextGEQ(ListData* list_data, uint32_t doc_id) {
  assert(list_data != NULL);

  while (list_data->num_docs_left() > 0) {
    BlockDecoder* block = list_data->curr_block_decoder();

    int curr_chunk_num = block->curr_chunk();
    if (curr_chunk_num < block->num_chunks()) {
      // Check the last doc id of the chunk against the current doc id we're looking for and skip the chunk if possible.
      if (doc_id <= block->chunk_last_doc_id(curr_chunk_num)) {
        ChunkDecoder* chunk = block->curr_chunk_decoder();

        // Check if we previously decoded this chunk and decode if necessary.
        if (block->curr_chunk_decoded() == false) {
          // Create a new chunk and add it to the block.
          ChunkDecoder::ChunkProperties chunk_properties;
          chunk_properties.includes_contexts = includes_contexts_;
          chunk_properties.includes_positions = includes_positions_;
          chunk->InitChunk(chunk_properties, block->curr_block_data(), std::min(ChunkDecoder::kChunkSize, list_data->num_docs_left()));
          chunk->DecodeDocIds();

          // Need to do these offsets only if we haven't done them before for this chunk. This is to handle d-gap coding.
          // List is across blocks and we need offset from previous block.
          if (!list_data->initial_block() && curr_chunk_num == 0) {
            uint32_t doc_id_offset = list_data->prev_block_last_doc_id();
            chunk->update_prev_decoded_doc_id(doc_id_offset);
          }

          // We need offset from previous chunk if this is not the first chunk in the list.
          if (curr_chunk_num > block->starting_chunk()) {
            uint32_t doc_id_offset = block->chunk_last_doc_id(curr_chunk_num - 1);
            chunk->update_prev_decoded_doc_id(doc_id_offset);
          }

          // Can decode all d-gaps in one swoop.
          /*uint32_t prev_doc_id = chunk->prev_decoded_doc_id();
          for (int k = 0; k < chunk->num_docs(); ++k) {
            prev_doc_id += chunk->doc_id(k);
            chunk->set_doc_id(k, prev_doc_id);
          }*/
        }

        uint32_t curr_doc_id = chunk->prev_decoded_doc_id();

        // The current document offset was the last docID processed, so we increment by 1 in order to not process it again. But not for the first chunk processed.
        int curr_document_offset = chunk->curr_document_offset() == -1 ? 0 : chunk->curr_document_offset() + 1;
        for (int k = curr_document_offset; k < chunk->num_docs(); ++k) {
          curr_doc_id += chunk->doc_id(k);  // Necessary for d-gap coding.

          // Found the docID we're looking for.
          if (curr_doc_id >= doc_id) {
            chunk->set_curr_document_offset(k);  // Offset for the frequency.
            chunk->set_prev_decoded_doc_id(curr_doc_id);  // Comment out if you want to decode all d-gaps in one swoop.
            return curr_doc_id;
            /*return chunk->doc_id(k);*/  // When we decode all d-gaps in one swoop.
          }
        }
      }

      // Moving on to the next chunk.
      // We could not find the docID we were looking for, so we need to set the current chunk in the block
      // so the pointer to the next chunk is correctly offset in the block data.
      block->advance_curr_chunk();
      block->curr_chunk_decoder()->set_decoded(false);

      // Can update the number of documents left to process after processing the complete chunk.
      list_data->update_num_docs_left();
    } else {
      // If list is across blocks, need to get offset from previous block, to be used when decoding docID gaps.
      list_data->set_prev_block_last_doc_id(block->chunk_last_doc_id(block->num_chunks() - 1));

      // We're moving on to process the next block. This block is of no use to us anymore.
      list_data->AdvanceToNextBlock();
    }
  }

  // No other chunks in this list have a docID >= 'doc_id', so return this sentinel value to indicate this.
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

  // Need to set the correct offset for the positions and other frequency dependent properties.
  chunk->UpdatePropertiesOffset();
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

// TODO: Would be useful to store the total number of chunks contained in a block and the size of the block header.
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
      list_data->AdvanceToNextBlock();
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
  // TODO: Get value from doc map.
  return 100;
}

const char* IndexReader::GetDocUrl(uint32_t doc_id) {
  // TODO: Get value from doc map.
  return "url";
}

void IndexReader::LoadDocMap(const char* doc_map_filename) {
  // TODO: Load doc map.
}

void IndexReader::LoadMetaInfo() {
  includes_contexts_ = IndexConfiguration::GetResultValue(meta_info_.GetNumericalValue(meta_properties::kIncludesContexts), false);
  includes_positions_ = IndexConfiguration::GetResultValue(meta_info_.GetNumericalValue(meta_properties::kIncludesPositions), false);
}
