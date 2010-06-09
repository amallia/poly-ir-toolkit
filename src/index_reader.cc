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

#include "compression_toolkit/pfor_coding.h"
#include "compression_toolkit/rice_coding.h"
#include "compression_toolkit/rice_coding2.h"
#include "compression_toolkit/s9_coding.h"
#include "compression_toolkit/s16_coding.h"
#include "compression_toolkit/vbyte_coding.h"
#include "config_file_properties.h"
#include "configuration.h"
#include "globals.h"
#include "index_layout_parameters.h"
#include "logger.h"
#include "meta_file_properties.h"
#include "timer.h"
using namespace std;

/**************************************************************************************************************************************************************
 * DecodedChunk
 *
 **************************************************************************************************************************************************************/
const int DecodedChunk::kChunkSize;  // Initialized in the class definition.
const int DecodedChunk::kMaxProperties;  // Initialized in the class definition.

DecodedChunk::DecodedChunk(const DecodedChunkProperties& properties, const uint32_t* buffer, int num_docs) :
  properties_(properties), num_docs_(num_docs), curr_document_offset_(0), prev_document_offset_(0), curr_position_offset_(0), prev_decoded_doc_id_(0),
      num_positions_(0), decoded_properties_(false), curr_buffer_position_(buffer) {
  curr_buffer_position_ += DecodeDocIds(curr_buffer_position_);
}

int DecodedChunk::DecodeDocIds(const uint32_t* compressed_doc_ids) {
  assert(compressed_doc_ids != NULL);

  s16_coding decompressor;
  // A word is meant to be sizeof(uint32_t) bytes in this context.
  int num_words_consumed = decompressor.Decompression(const_cast<uint32_t*> (compressed_doc_ids), doc_ids_, num_docs_);

  // PForDelta coding with total padding, results in bad compression.
  //pfor_coding decompressor;
  //decompressor.set_size(DecodedChunk::kChunkSize);
  //// A word is meant to be sizeof(uint32_t) bytes in this context.
  //int num_words_consumed = decompressor.Decompression(const_cast<uint32_t*>(compressed_doc_ids), doc_ids_);
  return num_words_consumed;
}

void DecodedChunk::DecodeProperties() {
  curr_buffer_position_ += DecodeFrequencies(curr_buffer_position_);
  if (properties_.includes_positions)
    curr_buffer_position_ += DecodePositions(curr_buffer_position_);

  curr_buffer_position_ = NULL;  // This pointer is no longer valid
  decoded_properties_ = true;
}

int DecodedChunk::DecodeFrequencies(const uint32_t* compressed_frequencies) {
  assert(compressed_frequencies != NULL);

  s9_coding decompressor;
  // A word is meant to be sizeof(uint32_t) bytes in this context.
  int num_words_consumed = decompressor.Decompression(const_cast<uint32_t*> (compressed_frequencies), frequencies_, num_docs_);
  return num_words_consumed;
}

int DecodedChunk::DecodePositions(const uint32_t* compressed_positions) {
  assert(compressed_positions != NULL);

  // Count the number of positions we have in total by summing up all the frequencies.
  for (int i = 0; i < num_docs_; ++i) {
    assert(frequencies_[i] != 0);  // This condition indicates a bug in the program.
    num_positions_ += frequencies_[i];
  }

  assert(num_positions_ <= (DecodedChunk::kChunkSize * DecodedChunk::kMaxProperties));

  pfor_coding decompressor;
  decompressor.set_size(DecodedChunk::kChunkSize);

  // TODO: Make sure parameter matches in encoding code (make global setting).
  const int kPadUnless = 100;  // Pad unless we have < kPadUntil documents left to code into a block.

  int num_whole_chunklets = num_positions_ / DecodedChunk::kChunkSize;
  int compressed_positions_offset = 0;
  int decompressed_positions_offset = 0;
  while (num_whole_chunklets-- > 0) {
    compressed_positions_offset += decompressor.Decompression(const_cast<uint32_t*> (compressed_positions) + compressed_positions_offset, positions_
        + decompressed_positions_offset, DecodedChunk::kChunkSize);
    decompressed_positions_offset += DecodedChunk::kChunkSize;
  }

  int positions_left = num_positions_ % DecodedChunk::kChunkSize;
  if (positions_left == 0) {
    // Nothing to do here.
  } else if (positions_left < kPadUnless) {
    // Decode leftover portion with a non-blockwise decompressor.
    s16_coding leftover_decompressor;
    compressed_positions_offset += leftover_decompressor.Decompression(const_cast<uint32_t*> (compressed_positions) + compressed_positions_offset, positions_
        + decompressed_positions_offset, positions_left);
  } else {
    // Decode leftover portion with a blockwise decompressor, since it was padded to the blocksize.
    compressed_positions_offset += decompressor.Decompression(const_cast<uint32_t*> (compressed_positions) + compressed_positions_offset, positions_
        + decompressed_positions_offset, DecodedChunk::kChunkSize);
  }

  return compressed_positions_offset;
}

/**************************************************************************************************************************************************************
 * BlockData
 *
 **************************************************************************************************************************************************************/
BlockData::BlockData(int block_num, int starting_chunk, CacheManager& cache_manager) :
  num_chunks_(0), curr_chunk_(0), starting_chunk_(starting_chunk), curr_block_data_(cache_manager.GetBlock(block_num)), chunk_properties_(NULL), chunks_(NULL) {
  uint32_t num_chunks;
  memcpy(&num_chunks, curr_block_data_, sizeof(num_chunks));
  num_chunks_ = num_chunks;
  assert(num_chunks_ > 0);
  curr_block_data_ += 1;

  curr_block_data_ += DecompressHeader(curr_block_data_);

  for (int i = 0; i < starting_chunk_; ++i) {
    curr_block_data_ += GetChunkSize(i);
  }

  chunks_ = new DecodedChunk*[num_chunks_ - starting_chunk_];
  for (int i = 0; i < (num_chunks_ - starting_chunk_); i++) {
    chunks_[i] = NULL;
  }
}

BlockData::~BlockData() {
  for (int i = 0; i < (num_chunks_ - starting_chunk_); ++i) {
    delete chunks_[i];
  }
  delete[] chunks_;
  delete[] chunk_properties_;
}

int BlockData::DecompressHeader(uint32_t* compressed_header) {
  int header_len = num_chunks_ * 2;  // Number of integer entries we have to decompress (two integers per chunk).

  // TODO: Abstract away calculation of upper bound for S9/S16.
  // For S9/S16 (due to the way it works internally), need to round number of compressed integers to a 'kS9S16MaxIntsPerWord' multiple
  // and make sure there is at least 'kS9S16MaxIntsPerWord' extra space at the end of the array (hence the +2)
  // to ensure there is ample room for decompression. Can also make 'kS9S16MaxIntsPerWord' a multiple of 2 (make it 32) and use shifts instead of
  // division/multiplication to calculate the upper bound.
  //const int kS9S16MaxIntsPerWord = 28; // In S9/S16 coding, for every compressed word, there is a case where it will have a max of 28 integers compressed within.
  //int header_len_upper_bound = ((header_len / kS9S16MaxIntsPerWord) + 2) * kS9S16MaxIntsPerWord; // Slower to calculate, slightly smaller upper bound.
  int header_len_upper_bound = ((header_len >> 5) + 2) << 5; // Faster to calculate, slightly larger upper bound.

  // 'chunk_properties_' holds the last_doc_id followed by the size for every chunk in this block, in this order.
  // TODO: Try putting an upper bound on this array so as to not make dynamic allocation. This will require lots of memory; each chunk will be at minimum.
  // 3*sizeof(uint32_t) size in bytes (one int for doc id, frequency, position). Upper bound on the number of chunks will be
  // ((block size) / (3*sizeof(uint32_t))).  Then need to do as above, and double this by 2 (we have 2 ints to decode per chunk) and do an upper bound for
  // S9/S16 coding as above. What is the impact on performance and memory usage?
  chunk_properties_ = new uint32_t[header_len_upper_bound];

  s16_coding decompressor;
  // A word is meant to be sizeof(uint32_t) bytes in this context.
  int num_words_consumed = decompressor.Decompression(compressed_header, chunk_properties_, header_len);
  return num_words_consumed;
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

  int close_ret = close(lexicon_fd_);
  assert(close_ret != -1);
}

void Lexicon::Open(const char* lexicon_filename, bool random_access) {
  lexicon_fd_ = open(lexicon_filename, O_RDONLY);
  if (lexicon_fd_ == -1) {
    GetErrorLogger().LogErrno("open(), trying to open lexicon file with read only access", errno, true);
  }

  struct stat stat_buf;
  if (fstat(lexicon_fd_, &stat_buf) == -1) {
    GetErrorLogger().LogErrno("fstat() in Lexicon::Open()", errno, true);
  }
  lexicon_file_size_ = stat_buf.st_size;

  int read_ret = read(lexicon_fd_, lexicon_buffer_, kLexiconBufferSize);
  assert(read_ret != -1);

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
    assert(read_ret != -1);
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
    assert(read_ret != -1);
    lexicon_buffer_ptr_ = lexicon_buffer_;
  }

  // We couldn't read the whole term plus the rest of the integer fields of a lexicon entry into the buffer.
  if (lexicon_buffer_ptr_ + ((kNumLexiconIntEntries - 1) * sizeof(int)) + term_len > lexicon_buffer_ + kLexiconBufferSize) {
    lseek(lexicon_fd_, num_bytes_read_, SEEK_SET);  // Seek just past where we last read data.
    int read_ret = read(lexicon_fd_, lexicon_buffer_, kLexiconBufferSize);
    assert(read_ret != -1);
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
 * ListData
 *
 **************************************************************************************************************************************************************/
ListData::ListData(uint64_t initial_block_num, int starting_chunk, int num_docs, CacheManager& cache_manager) :
  kReadAheadBlocks(atol(Configuration::GetConfiguration().GetValue(config_properties::kReadAheadBlocks).c_str())), cache_manager_(cache_manager),
      initial_block_num_(initial_block_num), last_queued_block_num_(initial_block_num + kReadAheadBlocks), curr_block_num_(initial_block_num_),
      curr_block_(NULL), num_docs_left_(num_docs), num_chunks_left_((num_docs / DecodedChunk::kChunkSize) + ((num_docs % DecodedChunk::kChunkSize == 0) ? 0 : 1)),
      prev_block_last_doc_id_(0) {
  if (kReadAheadBlocks <= 0) {
    GetErrorLogger().Log("Incorrect configuration value for '" + string(config_properties::kReadAheadBlocks) + "'", true);
  }

  cache_manager_.QueueBlocks(initial_block_num_, last_queued_block_num_);
  curr_block_ = new BlockData(curr_block_num_, starting_chunk, cache_manager_);
}

void ListData::AdvanceToNextBlock() {
  delete curr_block_;
  cache_manager_.FreeBlock(curr_block_num_);
  ++curr_block_num_;

  if (num_chunks_left_ > 0) {
    // Read ahead the next several MBs worth of blocks.
    if (curr_block_num_ == last_queued_block_num_) {
      last_queued_block_num_ += kReadAheadBlocks;
      cache_manager_.QueueBlocks(curr_block_num_, last_queued_block_num_);
    }

    // Any subsequent blocks in the list (after the first block, will have the list start at chunk 0).
    curr_block_ = new BlockData(curr_block_num_, 0, cache_manager_);
  } else {
    curr_block_ = NULL;

    // Free any blocks we read ahead, but won't be using since they're not part of the list.
    for (uint64_t i = curr_block_num_; i < last_queued_block_num_; ++i) {
      cache_manager_.FreeBlock(i);
    }
  }
}

ListData::~ListData() {
  if (curr_block_ != NULL) {
    // Free the current block plus any blocks we read ahead.
    for (uint64_t i = curr_block_num_; i < last_queued_block_num_; ++i) {
      cache_manager_.FreeBlock(i);
    }

    delete curr_block_;
  }
}

/**************************************************************************************************************************************************************
 * IndexReader
 *
 * Initiates loading of several MBs worth of blocks ahead since we don't know exactly how many blocks are in the list.  Each block is processed as soon as it's
 * needed and we know it has been loaded into memory.
 **************************************************************************************************************************************************************/
IndexReader::IndexReader(Purpose purpose, DocumentOrder document_order, CacheManager& cache_manager, const char* lexicon_filename,
                         const char* doc_map_filename, const char* meta_info_filename) :
  purpose_(purpose), document_order_(document_order), kLexiconSize(atol(Configuration::GetConfiguration().GetValue(config_properties::kLexiconSize).c_str())),
      lexicon_(kLexiconSize, lexicon_filename, (purpose_ == kRandomQuery)), cache_manager_(cache_manager), includes_contexts_(false),
      includes_positions_(false) {
  if (kLexiconSize <= 0) {
    GetErrorLogger().Log("Incorrect configuration value for '" + string(config_properties::kLexiconSize) + "'", true);
  }

  LoadDocMap(doc_map_filename);
  LoadMetaInfo(meta_info_filename);
}

ListData* IndexReader::OpenList(const LexiconData& lex_data) {
  assert(lex_data.block_number() >= 0 && lex_data.chunk_number() >= 0 && lex_data.num_docs() >= 0);
  ListData* list_data = new ListData(lex_data.block_number(), lex_data.chunk_number(), lex_data.num_docs(), cache_manager_);
  return list_data;
}

// TODO: Improvement would be to delete chunks right away after we're done processing them (and set it's position to NULL in the BlockData).
// We can detect when we no longer need a chunk when we look for a doc id in the chunk, and we're about to move to the next chunk because we haven't
// found the doc id in the current chunk. This is because we're always going to start looking for a doc id at the current chunk before moving to the next one,
// even if we know that we just processed the last doc id of the current chunk.
uint32_t IndexReader::NextGEQ(ListData* list_data, uint32_t doc_id) {
  assert(list_data != NULL);

  // Loop through blocks in this list.
  BlockData* block_data;
  while((block_data = list_data->curr_block()) != NULL) {
    // Loop while we have more chunks in the block and while we still have more chunks in the list, since a block might have more chunks but they're for different lists.
    for (int j = (block_data->starting_chunk() + block_data->curr_chunk()); j < block_data->num_chunks() && list_data->num_chunks_left() > 0; ++j, list_data->decrement_num_chunks_left()) {
      // Check the last doc id of the chunk against the current doc id we're looking for and skip the chunk if possible.
      if (block_data->GetChunkLastDocId(j) >= doc_id) {
        DecodedChunk* chunk;

        // Check if we previously decoded this chunk and decode if necessary.
        if (block_data->GetChunk(j) == NULL) {
          // Find the number of documents contained in this chunk and update the number of unprocessed documents left in this list.
          int num_docs_left = list_data->num_docs_left();
          int docs_in_chunk = DecodedChunk::CalculateNumDocsInChunk(num_docs_left);
          // Update the number of documents left to process in the list.
          list_data->set_num_docs_left(num_docs_left - docs_in_chunk);

          // Create a new chunk and add it to the block.
          block_data->UpdateCurrBlockData(j); // Moves the block data pointer to the current chunk we have to decode and takes into account chunk skipping.
          DecodedChunk::DecodedChunkProperties chunk_properties;
          chunk_properties.includes_contexts = includes_contexts_;
          chunk_properties.includes_positions = includes_positions_;
          chunk = new DecodedChunk(chunk_properties, block_data->curr_block_data(), docs_in_chunk);
          block_data->AddChunk(chunk, j);
        } else {
          chunk = block_data->GetChunk(j);
        }

        uint32_t curr_doc_id_offset = chunk->prev_decoded_doc_id();
        for (int k = chunk->curr_document_offset(); k < chunk->num_docs(); ++k) {
          uint32_t curr_doc_id = chunk->GetDocId(k);

          // Here we handle doc id gaps.
          // The current doc id is offset by the last doc id in the previous chunk for this list (if it exists)
          // and by the previous doc id we have decoded in this chunk.
          if (document_order_ == kSortedGapCoded) {
            chunk->set_prev_decoded_doc_id(curr_doc_id_offset);  // TODO: don't have to set every time in loop, just right before we return the found doc id.
            curr_doc_id_offset += curr_doc_id;
            curr_doc_id = curr_doc_id_offset;

            // List is across blocks and we need offset from previous block.
            if (!list_data->initial_block() && j == 0 && block_data->curr_chunk() == block_data->starting_chunk()) {
              curr_doc_id += list_data->prev_block_last_doc_id();
            }

            // We need offset from previous chunk if this is the first chunk in the list.
            if (j > block_data->starting_chunk()) {
              curr_doc_id += block_data->GetChunkLastDocId(j - 1);
            }
          }

          // Found the doc id we're looking for.
          if (curr_doc_id >= doc_id) {
            block_data->SetCurrChunk(j);
            chunk->SetDocumentOffset(k);  // Important for getting the correct frequency and positions list for document during ranking.
            return curr_doc_id;
          }
        }

        // Moving on to the next chunk.
        // We could not find the doc id we were looking for, so we need to set the current chunk in the block
        // so the pointer to the next chunk is correctly offset in the block data.
        block_data->SetCurrChunk(j);
      } else {
        // If we'll be skipping this chunk completely without decoding, we still need to update the number of documents left to process in this list.
        // First make sure we haven't previously decoded this chunk, so we don't double count it.
        if (block_data->GetChunk(j) == NULL) {
          int num_docs_left = list_data->num_docs_left();
          list_data->set_num_docs_left(num_docs_left - DecodedChunk::CalculateNumDocsInChunk(num_docs_left));
        }
      }
    }

    // If list is across blocks, need to get offset from previous block, to be used when decoding doc id gaps.
    list_data->set_prev_block_last_doc_id(block_data->GetChunkLastDocId(block_data->num_chunks() - 1));

    // We're moving on to process the next block. This block is of no use to us anymore.
    list_data->AdvanceToNextBlock();
  }

  // No other chunks in this list have a doc id >= 'doc_id', so return this sentinel value to indicate this.
  return numeric_limits<uint32_t>::max();
}

uint32_t IndexReader::GetFreq(ListData* list_data, uint32_t doc_id) {
  assert(list_data != NULL);
  DecodedChunk* chunk = list_data->curr_block()->GetCurrChunk();
  assert(chunk != NULL);
  // Decode the frequencies and positions if we haven't already.
  if (!chunk->decoded_properties()) {
    chunk->DecodeProperties();
  }

  // Need to set the correct offset for the positions and other frequency dependent properties.
  chunk->SetPropertiesOffset();
  return chunk->GetCurrentFrequency();
}

void IndexReader::CloseList(ListData* list_data) {
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

void IndexReader::LoadMetaInfo(const char* meta_info_filename) {
  meta_info_.LoadKeyValueStore(meta_info_filename);

  string includes_contexts = meta_info_.GetValue(meta_properties::kIncludesContexts);
  if (includes_contexts.size() > 0) {
    includes_contexts_ = atoi(includes_contexts.c_str());
  } else {
    GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kIncludesContexts) + "' value.", false);
  }

  string includes_positions = meta_info_.GetValue(meta_properties::kIncludesPositions);
  if (includes_positions.size() > 0) {
    includes_positions_ = atoi(includes_positions.c_str());
  } else {
    GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kIncludesPositions) + "' value.", false);
  }
}
