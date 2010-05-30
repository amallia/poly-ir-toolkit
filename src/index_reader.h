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
// The query processor. Implements block caching, skipping over chunks, decoding frequencies and positions on demand.
//==============================================================================================================================================================

#ifndef INDEX_READER_H_
#define INDEX_READER_H_

#include <cassert>
#include <stdint.h>

#include "index_layout_parameters.h"
#include "term_hash_table.h"
#include "cache_manager.h"
#include "key_value_store.h"

/**************************************************************************************************************************************************************
 * DecodedChunk
 *
 * A decoded chunk consists of doc ids, frequencies, and positions.
 **************************************************************************************************************************************************************/
class DecodedChunk {
public:
  DecodedChunk(const uint32_t* buffer, int num_docs);

  int DecodeDocIds(const uint32_t* compressed_doc_ids);
  int DecodeFrequencies(const uint32_t* compressed_frequencies);
  int DecodePositions(const uint32_t* compressed_positions);
  void DecodeProperties();

  uint32_t GetDocId(int doc_id_idx) const {
    assert(doc_id_idx < num_docs_);
    return doc_ids_[doc_id_idx];
  }

  uint32_t GetCurrentFrequency() const {
    assert(curr_document_offset_ < num_docs_);
    assert(frequencies_[curr_document_offset_] != 0);
    return frequencies_[curr_document_offset_];
  }

  const uint32_t* GetCurrentPositions() const {
    assert(curr_position_offset_ < num_positions_);
    return positions_ + curr_position_offset_;
  }

  // Set the offset into the 'doc_ids_' and 'frequencies_' arrays.
  // This allows us to quickly get to the frequency information for the current
  // document being processed as well as speed up finding the next document to process in this chunk.
  void SetDocumentOffset(int doc_offset) {
    assert(doc_offset >= 0 && doc_offset >= curr_document_offset_);
    curr_document_offset_ = doc_offset;
  }

  // Update the offset of the properties for the current document.
  // Requires 'frequencies_' to have been already decoded.
  void SetPropertiesOffset() {
    assert(decoded_properties_ == true);
    for (int i = prev_document_offset_; i < curr_document_offset_; ++i) {
      assert(frequencies_[i] != 0);  // This indicates a bug in the program.
      curr_position_offset_ += frequencies_[i];
    }
    prev_document_offset_ = curr_document_offset_;
  }

  // Calculates the number of documents in the next undecoded chunk
  // given the number of documents left unprocessed in the list.
  static int CalculateNumDocsInChunk(int num_docs_left) {
    return (num_docs_left >= DecodedChunk::kChunkSize) ? DecodedChunk::kChunkSize : num_docs_left;
  }

  // Returns the number of documents in this chunk.
  int num_docs() const {
    return num_docs_;
  }

  // Returns true when the document properties have been decoded.
  bool decoded_properties() const {
    return decoded_properties_;
  }

  int curr_document_offset() const {
    return curr_document_offset_;
  }

  int curr_position_offset() const {
    return curr_position_offset_;
  }

  uint32_t prev_decoded_doc_id() const {
    return prev_decoded_doc_id_;
  }

  void set_prev_decoded_doc_id(uint32_t decoded_doc_id) {
    prev_decoded_doc_id_ = decoded_doc_id;
  }

  // The maximum number of documents that can be contained within a chunk.
  static const int kChunkSize = CHUNK_SIZE;
  // The maximum number of properties per document.
  static const int kMaxProperties = MAX_FREQUENCY_PROPERTIES;

private:
  // The number of documents in this chunk.
  int num_docs_;
  // Array of decompressed doc ids. If stored gap coded, the gaps are not decoded here, but rather during query processing.
  uint32_t doc_ids_[kChunkSize];
  // Array of decompressed frequencies.
  uint32_t frequencies_[kChunkSize];
  // Array of decomrpessed positions. The position gaps are not decoded here, but rather during query processing, if necessary at all.
  uint32_t positions_[(((kChunkSize * kMaxProperties) >> 5) + 2) << 5]; //TODO: Abstract away calculation of upper bound for S9/S16.

  // The offset into the 'doc_ids_' array of the current document we're processing.
  int curr_document_offset_;
  // The position we last stopped at when updating the 'curr_position_offset_'.
  int prev_document_offset_;
  // The offset into the 'positions_' array for the current document being processed.
  int curr_position_offset_;

  // The previously decoded doc id. This is used during query processing when decoding doc id gaps.
  uint32_t prev_decoded_doc_id_;

  // Total number of decoded positions in this list.
  int num_positions_;

  // True when we have decoded the document properties (that is, frequencies, positions, etc).
  // Necessary because we don't want to decode the frequencies/contexts/positions until we're certain we actually need them.
  bool decoded_properties_;

  // Pointer to the raw data of stuff we have to decode next.
  const uint32_t* curr_buffer_position_;
};

/**************************************************************************************************************************************************************
 * BlockData
 *
 * Holds the decoded chunks contained within one block of a particular inverted list.
 * The decoded chunks stored are only those that belong to a particular term, but chunks should still be accessed with their actual index within a block on disk.
 * For example, if accessing the first chunk in a list, but the chunk happens to be the 100th chunk in the block
 * (because of chunks corresponding to other lists in the same block), you'll need to access it with index 99 (index count starts at 0).
 **************************************************************************************************************************************************************/
class BlockData {
public:
  BlockData(int block_num, int starting_chunk, CacheManager& cache_manager);
  ~BlockData();

  // Adds a decoded chunk to this block that belongs at the 'chunk_idx' location.
  // This method will actually remap the 'chunk_idx' since we don't store all chunks in this block,
  // but only those that belong to a an inverted list for a particular term, as described in the class description.
  void AddChunk(DecodedChunk* chunk, int chunk_idx) {
    int idx = chunk_idx - starting_chunk_;
    assert(idx < num_actual_chunks());
    chunks_[idx] = chunk;
  }

  // Decompresses the block header starting at the 'compressed_header' location in memory.
  // The last doc ids and sizes of chunks will be stored internally in this class after decoding,
  // and the memory freed during destruction.
  int DecompressHeader(uint32_t* compressed_header);

  // Returns the last fully decoded doc id of the ('chunk_idx'+1)th chunk in the block.
  uint32_t GetChunkLastDocId(int chunk_idx) const {
    int idx = 2 * chunk_idx;
    assert(idx < (num_chunks_ * 2));
    return chunk_properties_[idx];
  }

  // Returns the size of the ('chunk_idx'+1)th chunk in the block in words (where a word is sizeof(uint32_t)).
  uint32_t GetChunkSize(int chunk_idx) const {
    int idx = 2 * (chunk_idx + 1) - 1;
    assert(idx < (num_chunks_ * 2));
    return chunk_properties_[idx];
  }

  // Updates 'curr_block_data_' to point to the next chunk in the raw data block for decoding.
  // 'chunk_idx' is the index of the chunk in the block that we should be decoding next.
  void UpdateCurrBlockData(int chunk_idx) {
    for (int i = (starting_chunk_ + curr_chunk_); i < chunk_idx; ++i) {
      curr_block_data_ += GetChunkSize(i);
    }
  }

  // Returns a pointer to the decoded chunk which is the ('chunk_idx'+1)th chunk in the block.
  // The decoded chunk returned could be NULL if it was never decoded.
  DecodedChunk* GetChunk(int chunk_idx) const {
    int idx = chunk_idx - starting_chunk_;
    assert(idx < num_actual_chunks());
    return chunks_[idx];
  }

  // Returns a pointer to the current decoded chunk, that is, the one currently being traversed.
  DecodedChunk* GetCurrChunk() const {
    assert(curr_chunk_ < num_actual_chunks());
    return chunks_[curr_chunk_];
  }

  // Sets the current chunk to be the ('chunk_idx'+1)th chunk in the block.
  // Of course, the actual index will be remapped.
  void SetCurrChunk(int chunk_idx) {
    curr_chunk_ = chunk_idx - starting_chunk_;
  }

  // Returns the total number of chunks in this block. This includes any chunks that are not part of the inverted list for this particular term.
  int num_chunks() const {
    return num_chunks_;
  }

  // Returns the actual number of chunks stored in this block, that are actually part of the inverted list for this particular term.
  int num_actual_chunks() const {
    return num_chunks_ - starting_chunk_;
  }

  // Returns a pointer to the start of the next raw chunk that needs to be decoded.
  const uint32_t* curr_block_data() const {
    return curr_block_data_;
  }

  // The index of the starting chunk in this block for the inverted list for this particular term.
  int starting_chunk() const {
    return starting_chunk_;
  }

  // Returns the index of the current chunk being traversed.
  // Note: this index is not the index of the chunk in the actual raw block, but rather the location of the current chunk in our internal array
  // of chunks related to the inverted list for a particular term.
  int curr_chunk() const {
    return curr_chunk_;
  }

private:
  // The total number of chunks this block holds, regardless of which list it is.
  int num_chunks_;
  // The current chunk number we're up to in reading documents (0 is actually 'starting_chunk_').
  int curr_chunk_;
  // The chunk number of the first chunk in the block of the associated list.
  int starting_chunk_;
  // Points to the start of the next chunk to be decoded.
  uint32_t* curr_block_data_;
  // For each chunk, holds it's size (in words, where a word is sizeof(uint32_t)) and last doc id.
  uint32_t* chunk_properties_;

  // These chunks are only for the corresponding list that is inside this block!
  // This means that 'chunks_[0]' actually corresponds to the 'starting_chunk_' of the physical block on disk.
  // The size of this array is 'num_chunks_ - starting_chunk_'.
  DecodedChunk** chunks_;
};

/**************************************************************************************************************************************************************
 * LexiconData
 *
 * Container for entries in the lexicon.
 * TODO: Consider storing the integer entries var byte coded to conserve memory.
 **************************************************************************************************************************************************************/
class LexiconData {
public:
  LexiconData() :
    term_(NULL), term_len_(0), block_number_(0), chunk_number_(0), num_docs_(0), next_(NULL) {
  }

  LexiconData(const char* term, int term_len) :
    term_(new char[term_len]), term_len_(term_len), block_number_(0), chunk_number_(0), num_docs_(0), next_(NULL) {
    memcpy(term_, term, term_len);
  }

  LexiconData(const char* term, int term_len, int block_number, int chunk_number, int num_docs) :
    term_(new char[term_len]), term_len_(term_len), block_number_(block_number), chunk_number_(chunk_number), num_docs_(num_docs), next_(NULL) {
    memcpy(term_, term, term_len);
  }

  ~LexiconData() {
    delete[] term_;
  }

  const char* term() const {
    return term_;
  }

  int term_len() const {
    return term_len_;
  }

  int block_number() const {
    return block_number_;
  }

  void set_block_number(int block_number) {
    block_number_ = block_number;
  }

  int chunk_number() const {
    return chunk_number_;
  }

  void set_chunk_number(int chunk_number) {
    chunk_number_ = chunk_number;
  }

  int num_docs() const {
    return num_docs_;
  }

  void set_num_docs(int num_docs) {
    num_docs_ = num_docs;
  }

  void update_num_docs(int more_docs) {
    num_docs_ += more_docs;
  }

  LexiconData* next() const {
    return next_;
  }

  void set_next(LexiconData* next) {
    next_ = next;
  }

private:
  // Pointer to the term this lexicon entry holds (It is not NULL terminated!).
  char* term_;
  // The length of the 'term_', since it's not NULL terminated.
  int term_len_;

  // The initial block number of the inverted list for this term.
  int block_number_;
  // The initial chunk number in the initial block of the inverted list for this term.
  int chunk_number_;

  // The total number of documents in the inverted list for this term.
  int num_docs_;

  // Pointer to the next lexicon entry.
  LexiconData* next_;
};

/**************************************************************************************************************************************************************
 * Lexicon
 *
 **************************************************************************************************************************************************************/
class Lexicon {
public:
  Lexicon(int hash_table_size, const char* lexicon_filename, bool random_access);
  ~Lexicon();

  void Open(const char* lexicon_filename, bool random_access);
  void Close();

  LexiconData* GetEntry(const char* term, int term_len);

  // Should be used only when index is in 'kMerge' mode.
  // Returns the next lexicon entry lexicographically, NULL if no more.
  LexiconData* GetNextEntry();

private:
  struct LexiconEntry {
    char* term;
    int term_len;
    int block_number;
    int chunk_number;
    int num_docs;
  };

  void GetNext(LexiconEntry* lexicon_entry);

  MoveToFrontHashTable<LexiconData>* lexicon_;
  int kLexiconBufferSize;  // Size of buffer used for reading parts of the lexicon.
  char* lexicon_buffer_;
  char* lexicon_buffer_ptr_;
  int lexicon_fd_;
  off_t lexicon_file_size_;
  int num_bytes_read_;  // Number of bytes of lexicon read so far.
};

/**************************************************************************************************************************************************************
 * ListData
 *
 * Used by the IndexReader during list traversal to hold current state of the list we're traversing.
 **************************************************************************************************************************************************************/
class ListData {
public:
  ListData(int initial_block_num, int starting_chunk, int num_docs, CacheManager& cache_manager);
  ~ListData();

  void AdvanceToNextBlock();

  BlockData* curr_block() const {
    return curr_block_;
  }

  int num_docs_left() const {
    return num_docs_left_;
  }

  void set_num_docs_left(int num_docs) {
    num_docs_left_ = num_docs;
  }

  int num_chunks_left() const {
    return num_chunks_left_;
  }

  void set_num_chunks_left(int num_chunks) {
    num_chunks_left_ = num_chunks;
  }

  void decrement_num_chunks_left() {
    --num_chunks_left_;
  }

  uint32_t prev_block_last_doc_id() const {
    return prev_block_last_doc_id_;
  }

  void set_prev_block_last_doc_id(uint32_t doc_id) {
    prev_block_last_doc_id_ = doc_id;
  }

  bool initial_block() const {
    return (curr_block_num_ == initial_block_num_) ? true : false;
  }

private:
  CacheManager& cache_manager_;

  // FIXME: the block nums should be uint64_t

  // The initial block number this list starts in.
  int initial_block_num_;

  // The number of blocks we want to read ahead into the cache.
  // TODO: Make configurable.
  const int kReadAheadBlocks;

  // The (next to) last block number queued for transfer. Once we are up to the this block number,
  // we know that we need to queue up this block as well as the next 'kReadAheadBlocks' more blocks for transfer.
  int last_queued_block_num_;

  // The current block number we're up to during traversal of the inverted list.
  int curr_block_num_;

  // Pointer to the current block being processed in this inverted list.
  BlockData* curr_block_;

  // The number of documents we have left to traverse in this inverted list.
  // Updated and used by the IndexReader in NextGEQ().
  int num_docs_left_;

  // The number of chunks we have left to traverse in this inverted list.
  // Updated and used by the IndexReader in NextGEQ().
  int num_chunks_left_;

  // The last doc id of the last chunk of the previous block.
  // This is set and get by the IndexReader as necessary in NextGEQ().
  // It's necessary for the case when a list spans several blocks and the
  // doc ids are gap coded, so that when decoding gaps from doc ids appearing
  // in subsequent blocks (subsequent from the first block), we need to know
  // from what offset (the last doc id of the previous block) those doc ids
  // start at.
  uint32_t prev_block_last_doc_id_;
};

/**************************************************************************************************************************************************************
 * IndexReader
 *
 * Contains useful methods for traversing inverted lists.
 * TODO: Query processing across different indexes...probably should be supported by QueryProcessor class.
 **************************************************************************************************************************************************************/
class IndexReader {
public:
  enum Purpose {
    kRandomQuery, kMerge
  };

  enum DocumentOrder {
    kSorted, kSortedGapCoded
  };

  IndexReader(Purpose purpose, DocumentOrder document_order, CacheManager& cache_manager, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename);

  ListData* OpenList(const LexiconData& lex_data);
  void CloseList(ListData* list_data);
  uint32_t NextGEQ(ListData* list_data, uint32_t doc_id);
  uint32_t GetFreq(ListData* list_data, uint32_t doc_id);

  int GetDocLen(uint32_t doc_id);
  const char* GetDocUrl(uint32_t doc_id);

  void LoadDocMap(const char* doc_map_filename);
  void LoadMetaInfo(const char* meta_info_filename);

  uint32_t collection_average_doc_len() const {
    return collection_average_doc_len_;
  }

  uint32_t collection_total_num_docs() const {
    return collection_total_num_docs_;
  }

  Lexicon& lexicon() {
    return lexicon_;
  }

  const KeyValueStore& meta_info() const {
    return meta_info_;
  }

private:
  // Changes index reader behavior based on what we're using it for.
  Purpose purpose_;

  // The way the doc ids are ordered in the index.
  DocumentOrder document_order_;

  // The key in the configuration file used to define the lexicon size.
  const char* kLexiconSizeKey;
  // The size of the hash table for the lexicon.
  const int kLexiconSize;
  // The lexicon data structure.
  Lexicon lexicon_;

  // Manages the block cache.
  CacheManager& cache_manager_;

  // The average document length of a document in the indexed collection.
  // This plays a role in the ranking function.
  uint32_t collection_average_doc_len_;
  // The total number of documents in the indexed collection.
  // This plays a role in the ranking function.
  uint32_t collection_total_num_docs_;

  KeyValueStore meta_info_;
};

/**************************************************************************************************************************************************************
 * ListCompare
 *
 * Compares the inverted list lengths, used to sort query terms in order from shortest list to longest.
 **************************************************************************************************************************************************************/
struct ListCompare {
  bool operator()(const LexiconData* l, const LexiconData* r) const {
    return l->num_docs() < r->num_docs();
  }
};

#endif /* INDEX_READER_H_ */
