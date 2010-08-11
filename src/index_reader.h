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
// Responsible for providing low level index access operations.
//==============================================================================================================================================================

#ifndef INDEX_READER_H_
#define INDEX_READER_H_

#include <cassert>
#include <stdint.h>

#include "cache_manager.h"
#include "coding_policy.h"
#include "coding_policy_helper.h"
#include "index_configuration.h"
#include "index_layout_parameters.h"
#include "term_hash_table.h"

/**************************************************************************************************************************************************************
 * ChunkDecoder
 *
 * Responsible for decoding the data contained within a chunk and maintains some state during list traversal.
 **************************************************************************************************************************************************************/
class ChunkDecoder {
public:
  struct ChunkProperties {
    bool includes_contexts;
    bool includes_positions;
  };

  ChunkDecoder(const CodingPolicy& doc_id_decompressor, const CodingPolicy& frequency_decompressor, const CodingPolicy& position_decompressor);

  void InitChunk(const ChunkProperties& properties, const uint32_t* buffer, int num_docs);
  void DecodeDocIds();
  void DecodeProperties();
  int DecodeFrequencies(const uint32_t* compressed_frequencies);
  int DecodePositions(const uint32_t* compressed_positions);

  // Update the offset of the properties for the current document.
  // Requires 'frequencies_' to have been already decoded.
  void UpdatePropertiesOffset();

  uint32_t doc_id(int doc_id_idx) const {
    assert(doc_id_idx < num_docs_);
    return doc_ids_[doc_id_idx];
  }

  void set_doc_id(int doc_id_idx, uint32_t doc_id) {
    assert(doc_id_idx < num_docs_);
    doc_ids_[doc_id_idx] = doc_id;
  }

  uint32_t current_frequency() const {
    assert(curr_document_offset_ < num_docs_);
    assert(frequencies_[curr_document_offset_] != 0);
    return frequencies_[curr_document_offset_];
  }

  const uint32_t* current_positions() const {
    assert(curr_position_offset_ < num_positions_);
    return positions_ + curr_position_offset_;
  }

  // Set the offset into the 'doc_ids_' and 'frequencies_' arrays.
  // This allows us to quickly get to the frequency information for the current
  // document being processed as well as speed up finding the next document to process in this chunk.
  void set_curr_document_offset(int doc_offset) {
    assert(doc_offset >= 0 && doc_offset >= curr_document_offset_);
    curr_document_offset_ = doc_offset;
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

  void update_prev_decoded_doc_id(uint32_t doc_id_offset) {
    prev_decoded_doc_id_ += doc_id_offset;
  }

  bool decoded() const {
    return decoded_;
  }

  void set_decoded(bool decoded) {
    decoded_ = decoded;
  }

  // The maximum number of documents that can be contained within a chunk.
  static const int kChunkSize = CHUNK_SIZE;
  // The maximum number of properties per document.
  static const int kMaxProperties = MAX_FREQUENCY_PROPERTIES;

private:
  ChunkProperties properties_;  // Controls aspects of how this chunk performs decoding.

  int num_docs_;  // The number of documents in this chunk.

  // These buffers are used for decompression of chunks.
  uint32_t doc_ids_[UncompressedOutBufferUpperbound(kChunkSize)];                     // Array of decompressed docIDs. If stored gap coded, the gaps are not decoded here,
                                                                                      // but rather during query processing.
  uint32_t frequencies_[UncompressedOutBufferUpperbound(kChunkSize)];                 // Array of decompressed frequencies.
  uint32_t positions_[UncompressedOutBufferUpperbound(kChunkSize * kMaxProperties)];  // Array of decomrpessed positions. The position gaps are not decoded here,
                                                                                      // but rather during query processing, if necessary at all.

  int curr_document_offset_;      // The offset into the 'doc_ids_' array of the current document we're processing.
  int prev_document_offset_;      // The position we last stopped at when updating the 'curr_position_offset_'.
  int curr_position_offset_;      // The offset into the 'positions_' array for the current document being processed.
  uint32_t prev_decoded_doc_id_;  // The previously decoded doc id. This is used during query processing when decoding docID gaps.

  int num_positions_;             // Total number of decoded positions in this list.
  bool decoded_properties_;       // True when we have decoded the document properties (that is, frequencies, positions, etc).
                                  // Necessary because we don't want to decode the frequencies/contexts/positions until we're certain we actually need them.

  const uint32_t* curr_buffer_position_;  // Pointer to the raw data of stuff we have to decode next.

  bool decoded_;  // True when we have decoded the docIDs.

  // Decompressors for various portions of the chunk.
  const CodingPolicy& doc_id_decompressor_;
  const CodingPolicy& frequency_decompressor_;
  const CodingPolicy& position_decompressor_;
};

/**************************************************************************************************************************************************************
 * BlockDecoder
 *
 * Responsible for decoding a block from a particular inverted list.
 **************************************************************************************************************************************************************/
class BlockDecoder {
public:
  BlockDecoder(const CodingPolicy& doc_id_decompressor, const CodingPolicy& frequency_decompressor, const CodingPolicy& position_decompressor,
               const CodingPolicy& block_header_decompressor);

  void InitBlock(uint64_t block_num, int starting_chunk, uint32_t* block_data);

  // Decompresses the block header starting at the 'compressed_header' location in memory.
  // The last docIDs and sizes of chunks will be stored internally in this class after decoding,
  // and the memory freed during destruction.
  int DecodeHeader(uint32_t* compressed_header);

  // Returns the last fully decoded docID of the ('chunk_idx'+1)th chunk in the block.
  uint32_t chunk_last_doc_id(int chunk_idx) const {
    int idx = 2 * chunk_idx;
    assert(idx < (num_chunks_ * 2));
    return chunk_properties_[idx];
  }

  // Returns the size of the ('chunk_idx'+1)th chunk in the block in words (where a word is sizeof(uint32_t)).
  uint32_t chunk_size(int chunk_idx) const {
    int idx = 2 * (chunk_idx + 1) - 1;
    assert(idx < (num_chunks_ * 2));
    return chunk_properties_[idx];
  }

  // Returns a pointer to the current chunk decoder, that is, the one currently being traversed.
  ChunkDecoder* curr_chunk_decoder() {
    return &curr_chunk_decoder_;
  }

  // Returns true if the current chunk has been decoded (the docIDs were decoded).
  bool curr_chunk_decoded() const {
    return curr_chunk_decoder_.decoded();
  }

  // Returns the total number of chunks in this block.
  // This includes any chunks that are not part of the inverted list for this particular term.
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
  int curr_chunk() const {
    return curr_chunk_;
  }

  // Updates 'curr_block_data_' to point to the next chunk in the raw data block for decoding.
  void advance_curr_chunk() {
    curr_block_data_ += chunk_size(curr_chunk_);
    ++curr_chunk_;
  }

  static const int kBlockSize = BLOCK_SIZE;

private:
  static const int kChunkSizeLowerBound = MIN_COMPRESSED_CHUNK_SIZE;

  // The upper bound on the number of chunk properties in a single block (with upperbounds for proper decompression by various coding policies),
  // calculated by getting the max number of chunks in a block and multiplying by 2 properties per chunk.
  static const int kChunkPropertiesDecompressedUpperbound = UncompressedOutBufferUpperbound(2 * (kBlockSize / kChunkSizeLowerBound));

  // For each chunk in the block corresponding to this current BlockDecoder, holds the last docIDs and sizes,
  // where the size is in words, and a word is sizeof(uint32_t). The last docID is always followed by the size, for every chunk, in this order.
  uint32_t chunk_properties_[kChunkPropertiesDecompressedUpperbound];  // This will require ~11KiB of memory. Alternative is to make dynamic allocations and resize if necessary.

  uint64_t curr_block_num_;                             // The current block number.
  int num_chunks_;                                      // The total number of chunks this block holds, regardless of which list it is.
  int starting_chunk_;                                  // The chunk number of the first chunk in the block of the associated list.
  int curr_chunk_;                                      // The current actual chunk number we're up to within a block.
  uint32_t* curr_block_data_;                           // Points to the start of the next chunk to be decoded.
  ChunkDecoder curr_chunk_decoder_;                     // Decoder for the current chunk we're processing.
  const CodingPolicy& block_header_decompressor_;       // Decompressor for the block header.
};

/**************************************************************************************************************************************************************
 * ListData
 *
 * Used by the IndexReader during list traversal to hold current state of the list we're traversing.
 **************************************************************************************************************************************************************/
class ListData {
public:
  ListData(uint64_t initial_block_num, int starting_chunk, int num_docs, CacheManager& cache_manager, const CodingPolicy& doc_id_decompressor,
           const CodingPolicy& frequency_decompressor, const CodingPolicy& position_decompressor, const CodingPolicy& block_header_decompressor);
  ~ListData();

  void AdvanceToNextBlock();

  BlockDecoder* curr_block_decoder() {
    return &curr_block_decoder_;
  }

  int num_docs() const {
    return num_docs_;
  }

  int num_docs_left() const {
    return num_docs_left_;
  }

  void update_num_docs_left() {
    num_docs_left_ -= ChunkDecoder::kChunkSize;
  }

  void set_num_docs_left(int num_docs) {
    num_docs_left_ = num_docs;
  }

  int num_chunks() const {
    return num_chunks_;
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

  uint64_t cached_bytes_read() const {
    return cached_bytes_read_;
  }

  uint64_t disk_bytes_read() const {
    return disk_bytes_read_;
  }

private:
  const int kReadAheadBlocks;        // The number of blocks we want to read ahead into the cache.
  CacheManager& cache_manager_;      // Used to retrieve blocks from the inverted list.
  uint64_t initial_block_num_;       // The initial block number this list starts in.
  uint64_t last_queued_block_num_;   // The (next to) last block number queued for transfer. Once we are up to the this block number,
                                     // we know that we need to queue up this block as well as the next 'kReadAheadBlocks' more blocks for transfer.
  uint64_t curr_block_num_;          // The current block number we're up to during traversal of the inverted list.
  BlockDecoder curr_block_decoder_;  // The current block being processed in this inverted list.
  int num_docs_;                     // The total number of documents in this inverted list.
  int num_docs_left_;                // The number of documents we have left to traverse in this inverted list (it could be negative by the end).
                                     // Only updated after NextGEQ() finishes with a chunk and moves on to the next one.
                                     // Updated and used by the IndexReader in NextGEQ().
  int num_chunks_;                   // The total number of chunks we have in this inverted list.
                                     // Updated and used by the IndexReader in NextGEQ().
  uint32_t prev_block_last_doc_id_;  // The last docID of the last chunk of the previous block. This is used by the IndexReader as necessary in NextGEQ().
                                     // It's necessary for the case when a list spans several blocks and the docIDs are gap coded, so that when decoding gaps
                                     // from docIDs appearing in subsequent blocks (subsequent from the first block), we need to know from what offset
                                     // (the last docID of the previous block) those docIDs start at.

  uint64_t cached_bytes_read_;       // Keeps track of the number of bytes read from the cache for this list.
  uint64_t disk_bytes_read_;         // Keeps track of the number of bytes read from the disk for this list.
};

/**************************************************************************************************************************************************************
 * LexiconData
 *
 * Container for entries in the lexicon.
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
  char* term_;         // Pointer to the term this lexicon entry holds (it is not NULL terminated!).
  int term_len_;       // The length of the 'term_', since it's not NULL terminated.
  int block_number_;   // The initial block number of the inverted list for this term.
  int chunk_number_;   // The initial chunk number in the initial block of the inverted list for this term.
  int num_docs_;       // The total number of documents in the inverted list for this term.
  LexiconData* next_;  // Pointer to the next lexicon entry.
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

  MoveToFrontHashTable<LexiconData>* lexicon_;  // The pointer to the hash table for randomly querying the lexicon.
  int kLexiconBufferSize;                       // Size of buffer used for reading parts of the lexicon.
  char* lexicon_buffer_;                        // Pointer to the current portion of the lexicon we're buffering.
  char* lexicon_buffer_ptr_;                    // Current position in the lexicon buffer.
  int lexicon_fd_;                              // File descriptor for the lexicon.
  off_t lexicon_file_size_;                     // The size of the on disk lexicon file.
  int num_bytes_read_;                          // Number of bytes of lexicon read so far.
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

  enum IndexDataType {
    kDocId, kFrequency, kPosition
  };

  IndexReader(Purpose purpose, DocumentOrder document_order, CacheManager& cache_manager, const char* lexicon_filename, const char* doc_map_filename,
              const char* meta_info_filename);

  ListData* OpenList(const LexiconData& lex_data);
  void CloseList(ListData* list_data);
  uint32_t NextGEQ(ListData* list_data, uint32_t doc_id);
  uint32_t GetFreq(ListData* list_data, uint32_t doc_id);

  // 'list_data' is a pointer to the inverted list information, returned by 'OpenList()'.
  // 'data_type' represents the type of index information we want to retrieve.
  // 'index_data' is an array of size 'index_data_size' where we will store the index data (up to the maximum size it can hold).
  // Returns the number of index data elements that have been stored into 'index_data'. A return of 0 indicates we retrieved all elements from the index.
  // Note: when retrieving position data, a return of -1 means that the supplied array needs to be larger to copy all positions for a single docID.
  // This function is designed to be called multiple times (with the same 'list_data' and 'data_type') to get all the index data in chunks.
  int GetList(ListData* list_data, IndexDataType data_type, uint32_t* index_data, int index_data_size);

  int LoopOverList(ListData* list_data, IndexDataType data_type);

  int GetDocLen(uint32_t doc_id);
  const char* GetDocUrl(uint32_t doc_id);

  void LoadDocMap(const char* doc_map_filename);
  void LoadMetaInfo();

  void ResetStats() {
    total_cached_bytes_read_ = 0;
    total_disk_bytes_read_ = 0;
    total_num_lists_accessed_ = 0;
  }

  Lexicon& lexicon() {
    return lexicon_;
  }

  const IndexConfiguration& meta_info() const {
    return meta_info_;
  }

  uint64_t total_index_bytes() const {
    return cache_manager_.total_index_blocks() * CacheManager::kBlockSize;
  }

  bool includes_contexts() const {
    return includes_contexts_;
  }

  bool includes_positions() const {
    return includes_positions_;
  }

  const CodingPolicy& doc_id_decompressor() const {
    return doc_id_decompressor_;
  }

  const CodingPolicy& frequency_decompressor() const {
    return frequency_decompressor_;
  }

  const CodingPolicy& position_decompressor() const {
    return position_decompressor_;
  }

  const CodingPolicy& block_header_decompressor() const {
    return block_header_decompressor_;
  }

  uint64_t total_cached_bytes_read() const {
    return total_cached_bytes_read_;
  }

  uint64_t total_disk_bytes_read() const {
    return total_disk_bytes_read_;
  }

  uint64_t total_num_lists_accessed() const {
    return total_num_lists_accessed_;
  }

private:
  Purpose purpose_;                    // Changes index reader behavior based on what we're using it for.
  DocumentOrder document_order_;       // The way the docIDs are ordered in the index.
  const char* kLexiconSizeKey;         // The key in the configuration file used to define the lexicon size.
  const long int kLexiconSize;         // The size of the hash table for the lexicon.
  Lexicon lexicon_;                    // The lexicon data structure.
  CacheManager& cache_manager_;        // Manages the block cache.
  IndexConfiguration meta_info_;       // The index meta information.
  bool includes_contexts_;             // True if the index contains context data.
  bool includes_positions_;            // True if the index contains position data.

  // Decompressors for various portions of the index.
  CodingPolicy doc_id_decompressor_;
  CodingPolicy frequency_decompressor_;
  CodingPolicy position_decompressor_;
  CodingPolicy block_header_decompressor_;

  uint64_t total_cached_bytes_read_;   // Keeps track of the number of bytes read from the cache.
  uint64_t total_disk_bytes_read_;     // Keeps track of the number of bytes read from the disk.
  uint64_t total_num_lists_accessed_;  // Keeps track of the total number of inverted lists that were accessed (updated at the time that the list is closed).
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

#include "index_reader-inl.h"

#endif /* INDEX_READER_H_ */
