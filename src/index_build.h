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
// Responsible for building the index. Writes out the lexicon to be used when doing query processing or merging indices. The index is chunkwise compressed and
// also structured in blocks for efficiency and list caching.
//==============================================================================================================================================================

#ifndef INDEX_BUILD_H_
#define INDEX_BUILD_H_

#include <cassert>
#include <stdint.h>

#include "coding_policy.h"
#include "coding_policy_helper.h"
#include "configuration.h"
#include "index_layout_parameters.h"

/**************************************************************************************************************************************************************
 * InvertedListMetaData
 *
 **************************************************************************************************************************************************************/
class InvertedListMetaData {
public:
  InvertedListMetaData();
  InvertedListMetaData(const char* term, int term_len);
  InvertedListMetaData(const char* term, int term_len, int block_number, int chunk_number, int num_docs);
  ~InvertedListMetaData();

  const char* term() const {
    return term_;
  }

  int term_len() const {
    return term_len_;
  }

  int num_layers() const {
    return num_layers_;
  }

  const int* num_docs() const {
    return num_docs_;
  }

  const int* num_chunks() const {
    return num_chunks_;
  }

  const int* num_chunks_last_block() const {
    return num_chunks_last_block_;
  }

  const int* num_blocks() const {
    return num_blocks_;
  }

  const int* block_numbers() const {
    return block_numbers_;
  }

  const int* chunk_numbers() const {
    return chunk_numbers_;
  }

  const float* score_thresholds() const {
    return score_thresholds_;
  }

  const uint32_t* external_index_offsets() const {
    return external_index_offsets_;
  }

  void increase_curr_layer() {
    ++num_layers_;
  }

  void set_curr_layer_num_docs(int num_docs) {
    assert(num_layers_ < kMaxLayers);
    num_docs_[num_layers_] = num_docs;
  }

  void increase_curr_layer_num_docs(int num_more_docs) {
    assert(num_layers_ < kMaxLayers);
    num_docs_[num_layers_] += num_more_docs;
  }

  void set_curr_layer_num_chunks(int num_chunks) {
    assert(num_layers_ < kMaxLayers);
    num_chunks_[num_layers_] = num_chunks;
  }

  void increase_curr_layer_num_chunks(int num_more_chunks) {
    assert(num_layers_ < kMaxLayers);
    num_chunks_[num_layers_] += num_more_chunks;
  }

  void set_curr_layer_num_chunks_last_block(int num_chunks_last_block) {
    assert(num_layers_ < kMaxLayers);
    num_chunks_last_block_[num_layers_] = num_chunks_last_block;
  }

  void increase_curr_layer_num_chunks_last_block(int num_more_chunks_last_block) {
    assert(num_layers_ < kMaxLayers);
    num_chunks_last_block_[num_layers_] += num_more_chunks_last_block;
  }

  void set_curr_layer_num_blocks(int num_blocks) {
    assert(num_layers_ < kMaxLayers);
    num_blocks_[num_layers_] = num_blocks;
  }

  void increase_curr_layer_num_blocks(int num_more_blocks) {
    assert(num_layers_ < kMaxLayers);
    num_blocks_[num_layers_] += num_more_blocks;
  }

  void set_curr_layer_offset(int block_number, int chunk_number) {
    assert(num_layers_ < kMaxLayers);
    block_numbers_[num_layers_] = block_number;
    chunk_numbers_[num_layers_] = chunk_number;
  }

  void set_curr_layer_threshold(float score_threshold) {
    assert(num_layers_ < kMaxLayers);
    score_thresholds_[num_layers_] = score_threshold;
  }

  void set_curr_layer_external_index_offset(uint32_t index_offset) {
    assert(num_layers_ < kMaxLayers);
    external_index_offsets_[num_layers_] = index_offset;
  }

private:
  void Init();

  char* term_;
  int term_len_;

  const static int kMaxLayers = MAX_LIST_LAYERS;  // The max number of layers we support.
  int num_layers_;                                // Used to keep track of the current layer we're processing info for;
                                                  // at the end, will hold the number of layers we have for a particular inverted list.
  int num_docs_[kMaxLayers];
  int num_chunks_[kMaxLayers];
  int num_chunks_last_block_[kMaxLayers];
  int num_blocks_[kMaxLayers];
  int block_numbers_[kMaxLayers];
  int chunk_numbers_[kMaxLayers];
  float score_thresholds_[kMaxLayers];
  uint32_t external_index_offsets_[kMaxLayers];   // The integer offset into the external index where data for the current term layer starts.
};

/**************************************************************************************************************************************************************
 * ChunkEncoder
 *
 * Assumes that all docIDs are in sorted, monotonically increasing order, so naturally, they are assumed to be d-gap coded.
 **************************************************************************************************************************************************************/
class ChunkEncoder {
public:
  ChunkEncoder(uint32_t* doc_ids, uint32_t* frequencies, uint32_t* positions, unsigned char* contexts, int num_docs, int num_properties,
        uint32_t prev_chunk_last_doc_id, const CodingPolicy& doc_id_compressor, const CodingPolicy& frequency_compressor,
        const CodingPolicy& position_compressor);

  void CompressDocIds(uint32_t* doc_ids, int doc_ids_len, const CodingPolicy& doc_id_compressor);
  void CompressFrequencies(uint32_t* frequencies, int frequencies_len, const CodingPolicy& frequency_compressor);
  void CompressPositions(uint32_t* positions, int positions_len, const CodingPolicy& position_compressor);

  uint32_t first_doc_id() const {
    return first_doc_id_;
  }

  uint32_t last_doc_id() const {
    return last_doc_id_;
  }

  float max_score() const {
    return max_score_;
  }

  void set_max_score(float max_score) {
    max_score_ = max_score;
  }

  // Returns the total size of the compressed portions of this chunk in words.
  int size() const {
    return size_;
  }

  int num_docs() const {
    return num_docs_;
  }

  int num_properties() const {
    return num_properties_;
  }

  const uint32_t* compressed_doc_ids() const {
    return compressed_doc_ids_;
  }

  int compressed_doc_ids_len() const {
    return compressed_doc_ids_len_;
  }

  const uint32_t* compressed_frequencies() const {
    return compressed_frequencies_;
  }

  int compressed_frequencies_len() const {
    return compressed_frequencies_len_;
  }

  const uint32_t* compressed_positions() const {
    return compressed_positions_;
  }

  int compressed_positions_len() const {
    return compressed_positions_len_;
  }

  static const int kChunkSize = CHUNK_SIZE;
  static const int kMaxProperties = MAX_FREQUENCY_PROPERTIES;

private:
  int num_docs_;           // Number of unique documents included in this chunk.
  int num_properties_;     // Indicates the total number of frequencies (also positions and contexts if they are included).
                           // Also the same as the number of postings.
  int size_;               // Size of the compressed chunk in bytes.

  uint32_t first_doc_id_;  // Decoded first docID in this chunk.
  uint32_t last_doc_id_;   // Decoded last docID in this chunk.
  float max_score_;        // Maximum partial docID score within this chunk.

  // These buffers are used for compression of chunks.
  uint32_t compressed_doc_ids_[CompressedOutBufferUpperbound(kChunkSize)];                     // Array of compressed docIDs.
  int compressed_doc_ids_len_;                                                                 // Actual compressed length of docIDs in number of words.
  uint32_t compressed_frequencies_[CompressedOutBufferUpperbound(kChunkSize)];                 // Array of compressed frequencies.
  int compressed_frequencies_len_;                                                             // Actual compressed length of frequencies in number of words.
  uint32_t compressed_positions_[CompressedOutBufferUpperbound(kChunkSize * kMaxProperties)];  // Array of compressed positions.
  int compressed_positions_len_;                                                               // Actual compressed length of positions in number of words.
};

/**************************************************************************************************************************************************************
 * BlockEncoder
 *
 * Block header format: 4 byte unsigned integer representing the number of chunks in this block,
 * followed by compressed list of chunk sizes and chunk last docIDs.
 **************************************************************************************************************************************************************/
class BlockEncoder {
public:
  BlockEncoder(const CodingPolicy& block_header_compressor);
  ~BlockEncoder();

  // Attempts to add 'chunk' to the current block.
  // Returns true if 'chunk' was added, false if 'chunk' did not fit into the block.
  bool AddChunk(const ChunkEncoder& chunk);

  // Calling this compresses the header.
  // The header will already be compressed if AddChunk() returned false at one point.
  // So it's only to be used in the special case if this block was not filled to capacity.
  void Finalize();

  void GetBlockBytes(unsigned char* block_bytes, int block_bytes_len);

  uint32_t num_chunks() const {
    return num_chunks_;
  }

  int num_block_header_bytes() const {
    return num_block_header_bytes_;
  }

  int num_doc_ids_bytes() const {
    return num_doc_ids_bytes_;
  }

  int num_frequency_bytes() const {
    return num_frequency_bytes_;
  }

  int num_positions_bytes() const {
    return num_positions_bytes_;
  }

  int num_wasted_space_bytes() const {
    return num_wasted_space_bytes_;
  }

  static const int kBlockSize = BLOCK_SIZE;

private:
  void CopyChunkData(const ChunkEncoder& chunk);
  int CompressHeader(uint32_t* header, uint32_t* output, int header_len);

  static const int kChunkSizeLowerBound = MIN_COMPRESSED_CHUNK_SIZE;

  // The upper bound on the number of chunk properties in a block,
  // calculated by getting the max number of chunks in a block and multiplying by 2 properties per chunk.
  static const int kChunkPropertiesUpperbound = 2 * (kBlockSize / kChunkSizeLowerBound);

  // The upper bound on the number of chunk properties in a single block (sized for proper compression for various coding policies).
  static const int kChunkPropertiesCompressedUpperbound = CompressedOutBufferUpperbound(kChunkPropertiesUpperbound);

  const CodingPolicy& block_header_compressor_;

  uint32_t block_header_size_;  // The size of the block header including the number of chunks; determined in 'Finalize()'.
  uint32_t num_chunks_;         // The number of chunks contained within this block.

  uint32_t block_data_[kBlockSize / sizeof(uint32_t)];  // The compressed chunk data.
  int block_data_offset_;                               // Current offset within the 'block_data_'.

  int chunk_properties_uncompressed_size_;    // Size of the 'chunk_properties_uncompressed_' buffer.
  uint32_t* chunk_properties_uncompressed_;   // Holds the chunk last docIDs and chunk sizes. Needs to be dynamically allocated.
  int chunk_properties_uncompressed_offset_;  // Current offset within the 'chunk_properties_uncompressed_' buffer.

  // This will require ~22KiB of memory. Alternative is to make dynamic allocations and resize if necessary.
  uint32_t chunk_properties_compressed_[kChunkPropertiesCompressedUpperbound];
  int chunk_properties_compressed_len_;  // The current actual size of the compressed chunk properties buffer.

  // The breakdown of bytes in this block.
  int num_block_header_bytes_;
  int num_doc_ids_bytes_;
  int num_frequency_bytes_;
  int num_positions_bytes_;
  int num_wasted_space_bytes_;
};

/**************************************************************************************************************************************************************
 * IndexBuilder
 *
 **************************************************************************************************************************************************************/
class ExternalIndexBuilder;
class IndexBuilder {
public:
  IndexBuilder(const char* lexicon_filename, const char* index_filename, const CodingPolicy& block_header_compressor,
               ExternalIndexBuilder* external_index_builder = NULL);
  ~IndexBuilder();

  void WriteBlocks();

  void Add(const ChunkEncoder& chunk, const char* term, int term_len);

  void WriteLexicon();

  void Finalize();

  void FinalizeLayer(float score_threshold);

  uint64_t total_num_chunks() const {
    return total_num_chunks_;
  }

  uint64_t total_num_per_term_blocks() const {
    return total_num_per_term_blocks_;
  }

  uint64_t num_unique_terms() const {
    return num_unique_terms_;
  }

  uint64_t posting_count() const {
    return posting_count_;
  }

  uint64_t total_num_block_header_bytes() const {
    return total_num_block_header_bytes_;
  }

  uint64_t total_num_doc_ids_bytes() const {
    return total_num_doc_ids_bytes_;
  }

  uint64_t total_num_frequency_bytes() const {
    return total_num_frequency_bytes_;
  }

  uint64_t total_num_positions_bytes() const {
    return total_num_positions_bytes_;
  }

  uint64_t total_num_wasted_space_bytes() const {
    return total_num_wasted_space_bytes_;
  }

private:
  // Buffer up blocks in memory before writing them out to disk.
  const int kBlocksBufferSize;
  BlockEncoder** blocks_buffer_;
  int blocks_buffer_offset_;
  BlockEncoder* curr_block_;

  int curr_block_number_;
  int curr_chunk_number_;

  bool insert_layer_offset_;

  int index_fd_;

  const int kLexiconBufferSize;
  InvertedListMetaData** lexicon_;
  int lexicon_offset_;
  int lexicon_fd_;

  const CodingPolicy& block_header_compressor_;

  // Not all indices need an external index to be built, so it's passed in as a pointer, which could be NULL.
  ExternalIndexBuilder* external_index_builder_;

  // Used to build an in-memory block header index during query processing (when the index is loaded into main memory).
  uint64_t total_num_chunks_;           // The total number of chunks in this index.
  uint64_t total_num_per_term_blocks_;  // The total number of per term blocks in this index
                                        // (as if each term had it's own block, not shared with other terms).

  // Index statistics.
  uint64_t num_unique_terms_;
  uint64_t posting_count_;

  // The breakdown of bytes in this index.
  uint64_t total_num_block_header_bytes_;
  uint64_t total_num_doc_ids_bytes_;
  uint64_t total_num_frequency_bytes_;
  uint64_t total_num_positions_bytes_;
  uint64_t total_num_wasted_space_bytes_;
};

#endif /* INDEX_BUILD_H_ */
