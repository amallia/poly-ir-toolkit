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

#include <vector>

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

  void UpdateNumDocs(int more_docs) {
    num_docs_ += more_docs;
  }

private:
  char* term_;
  int term_len_;

  int block_number_;  // The offset for a list is ('block_number_' * 'BLOCK_SIZE'), since each block is the same exact size.
  int chunk_number_;

  int num_docs_;
};

/**************************************************************************************************************************************************************
 * Chunk
 *
 **************************************************************************************************************************************************************/
class Chunk
{
public:
    Chunk(uint32_t* doc_ids, uint32_t* frequencies, uint32_t* positions, unsigned char* contexts, int num_docs, int num_properties, uint32_t prev_chunk_last_doc_id, bool ordered_doc_ids);
    ~Chunk();

    void CompressDocIds(uint32_t* doc_ids, int doc_ids_len);
    void CompressFrequencies(uint32_t* frequencies, int frequencies_len);
    void CompressPositions(uint32_t* positions, int positions_len);

    uint32_t last_doc_id() const {
      return last_doc_id_;
    }

    // Returns the total size of the compressed portions of this chunk in bytes.
    int size() const {
      return size_;
    }

    int num_docs() const {
      return num_docs_;
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

    const unsigned char* compressed_contexts() const {
      return compressed_contexts_;
    }

    int compressed_contexts_len() const {
      return compressed_contexts_len_;
    }

    static const int kChunkSize = CHUNK_SIZE;
    static const int kMaxProperties = MAX_FREQUENCY_PROPERTIES;
private:
    int num_docs_;
    int size_;  // Size of the compressed chunk in bytes.

    uint32_t last_doc_id_;

    uint32_t* compressed_doc_ids_;
    int compressed_doc_ids_len_;

    uint32_t* compressed_frequencies_;
    int compressed_frequencies_len_;

    uint32_t* compressed_positions_;
    int compressed_positions_len_;

    unsigned char* compressed_contexts_;
    int compressed_contexts_len_;
};

/**************************************************************************************************************************************************************
 * Block
 *
 * Block Header format: 4 byte unsigned integer represents # of chunks in this block, followed by compressed list of chunk sizes, followed by compressed list
 * of chunk last doc_ids.
 **************************************************************************************************************************************************************/
class Block {
public:
  Block();
  ~Block();

  // Attempts to add 'chunk' to the current block.
  // Returns true if 'chunk' was added, false if 'chunk' did not fit into the block.
  bool AddChunk(const Chunk& chunk);

  // Calling this compresses the header.
  // The header will already be compressed if AddChunk() returned false at one point.
  // So it's only to be used in the special case if this Block was not filled to capacity.
  void Finalize();

  int block_data_size() const {
    return block_data_size_;
  }

  uint32_t ConvertChunkSize(int chunk_size) const {
    // The 'chunk_size' unit is converted from bytes to integers.
    assert(chunk_size % sizeof(uint32_t) == 0);
    return chunk_size / sizeof(uint32_t);
  }

  uint32_t ConvertChunkLastDocId(int chunk_last_doc_id) const {
    return chunk_last_doc_id;
  }

  void GetBlockBytes(unsigned char* block_bytes, int block_bytes_len);

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

  int num_chunks() const {
    return chunks_.size();
  }

  static const int kBlockSize = BLOCK_SIZE;
private:
  int CompressHeader(uint32_t* in, uint32_t* out, int n);

  int block_data_size_;  // Does not include header size.
  uint32_t* chunk_properties_compressed_;
  int chunk_properties_compressed_len_;
  std::vector<const Chunk*> chunks_;

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
class IndexBuilder {
public:
  IndexBuilder(const char* lexicon_filename, const char* index_filename);
  ~IndexBuilder();

  void WriteBlocks();

  void Add(const Chunk& chunk, const char* term, int term_len);

  void WriteLexicon();

  void Finalize();

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
  Block** blocks_buffer_;
  int blocks_buffer_offset_;
  Block* curr_block_;

  int curr_block_number_;
  int curr_chunk_number_;
  const char* kIndexFilename;
  int index_fd_;

  const int kLexiconBufferSize;
  InvertedListMetaData** lexicon_;
  int lexicon_offset_;
  int lexicon_fd_;

  // The breakdown of bytes in this index.
  uint64_t total_num_block_header_bytes_;
  uint64_t total_num_doc_ids_bytes_;
  uint64_t total_num_frequency_bytes_;
  uint64_t total_num_positions_bytes_;
  uint64_t total_num_wasted_space_bytes_;
};

#endif /* INDEX_BUILD_H_ */
