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

#ifndef INDEX_REMAPPER_H_
#define INDEX_REMAPPER_H_

#include <cassert>
#include <stdint.h>

#include <vector>

#include "coding_policy.h"
#include "index_util.h"

/**************************************************************************************************************************************************************
 * IndexRemapper
 *
 **************************************************************************************************************************************************************/
class IndexBuilder;
class IndexRemapper {
public:
  IndexRemapper(const IndexFiles& input_index_files, const std::string& output_index_prefix);
  ~IndexRemapper();

  void GenerateMap(const char* doc_id_map_filename);
  void Remap();

private:
  bool CopyToIndexEntry(IndexEntry* index_entry, PositionsPool* positions_pool);
  void DumpToIndex(IndexEntry* index_entries, int num_index_entries, const char* curr_term, int curr_term_len);
  void DumpIndex();
  void WriteMetaFile(const std::string& meta_filename);

  IndexFiles output_index_files_;             // The index filenames for the current temporary remapped index.
  IndexFiles final_output_index_files_;       // The index filenames for the final remapped index.
  Index* index_;                              // The index we're remapping.
  IndexBuilder* index_builder_;               // The current remapped index we're building.
  int index_count_;                           // The number of the current remapped index we're building.
  std::vector<IndexFiles> remapped_indices_;  // The remapped index files that will need to be merged.

  uint32_t* doc_id_map_;  // The docID map array, indexed by the original docID.
  int doc_id_map_size_;   // The size of the docID map array.
  int doc_id_map_start_;  // The smallest original docID. Used to conserve the size of the docID map array, in case the original docIDs don't start from 0.

  // Some index properties.
  bool includes_contexts_;
  bool includes_positions_;

  // Compressors to be used for various parts of the index.
  CodingPolicy doc_id_compressor_;
  CodingPolicy frequency_compressor_;
  CodingPolicy position_compressor_;
  CodingPolicy block_header_compressor_;

  // The following properties are derived from the original index meta info file.
  uint32_t total_num_docs_;          // The total number of documents.
  uint32_t total_unique_num_docs_;   // The total number of unique documents.
  uint64_t total_document_lengths_;  // The total document lengths of all documents.
  uint64_t document_posting_count_;  // The total document posting count.
  uint64_t index_posting_count_;     // The total index posting count.

  // Assigned as necessary during the remapping operation.
  uint32_t first_doc_id_in_index_;   // The first docID in the remapped index.
  uint32_t last_doc_id_in_index_;    // The last docID in the remapped index.
};

#endif /* INDEX_REMAPPER_H_ */
