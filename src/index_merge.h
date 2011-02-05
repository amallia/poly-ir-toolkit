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

#ifndef INDEX_MERGE_H_
#define INDEX_MERGE_H_

#include <stdint.h>

#include <vector>

#include "coding_policy.h"
#include "index_build.h"
#include "index_util.h"

/**************************************************************************************************************************************************************
 * IndexMerger
 *
 **************************************************************************************************************************************************************/
class IndexMerger {
public:
  IndexMerger(const std::vector<IndexFiles>& input_index_files, const IndexFiles& out_index_files);
  ~IndexMerger();

  void StartMerge();
  float QueryProgress() const;

private:
  void MergeOneHeap();
  void MergeTwoHeaps();
  void PrepareLists(Index** list_heap, int* list_heap_size, Index** same_term_indices, int* num_same_term_indices, const char* term, int term_len);
  void MergeLists(Index** posting_heap, int posting_heap_size, const char* term, int term_len);
  void WriteMetaFile();

  IndexFiles out_index_files_;
  IndexBuilder* index_builder_;
  std::vector<Index*> indices_;

  // Some index properties.
  bool includes_contexts_;
  bool includes_positions_;
  bool remapped_index_;

  // Compressors to be used for various parts of the index.
  CodingPolicy doc_id_compressor_;
  CodingPolicy frequency_compressor_;
  CodingPolicy position_compressor_;
  CodingPolicy block_header_compressor_;

  // The following properties are derived from the individual index meta info files.
  uint32_t total_num_docs_;          // The total number of documents in the merged index.
  uint32_t total_unique_num_docs_;   // The total number of unique documents in the merged index.
  uint64_t total_document_lengths_;  // The total document lengths of all documents in the merged index.
  uint64_t document_posting_count_;  // The total document posting count in the merged index.
  uint64_t index_posting_count_;     // The total index posting count in the merged index.
  uint32_t first_doc_id_in_index_;   // The first docID in the merged index.
  uint32_t last_doc_id_in_index_;    // The last docID in the merged index.
};

/**************************************************************************************************************************************************************
 * CollectionMerger
 *
 **************************************************************************************************************************************************************/
class CollectionMerger {
public:
  CollectionMerger(int num_initial_indices, int merge_degree, bool delete_merged_files);
  CollectionMerger(const std::vector<IndexFiles>& input_index_files, int merge_degree, bool delete_merged_files);
  CollectionMerger(const std::vector<IndexFiles>& input_index_files, const IndexFiles& output_index_files, bool delete_merged_files);

  void Partition(const std::vector<IndexFiles>& input_index_files, int pass_num);
  float CurrentMergeProgress() const;

private:
  int GetNumPasses(int num_indices, int merge_degree) const;
  void RemoveIndexFiles(const IndexFiles& index_files);
  void RenameIndexFiles(const IndexFiles& curr_index_files, const IndexFiles& final_index_files);

  const int kMergeDegree;
  const bool kDeleteMergedFiles;
  IndexMerger* curr_merger_;
  bool curr_merger_active_;
};

#endif /* INDEX_MERGE_H_ */
