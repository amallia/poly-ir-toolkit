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
// TODO: It will be useful to also merge positions. This happens when indices have overlapping doc ids, such as when combining versioned collections or in recrawling, etc.
// This would makes it more flexible.
//
// TODO: When implementing document reordering, the Chunks created to be placed into the new index have a boolean parameter that indicates whether the doc ids
// are sequentially ordered. This will need to be modified.
//
// TODO: Need to write a meta file for merged indices and would be useful to confirm the statistics from the new index with the aggregated statistics from
// the metafiles of the indices that were merged, and warn about inconsistencies.
//
// TODO: Merging Optimizations: Think about cases where we don't need to decompress chunks, and simply copy them to save time. Will need interfaces for this at index reader level.
// * When a term appears in only one of the indices being merged, we can simply "copy" certain blocks into the new index.
//   (The first and last blocks will need to be decompressed and reconstructed due to other lists that need to be contained in the blocks).
// * When merging indices where you know that doc ids are strictly increasing, and there is no overlap, probably don't have to decompress chunks.
// * Can specialize merging for say, 2 indices, where you don't have to use a heap.
//==============================================================================================================================================================

#include "index_merge.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <iostream>

#include "globals.h"
#include "index_build.h"
#include "index_reader.h"
#include "key_value_store.h"
#include "logger.h"
using namespace std;

/**************************************************************************************************************************************************************
 * IndexMerger
 *
 **************************************************************************************************************************************************************/
IndexMerger::IndexMerger(const std::vector<IndexFiles>& input_index_files, const IndexFiles& out_index_files) :
  index_builder_(new IndexBuilder(out_index_files.lexicon_filename().c_str(), out_index_files.index_filename().c_str())) {

  for (size_t i = 0; i < input_index_files.size(); ++i) {
    const IndexFiles& curr_index_files = input_index_files[i];

    CacheManager* cache_policy = new MergingCachePolicy(curr_index_files.index_filename().c_str());
    IndexReader* index_reader = new IndexReader(IndexReader::kMerge, IndexReader::kSortedGapCoded, *cache_policy, curr_index_files.lexicon_filename().c_str(),
                                                curr_index_files.document_map_filename().c_str(), curr_index_files.meta_info_filename().c_str());

    Index* index = new Index(cache_policy, index_reader);
    indices_.push_back(index);
  }

  Merge();
  index_builder_->Finalize();

  GetDefaultLogger().Log("Finished merging.", false);
}

// TODO: MergeOneHeap() algorithm seems slower than MergeTwoHeaps(). But what about on very long lists?
void IndexMerger::Merge() {
  MergeOneHeap();
//  MergeTwoHeaps();
}

void IndexMerger::MergeOneHeap() {
  Index** heap = new Index*[indices_.size()];
  int heap_size = 0;

  char* curr_term = NULL;
  int curr_term_size = 0;
  int curr_term_len = 0;

  int doc_ids_offset = 0;
  int properties_offset = 0;
  uint32_t doc_ids[Chunk::kChunkSize];
  uint32_t frequencies[Chunk::kChunkSize];
  uint32_t positions[Chunk::kChunkSize * Chunk::kMaxProperties];
  unsigned char contexts[Chunk::kChunkSize * Chunk::kMaxProperties];

  uint32_t prev_chunk_last_doc_id = 0;
  uint32_t prev_doc_id = 0;

  // Initialize the heap.
  for (size_t i = 0; i < indices_.size(); ++i) {
    Index* index = indices_[i];

    if (index->NextTerm() && index->NextDocId())
      heap[heap_size++] = index;
  }

  make_heap(heap, heap + heap_size, IndexComparison());

  int j = 0;
  while (heap_size) {
    Index* top_list = heap[0];

    // A continuation of the same list.
    if (top_list->curr_term_len() == curr_term_len && strncmp(top_list->curr_term(), curr_term, min(top_list->curr_term_len(), curr_term_len)) == 0) {
      uint32_t curr_frequency = top_list->index_reader()->GetFreq(top_list->curr_list_data(), top_list->curr_doc_id());
      const uint32_t* curr_positions = top_list->curr_list_data()->curr_block()->GetCurrChunk()->GetCurrentPositions();

      // Copy the positions.
      for (size_t i = 0; i < curr_frequency; ++i) {
        positions[properties_offset + i] = curr_positions[i];
      }

      doc_ids[doc_ids_offset] = top_list->curr_doc_id() - prev_doc_id;
      // Check for duplicate doc ids (when the difference between the 'top_list->curr_doc_id()' and 'prev_doc_id' is zero),
      // which is considered a bug.
      // But since 'prev_doc_id' is initialized to 0, which is a valid doc,
      // we have a case where the 'top_list->curr_doc_id()' could start from 0, which is an exception to the rule.
      // Thus, if this is the first iteration and 'top_list->curr_doc_id()' is 0, it is an acceptable case.
      assert(doc_ids[doc_ids_offset] != 0 || (j == 0 && top_list->curr_doc_id() == 0));
      prev_doc_id = top_list->curr_doc_id();

      frequencies[doc_ids_offset] = curr_frequency;
      ++doc_ids_offset;
      properties_offset += curr_frequency;

      if (doc_ids_offset == Chunk::kChunkSize) {
        Chunk* chunk = new Chunk(doc_ids, frequencies, positions, contexts, doc_ids_offset, properties_offset, prev_chunk_last_doc_id, true);
        prev_chunk_last_doc_id = chunk->last_doc_id();
        index_builder_->Add(*chunk, curr_term, curr_term_len);

        doc_ids_offset = 0;
        properties_offset = 0;
      }

      // Need to pop and push to make sure heap property is maintained.
      pop_heap(heap, heap + heap_size, IndexComparison());
      if (top_list->NextDocId() || (top_list->NextTerm() && top_list->NextDocId())) {
        heap[heap_size - 1] = top_list;
        push_heap(heap, heap + heap_size, IndexComparison());
      } else {
        --heap_size;
      }

      ++j;
    } else {
      // A new list found, so we have to dump the chunk from the previous list, if any.
      if (doc_ids_offset > 0) {
        Chunk* chunk = new Chunk(doc_ids, frequencies, positions, contexts, doc_ids_offset, properties_offset, prev_chunk_last_doc_id, true);
        index_builder_->Add(*chunk, curr_term, curr_term_len);
      }

      if (top_list->curr_term_len() > curr_term_size) {
        delete[] curr_term;
        curr_term_size = top_list->curr_term_len() * 2;
        curr_term = new char[curr_term_size];
      }
      memcpy(curr_term, top_list->curr_term(), top_list->curr_term_len());
      curr_term_len = top_list->curr_term_len();

      doc_ids_offset = 0;
      properties_offset = 0;

      prev_chunk_last_doc_id = 0;
      prev_doc_id = 0;

      j = 0;
    }
  }

  // Leftover chunk.
  if (doc_ids_offset > 0) {
    Chunk* chunk = new Chunk(doc_ids, frequencies, positions, contexts, doc_ids_offset, properties_offset, prev_chunk_last_doc_id, true);
    index_builder_->Add(*chunk, curr_term, curr_term_len);
  }
  delete[] curr_term;
}

void IndexMerger::MergeTwoHeaps() {
  Index** list_heap = new Index*[indices_.size()];
  int list_heap_size = 0;

  // Initialize the list heap.
  for (size_t i = 0; i < indices_.size(); ++i) {
    Index* index = indices_[i];

    if (index->NextTerm())
      list_heap[list_heap_size++] = index;
  }

  make_heap(list_heap, list_heap + list_heap_size, IndexTermComparison());

  Index** same_term_indices = new Index*[indices_.size()];
  int num_same_term_indices = 0;

  const char* curr_term = NULL;
  int curr_term_len = 0;

  while (true) {
    if (list_heap_size) {
      Index* top_list = list_heap[0];

      if (top_list->curr_term_len() == curr_term_len && strncmp(top_list->curr_term(), curr_term, min(top_list->curr_term_len(), curr_term_len)) == 0) {
        if (top_list->NextDocId()) {
          assert(num_same_term_indices < static_cast<int>(indices_.size()));
          same_term_indices[num_same_term_indices++] = top_list;
        }

        // Need to pop and push to make sure heap property is maintained.
        pop_heap(list_heap, list_heap + list_heap_size, IndexTermComparison());
        --list_heap_size;
      } else {
        if (num_same_term_indices > 0)
          PrepareLists(list_heap, &list_heap_size, same_term_indices, &num_same_term_indices, curr_term, curr_term_len);

        curr_term = top_list->curr_term();
        curr_term_len = top_list->curr_term_len();
      }
    } else if (num_same_term_indices > 0) {
      PrepareLists(list_heap, &list_heap_size, same_term_indices, &num_same_term_indices, curr_term, curr_term_len);
    } else {
      break;
    }
  }
}

void IndexMerger::PrepareLists(Index** list_heap, int* list_heap_size, Index** same_term_indices, int* num_same_term_indices, const char* term, int term_len) {
  make_heap(same_term_indices, same_term_indices + *num_same_term_indices, IndexDocIdComparison());

  MergeLists(same_term_indices, *num_same_term_indices, term, term_len);

  // After using these indices for merging, put them back on the list heap, with their next term.
  for (int i = 0; i < *num_same_term_indices; ++i) {
    if (same_term_indices[i]->NextTerm()) {
      list_heap[(*list_heap_size)++] = same_term_indices[i];
      push_heap(list_heap, list_heap + *list_heap_size, IndexTermComparison());
    }
  }

  *num_same_term_indices = 0;
}

void IndexMerger::MergeLists(Index** posting_heap, int posting_heap_size, const char* term, int term_len) {
  int doc_ids_offset = 0;
  int properties_offset = 0;
  uint32_t doc_ids[Chunk::kChunkSize];
  uint32_t frequencies[Chunk::kChunkSize];
  uint32_t positions[Chunk::kChunkSize * Chunk::kMaxProperties];
  unsigned char contexts[Chunk::kChunkSize * Chunk::kMaxProperties];

  uint32_t prev_chunk_last_doc_id = 0;
  uint32_t prev_doc_id = 0;

  int j = 0;
  while (posting_heap_size) {
    Index* top_list = posting_heap[0];

    uint32_t curr_frequency = top_list->index_reader()->GetFreq(top_list->curr_list_data(), top_list->curr_doc_id());
    const uint32_t* curr_positions = top_list->curr_list_data()->curr_block()->GetCurrChunk()->GetCurrentPositions();

    // Copy the positions.
    for (size_t i = 0; i < curr_frequency; ++i) {
      positions[properties_offset + i] = curr_positions[i];
    }

    doc_ids[doc_ids_offset] = top_list->curr_doc_id() - prev_doc_id;
    // Check for duplicate doc ids (when the difference between the 'top_list->curr_doc_id()' and 'prev_doc_id' is zero),
    // which is considered a bug.
    // But since 'prev_doc_id' is initialized to 0, which is a valid doc,
    // we have a case where the 'top_list->curr_doc_id()' could start from 0, which is an exception to the rule.
    // Thus, if this is the first iteration and 'top_list->curr_doc_id()' is 0, it is an acceptable case.
    assert(doc_ids[doc_ids_offset] != 0 || (j == 0 && top_list->curr_doc_id() == 0));
    prev_doc_id = top_list->curr_doc_id();

    frequencies[doc_ids_offset] = curr_frequency;
    ++doc_ids_offset;
    properties_offset += curr_frequency;

    if (doc_ids_offset == Chunk::kChunkSize) {
      Chunk* chunk = new Chunk(doc_ids, frequencies, positions, contexts, doc_ids_offset, properties_offset, prev_chunk_last_doc_id, true);
      prev_chunk_last_doc_id = chunk->last_doc_id();
      index_builder_->Add(*chunk, term, term_len);

      doc_ids_offset = 0;
      properties_offset = 0;
    }

    // Need to pop and push to make sure heap property is maintained.
    pop_heap(posting_heap, posting_heap + posting_heap_size, IndexDocIdComparison());
    if (top_list->NextDocId()) {
      posting_heap[posting_heap_size - 1] = top_list;
      push_heap(posting_heap, posting_heap + posting_heap_size, IndexDocIdComparison());
    } else {
      --posting_heap_size;
    }

    ++j;
  }

  // Leftover chunk.
  if (doc_ids_offset > 0) {
    Chunk* chunk = new Chunk(doc_ids, frequencies, positions, contexts, doc_ids_offset, properties_offset, prev_chunk_last_doc_id, true);
    index_builder_->Add(*chunk, term, term_len);
  }
}

IndexMerger::~IndexMerger() {
  for (size_t i = 0; i < indices_.size(); ++i) {
    delete indices_[i];
  }
  delete index_builder_;
}

/**************************************************************************************************************************************************************
 * CollectionMerger
 *
 * TODO: Current parameters assume that the input files are all from the initial indexing step; other cases should also be possible.
 **************************************************************************************************************************************************************/
CollectionMerger::CollectionMerger(int num_initial_indices, int merge_degree) :
  kMergeDegree(merge_degree) {
  GetDefaultLogger().Log("Merging " + logger::Stringify(num_initial_indices) + " indices with merge degree " + logger::Stringify(merge_degree), false);

  vector<IndexFiles> input_index_files;
  for (int i = 0; i < num_initial_indices; ++i) {
    IndexFiles index_files(0, i);
    input_index_files.push_back(index_files);
  }

  // Calculate number of passes required to completely merge the initial indices.
  // Need to take logarithm with base 'kMergeDegree' of 'num_initial_indices'.
  // Use the change of base formula to do this.
  int num_passes = ceil(log(num_initial_indices) / log(kMergeDegree));

  Partition(input_index_files, 1, num_passes);
}

CollectionMerger::CollectionMerger(vector<IndexFiles>& input_index_files, int merge_degree) :
  kMergeDegree(merge_degree) {
  GetDefaultLogger().Log("Merging " + logger::Stringify(input_index_files.size()) + " indices with merge degree " + logger::Stringify(merge_degree), false);

  // Calculate number of passes required to completely merge the initial indices.
  // Need to take logarithm with base 'kMergeDegree' of 'num_initial_indices'.
  // Use the change of base formula to do this.
  int num_passes = ceil(log(input_index_files.size()) / log(kMergeDegree));
  Partition(input_index_files, 1, num_passes);
}

// TODO: Check return values for rename() and remove().
void CollectionMerger::Partition(const vector<IndexFiles>& input_index_files, int pass_num, int num_passes) {
  if (input_index_files.size() == 1) {
    IndexFiles final_index_files;
    const IndexFiles& curr_index_files = input_index_files.front();

    rename(curr_index_files.index_filename().c_str(), final_index_files.index_filename().c_str());
    rename(curr_index_files.lexicon_filename().c_str(), final_index_files.lexicon_filename().c_str());
    rename(curr_index_files.document_map_filename().c_str(), final_index_files.document_map_filename().c_str());
    rename(curr_index_files.meta_info_filename().c_str(), final_index_files.meta_info_filename().c_str());
    return;
  }

  if (num_passes == 0 || input_index_files.empty()) {
    return;
  }

  vector<IndexFiles> next_pass_index_files, curr_pass_index_files;

  size_t last_group = input_index_files.size() % kMergeDegree;
  size_t num_groups = (input_index_files.size() / kMergeDegree) + (last_group > 0 ? 1 : 0);

  for (size_t i = 0; i < num_groups; ++i) {
    GetDefaultLogger().Log("Merging files:", false);

    for (size_t j = 0; j < ((i == num_groups - 1) && (last_group != 0) ? last_group : kMergeDegree); ++j) {
      GetDefaultLogger().Log(input_index_files[i * kMergeDegree + j].index_filename(), false);

      curr_pass_index_files.push_back(input_index_files[i * kMergeDegree + j]);
    }

    IndexFiles curr_out_index_files(pass_num, i);

    switch (curr_pass_index_files.size()) {
      case 0:
        assert(false);  // This shouldn't happen.
        break;
      case 1:
        rename(curr_pass_index_files[0].index_filename().c_str(), curr_out_index_files.index_filename().c_str());
        rename(curr_pass_index_files[0].lexicon_filename().c_str(), curr_out_index_files.lexicon_filename().c_str());
        rename(curr_pass_index_files[0].document_map_filename().c_str(), curr_out_index_files.document_map_filename().c_str());
        rename(curr_pass_index_files[0].meta_info_filename().c_str(), curr_out_index_files.meta_info_filename().c_str());
        break;
      default:
        IndexMerger* merger = new IndexMerger(curr_pass_index_files, curr_out_index_files);
        delete merger;
        next_pass_index_files.push_back(curr_out_index_files);
        break;
    }

    // Delete files we no longer need to conserve disk space.
    for (size_t j = 0; j < curr_pass_index_files.size(); j++) {
      remove(curr_pass_index_files[j].index_filename().c_str());
      remove(curr_pass_index_files[j].lexicon_filename().c_str());
      remove(curr_pass_index_files[j].document_map_filename().c_str());
      remove(curr_pass_index_files[j].meta_info_filename().c_str());
    }

    curr_pass_index_files.clear();
  }

  Partition(next_pass_index_files, pass_num + 1, num_passes - 1);
}
