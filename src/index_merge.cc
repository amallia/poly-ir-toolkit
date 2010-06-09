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
// TODO: It will be useful to also merge positions. This happens when indices have overlapping docIDs, such as when combining versioned collections or in recrawling, etc.
// This would makes it more flexible.
//
// TODO: Merging Optimizations: Think about cases where we don't need to decompress chunks, and simply copy them to save time. Will need interfaces for this at index reader level.
// * When a term appears in only one of the indices being merged, we can simply "copy" certain blocks into the new index.
//   (The first and last blocks will need to be decompressed and reconstructed due to other lists that need to be contained in the blocks).
// * When merging indices where you know that docIDs are strictly increasing, and there is no overlap, probably don't have to decompress chunks.
// * Can specialize merging for say, 2 indices, where you don't have to use a heap.
//==============================================================================================================================================================

#include "index_merge.h"

#include <cerrno>
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
#include "meta_file_properties.h"
using namespace std;

/**************************************************************************************************************************************************************
 * IndexMerger
 *
 **************************************************************************************************************************************************************/
IndexMerger::IndexMerger(const std::vector<IndexFiles>& input_index_files, const IndexFiles& out_index_files) :
  out_index_files_(out_index_files), index_builder_(new IndexBuilder(out_index_files_.lexicon_filename().c_str(), out_index_files_.index_filename().c_str())),
      includes_contexts_(true), includes_positions_(true), total_num_docs_(0), total_unique_num_docs_(0), total_document_lengths_(0),
      document_posting_count_(0), index_posting_count_(0), first_doc_id_in_index_(0), last_doc_id_in_index_(0) {
  for (size_t i = 0; i < input_index_files.size(); ++i) {
    const IndexFiles& curr_index_files = input_index_files[i];

    CacheManager* cache_policy = new MergingCachePolicy(curr_index_files.index_filename().c_str());
    IndexReader* index_reader = new IndexReader(IndexReader::kMerge, IndexReader::kSortedGapCoded, *cache_policy, curr_index_files.lexicon_filename().c_str(),
                                                curr_index_files.document_map_filename().c_str(), curr_index_files.meta_info_filename().c_str());

    // If one index from the ones to be merged does not contain contexts or positions
    // then the whole merged index will not contain them.
    if (!index_reader->includes_contexts())
      includes_contexts_ = false;

    if (!index_reader->includes_positions())
      includes_positions_ = false;

    string total_num_docs = index_reader->meta_info().GetValue(meta_properties::kTotalNumDocs);
    if (total_num_docs.size() > 0) {
      total_num_docs_ += atol(total_num_docs.c_str());
    } else {
      GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kTotalNumDocs) + "' value.", false);
    }

    string total_unique_num_docs = index_reader->meta_info().GetValue(meta_properties::kTotalUniqueNumDocs);
    if (total_unique_num_docs.size() > 0) {
      total_unique_num_docs_ += atol(total_unique_num_docs.c_str());
    } else {
      GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kTotalUniqueNumDocs) + "' value.", false);
    }

    string total_document_lengths = index_reader->meta_info().GetValue(meta_properties::kTotalDocumentLengths);
    if (total_document_lengths.size() > 0) {
      total_document_lengths_ += atol(total_document_lengths.c_str());
    } else {
      GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kTotalDocumentLengths) + "' value.", false);
    }

    string document_posting_count = index_reader->meta_info().GetValue(meta_properties::kDocumentPostingCount);
    if (document_posting_count.size() > 0) {
      document_posting_count_ += atol(document_posting_count.c_str());
    } else {
      GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kDocumentPostingCount) + "' value.", false);
    }

    string index_posting_count = index_reader->meta_info().GetValue(meta_properties::kIndexPostingCount);
    if (index_posting_count.size() > 0) {
      index_posting_count_ += atol(index_posting_count.c_str());
    } else {
      GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kIndexPostingCount) + "' value.", false);
    }

    string first_doc_id_in_index = index_reader->meta_info().GetValue(meta_properties::kFirstDocId);
    if (first_doc_id_in_index.size() > 0) {
      uint32_t doc_id = atol(first_doc_id_in_index.c_str());
      if (doc_id < first_doc_id_in_index_)
        first_doc_id_in_index_ = doc_id;
    } else {
      GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kFirstDocId) + "' value.", false);
    }

    string last_doc_id_in_index = index_reader->meta_info().GetValue(meta_properties::kLastDocId);
    if (last_doc_id_in_index.size() > 0) {
      uint32_t doc_id = atol(last_doc_id_in_index.c_str());
      if (doc_id > last_doc_id_in_index_)
        last_doc_id_in_index_ = doc_id;
    } else {
      GetErrorLogger().Log("Index meta file missing the '" + string(meta_properties::kLastDocId) + "' value.", false);
    }

    Index* index = new Index(cache_policy, index_reader);
    indices_.push_back(index);
  }

  Merge();
  index_builder_->Finalize();
  WriteMetaFile();

  GetDefaultLogger().Log("Finished merging.", false);
}

IndexMerger::~IndexMerger() {
  for (size_t i = 0; i < indices_.size(); ++i) {
    delete indices_[i];
  }
  delete index_builder_;
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

      if (includes_positions_) {
        const uint32_t* curr_positions = top_list->curr_list_data()->curr_block()->GetCurrChunk()->GetCurrentPositions();
        // Copy the positions.
        for (size_t i = 0; i < curr_frequency; ++i) {
          positions[properties_offset + i] = curr_positions[i];
        }
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
        Chunk* chunk = new Chunk(doc_ids, frequencies, (includes_positions_ ? positions : NULL), (includes_contexts_ ? contexts : NULL), doc_ids_offset, properties_offset, prev_chunk_last_doc_id);
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
        Chunk* chunk = new Chunk(doc_ids, frequencies, (includes_positions_ ? positions : NULL), (includes_contexts_ ? contexts : NULL), doc_ids_offset, properties_offset, prev_chunk_last_doc_id);
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
    Chunk* chunk = new Chunk(doc_ids, frequencies, (includes_positions_ ? positions : NULL), (includes_contexts_ ? contexts : NULL), doc_ids_offset, properties_offset, prev_chunk_last_doc_id);
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
    if (includes_positions_) {
      const uint32_t* curr_positions = top_list->curr_list_data()->curr_block()->GetCurrChunk()->GetCurrentPositions();
      // Copy the positions.
      for (size_t i = 0; i < curr_frequency; ++i) {
        positions[properties_offset + i] = curr_positions[i];
      }
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
      Chunk* chunk = new Chunk(doc_ids, frequencies, (includes_positions_ ? positions : NULL), (includes_contexts_ ? contexts : NULL), doc_ids_offset, properties_offset, prev_chunk_last_doc_id);
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
    Chunk* chunk = new Chunk(doc_ids, frequencies, (includes_positions_ ? positions : NULL), (includes_contexts_ ? contexts : NULL), doc_ids_offset, properties_offset, prev_chunk_last_doc_id);
    index_builder_->Add(*chunk, term, term_len);
  }
}

void IndexMerger::WriteMetaFile() {
  KeyValueStore index_metafile;
  ostringstream metafile_values;

  // TODO: Need to write the document offset to be used for the true docIDs in the index.
  // This will allow us to store smaller docIDs (for the non-gap-coded ones, anyway) resulting in better compression.

  metafile_values << includes_positions_;
  index_metafile.AddKeyValuePair(meta_properties::kIncludesPositions, metafile_values.str());
  metafile_values.str("");

  metafile_values << includes_contexts_;
  index_metafile.AddKeyValuePair(meta_properties::kIncludesContexts, metafile_values.str());
  metafile_values.str("");

  metafile_values << total_document_lengths_;
  index_metafile.AddKeyValuePair(meta_properties::kTotalDocumentLengths, metafile_values.str());
  metafile_values.str("");

  metafile_values << total_num_docs_;
  index_metafile.AddKeyValuePair(meta_properties::kTotalNumDocs, metafile_values.str());
  metafile_values.str("");

  metafile_values << total_unique_num_docs_;
  index_metafile.AddKeyValuePair(meta_properties::kTotalUniqueNumDocs, metafile_values.str());
  metafile_values.str("");

  metafile_values << first_doc_id_in_index_;
  index_metafile.AddKeyValuePair(meta_properties::kFirstDocId, metafile_values.str());
  metafile_values.str("");

  metafile_values << last_doc_id_in_index_;
  index_metafile.AddKeyValuePair(meta_properties::kLastDocId, metafile_values.str());
  metafile_values.str("");

  metafile_values << document_posting_count_;
  index_metafile.AddKeyValuePair(meta_properties::kDocumentPostingCount, metafile_values.str());
  metafile_values.str("");

  if (index_posting_count_ != index_builder_->posting_count()) {
    GetErrorLogger().Log("Inconsistency in the '" + string(meta_properties::kIndexPostingCount) + "' meta file property detected: "
        + "sum of the values from the merged index meta files don't add up to the value calculated by the index builder.", false);
  }
  metafile_values << index_builder_->posting_count();
  index_metafile.AddKeyValuePair(meta_properties::kIndexPostingCount, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder_->num_unique_terms();
  index_metafile.AddKeyValuePair(meta_properties::kNumUniqueTerms, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder_->total_num_block_header_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalHeaderBytes, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder_->total_num_doc_ids_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalDocIdBytes, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder_->total_num_frequency_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalFrequencyBytes, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder_->total_num_positions_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalPositionBytes, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder_->total_num_wasted_space_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalWastedBytes, metafile_values.str());
  metafile_values.str("");

  ostringstream meta_filename;
  meta_filename << out_index_files_.meta_info_filename();
  index_metafile.WriteKeyValueStore(meta_filename.str().c_str());
}

/**************************************************************************************************************************************************************
 * CollectionMerger
 *
 * TODO: Current parameters assume that the input files are all from the initial indexing step; other cases should also be possible.
 **************************************************************************************************************************************************************/
CollectionMerger::CollectionMerger(int num_initial_indices, int merge_degree, bool delete_merged_files) :
  kMergeDegree(merge_degree), kDeleteMergedFiles(delete_merged_files) {
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

CollectionMerger::CollectionMerger(vector<IndexFiles>& input_index_files, int merge_degree, bool delete_merged_files) :
  kMergeDegree(merge_degree), kDeleteMergedFiles(delete_merged_files) {
  GetDefaultLogger().Log("Merging " + logger::Stringify(input_index_files.size()) + " indices with merge degree " + logger::Stringify(merge_degree), false);

  // Calculate number of passes required to completely merge the initial indices.
  // Need to take logarithm with base 'kMergeDegree' of 'num_initial_indices'.
  // Use the change of base formula to do this.
  int num_passes = ceil(log(input_index_files.size()) / log(kMergeDegree));
  Partition(input_index_files, 1, num_passes);
}

void CollectionMerger::Partition(const vector<IndexFiles>& input_index_files, int pass_num, int num_passes) {
  if (input_index_files.size() == 1) {
    IndexFiles final_index_files;
    const IndexFiles& curr_index_files = input_index_files.front();

    int rename_ret;

    rename_ret = rename(curr_index_files.index_filename().c_str(), final_index_files.index_filename().c_str());
    if (rename_ret == -1) {
      GetErrorLogger().LogErrno("Could not rename index file '" + curr_index_files.index_filename() + "' to final name '" + final_index_files.index_filename()
          + "'", errno, false);
    }

    rename_ret = rename(curr_index_files.lexicon_filename().c_str(), final_index_files.lexicon_filename().c_str());
    if (rename_ret == -1) {
      GetErrorLogger().LogErrno("Could not rename lexicon file '" + curr_index_files.lexicon_filename() + "' to final name '"
          + final_index_files.lexicon_filename() + "'", errno, false);
    }

    rename_ret = rename(curr_index_files.document_map_filename().c_str(), final_index_files.document_map_filename().c_str());
    if (rename_ret == -1) {
      GetErrorLogger().LogErrno("Could not rename document map file '" + curr_index_files.document_map_filename() + "' to final name '"
          + final_index_files.document_map_filename() + "'", errno, false);
    }

    rename_ret = rename(curr_index_files.meta_info_filename().c_str(), final_index_files.meta_info_filename().c_str());
    if (rename_ret == -1) {
      GetErrorLogger().LogErrno("Could not rename meta info file '" + curr_index_files.meta_info_filename() + "' to final name '"
          + final_index_files.meta_info_filename() + "'", errno, false);
    }

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
        int rename_ret;

        rename_ret = rename(curr_pass_index_files[0].index_filename().c_str(), curr_out_index_files.index_filename().c_str());
        if (rename_ret == -1) {
          GetErrorLogger().LogErrno("Could not rename index file '" + curr_pass_index_files[0].index_filename() + "' to '"
              + curr_out_index_files.index_filename() + "'", errno, false);
        }

        rename_ret = rename(curr_pass_index_files[0].lexicon_filename().c_str(), curr_out_index_files.lexicon_filename().c_str());
        if (rename_ret == -1) {
          GetErrorLogger().LogErrno("Could not rename lexicon file '" + curr_pass_index_files[0].lexicon_filename() + "' to '"
              + curr_out_index_files.lexicon_filename() + "'", errno, false);
        }

        rename_ret = rename(curr_pass_index_files[0].document_map_filename().c_str(), curr_out_index_files.document_map_filename().c_str());
        if (rename_ret == -1) {
          GetErrorLogger().LogErrno("Could not rename document map file '" + curr_pass_index_files[0].document_map_filename() + "' to '"
              + curr_out_index_files.document_map_filename() + "'", errno, false);
        }

        rename_ret = rename(curr_pass_index_files[0].meta_info_filename().c_str(), curr_out_index_files.meta_info_filename().c_str());
        if (rename_ret == -1) {
          GetErrorLogger().LogErrno("Could not rename meta info file '" + curr_pass_index_files[0].meta_info_filename() + "' to '"
              + curr_out_index_files.meta_info_filename() + "'", errno, false);
        }
        break;
      default:
        IndexMerger* merger = new IndexMerger(curr_pass_index_files, curr_out_index_files);
        delete merger;
        next_pass_index_files.push_back(curr_out_index_files);
        break;
    }

    if (kDeleteMergedFiles) {
      // Delete files we no longer need to conserve disk space.
      for (size_t j = 0; j < curr_pass_index_files.size(); j++) {
        int remove_ret;

        remove_ret = remove(curr_pass_index_files[j].index_filename().c_str());
        if (remove_ret == -1) {
          GetErrorLogger().LogErrno("Could not remove index file '" + curr_pass_index_files[j].index_filename() + "'", errno, false);
        }

        remove_ret = remove(curr_pass_index_files[j].lexicon_filename().c_str());
        if (remove_ret == -1) {
          GetErrorLogger().LogErrno("Could not remove lexicon file '" + curr_pass_index_files[j].lexicon_filename() + "'", errno, false);
        }

        remove_ret = remove(curr_pass_index_files[j].document_map_filename().c_str());
        if (remove_ret == -1) {
          GetErrorLogger().LogErrno("Could not remove document map file '" + curr_pass_index_files[j].document_map_filename() + "'", errno, false);
        }

        remove_ret = remove(curr_pass_index_files[j].meta_info_filename().c_str());
        if (remove_ret == -1) {
          GetErrorLogger().LogErrno("Could not remove meta info file '" + curr_pass_index_files[j].meta_info_filename() + "'", errno, false);
        }
      }
    }

    curr_pass_index_files.clear();
  }

  Partition(next_pass_index_files, pass_num + 1, num_passes - 1);
}
