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
// Reads a file that contains the mappings of the original docIDs to the remapped docIDs and stores it in an array, indexed by the original docIDs.
// Reads index entries from the specified index into a buffer whose size is defined in the configuration file (there is also a separate pool for positions).
// During the copying to the buffer, docIDs are remapped using the docID map array that was previously created.
// Remapping an index is really an I/O efficient merge sort. First, the index is broken up into small pieces which are sorted, and then these pieces are merged
// into a new completely sorted index. Details follow below.
// The index entry buffer is dumped to an index in several cases, described below; prior to dumping it to an index, it's sorted by docID
// (which are the new remapped docIDs) and stored to a temporary index.
// The index entry buffer is only valid for one unique term (since we only sort by docID), so whenever we encounter a new term, we have to dump to an index.
// The index entry buffer is also dumped to an index whenever it's full. After this happens, a new index must be created; this is because the index entry
// buffer must have been full because we're reading a long list (unless the list happens to fit perfectly into the buffer), and we must ensure that a complete
// list fits into a buffer and is fully sorted before written to an index or that a list is split over several indices and then merged later into a complete
// sorted list. Finally, all the remapped indices are merged into one complete remapped index.
//==============================================================================================================================================================

#include "index_remapper.h"

#include <cstdlib>
#include <cstring>

#include <algorithm>
#include <limits>
#include <fstream>
#include <sstream>

#include "coding_policy_helper.h"
#include "config_file_properties.h"
#include "configuration.h"
#include "globals.h"
#include "index_build.h"
#include "index_merge.h"
#include "index_reader.h"
#include "key_value_store.h"
#include "logger.h"
#include "meta_file_properties.h"
using namespace std;

/**************************************************************************************************************************************************************
 * IndexRemapper
 *
 **************************************************************************************************************************************************************/
IndexRemapper::IndexRemapper(const IndexFiles& input_index_files, const string& output_index_prefix) :
  output_index_files_(output_index_prefix, 0, 0),
  final_output_index_files_(output_index_prefix),
  index_(NULL),
  index_builder_(NULL),
  index_count_(0),
  doc_id_map_(NULL),
  doc_id_map_size_(0),
  doc_id_map_start_(0),
  includes_contexts_(true),
  includes_positions_(true),
  doc_id_compressor_(CodingPolicy::kDocId),
  frequency_compressor_(CodingPolicy::kFrequency),
  position_compressor_(CodingPolicy::kPosition),
  block_header_compressor_(CodingPolicy::kBlockHeader),
  first_doc_id_in_index_(numeric_limits<uint32_t>::max()),
  last_doc_id_in_index_(0) {
  index_builder_ = new IndexBuilder(output_index_files_.lexicon_filename().c_str(), output_index_files_.index_filename().c_str(), block_header_compressor_);

  CacheManager* cache_policy = new MergingCachePolicy(input_index_files.index_filename().c_str());
  IndexReader* index_reader = new IndexReader(IndexReader::kMerge, *cache_policy, input_index_files.lexicon_filename().c_str(),
                                              input_index_files.document_map_filename().c_str(), input_index_files.meta_info_filename().c_str(), true);

  // Coding policy for the remapped index remains the same as that of the original index.
  coding_policy_helper::LoadPolicyAndCheck(doc_id_compressor_, index_reader->meta_info().GetValue(meta_properties::kIndexDocIdCoding), "docID");
  coding_policy_helper::LoadPolicyAndCheck(frequency_compressor_, index_reader->meta_info().GetValue(meta_properties::kIndexFrequencyCoding), "frequency");
  coding_policy_helper::LoadPolicyAndCheck(position_compressor_, index_reader->meta_info().GetValue(meta_properties::kIndexPositionCoding), "position");
  coding_policy_helper::LoadPolicyAndCheck(block_header_compressor_, index_reader->meta_info().GetValue(meta_properties::kIndexBlockHeaderCoding), "block header");

  if (!index_reader->includes_contexts())
    includes_contexts_ = false;

  if (!index_reader->includes_positions())
    includes_positions_ = false;

  index_ = new Index(cache_policy, index_reader);
}

IndexRemapper::~IndexRemapper() {
  delete index_;
  delete index_builder_;
}

void IndexRemapper::GenerateMap(const char* doc_id_map_filename) {
  // Scan docID remapping file, and find the largest docID that we have to deal with.
  ifstream doc_id_mapping_stream(doc_id_map_filename);
  if (!doc_id_mapping_stream) {
    GetErrorLogger().Log("Could not open docID remapping file '" + string(doc_id_map_filename) + "'.", true);
  }

  string curr_line;
  istringstream curr_line_stream;
  uint32_t curr_doc_id, remapped_doc_id;
  uint32_t min_curr_doc_id = numeric_limits<uint32_t>::max();
  uint32_t min_remapped_doc_id = numeric_limits<uint32_t>::max();
  uint32_t max_curr_doc_id = 0;
  uint32_t max_remapped_doc_id = 0;

  while (getline(doc_id_mapping_stream, curr_line)) {
    curr_line_stream.str(curr_line);
    curr_line_stream >> curr_doc_id >> remapped_doc_id;

    if (curr_doc_id < min_curr_doc_id)
      min_curr_doc_id = curr_doc_id;

    if (remapped_doc_id < min_remapped_doc_id)
      min_remapped_doc_id = remapped_doc_id;

    if (curr_doc_id > max_curr_doc_id)
      max_curr_doc_id = curr_doc_id;

    if (remapped_doc_id > max_remapped_doc_id)
      max_remapped_doc_id = remapped_doc_id;
  }

  // Reset stream to the beginning.
  doc_id_mapping_stream.clear();
  doc_id_mapping_stream.seekg(0, ios::beg);

  // Create one array, indexed by the old docID.
  doc_id_map_start_ = min_remapped_doc_id;  // TODO: Need to use this.
  doc_id_map_size_ = max_remapped_doc_id;
  doc_id_map_ = new uint32_t[doc_id_map_size_];
  memset(doc_id_map_, 0, doc_id_map_size_);

  while (getline(doc_id_mapping_stream, curr_line)) {
    curr_line_stream.str(curr_line);
    curr_line_stream >> curr_doc_id >> remapped_doc_id;
    doc_id_map_[remapped_doc_id] = curr_doc_id;
  }
}

// Once the buffer space for reading the portion of the index runs out,
// we sort the buffer according to the docIDs and write out a temporary index.
// These temporary indices will eventually be merged into the final remapped index.
void IndexRemapper::Remap() {
  // Allocate memory for the index entries buffer and for the positions pool (if necessary).
  const int kPositionsPoolMemory = Configuration::GetResultValue(Configuration::GetConfiguration().GetNumericalValue(config_properties::kPositionsPoolBufferSize));
  const int kIndexEntriesMemory = Configuration::GetResultValue(Configuration::GetConfiguration().GetNumericalValue(config_properties::kIndexEntryBufferSize));

  const int kPositionsPoolSize = (kPositionsPoolMemory << 20) / sizeof(uint32_t);
  const int kIndexEntriesSize = (kIndexEntriesMemory << 20) / sizeof(IndexEntry);

  PositionsPool positions_pool;
  if (includes_positions_)
    positions_pool = PositionsPool(kPositionsPoolSize);

  IndexEntry* index_entry_buffer = new IndexEntry[kIndexEntriesSize];

  int index_entry_offset = 0;
  while (index_->NextTerm()) {
    while (index_->NextDocId()) {
      IndexEntry& curr_index_entry = index_entry_buffer[index_entry_offset];

      if (index_entry_offset < kIndexEntriesSize) {
        if (!CopyToIndexEntry(&curr_index_entry, &positions_pool)) {
          // Buffer space ran out for positions, need to dump buffer to an index and insert the current index entry into the now empty buffer.
          DumpToIndex(index_entry_buffer, index_entry_offset, index_->curr_term(), index_->curr_term_len());
          positions_pool.Reset();
          index_entry_offset = 0;
          bool copy_index_entry = CopyToIndexEntry(index_entry_buffer, &positions_pool);
          if (!copy_index_entry)
            assert(false);

          DumpIndex();
        }
      } else {
        // Buffer space ran out, dump to disk as a mini index.
        DumpToIndex(index_entry_buffer, index_entry_offset, index_->curr_term(), index_->curr_term_len());
        positions_pool.Reset();
        index_entry_offset = 0;
        bool copy_index_entry = CopyToIndexEntry(index_entry_buffer, &positions_pool);
        if (!copy_index_entry)
          assert(false);

        DumpIndex();
      }

      ++index_entry_offset;
    }

    // End of this list. Dump it to the index.
    DumpToIndex(index_entry_buffer, index_entry_offset, index_->curr_term(), index_->curr_term_len());
    index_entry_offset = 0;
  }

  index_builder_->Finalize();
  WriteMetaFile(output_index_files_.meta_info_filename());
  remapped_indices_.push_back(output_index_files_);

  GetDefaultLogger().Log("Finished remapping.", false);

  delete[] index_entry_buffer;
  // TODO: Would be a good idea to delete the positions pool right here, before starting the merge.
  //       'positions_pool' would need to be a pointer in this case (or provide a method to explicitly deallocate the memory).

  // Now merge the remapped files.
  const bool kDeleteIntermediateRemappedFiles = (Configuration::GetConfiguration().GetValue(config_properties::kDeleteIntermediateRemappedFiles) == "true") ? true : false;
  CollectionMerger collection_merger = CollectionMerger(remapped_indices_, final_output_index_files_, kDeleteIntermediateRemappedFiles);

  GetDefaultLogger().Log("Finished remapping index.", false);
}

bool IndexRemapper::CopyToIndexEntry(IndexEntry* index_entry, PositionsPool* positions_pool) {
  IndexEntry& curr_index_entry = *index_entry;

  curr_index_entry.doc_id = doc_id_map_[index_->curr_doc_id()];  // Remap the docID here.

  // Update the last and first docIDs in the index, for the index meta file.
  if (curr_index_entry.doc_id > last_doc_id_in_index_)
    last_doc_id_in_index_ = curr_index_entry.doc_id;
  if (curr_index_entry.doc_id < first_doc_id_in_index_)
    first_doc_id_in_index_ = curr_index_entry.doc_id;

  curr_index_entry.frequency = index_->curr_list_data()->GetFreq();

  if (includes_positions_) {
    const uint32_t* curr_positions = index_->curr_list_data()->curr_chunk_decoder().current_positions();
    uint32_t num_positions = index_->curr_list_data()->GetNumDocProperties();
    if ((curr_index_entry.positions = positions_pool->StorePositions(curr_positions, num_positions)) == NULL) {
      // Positions buffer out of space.
      return false;
    }
  }

  return true;
}

// This dumps a single list (or part of a single list if it's long enough) into an index.
void IndexRemapper::DumpToIndex(IndexEntry* index_entries, int num_index_entries, const char* curr_term, int curr_term_len) {
  // Need to sort the index entries based on their new docID mapping.
  sort(index_entries, index_entries + num_index_entries, IndexEntryDocIdComparison());

  // Since the following input arrays will be used as input to the various coding policies, and the coding policy might apply a blockwise coding compressor
  // (which would pad the array to the block size), the following rules apply:
  // For the docID and frequency arrays, the block size is expected to be the chunk size.
  // For the position and context arrays, the block size is expected to be a multiple of the maximum positions/contexts possible for a particular docID.
  // Some alternative designs would be to define a fixed maximum block size and make sure the arrays are properly sized for this maximum
  // (the position/context arrays in particular).
  // Another alternative is to make these arrays dynamically allocated.
  assert(doc_id_compressor_.block_size() == 0 || ChunkEncoder::kChunkSize == doc_id_compressor_.block_size());
  assert(frequency_compressor_.block_size() == 0 || ChunkEncoder::kChunkSize == frequency_compressor_.block_size());
  assert(position_compressor_.block_size() == 0 || (ChunkEncoder::kChunkSize * ChunkEncoder::kMaxProperties) % position_compressor_.block_size() == 0);

  uint32_t doc_ids[ChunkEncoder::kChunkSize];
  uint32_t frequencies[ChunkEncoder::kChunkSize];
  uint32_t positions[ChunkEncoder::kChunkSize * ChunkEncoder::kMaxProperties];
  unsigned char contexts[ChunkEncoder::kChunkSize * ChunkEncoder::kMaxProperties];

  uint32_t prev_chunk_last_doc_id = 0;
  uint32_t prev_doc_id = 0;

  int index_entries_offset = 0;
  while(index_entries_offset < num_index_entries) {
    int doc_ids_offset = 0;
    int properties_offset = 0;
    for(doc_ids_offset = 0; doc_ids_offset < ChunkEncoder::kChunkSize && index_entries_offset < num_index_entries; ++doc_ids_offset) {
      const IndexEntry& curr_index_entry = index_entries[index_entries_offset];

      doc_ids[doc_ids_offset] = curr_index_entry.doc_id - prev_doc_id;
      // Check for duplicate docIDs (when the difference between the 'curr_index_entry.doc_id' and 'prev_doc_id' is zero), which is considered a bug.
      // But since 'prev_doc_id' is initialized to 0, which is a valid doc,
      // we have a case where the 'curr_index_entry.doc_id' could start from 0, which is an exception to the rule.
      // Thus, if this is the first iteration and 'curr_index_entry.doc_id' is 0, it is an acceptable case.
      assert(doc_ids[doc_ids_offset] != 0 || (index_entries_offset == 0 && curr_index_entry.doc_id == 0));
      prev_doc_id = curr_index_entry.doc_id;

      frequencies[doc_ids_offset] = curr_index_entry.frequency;

      if (includes_positions_) {
        uint32_t num_positions = min(curr_index_entry.frequency, static_cast<uint32_t> (ChunkEncoder::kMaxProperties));
        for (uint32_t j = 0; j < num_positions; ++j) {
          positions[properties_offset++] = curr_index_entry.positions[j];
        }
      }

      ++index_entries_offset;
    }

    ChunkEncoder chunk(doc_ids, frequencies, (includes_positions_ ? positions : NULL), (includes_contexts_ ? contexts : NULL), doc_ids_offset,
                       properties_offset, prev_chunk_last_doc_id, doc_id_compressor_, frequency_compressor_, position_compressor_);
    prev_chunk_last_doc_id = chunk.last_doc_id();
    index_builder_->Add(chunk, curr_term, curr_term_len);
  }
}

void IndexRemapper::DumpIndex() {
  index_builder_->Finalize();
  WriteMetaFile(output_index_files_.meta_info_filename());
  remapped_indices_.push_back(output_index_files_);

  delete index_builder_;
  ++index_count_;
  output_index_files_.UpdateNums(0, index_count_);
  index_builder_ = new IndexBuilder(output_index_files_.lexicon_filename().c_str(), output_index_files_.index_filename().c_str(), block_header_compressor_);
}

void IndexRemapper::WriteMetaFile(const std::string& meta_filename) {
  KeyValueStore index_metafile;

  index_metafile.AddKeyValuePair(meta_properties::kRemappedIndex, Stringify(true));
  index_metafile.AddKeyValuePair(meta_properties::kIncludesPositions, Stringify(includes_positions_));
  index_metafile.AddKeyValuePair(meta_properties::kIncludesContexts, Stringify(includes_contexts_));
  index_metafile.AddKeyValuePair(meta_properties::kIndexDocIdCoding,
                                 IndexConfiguration::GetResultValue(index_->index_reader()->meta_info().GetStringValue(meta_properties::kIndexDocIdCoding),
                                                                    false));
  index_metafile.AddKeyValuePair(meta_properties::kIndexFrequencyCoding,
                                 IndexConfiguration::GetResultValue(index_->index_reader()->meta_info().GetStringValue(meta_properties::kIndexFrequencyCoding),
                                                                    false));
  index_metafile.AddKeyValuePair(meta_properties::kIndexPositionCoding,
                                 IndexConfiguration::GetResultValue(index_->index_reader()->meta_info().GetStringValue(meta_properties::kIndexPositionCoding),
                                                                    false));
  index_metafile.AddKeyValuePair(meta_properties::kIndexBlockHeaderCoding,
                                 IndexConfiguration::GetResultValue(index_->index_reader()->meta_info().GetStringValue(meta_properties::kIndexBlockHeaderCoding),
                                                                    false));

  index_metafile.AddKeyValuePair(meta_properties::kTotalNumChunks, Stringify(index_builder_->total_num_chunks()));
  index_metafile.AddKeyValuePair(meta_properties::kTotalNumPerTermBlocks, Stringify(index_builder_->total_num_per_term_blocks()));

  // These would only really apply to the final remapped index. They aren't necessary in the intermediate indices, which is why
  // the merger actually treats these as the values in the final merged index; i.e. they are not summed up as done in a usual merge.
  index_metafile.AddKeyValuePair(meta_properties::kTotalDocumentLengths,
                                 IndexConfiguration::GetResultValue(index_->index_reader()->meta_info().GetStringValue(meta_properties::kTotalDocumentLengths),
                                                                    false));
  index_metafile.AddKeyValuePair(meta_properties::kTotalNumDocs,
                                 IndexConfiguration::GetResultValue(index_->index_reader()->meta_info().GetStringValue(meta_properties::kTotalNumDocs),
                                                                    false));
  index_metafile.AddKeyValuePair(meta_properties::kTotalUniqueNumDocs,
                                 IndexConfiguration::GetResultValue(index_->index_reader()->meta_info().GetStringValue(meta_properties::kTotalUniqueNumDocs),
                                                                    false));
  index_metafile.AddKeyValuePair(meta_properties::kFirstDocId, Stringify(first_doc_id_in_index_));
  index_metafile.AddKeyValuePair(meta_properties::kLastDocId, Stringify(last_doc_id_in_index_));
  index_metafile.AddKeyValuePair(meta_properties::kDocumentPostingCount,
                                 IndexConfiguration::GetResultValue(index_->index_reader()->meta_info().GetStringValue(meta_properties::kDocumentPostingCount),
                                                                    false));

  index_metafile.AddKeyValuePair(meta_properties::kIndexPostingCount, Stringify(index_builder_->posting_count()));
  index_metafile.AddKeyValuePair(meta_properties::kNumUniqueTerms, Stringify(index_builder_->num_unique_terms()));

  index_metafile.AddKeyValuePair(meta_properties::kTotalHeaderBytes, Stringify(index_builder_->total_num_block_header_bytes()));
  index_metafile.AddKeyValuePair(meta_properties::kTotalDocIdBytes, Stringify(index_builder_->total_num_doc_ids_bytes()));
  index_metafile.AddKeyValuePair(meta_properties::kTotalFrequencyBytes, Stringify(index_builder_->total_num_frequency_bytes()));
  index_metafile.AddKeyValuePair(meta_properties::kTotalPositionBytes, Stringify(index_builder_->total_num_positions_bytes()));
  index_metafile.AddKeyValuePair(meta_properties::kTotalWastedBytes, Stringify(index_builder_->total_num_wasted_space_bytes()));

  index_metafile.WriteKeyValueStore(meta_filename.c_str());
}
