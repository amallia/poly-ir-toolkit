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

#ifndef INDEX_LAYERIFY_H_
#define INDEX_LAYERIFY_H_

#include <cassert>
#include <cmath>
#include <stdint.h>

#include <iostream>//TODO
#include <vector>

#include "coding_policy.h"
#include "document_map.h"
#include "index_layout_parameters.h"
#include "index_util.h"

/**************************************************************************************************************************************************************
 * LayeredIndexGenerator
 *
 **************************************************************************************************************************************************************/
class IndexBuilder;
class DocIdScoreComparison;

class LayeredIndexGenerator {
public:
  LayeredIndexGenerator(const IndexFiles& input_index_files, const std::string& output_index_prefix);
  ~LayeredIndexGenerator();

  void CreateLayeredIndex();

private:
  void DumpToIndex(const DocIdScoreComparison& doc_id_score_comparator, IndexEntry* index_entries, int num_index_entries, const char* curr_term,
                   int curr_term_len);
  float GetChunkMaxScore(const DocIdScoreComparison& doc_id_score_comparator, IndexEntry* chunk_entries, int num_chunk_entries);
  void WriteMetaFile(const std::string& meta_filename);

  IndexFiles output_index_files_;             // The index filenames for the layered index.
  Index* index_;                              // The index we're creating layers for.
  IndexBuilder* index_builder_;               // The current layered index we're building.

  // Some index properties.
  bool includes_contexts_;
  bool includes_positions_;
  bool overlapping_layers_;  // TODO: Get from config file (make new section for Index Layers). Right now, default is true.
  int num_layers_;

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
  uint32_t first_doc_id_in_index_;   // The first docID in the index.
  uint32_t last_doc_id_in_index_;    // The last docID in the index.
};

/**************************************************************************************************************************************************************
 * DocIdScoreComparison
 *
 * Uses the partial BM25 score to compare two documents from the same list.
 **************************************************************************************************************************************************************/
class DocIdScoreComparison {
public:
  DocIdScoreComparison(const DocumentMapReader& doc_map_reader, int num_docs_t, int average_doc_len, int total_num_docs) :
    kBm25K1(2.0), kBm25B(0.75), kBm25NumeratorMul(kBm25K1 + 1), kBm25DenominatorAdd(kBm25K1 * (1 - kBm25B)),
    kBm25DenominatorDocLenMul(kBm25K1 * kBm25B / average_doc_len), kIdfT(log10(1 + (total_num_docs - num_docs_t + 0.5) / (num_docs_t + 0.5))),
      doc_map_reader_(doc_map_reader) {
  }

  float Bm25Score(const IndexEntry& entry) const {
    uint32_t f_d_t = entry.frequency;
    int doc_len = doc_map_reader_.GetDocumentLen(entry.doc_id);
    float bm25 = kIdfT * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);

    assert(!isnan(bm25));
    return bm25;
  }

  bool operator()(const IndexEntry& lhs, const IndexEntry& rhs) const {
    return Bm25Score(lhs) > Bm25Score(rhs);
  }

private:
  // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
  const float kBm25K1;  // k1
  const float kBm25B;   // b

  // We can precompute a few of the BM25 values here.
  const float kBm25NumeratorMul;
  const float kBm25DenominatorAdd;
  const float kBm25DenominatorDocLenMul;
  const float kIdfT;  // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for the entire list.

  const DocumentMapReader& doc_map_reader_;
};

#endif /* INDEX_LAYERIFY_H_ */
