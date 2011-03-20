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

#ifndef QUERY_PROCESSOR_H_
#define QUERY_PROCESSOR_H_

// Enables debugging output for this module.
//#define QUERY_PROCESSOR_DEBUG

// Enables debugging output for various query algorithms.
//#define MAX_SCORE_DEBUG
//#define WAND_DEBUG

#define CUSTOM_HASH
//#define HASH_HEAP_METHOD_OR
//#define HASH_HEAP_METHOD_AND

#include <cassert>
#include <stdint.h>

#include <fstream>
#include <iostream>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "index_layout_parameters.h"
#include "index_reader.h"
#include "index_util.h"
#ifdef CUSTOM_HASH
#include "integer_hash_table.h"
#else
#include <tr1/unordered_set>
#endif
/**************************************************************************************************************************************************************
 * QueryProcessor
 *
 * Implements various query algorithms. These support block caching (for on-disk indices) and chunk skipping. Frequencies and positions (if used) are
 * only decoded if the algorithm needs to score a docID.
 **************************************************************************************************************************************************************/
class CacheManager;
class ExternalIndexReader;
struct Accumulator;

typedef std::pair<float, uint32_t> Result;

class QueryProcessor {
public:
#ifdef CUSTOM_HASH
  typedef OpenAddressedIntegerHashTable TopKTable;
//  typedef ChainedIntegerHashTable TopKTable;
#else
  typedef std::tr1::unordered_set<uint32_t> TopKTable;
#endif

  enum QueryAlgorithm {
    kDefault,  // The query algorithm to use will be the default one used for the type of index that's being queried.
    kDaatAnd,  // Standard DAAT processing with AND mode semantics.
    kDaatOr,   // Standard DAAT processing with OR mode semantics.
    kTaatOr,   // Standard TAAT processing with OR mode semantics.

    // DAAT processing with an overlapping layered index, with a maximum of two layers per list. AND mode semantics.
    // These algorithms are early terminating only if certain conditions are met, otherwise,
    // queries are rerun with standard DAAT processing on the last layers of the lists.
    kDualLayeredOverlappingDaat,       // Each first layer is intersected with the other second layers. The results are then merged.
    kDualLayeredOverlappingMergeDaat,  // The first layers are merged into a single list and this list is then intersected with all the second layers.

    kLayeredTaatOrEarlyTerminated,  // TAAT processing on a multiple layered, but not overlapping index. Early termination possible. Also has accumulator trimming.

    kWand,
    kDualLayeredWand,

    kMaxScore,
    kDualLayeredMaxScore,

    kDaatAndTopPositions
  };

  enum QueryMode {
    kInteractive, kInteractiveSingle, kBatch, kBatchBench
  };

  enum ResultFormat {
    kTrec, kNormal, kCompare, kDiscard
  };

  QueryProcessor(const IndexFiles& input_index_files, const char* stop_words_list_filename, QueryAlgorithm query_algorithm, QueryMode query_mode,
                 ResultFormat result_format);
  ~QueryProcessor();

  void LoadStopWordsList(const char* stop_words_list_filename);

  void BuildBlockLevelIndex();

  void AcceptQuery();

  void OpenListLayers(LexiconData** query_term_data, int num_query_terms, int max_layers, ListData* list_data_pointers[][MAX_LIST_LAYERS],
                      bool* single_term_query, int* single_layer_list_idx, int* total_num_layers);

  void CloseListLayers(int num_query_terms, int max_layers, ListData* list_data_pointers[][MAX_LIST_LAYERS]);

  int ProcessQuery(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results);

  int ProcessLayeredTaatPrunedEarlyTerminatedQuery(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results);
  float ProcessListLayerOr(ListData* list, Accumulator** accumulators_array, int* accumulators_array_size, int* num_accumulators, std::pair<uint32_t, float>* top_k, int& num_top_k, TopKTable& top_k_table, int k);
  float ProcessListLayerAnd(ListData* list, Accumulator* accumulators, int num_accumulators, std::pair<uint32_t, float>* top_k, int& num_top_k, TopKTable& top_k_table, int k);

  int ProcessLayeredQuery(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results);

  void KthAccumulator(const Accumulator& new_accumulator, std::pair<uint32_t, float>* accumulators, int num_accumulators, TopKTable& top_k_table, int kth_score);
  void BubbleDownHeap(std::pair<uint32_t, float>* top_k, int top_k_size, int node_idx);
  void KthScore(float new_score, float* scores, int num_scores, int kth_score);

  int IntersectLists(ListData** lists, int num_lists, Result* results, int num_results);
  int IntersectLists(ListData** merge_lists, int num_merge_lists, ListData** lists, int num_lists, Result* results, int num_results);
  int IntersectListsTopPositions(ListData** lists, int num_lists, Result* results, int num_results);

  int MergeLists(ListData** lists, int num_lists, uint32_t* merged_doc_ids, int max_merged_doc_ids);
  int MergeLists(ListData** lists, int num_lists, Result* results, int num_results);
  int MergeListsWand(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results, bool two_tiered);
  int MergeListsMaxScore(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results, bool two_tiered);

  void ExecuteQuery(std::string query_line, int qid);

  void RunBatchQueries(const std::string& input_source, bool warmup, int num_timed_runs);

  void LoadIndexProperties();

  void PrintQueryingParameters();

private:
  CacheManager* GetCacheManager(const char* index_filename) const;
  const ExternalIndexReader* GetExternalIndexReader(QueryAlgorithm query_algorithm, const char* external_index_filename) const;

  QueryAlgorithm query_algorithm_;  // The query algorithm to use. This is also dependent on the type of index we're using.
  QueryMode query_mode_;            // The way we'll be accepting queries.
  ResultFormat result_format_;      // The result format we'll be using for the output.

  std::set<std::string> stop_words_;

  int max_num_results_;  // The max number of results to display.
  bool silent_mode_;     // When true, don't produce any output.
  bool warm_up_mode_;    // When true, don't time or count the queries. Queries issued during this time will be used for warming up the cache.
  bool use_positions_;   // Whether positions will be utilized during ranking (requires index built with positions).

  uint32_t collection_average_doc_len_;  // The average document length of a document in the indexed collection.
                                         // This plays a role in the ranking function (for document length normalization).
  uint32_t collection_total_num_docs_;   // The total number of documents in the indexed collection.

  const ExternalIndexReader* external_index_reader_;  // Only used for certain querying algorithms.

  CacheManager* cache_policy_;

  IndexReader index_reader_;

  // Some additional index information.
  bool index_layered_;
  bool index_overlapping_layers_;
  int index_num_layers_;  // This is really the max number of layers, since small inverted lists might have less layers.

  // Query statistics.
  double total_querying_time_;             // Keeps track of the total elapsed query times.
  uint64_t total_num_queries_;             // Keeps track of the number of queries issued.
  uint64_t num_early_terminated_queries_;  // Keeps track of the number of queries which were able to early terminate (when using a layered index).
  uint64_t num_single_term_queries_;       // Keeps track of the number of single term queries issued.

  // Statistics related to various query processing strategies.
  uint64_t not_enough_results_definitely_;
  uint64_t not_enough_results_possibly_;
  uint64_t num_queries_containing_single_layered_terms_;

  uint64_t num_queries_kth_result_meeting_threshold_;
  uint64_t num_queries_kth_result_not_meeting_threshold_;

  uint64_t num_postings_scored_;
  uint64_t num_postings_skipped_;
};

/**************************************************************************************************************************************************************
 * ResultPositionTuple
 *
 **************************************************************************************************************************************************************/
struct ResultPositionTuple {
  uint32_t doc_id;
  int doc_len;
  float score;
  uint32_t* positions;

  // Used to implement min heap for candidate documents.
  bool operator<(const ResultPositionTuple& rhs) const {
    return score > rhs.score;
  }

  // Used to sort documents in descending order by score.
  bool operator()(const ResultPositionTuple& l, const ResultPositionTuple& r) const {
    return l.score > r.score;
  }
};

/**************************************************************************************************************************************************************
 * Accumulator
 *
 **************************************************************************************************************************************************************/
struct Accumulator {
  uint32_t doc_id;
  float curr_score;
  uint32_t term_bitmap;  // Bit is on if the docID belonging to the corresponding term has been accounted for in the current score.

  bool operator<(const Accumulator& rhs) const {
    return doc_id < rhs.doc_id;
  }
};

/**************************************************************************************************************************************************************
 * AccumulatorScoreDescendingCompare
 *
 **************************************************************************************************************************************************************/
struct AccumulatorScoreDescendingCompare {
  bool operator()(const Accumulator& l, const Accumulator& r) const {
    return l.curr_score > r.curr_score;
  }
};

/**************************************************************************************************************************************************************
 * AccumulatorScoreAscendingCompare
 *
 **************************************************************************************************************************************************************/
struct AccumulatorScoreAscendingCompare {
  bool operator()(const Accumulator& l, const Accumulator& r) const {
    return l.curr_score < r.curr_score;
  }
};

/**************************************************************************************************************************************************************
 * DocIdScorePairScoreDescendingCompare
 *
 **************************************************************************************************************************************************************/
struct DocIdScorePairScoreDescendingCompare {
  bool operator()(const std::pair<uint32_t, float>& l, const std::pair<uint32_t, float>& r) const {
    return l.second > r.second;
  }
};

/**************************************************************************************************************************************************************
 * ResultCompare
 *
 **************************************************************************************************************************************************************/
struct ResultCompare {
  // Only compare the document score, don't care about order of same scoring documents.
  bool operator()(const Result& l, const Result& r) const {
    return l.first > r.first;
  }
};

/**************************************************************************************************************************************************************
 * ListLayerMaxScoreCompare
 *
 * Compares the max document scores of list layers, used to sort list layers by order of importance (the highest scoring layers first).
 **************************************************************************************************************************************************************/
struct ListLayerMaxScoreCompare {
  bool operator()(const ListData* l, const ListData* r) const {
    return l->score_threshold() > r->score_threshold();
  }
};

/**************************************************************************************************************************************************************
 * ListDocIdCompare
 *
 **************************************************************************************************************************************************************/
struct ListDocIdCompare {
  bool operator()(const std::pair<uint32_t, int>& l, const std::pair<uint32_t, int>& r) const {
    return l.first < r.first;
  }
};

/**************************************************************************************************************************************************************
 * ListMaxDocIdCompare
 *
 **************************************************************************************************************************************************************/
struct ListMaxDocIdCompare {
  bool operator()(const std::pair<uint32_t, int>& l, const std::pair<uint32_t, int>& r) const {
    return l.first > r.first;
  }
};

/**************************************************************************************************************************************************************
 * ListDocIdCompare
 *
 * TODO: Not using right now.
 **************************************************************************************************************************************************************/
/*struct ListDocIdCompare {
  bool operator()(ListData* l, ListData* r) const {
    return l->curr_block_decoder()->curr_chunk_decoder()->prev_decoded_doc_id() <= r->curr_block_decoder()->curr_chunk_decoder()->prev_decoded_doc_id();
  }
};*/

#endif /* QUERY_PROCESSOR_H_ */
