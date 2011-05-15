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
#include "query_processor.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>

#include "cache_manager.h"
#include "config_file_properties.h"
#include "configuration.h"
#include "external_index.h"
#include "globals.h"
#include "logger.h"
#include "meta_file_properties.h"
#include "timer.h"
using namespace std;

/**************************************************************************************************************************************************************
 * QueryProcessor
 *
 **************************************************************************************************************************************************************/
QueryProcessor::QueryProcessor(const IndexFiles& input_index_files, const char* stop_words_list_filename, QueryAlgorithm query_algorithm, QueryMode query_mode,
                               ResultFormat result_format) :
  query_algorithm_(query_algorithm),
  query_mode_(query_mode),
  result_format_(result_format),
  max_num_results_(Configuration::GetResultValue<long int>(Configuration::GetConfiguration().GetNumericalValue(config_properties::kMaxNumberResults))),
  silent_mode_(false),
  warm_up_mode_(false),
  use_positions_(Configuration::GetResultValue(Configuration::GetConfiguration().GetBooleanValue(config_properties::kUsePositions))),
  collection_average_doc_len_(0),
  collection_total_num_docs_(0),
  external_index_reader_(GetExternalIndexReader(query_algorithm_, input_index_files.external_index_filename().c_str())),
  cache_policy_(GetCacheManager(input_index_files.index_filename().c_str())),
  index_reader_(IndexReader::kRandomQuery,
                *cache_policy_,
                input_index_files.lexicon_filename().c_str(),
                input_index_files.document_map_basic_filename().c_str(),
                input_index_files.document_map_extended_filename().c_str(),
                input_index_files.meta_info_filename().c_str(),
                use_positions_,
                external_index_reader_),
  index_layered_(false),
  index_overlapping_layers_(false),
  index_num_layers_(1),
  total_querying_time_(0),
  total_num_queries_(0),
  num_early_terminated_queries_(0),
  num_single_term_queries_(0),

  not_enough_results_definitely_(0),
  not_enough_results_possibly_(0),
  num_queries_containing_single_layered_terms_(0),
  num_queries_kth_result_meeting_threshold_(0),
  num_queries_kth_result_not_meeting_threshold_(0),

  num_postings_scored_(0),
  num_postings_skipped_(0) {
  if (max_num_results_ <= 0) {
    Configuration::ErroneousValue(config_properties::kMaxNumberResults, Configuration::GetConfiguration().GetValue(config_properties::kMaxNumberResults));
  }

  if (stop_words_list_filename != NULL) {
    LoadStopWordsList(stop_words_list_filename);
  }
  LoadIndexProperties();
  PrintQueryingParameters();

  /*bool in_memory_index = IndexConfiguration::GetResultValue(Configuration::GetConfiguration().GetBooleanValue(config_properties::kMemoryResidentIndex), false);
  bool memory_mapped_index = IndexConfiguration::GetResultValue(Configuration::GetConfiguration().GetBooleanValue(config_properties::kMemoryMappedIndex), false);*/
  bool use_block_level_index = IndexConfiguration::GetResultValue(Configuration::GetConfiguration().GetBooleanValue(config_properties::kUseBlockLevelIndex), false);

  // TODO: Using an in-memory block index (for standard DAAT-AND) does not provide us any benefit. Most likely, the blocks should be smaller, or we should instead index the chunk last docIDs.
  //       Sequential block search performs better than binary block search in this case.
  //       This might be a better speed up for when the index is on disk and we are I/O bounded. Then we should also configure so we don't read ahead many blocks at a time.
  //       If the index is in main memory, the only improvement would be to avoid decoding the block header, and the overhead of that should be small.
  if (use_block_level_index) {
    cout << "Building in-memory block level index." << endl;
    BuildBlockLevelIndex();
  }
  /*if (memory_mapped_index || in_memory_index) {
    if (query_algorithm_ != kDaatOr && query_algorithm_ != kTaatOr) {
      cout << "Building in-memory block level index." << endl;
      BuildBlockLevelIndex();
    }
  }*/

  // For the case where we run batch queries, input can either come from stdin or from a file.
  // Reading input directly from a file is especially useful in case you can't redirect a file to stdin,
  // like when using gdb to debug (or at least I can't figure out how to do redirection when using gdb).
  string batch_query_input = IndexConfiguration::GetResultValue(Configuration::GetConfiguration().GetStringValue(config_properties::kBatchQueryInputFile), false);

  switch (query_mode_) {
    case kInteractive:
    case kInteractiveSingle:
      AcceptQuery();
      break;

    // In this mode, the query log will only be run once, and the output of each query will be printed to the screen.
    // If running in mode for TREC results, we will suppress some output, so as not to interfere with the 'trec_eval' program.
    case kBatch:
      if (result_format_ == kTrec)
        silent_mode_ = true;
      else
        silent_mode_ = false;
      RunBatchQueries(batch_query_input, false, 1);
      break;

    // In this mode, the query log will be run once without any timing to warm up the caches. Then, it will be run a second time to generate the timed run.
    // These runs will be silent, with only the querying statistics being displayed at the end of the final run.
    case kBatchBench:
      silent_mode_ = true;
      RunBatchQueries(batch_query_input, true, 1);
      break;

    default:
      assert(false);
      break;
  }

  // Output some querying statistics.
  double total_num_queries_issued = total_num_queries_;

  cout << "Number of queries executed: " << total_num_queries_ << endl;
  cout << "Number of single term queries: " << num_single_term_queries_ << endl;
  cout << "Total querying time: " << total_querying_time_ << " seconds\n";

  cout << "\n";
  cout << "Early Termination Statistics:\n";
  cout << "Number of early terminated queries: " << num_early_terminated_queries_ << endl;
  cout << "not_enough_results_definitely_: " << not_enough_results_definitely_ << endl;
  cout << "not_enough_results_possibly_: " << not_enough_results_possibly_ << endl;
  cout << "num_queries_containing_single_layered_terms_: " << num_queries_containing_single_layered_terms_ << endl;
  cout << "num_queries_kth_result_meeting_threshold_: " << num_queries_kth_result_meeting_threshold_ << endl;
  cout << "num_queries_kth_result_not_meeting_threshold_: " << num_queries_kth_result_not_meeting_threshold_ << endl;

  cout << "Average postings scored: " << (num_postings_scored_ / total_num_queries_issued) << endl;
  cout << "Average postings skipped: " << (num_postings_skipped_ / total_num_queries_issued) << endl;

  cout << "\n";
  cout << "Per Query Statistics:\n";
  cout << "  Average data read from cache: " << (index_reader_.total_cached_bytes_read() / total_num_queries_issued / (1 << 20)) << " MiB\n";
  cout << "  Average data read from disk: " << (index_reader_.total_disk_bytes_read() / total_num_queries_issued / (1 << 20)) << " MiB\n";
  cout << "  Average number of blocks skipped: " << (index_reader_.total_num_blocks_skipped() / total_num_queries_issued) << "\n";

  cout << "  Average query running time (latency): " << (total_querying_time_ / total_num_queries_issued * (1000)) << " ms\n";
}

QueryProcessor::~QueryProcessor() {
  delete external_index_reader_;
  delete cache_policy_;
}

void QueryProcessor::LoadStopWordsList(const char* stop_words_list_filename) {
  assert(stop_words_list_filename != NULL);

  std::ifstream ifs(stop_words_list_filename);
  if (!ifs) {
    GetErrorLogger().Log("Could not load stop word list file '" + string(stop_words_list_filename) + "'", true);
  }

  std::string stop_word;
  while (ifs >> stop_word) {
    stop_words_.insert(stop_word);
  }
}

// Create a block level index to speed up "random" accesses and skips.
// We iterate through the lexicon and decode all the block headers for the current inverted list.
// We then make a block level index by storing the last docID of each block for our current inverted list.
// Each inverted list layer will have it's own block level index (pointed to by the lexicon).
void QueryProcessor::BuildBlockLevelIndex() {
  /*SetDebugFlag(false);*/

  index_reader_.set_block_skipping_enabled(true);

  // We make one long array for keeping all the block level indices.
  int num_per_term_blocks = IndexConfiguration::GetResultValue(index_reader_.meta_info().GetNumericalValue(meta_properties::kTotalNumPerTermBlocks), true);

  uint32_t* block_level_index = new uint32_t[num_per_term_blocks];
  int block_level_index_pos = 0;

  MoveToFrontHashTable<LexiconData>* lexicon = index_reader_.lexicon().lexicon();
  for (MoveToFrontHashTable<LexiconData>::Iterator it = lexicon->begin(); it != lexicon->end(); ++it) {
    LexiconData* curr_term_entry = *it;
    if (curr_term_entry != NULL) {
      int num_layers = curr_term_entry->num_layers();
      for (int i = 0; i < num_layers; ++i) {
        ListData* list_data = index_reader_.OpenList(*curr_term_entry, i, true);

        int num_chunks_left = curr_term_entry->layer_num_chunks(i);

        assert(block_level_index_pos < num_per_term_blocks);
        curr_term_entry->set_last_doc_ids_layer_ptr(block_level_index + block_level_index_pos, i);

        while (num_chunks_left > 0) {
          const BlockDecoder& block = list_data->curr_block_decoder();

          // We index only the last chunk in each block that's related to our current term.
          // So we always use the last chunk in a block, except the last block of this list, since that last chunk might belong to another list.
          int total_num_chunks = block.num_chunks();  // The total number of chunks in our current block.
          int chunk_num = block.starting_chunk() + num_chunks_left;
          int last_list_chunk_in_block = ((total_num_chunks > chunk_num) ? chunk_num : total_num_chunks);

          uint32_t last_block_doc_id = block.chunk_last_doc_id(last_list_chunk_in_block - 1);

          assert(block_level_index_pos < num_per_term_blocks);
          block_level_index[block_level_index_pos++] = last_block_doc_id;

          num_chunks_left -= block.num_actual_chunks();

          if (num_chunks_left > 0) {
            // We're moving on to process the next block. This block is of no use to us anymore.
            list_data->AdvanceBlock();
          }
        }

        index_reader_.CloseList(list_data);
      }
    }
  }

  // If everything is correct, these should be equal at the end.
  assert(num_per_term_blocks == block_level_index_pos);

  // Reset statistics about how much we read from disk/cache and how many lists we accessed.
  index_reader_.ResetStats();

  /*SetDebugFlag(true);*/
}

void QueryProcessor::AcceptQuery() {
  while (true) {
    cout << "Search: ";
    string queryLine;
    getline(cin, queryLine);

    if (cin.eof())
      break;

    ExecuteQuery(queryLine, 0);

    if (query_mode_ != kInteractive)
      break;
  }
}

int QueryProcessor::ProcessQuery(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results) {
  const int kMaxNumResults = *num_results;
  ListData* list_data_pointers[num_query_terms];  // Using a variable length array here.

  bool single_term_query = false;
  if (num_query_terms == 1) {
    if (!warm_up_mode_)
      ++num_single_term_queries_;
    single_term_query = true;
  }

  for (int i = 0; i < num_query_terms; ++i) {
    // Here, we always open the last layer for a term. This way, we can support standard querying on layered indices, however, if loading the entire
    // index into main memory, we'll also be loading list layers we'll never be using.
    // TODO: This only applies to indices with overlapping layers; need to check that first.
    //       Also need to override that the index is not layered, so that this function will be called.
    list_data_pointers[i] = index_reader_.OpenList(*query_term_data[i], query_term_data[i]->num_layers() - 1, single_term_query);
  }

  int total_num_results;
  switch (query_algorithm_) {
    case kDaatAnd:
      // Query terms must be arranged in order from shortest list to longest list.
      sort(list_data_pointers, list_data_pointers + num_query_terms, ListCompare());
      total_num_results = IntersectLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
      break;
    case kDaatOr:
      total_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
      break;
    case kDaatAndTopPositions:
      // Query terms must be arranged in order from shortest list to longest list.
      sort(list_data_pointers, list_data_pointers + num_query_terms, ListCompare());
      total_num_results = IntersectListsTopPositions(list_data_pointers, num_query_terms, results, kMaxNumResults);
      break;
    default:
      total_num_results = 0;
      assert(false);
  }

  *num_results = min(total_num_results, kMaxNumResults);
  for (int i = 0; i < num_query_terms; ++i) {
    index_reader_.CloseList(list_data_pointers[i]);
  }
  return total_num_results;
}

// Used by the query processing methods that utilize list layers.
void QueryProcessor::OpenListLayers(LexiconData** query_term_data, int num_query_terms, int max_layers, ListData* list_data_pointers[][MAX_LIST_LAYERS],
                                    bool* single_term_query, int* single_layer_list_idx, int* total_num_layers) {
#ifdef IRTK_DEBUG
  // Build the query string.
  string query;
  for (int i = 0; i < num_query_terms; ++i) {
    query += string(query_term_data[i]->term(), query_term_data[i]->term_len()) + string(" ");
  }
  cout << "Processing layered query: " << query << endl;
#endif

  *single_term_query = false;
  if (num_query_terms == 1) {
    if (!warm_up_mode_)
      ++num_single_term_queries_;
    *single_term_query = true;
  }

  *single_layer_list_idx = -1;
  *total_num_layers = 0;
  // Open up all the lists for processing (each layer of one list is considered a separate list for our purposes here).
  for (int i = 0; i < num_query_terms; ++i) {
    // Find the first list that's single layered (we'll be using this info to speed things up).
    if (query_term_data[i]->num_layers() == 1 && *single_layer_list_idx == -1) {
      *single_layer_list_idx = i;
    }

    for (int j = 0; j < max_layers; ++j) {
      // We might not always have all the layers.
      if (j < query_term_data[i]->num_layers()) {
        ++(*total_num_layers);
        list_data_pointers[i][j] = index_reader_.OpenList(*query_term_data[i], j, *single_term_query, i);

#ifdef IRTK_DEBUG
        cout << "Score threshold for list '" << string(query_term_data[i]->term(), query_term_data[i]->term_len()) << "', layer #" << j << " is: "
            << query_term_data[i]->layer_score_threshold(j) << ", num_docs: " << query_term_data[i]->layer_num_docs(j) << "\n";
#endif
      } else {
        // For any remaining layers we don't have, we just open up the last layer.
        list_data_pointers[i][j] = index_reader_.OpenList(*query_term_data[i], query_term_data[i]->num_layers() - 1, *single_term_query, i);
      }
    }

#ifdef IRTK_DEBUG
    cout << endl;
#endif
  }
}

void QueryProcessor::CloseListLayers(int num_query_terms, int max_layers, ListData* list_data_pointers[][MAX_LIST_LAYERS]) {
  for (int i = 0; i < num_query_terms; ++i) {
    for (int j = 0; j < max_layers; ++j) {
      index_reader_.CloseList(list_data_pointers[i][j]);
    }
  }
}

// A DAAT based approach to multi-layered, non-overlapping lists.
// This is an exhaustive algorithm. For optimization, use WAND or MaxScore.
/*int QueryProcessor::ProcessMultiLayeredDaatOrQuery(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results) {
  const int kMaxLayers = MAX_LIST_LAYERS;  // Assume our lists can contain this many layers.
  const int kMaxNumResults = *num_results;

  ListData* list_data_pointers[num_query_terms][kMaxLayers];  // Using a variable length array here.
  bool single_term_query;
  int single_layer_list_idx;
  int total_num_layers;
  OpenListLayers(query_term_data, num_query_terms, kMaxLayers, list_data_pointers, &single_term_query, &single_layer_list_idx, &total_num_layers);

  ListData** lists = new ListData*[total_num_layers];
  int curr_layer = 0;
  for (int i = 0; i < num_query_terms; ++i) {
    for (int j = 0; j < query_term_data[i]->num_layers(); ++j) {
      lists[curr_layer] = list_data_pointers[i][j];
      ++curr_layer;
    }
  }

  int total_num_results = MergeLists(lists, total_num_layers, results, kMaxNumResults);

  // Clean up.
  for (int i = 0; i < num_query_terms; ++i) {
    for (int j = 0; j < kMaxLayers; ++j) {
      index_reader_.CloseList(list_data_pointers[i][j]);
    }
  }

  *num_results = min(total_num_results, kMaxNumResults);

  delete[] lists;
  return total_num_results;
}*/

// A DAAT based approach to multi-layered, non-overlapping lists. This is using MaxScore to speed stuff up.
// TODO: Would be interesting to make equal sized layers...
int QueryProcessor::ProcessMultiLayeredDaatOrQuery(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results) {
  const int kMaxLayers = MAX_LIST_LAYERS;  // Assume our lists can contain this many layers.
  const int kMaxNumResults = *num_results;

  ListData* lists[num_query_terms][kMaxLayers];  // Using a variable length array here.
  bool single_term_query;
  int single_layer_list_idx;
  int total_num_layers;
  OpenListLayers(query_term_data, num_query_terms, kMaxLayers, lists, &single_term_query, &single_layer_list_idx, &total_num_layers);

  ListData* list_data_pointers[total_num_layers];  // Using a variable length array here.
  int curr_layer = 0;
  for (int i = 0; i < num_query_terms; ++i) {
    for (int j = 0; j < query_term_data[i]->num_layers(); ++j) {
      list_data_pointers[curr_layer] = lists[i][j];
      ++curr_layer;
    }
  }

  // For MaxScore to work correctly, need term upperbounds on the whole list.
  float list_thresholds[total_num_layers];  // Using a variable length array here.
  for (int i = 0; i < total_num_layers; ++i) {
    list_thresholds[i] = list_data_pointers[i]->score_threshold();
#ifdef IRTK_DEBUG
    cout << "Layer for Term Num: " << list_data_pointers[i]->term_num()
        << ", Layer Num: 0, Score Threshold: " << list_data_pointers[i]->score_threshold()
        << ", Num Docs: " << list_data_pointers[i]->num_docs()
        << ", Num Blocks: " << list_data_pointers[i]->num_blocks()
        << ", Num Chunks: " << list_data_pointers[i]->num_chunks() << endl;
#endif
  }

  int total_num_results = 0;

  float threshold = 0;

  // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
  const float kBm25K1 = 2.0; // k1
  const float kBm25B = 0.75; // b

  // We can precompute a few of the BM25 values here.
  const float kBm25NumeratorMul = kBm25K1 + 1;
  const float kBm25DenominatorAdd = kBm25K1 * (1 - kBm25B);
  const float kBm25DenominatorDocLenMul = kBm25K1 * kBm25B / collection_average_doc_len_;

  // BM25 components.
  float bm25_sum; // The BM25 sum for the current document we're processing in the intersection.
  int doc_len;
  uint32_t f_d_t;

  // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for each list.
  float idf_t[total_num_layers]; // Using a variable length array here.
  int num_docs_t;
  for (int i = 0; i < total_num_layers; ++i) {
    num_docs_t = list_data_pointers[i]->num_docs_complete_list();
    idf_t[i] = log10(1 + (collection_total_num_docs_ - num_docs_t + 0.5) / (num_docs_t + 0.5));
  }

  // We use this to get the next lowest docID from all the lists.
  uint32_t lists_curr_postings[total_num_layers]; // Using a variable length array here.
  for (int i = 0; i < total_num_layers; ++i) {
    lists_curr_postings[i] = list_data_pointers[i]->NextGEQ(0);
  }

  pair<float, int> list_upperbounds[total_num_layers]; // Using a variable length array here.
  int num_lists_remaining = 0; // The number of lists with postings remaining.
  for (int i = 0; i < total_num_layers; ++i) {
    if (lists_curr_postings[i] != ListData::kNoMoreDocs) {
      list_upperbounds[num_lists_remaining++] = make_pair(list_thresholds[i], i);
    }
  }

  sort(list_upperbounds, list_upperbounds + num_lists_remaining, greater<pair<float, int> > ());

  // Precalculate the upperbounds for all possibilities.
  for (int i = num_lists_remaining - 2; i >= 0; --i) {
    list_upperbounds[i].first += list_upperbounds[i + 1].first;
  }

  int i, j;
  int curr_list_idx;
  pair<float, int>* top;
  uint32_t curr_doc_id; // Current docID we're processing the score for.

  while (num_lists_remaining) {
    top = &list_upperbounds[0];
    // Find the lowest docID that can still possibly make it into the top-k (while being able to make it into the top-k).
    for (i = 1; i < num_lists_remaining; ++i) {
      curr_list_idx = list_upperbounds[i].second;
      if (threshold > list_upperbounds[i].first) {
        break;
      }

      if (lists_curr_postings[curr_list_idx] < lists_curr_postings[top->second]) {
        top = &list_upperbounds[i];
      }
    }

    // Check if we can early terminate. This might happen only after we have finished traversing at least one list.
    // This is because our upperbounds don't decrease unless we are totally finished traversing one list.
    // Must check this since we initialize top to point to the first element in the list upperbounds array by default.
    if (threshold > list_upperbounds[0].first) {
      break;
    }

    // At this point, 'curr_doc_id' can either not be able to exceed the threshold score, or it can be the max possible docID sentinel value.
    curr_doc_id = lists_curr_postings[top->second];

    // We score a docID fully here, making any necessary lookups right away into other lists.
    // Disadvantage with this approach is that you'll be doing a NextGEQ() more than once for some lists on the same docID.
    bm25_sum = 0;
    for (i = 0; i < num_lists_remaining; ++i) {
      curr_list_idx = list_upperbounds[i].second;

      // Check if we can early terminate the scoring of this particular docID.
      if (threshold > bm25_sum + list_upperbounds[i].first) {
        break;
      }

      // Move to the curr docID we're scoring.
      lists_curr_postings[curr_list_idx] = list_data_pointers[curr_list_idx]->NextGEQ(curr_doc_id);

      if (lists_curr_postings[curr_list_idx] == curr_doc_id) {
        // Compute BM25 score from frequencies.
        f_d_t = list_data_pointers[curr_list_idx]->GetFreq();
        doc_len = index_reader_.document_map().GetDocumentLength(lists_curr_postings[curr_list_idx]);
        bm25_sum += idf_t[curr_list_idx] * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);

        ++num_postings_scored_;

        // Can now move the list pointer further.
        lists_curr_postings[curr_list_idx] = list_data_pointers[curr_list_idx]->NextGEQ(lists_curr_postings[curr_list_idx] + 1);
      }

      if (lists_curr_postings[curr_list_idx] == ListData::kNoMoreDocs) {
        --num_lists_remaining;
        float curr_list_upperbound = list_thresholds[curr_list_idx];

        // Compact the list upperbounds array.
        for (j = i; j < num_lists_remaining; ++j) {
          list_upperbounds[j] = list_upperbounds[j + 1];
        }

        // Recalculate the list upperbounds. Note that we only need to recalculate those entries less than i.
        for (j = 0; j < i; ++j) {
          list_upperbounds[j].first -= curr_list_upperbound;
        }
        --i;
      }
    }

    // Need to keep track of the top-k documents.
    if (total_num_results < kMaxNumResults) {
      // We insert a document if we don't have k documents yet.
      results[total_num_results] = make_pair(bm25_sum, curr_doc_id);
      push_heap(results, results + total_num_results + 1, ResultCompare());
    } else {
      if (bm25_sum > results->first) {
        // We insert a document only if it's score is greater than the minimum scoring document in the heap.
        pop_heap(results, results + kMaxNumResults, ResultCompare());
        results[kMaxNumResults - 1].first = bm25_sum;
        results[kMaxNumResults - 1].second = curr_doc_id;
        push_heap(results, results + kMaxNumResults, ResultCompare());

        // Update the threshold.
        threshold = results->first;
      }
    }
    ++total_num_results;
  }

  // Sort top-k results in descending order by document score.
  sort(results, results + min(kMaxNumResults, total_num_results), ResultCompare());

  *num_results = min(total_num_results, kMaxNumResults);
  for (int i = 0; i < total_num_layers; ++i) {
    index_reader_.CloseList(list_data_pointers[i]);
  }

  return total_num_results;
}

// Implements approach described by Anh/Moffat with improvements by Strohman/Croft, but with standard BM25 scoring, instead of impacts.
// This technique is not score safe, but it is still rank safe.
int QueryProcessor::ProcessLayeredTaatPrunedEarlyTerminatedQuery(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results) {
  const int kMaxLayers = MAX_LIST_LAYERS;  // Assume our lists can contain this many layers.
  const int kMaxNumResults = *num_results;

  ListData* list_data_pointers[num_query_terms][kMaxLayers];  // Using a variable length array here.
  bool single_term_query;
  int single_layer_list_idx;
  int total_num_layers;
  OpenListLayers(query_term_data, num_query_terms, kMaxLayers, list_data_pointers, &single_term_query, &single_layer_list_idx, &total_num_layers);

  // TODO: We can only support queries of a certain length (32 words). Can fix this by doing unoptimized processing for the shortest lists which do not
  //       fit within the 32 word limit. This is not an issue on our current query log.
  assert(num_query_terms <= static_cast<int>((sizeof(uint32_t) * 8)));

  ListData* max_score_sorted_list_data_pointers[total_num_layers];  // Using a variable length array here.
  uint32_t max_num_accumulators = 0;
  int curr_layer = 0;
  for (int i = 0; i < num_query_terms; ++i) {
    for (int j = 0; j < query_term_data[i]->num_layers(); ++j) {
      max_score_sorted_list_data_pointers[curr_layer] = list_data_pointers[i][j];
      ++curr_layer;

      max_num_accumulators += list_data_pointers[i][j]->num_docs();
    }
  }
  assert(curr_layer == total_num_layers);

  // This is a very crude upperbound on the maximum number of accumulators we might need (the lists will have docIDs in common). This uses more memory, but
  // avoids resizing the accumulator array if we find that it's too small.
  // Note: If the accumulator array is sized to contain all docs in the collection, we can just update accumulators by finding them by their docID as the index.
  //       This is only necessary for OR mode processing. After it's safe to move into AND mode, we can just compact the array to get better locality and thus
  //       better cache performance.
  max_num_accumulators = min(max_num_accumulators, collection_total_num_docs_);

  // Sort all the layers by their max score.
  sort(max_score_sorted_list_data_pointers, max_score_sorted_list_data_pointers + total_num_layers, ListLayerMaxScoreCompare());

  enum ProcessingMode {
    kAnd, kOr
  };
  ProcessingMode curr_processing_mode = kOr;

  int accumulators_size = max_num_accumulators;
  Accumulator* accumulators = new Accumulator[accumulators_size];
  float threshold = -numeric_limits<float>::max();  // We set the initial threshold to the lowest possible score. Note that our partial BM25 scores cannot be
                                                    // negative (although the standard BM25 formula does allow negative scores).
  float total_remainder = 0;  // The upperbound for the score of any new document encountered.
  int num_accumulators = 0;

  // Necessary to keep track of the threshold. This is a min-heap. For each layer we process, we need to keep track of the top-k scores to figure out the
  // threshold. The big problem we have is that we need to reinitialize the heap before starting to process the next layer. We do this to handle updated
  // accumulators; when an accumulator is updated (and it's score is already in the heap), if we simply add it to the
  // heap again, we'll be artificially increasing the threshold score, which could lead to incorrect results. So, we have to reinitialize the heap and insert
  // every accumulator (whether updated or not) to the heap again. This method takes O(n*log(k)) where k is the number of scores we keep in the heap --- since
  // k is a constant, it's really O(n), but the constant factor is something to keep in mind.
  // -----------------------------------------------------------------------------------------------------------------------------------------------------------
  // Another solution is to keep a hash table of docIDs that are currently in the top-k heap. Then, when we update the score of an accumulator that is in the
  // heap, we just check the hash table, which is a cheap operation. If we find that the accumulator is in the top-k, to update the score of this accumulator,
  // we do have to linearly search the heap until we find the matching docID; we then can do a bubble down operation on just this accumulator (since it's score
  // can only increase and this is a min-heap). If the accumulator is not in the top-k heap we can insert it if it's new score is greater than the min
  // accumulator. When an accumulator is removed from the top-k heap, it must also be marked deleted or removed from the hash table.
  // This scheme would be good when the majority of accumulators are not updated, that is, their score won't make it into the top-k. According to a sample
  // query, there are significantly less updates than old accumulator scores. This is the solution we use; it's benchmarked faster, more so for large values of
  // top-k.
  // -----------------------------------------------------------------------------------------------------------------------------------------------------------
  // Another solution is to use a select (quick-select) algorithm to find the k-th largest score from the accumulators. This can be done after finishing
  // processing a layer. This is a O(n) operation, where n is the number of accumulators. Note: after testing, this does not work too well in practice.
#ifdef HASH_HEAP_METHOD_OR
  pair<uint32_t, float> top_k[kMaxNumResults];  // Using a variable length array here.
#endif
  TopKTable top_k_table(kMaxNumResults);  // Indicates whether a docID is present in the top-k heap.
  int num_top_k = 0;

  float term_upperbounds[num_query_terms];  // Using a variable length array here.

  int total_num_accumulators_created = 0;
  for (int i = 0; i < total_num_layers; ++i) {
#ifdef IRTK_DEBUG
    cout << "Processing layer #" << i << ", with upperbound " << max_score_sorted_list_data_pointers[i]->score_threshold() << ", for term #"
        << max_score_sorted_list_data_pointers[i]->term_num() << endl;
#endif

#ifdef IRTK_DEBUG
    switch (curr_processing_mode) {
      case kOr:
        cout << "Using OR mode (total_remainder: " << total_remainder << ", threshold: " << threshold << ")." << endl;
        break;
      case kAnd:
        cout << "Using AND mode (total_remainder: " << total_remainder << ", threshold: " << threshold << ")." << endl;
        break;
      default:
        assert(false);
    }
#endif

    // Accumulators should always be in docID sorted order before we start processing a layer.
    for (int j = 0; j < (num_accumulators - 1); ++j) {
      assert(accumulators[j+1].doc_id >= accumulators[j].doc_id);
    }

    // Process postings based on the mode we're in.
    // Note: Look into using binary search on the accumulator array to find the docID we need.
    //       Binary search is a good option here since we always start with a sorted accumulator array.
    switch (curr_processing_mode) {
      case kOr:
#ifdef HASH_HEAP_METHOD_OR
        threshold = ProcessListLayerOr(max_score_sorted_list_data_pointers[i], &accumulators, &accumulators_size, &num_accumulators, top_k, num_top_k,
                                       top_k_table, kMaxNumResults, &total_num_accumulators_created);
#else
        threshold = ProcessListLayerOr(max_score_sorted_list_data_pointers[i], &accumulators, &accumulators_size, &num_accumulators, NULL, num_top_k,
                                       top_k_table, kMaxNumResults, &total_num_accumulators_created);
#endif
        break;
      case kAnd:
#ifdef HASH_HEAP_METHOD_OR
        threshold = ProcessListLayerAnd(max_score_sorted_list_data_pointers[i], accumulators, num_accumulators, top_k, num_top_k, top_k_table, kMaxNumResults);
#else
        threshold = ProcessListLayerAnd(max_score_sorted_list_data_pointers[i], accumulators, num_accumulators, NULL, num_top_k, top_k_table, kMaxNumResults);
#endif
        break;
      default:
        assert(false);
    }

    // Figure out the new upperbounds for each of the terms based on the layer max scores.
    for (int j = 0; j < num_query_terms; ++j) {
      term_upperbounds[j] = 0;
      // We start at 'i+1' since we just processed this layer, and all accumulator scores are updated from within the current layer.
      for (int k = i + 1; k < total_num_layers; ++k) {
        if (max_score_sorted_list_data_pointers[k]->term_num() == j) {
          term_upperbounds[j] = max_score_sorted_list_data_pointers[k]->score_threshold();
          break;
        }
      }
#ifdef IRTK_DEBUG
      cout << "Now, the upperbound for term #" << j << " is: " << term_upperbounds[j] << endl;
#endif
    }

    // Check accumulators to see whether we can switch to AND mode.
    // We calculate the remainder function here over all terms; this is the upperbound score of any newly discovered docID.
    total_remainder = 0;
    for (int j = 0; j < num_query_terms; ++j) {
      for (int k = (i + 1); k < total_num_layers; ++k) {
        if (max_score_sorted_list_data_pointers[k]->term_num() == j) {
          total_remainder += max_score_sorted_list_data_pointers[k]->score_threshold();
          break;
        }
      }
    }

    // Set processing mode to AND for the next layer if the conditions are right.
    if (curr_processing_mode == kOr && total_remainder < threshold) {
      curr_processing_mode = kAnd;
    }

    // A slight deviation from the published algorithm, we only prune the accumulators and check for the early termination conditions only if we're already in
    // AND mode; made on the observation that it's rare that we prune any accumulators before moving into AND mode processing. During benchmarking, this
    // produced lower latencies among a range of top-k.
    if (curr_processing_mode == kAnd) {
      bool early_termination_condition_one = true;  // No documents with current scores below the threshold can make it above the threshold.
      bool early_termination_condition_two = true;  // All documents with potential scores above the threshold cannot change their final order.

      // Here we calculate the upperbound for each accumulator, and remove those whose upperbound is lower than the threshold.
      // We also compact the accumulator table here too, by moving accumulators together.
      int num_invalidated_accumulators = 0;
      for (int j = 0; j < num_accumulators; ++j) {
        Accumulator& acc = accumulators[j];

        float acc_upperbound = acc.curr_score;
        for (int k = 0; k < num_query_terms; ++k) {
          if (((acc.term_bitmap >> k) & 1) == 0) {
            acc_upperbound += term_upperbounds[k];
          }
        }

        // Checks for the first of the early termination conditions.
        if (early_termination_condition_one && acc.curr_score < threshold && acc_upperbound > threshold) {
          early_termination_condition_one = false;
        }

        if (acc_upperbound < threshold) {
          // Remove accumulator.
          ++num_invalidated_accumulators;
        } else {
          // We move the accumulator left, to compact the array. Note that this does not affect any accumulators beyond this one.
          accumulators[j - num_invalidated_accumulators] = acc;
        }
      }
      num_accumulators -= num_invalidated_accumulators;

  #ifdef IRTK_DEBUG
      cout << "Num Invalidated Accumulators: " << num_invalidated_accumulators << endl;
      cout << "Num Accumulators Remaining: " << num_accumulators << endl;
  #endif

      // Note: A problem that prevents us from early termination is when the upperbounds on some accumulators are all the same because of some low
      // scoring layer and we can't guarantee rank safety because the current scores are too close.
      // A possible solution is to just make lookups for the remaining accumulators (say, we could do this when we narrowed down the list of top-k candidates to
      // just the k accumulators; but we don't know the exact ranks of these, so we can't terminate processing).
      // For each accumulator, just skip ahead into the lists for which we don't have a score yet.
      // For this purpose it might make sense to have an overlapping layer; this will avoid making lookups into multiple layers, but at the same time, it's hard
      // to choose at which point to make an overlapping layer, and it's also expensive in storage costs (especially if we're memory mapping the index).

      // Check the other early termination condition.
      if (early_termination_condition_one) {
        // Sort accumulators in ascending order by their scores.
        sort(accumulators, accumulators + num_accumulators, AccumulatorScoreAscendingCompare());

        for (int j = 0; j < num_accumulators - 1; ++j) {
          float acc_upperbound = 0;
          for (int k = 0; k < num_query_terms; ++k) {
            // Note that there could be accumulators that are missing a partial score from a particular term even if the score upperbound for that term is
            // already 0 (meaning we processed all the layers of that term list); this is normal, because during AND mode processing we skip docIDs that do not
            // intersect with any accumulators. This does not affect early termination.
            if (((accumulators[j].term_bitmap >> k) & 1) == 0) {
              acc_upperbound += term_upperbounds[k];
            }
          }

          if (accumulators[j].curr_score == accumulators[j+1].curr_score && acc_upperbound > 0) {
            early_termination_condition_two = false;
            break;
          }

          if (acc_upperbound > (accumulators[j+1].curr_score - accumulators[j].curr_score)) {
            early_termination_condition_two = false;
            break;
          }
        }

        // Need to sort accumulator array by docID again if we couldn't terminate.
        sort(accumulators, accumulators + num_accumulators);  // Uses the internal operator<() of the Accumulator class to sort.
      }

      // We can terminate further processing.
      if (early_termination_condition_one && early_termination_condition_two) {
  #ifdef IRTK_DEBUG
        if (i < (total_num_layers - 1)) {
          cout << "Terminating at layer " << (i + 1) << " out of " << total_num_layers << " total layers." << endl;
        }
  #endif
        break;
      }
    }
  }

  // Sort accumulators by score and return the top-k.
  sort(accumulators, accumulators + num_accumulators, AccumulatorScoreDescendingCompare());
  for (int i = 0; i < min(kMaxNumResults, num_accumulators); ++i) {
    results[i].first = accumulators[i].curr_score;
    results[i].second = accumulators[i].doc_id;
  }

  delete[] accumulators;

  // Clean up.
  for (int i = 0; i < num_query_terms; ++i) {
    for (int j = 0; j < kMaxLayers; ++j) {
      index_reader_.CloseList(list_data_pointers[i][j]);
    }
  }

  *num_results = min(num_accumulators, kMaxNumResults);

  // We use the total number of accumulators created as the total number of results;
  // there are possibly more results, but we couldn't count them because they couldn't make it into the top-k.
  return total_num_accumulators_created;
}

float QueryProcessor::ProcessListLayerOr(ListData* list, Accumulator** accumulators_array, int* accumulators_array_size, int* num_accumulators,
                                         pair<uint32_t, float>* top_k, int& num_top_k, TopKTable& top_k_table, int k, int* total_num_accumulators_created) {
  assert(list != NULL);
  assert(accumulators_array != NULL && *accumulators_array != NULL);
  assert(accumulators_array_size != NULL && *accumulators_array_size > 0);
  assert(*num_accumulators <= *accumulators_array_size);

#ifndef HASH_HEAP_METHOD_OR
  float top_k_scores[k];  // Using a variable length array here.
  int num_top_k_scores = 0;
#endif

  Accumulator* accumulators = *accumulators_array;
  int accumulators_size = *accumulators_array_size;

  // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
  const float kBm25K1 =  2.0;  // k1
  const float kBm25B = 0.75;   // b

  // We can precompute a few of the BM25 values here.
  const float kBm25NumeratorMul = kBm25K1 + 1;
  const float kBm25DenominatorAdd = kBm25K1 * (1 - kBm25B);
  const float kBm25DenominatorDocLenMul = kBm25K1 * kBm25B / collection_average_doc_len_;

  // BM25 components.
  float partial_bm25_sum;  // The BM25 sum for the current document we're processing in the intersection.
  int doc_len;
  uint32_t f_d_t;

  // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for this list.
  int num_docs_t = list->num_docs_complete_list();
  float idf_t = log10(1 + (collection_total_num_docs_ - num_docs_t + 0.5) / (num_docs_t + 0.5));

  int num_sorted_accumulators = *num_accumulators;  // This marks the point at which our newly inserted, unsorted accumulators start.
  int curr_accumulator_idx = 0;  // We start the search for a docID at the start of the accumulator table.
  uint32_t curr_doc_id = 0;

  while ((curr_doc_id = list->NextGEQ(curr_doc_id)) < ListData::kNoMoreDocs) {
    // Search for an accumulator corresponding to the current docID or insert if not found.
    while (curr_accumulator_idx < num_sorted_accumulators && accumulators[curr_accumulator_idx].doc_id < curr_doc_id) {

#ifndef HASH_HEAP_METHOD_OR
      // Maintain the threshold score.
      // This is for all the old accumulators, whose scores we won't be updating, but still need to be accounted for.
      KthScore(accumulators[curr_accumulator_idx].curr_score, top_k_scores, num_top_k_scores++, k);
#endif

      ++curr_accumulator_idx;
    }

    // Compute partial BM25 sum.
    f_d_t = list->GetFreq();
    doc_len = index_reader_.document_map().GetDocumentLength(curr_doc_id);
    partial_bm25_sum = idf_t * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);

    if (curr_accumulator_idx < num_sorted_accumulators && accumulators[curr_accumulator_idx].doc_id == curr_doc_id) {  // Found a matching accumulator.
      accumulators[curr_accumulator_idx].curr_score += partial_bm25_sum;
      accumulators[curr_accumulator_idx].term_bitmap |= (1 << list->term_num());

#ifndef HASH_HEAP_METHOD_OR
      // Maintain the threshold score.
      // This is for the updated accumulator scores.
      KthScore(accumulators[curr_accumulator_idx].curr_score, top_k_scores, num_top_k_scores++, k);
#else
      // Must rebuild the heap after we update the score, only if this accumulator is already in the heap.
#ifdef CUSTOM_HASH
      if (top_k_table.Exists(accumulators[curr_accumulator_idx].doc_id)) {
#else
      if (top_k_table.find(accumulators[curr_accumulator_idx].doc_id) != top_k_table.end()) {
#endif
        // Already in the heap, so find it's score in the heap, update it, and make the heap again, so it satisfies the heap property.
        // This is expensive, hopefully, we won't do it much.
        int heap_size = min(num_top_k, k);
        for (int i = 0; i < heap_size; ++i) {
          if (top_k[i].first == accumulators[curr_accumulator_idx].doc_id) {
            top_k[i].second = accumulators[curr_accumulator_idx].curr_score;
            BubbleDownHeap(top_k, heap_size, i);
            break;
          }
        }
      } else {
        // Insert it, because it's score has been updated, and it's not currently in the top-k heap, so it might make it there now (with the updated score).
        // Don't need to update 'num_top_k' in this case, because we must have seen this accumulator before, and since it's not in the top-k, it must have
        // been evicted, so 'num_top_k' must be >= 'k' already.
        KthAccumulator(accumulators[curr_accumulator_idx], top_k, num_top_k, top_k_table, k);
      }
#endif

      ++curr_accumulator_idx;
    } else {  // Need to insert accumulator.
      if (*num_accumulators >= accumulators_size) {
#ifdef IRTK_DEBUG
        cout << "Resizing accumulator array (curr size: " + *num_accumulators << ", new size: " << (*num_accumulators * 2) << ")." << endl;
#endif
        // Resize accumulator array.
        *accumulators_array_size *= 2;
        Accumulator* new_accumulators = new Accumulator[*accumulators_array_size];
        memcpy(new_accumulators, accumulators, (*num_accumulators) * sizeof(Accumulator));
        delete[] *accumulators_array;
        *accumulators_array = new_accumulators;

        accumulators = *accumulators_array;
        accumulators_size = *accumulators_array_size;
      }
      accumulators[*num_accumulators].doc_id = curr_doc_id;
      accumulators[*num_accumulators].curr_score = partial_bm25_sum;
      accumulators[*num_accumulators].term_bitmap = (1 << list->term_num());

#ifndef HASH_HEAP_METHOD_OR
      // Maintain the threshold score.
      // This is for the new accumulator scores.
      KthScore(accumulators[*num_accumulators].curr_score, top_k_scores, num_top_k_scores++, k);
#else
      KthAccumulator(accumulators[*num_accumulators], top_k, num_top_k++, top_k_table, k);
#endif

      ++(*num_accumulators);
      ++(*total_num_accumulators_created);
    }

    ++curr_doc_id;
  }

  // Sort the accumulator array by docID.
  // Note that we only really need to sort any new accumulators we inserted and merge it with the already sorted part of the array.
  sort(accumulators + num_sorted_accumulators, accumulators + *num_accumulators);

  // Note: An in-place merge would still require a buffer if you want to take O(n) time instead of O(n*log(n))...
  //       This is probably what Strohman/Croft meant, writing that they always needed to allocate a new array for each segment they process.
  //       We don't do that here right now, so the merge below is free to implement either the O(n) or O(n*log(n)) scheme, depending on how much free memory is
  //       available (according to the documentation). The 'inplace_merge' used here is slightly faster in benchmarking than allocating another buffer and doing
  //       a merge, as in the commented out code below.
  inplace_merge(accumulators, accumulators + num_sorted_accumulators, accumulators + *num_accumulators);

  // Alternative to the 'inplace_merge' used above; this is slightly slower in practice.
  /*Accumulator* merged_accumulators = new Accumulator[*accumulators_array_size];
  merge (accumulators, accumulators + num_sorted_accumulators, accumulators + num_sorted_accumulators, accumulators + *num_accumulators, merged_accumulators);
  delete[] *accumulators_array;
  *accumulators_array = merged_accumulators;*/

#ifdef HASH_HEAP_METHOD_OR
  // We return the threshold score.
  if (num_top_k < k) {
    return -numeric_limits<float>::max();
  }

  return top_k[0].second;
#else
  if (num_top_k_scores < k) {
    return -numeric_limits<float>::max();
  }

  return top_k_scores[0];
#endif
}

float QueryProcessor::ProcessListLayerAnd(ListData* list, Accumulator* accumulators, int num_accumulators, pair<uint32_t, float>* top_k, int& num_top_k,
                                          TopKTable& top_k_table, int k) {
  assert(list != NULL);
  assert(accumulators != NULL);
  assert(num_accumulators >= 0);

#ifndef HASH_HEAP_METHOD_AND
  float top_k_scores[k];  // Using a variable length array here.
  int num_top_k_scores = 0;
#endif

  // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
  const float kBm25K1 =  2.0;  // k1
  const float kBm25B = 0.75;   // b

  // We can precompute a few of the BM25 values here.
  const float kBm25NumeratorMul = kBm25K1 + 1;
  const float kBm25DenominatorAdd = kBm25K1 * (1 - kBm25B);
  const float kBm25DenominatorDocLenMul = kBm25K1 * kBm25B / collection_average_doc_len_;

  // BM25 components.
  float partial_bm25_sum;  // The BM25 sum for the current document we're processing in the intersection.
  int doc_len;
  uint32_t f_d_t;

  // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for this list.
  int num_docs_t = list->num_docs_complete_list();
  float idf_t = log10(1 + (collection_total_num_docs_ - num_docs_t + 0.5) / (num_docs_t + 0.5));

  int accumulator_offset = 0;
  uint32_t curr_doc_id;

  while (accumulator_offset < num_accumulators) {
    curr_doc_id = list->NextGEQ(accumulators[accumulator_offset].doc_id);
    if (curr_doc_id == accumulators[accumulator_offset].doc_id) {
      // Compute partial BM25 sum.
      f_d_t = list->GetFreq();
      doc_len = index_reader_.document_map().GetDocumentLength(curr_doc_id);
      partial_bm25_sum = idf_t * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);

      // Update accumulator with the document score.
      accumulators[accumulator_offset].curr_score += partial_bm25_sum;
      accumulators[accumulator_offset].term_bitmap |= (1 << list->term_num());

#ifndef HASH_HEAP_METHOD_AND
      // Maintain the threshold score.
      // This is for the updated accumulator scores.
      KthScore(accumulators[accumulator_offset].curr_score, top_k_scores, num_top_k_scores++, k);
#else
      // Must rebuild the heap after we update the score, only if this accumulator is already in the heap.
#ifdef CUSTOM_HASH
      if (top_k_table.Exists(accumulators[accumulator_offset].doc_id)) {
#else
      if (top_k_table.find(accumulators[accumulator_offset].doc_id) != top_k_table.end()) {
#endif
        // Already in the heap, so find it's score in the heap, update it, and make the heap again, so it satisfies the heap property.
        // This is expensive, hopefully, we won't do it much.
        int heap_size = min(num_top_k, k);
        for (int i = 0; i < heap_size; ++i) {
          if (top_k[i].first == accumulators[accumulator_offset].doc_id) {
            top_k[i].second = accumulators[accumulator_offset].curr_score;
            BubbleDownHeap(top_k, heap_size, i);
            break;
          }
        }
      } else {
        // Insert it, because it's score has been updated, and it's not currently in the top-k heap, so it might make it there now (with the updated score).
        // Don't need to update 'num_top_k' in this case, because we must have seen this accumulator before, and since it's not in the top-k, it must have
        // been evicted, so 'num_top_k' must be >= 'k' already.
        KthAccumulator(accumulators[accumulator_offset], top_k, num_top_k, top_k_table, k);
      }
#endif
    } else {
#ifndef HASH_HEAP_METHOD_AND
      // Maintain the threshold score.
      // This is for all the old accumulators, whose scores we won't be updating, but still need to be accounted for.
      KthScore(accumulators[accumulator_offset].curr_score, top_k_scores, num_top_k_scores++, k);
#endif
    }

    ++accumulator_offset;
  }

#ifdef HASH_HEAP_METHOD_AND
  // We return the threshold score.
  if (num_top_k < k) {
    return -numeric_limits<float>::max();
  }

  return top_k[0].second;
#else
  if (num_top_k_scores < k) {
    return -numeric_limits<float>::max();
  }

  return top_k_scores[0];
#endif
}

void QueryProcessor::KthAccumulator(const Accumulator& new_accumulator, std::pair<uint32_t, float>* accumulators, int num_accumulators, TopKTable& top_k_table, int kth_score) {
  // We use a min heap to determine the k-th largest score (the lowest score of the k scores we keep).
  // Notice that we don't have to explicitly make the heap, since it's assumed to be maintained from the start.
  if (num_accumulators < kth_score) {  // We insert a document score if we don't have k documents yet.
    // Mark that this docID has been inserted into the top-k heap.
#ifdef CUSTOM_HASH
    top_k_table.Insert(new_accumulator.doc_id);
#else
    top_k_table.insert(new_accumulator.doc_id);
#endif
    accumulators[num_accumulators++] = make_pair(new_accumulator.doc_id, new_accumulator.curr_score);
    push_heap(accumulators, accumulators + num_accumulators, DocIdScorePairScoreDescendingCompare());
  } else {
    if (new_accumulator.curr_score > accumulators[0].second) {  // We insert a score only if it is greater than the minimum score in the heap.
      // Mark that this accumulator has been inserted into the top-k heap.
#ifdef CUSTOM_HASH
      top_k_table.Insert(new_accumulator.doc_id);
#else
      top_k_table.insert(new_accumulator.doc_id);
#endif
      pop_heap(accumulators, accumulators + kth_score, DocIdScorePairScoreDescendingCompare());
      // Unmark this accumulator (no longer in the heap).
#ifdef CUSTOM_HASH
      top_k_table.Remove(accumulators[kth_score - 1].first);
#else
      top_k_table.erase(accumulators[kth_score - 1].first);
#endif
      accumulators[kth_score - 1].first = new_accumulator.doc_id;
      accumulators[kth_score - 1].second = new_accumulator.curr_score;
      push_heap(accumulators, accumulators + kth_score, DocIdScorePairScoreDescendingCompare());
    }
  }
}

// A function that does a bubble down operation on the min heap 'top_k' with size 'top_k_size', on the node at index 'node_idx'.
// The score of the accumulator represented at 'node_idx' could only have increased.
// So we push it down (in place of it's lowest scoring child) if it's greater than either of its children.
void QueryProcessor::BubbleDownHeap(pair<uint32_t, float>* top_k, int top_k_size, int node_idx) {
  while (true) {
    int left_child_idx = (node_idx << 1) + 1;
    if (left_child_idx >= top_k_size) {
      // No more children.
      break;
    }

    int right_child_idx = left_child_idx + 1;
    int lowest_scoring_idx;
    if (right_child_idx >= top_k_size) {  // The right child does not exist.
      lowest_scoring_idx = left_child_idx;
    } else {  // Find the lower scoring of the children
      lowest_scoring_idx = (top_k[left_child_idx].second < top_k[right_child_idx].second) ? left_child_idx : right_child_idx;
    }

    pair<uint32_t, float> node = top_k[node_idx];
    if (node.second > top_k[lowest_scoring_idx].second) {
      top_k[node_idx] = top_k[lowest_scoring_idx];
      top_k[lowest_scoring_idx] = node;
      node_idx = lowest_scoring_idx;
      continue;
    }
    break;
  }
}

// This is used to keep track of the threshold value (the score of the k-th highest scoring accumulator).
// The 'max_scores' array is assumed to be the size of at least 'kth_score'.
void QueryProcessor::KthScore(float new_score, float* scores, int num_scores, int kth_score) {
  // We use a min heap to determine the k-th largest score (the lowest score of the k scores we keep).
  // Notice that we don't have to explicitly make the heap, since it's assumed to be maintained from the start.
  if (num_scores < kth_score) {
    // We insert a document score if we don't have k documents yet.
    scores[num_scores++] = new_score;
    push_heap(scores, scores + num_scores, greater<float> ());
  } else {
    if (new_score > scores[0]) {
      // We insert a score only if it is greater than the minimum score in the heap.
      pop_heap(scores, scores + kth_score, greater<float> ());
      scores[kth_score - 1] = new_score;
      push_heap(scores, scores + kth_score, greater<float> ());
    }
  }
}

// This is for querying indices with dual overlapping layers.
// We can actually process more than 2 terms at a time as follows:
// Say we have 3 lists, A, B, C (each with at most 2 layers, the higher levels having duplicated docID info):
// Process (A_1 x B x C), (B_1 x A x C), (C_1 x A x B), where A, B, C are the whole lists and A_1, B_1, C_1 are the first (or only) layers.
// Note that intersecting all 3 terms should give good skipping performance.
// We also assume that the whole index is in main memory.
// Now, we can also run all 3 intersections in parallel (this should be good given that all 3 lists are in main memory).
// Merge all the results using a heap.  Or store the results in an array (which is then sorted) and the top results determined (preferable).
// We only need k results from each of the 3 separate intersections.
//
// The drawback here (for queries that have lists with all layers) is you have to scan all the 2nd layers twice for the number of lists you have in the query.
// (But we intersect with small lists and with good skipping performance (the index is in main memory, plus a block level index to skip decoding block headers),
// so it makes the costs acceptable).
//
// Intersecting for more than 2 layers is redundant right now. That's why for 3 or more term queries, we get pretty high latencies.
// Idea: After each intersection, can already check the threshold...
//       If there are 3 lists A,B,C, then A_1 x B_2 x C_2 determines the intersection of the top A documents with everything else...including the top B and C
//       since the layers are overlapping.  This is even true for 2 lists.  After doing A_1 x B_2, we can check the kth score
//       (if we got k scores in the intersection) against the threshold m(A_2) + m(B_1).
//
// TODO: Two bugs that need fixing:
//       * k results from each intersection is not enough since some of them are duplicates. Solution is to merge all the results --- see comments within
//         function for more details.
//       * Some docIDs appear more than once in the final results --- because heapifying only by score
//         (the same docIDs have different scores for different intersections because of rounding errors during float addition).
//         Solution: don't sort by score in IntersectLists(), sort by the docIDs for each intersection, merge docIDs
//                   (output array needs to be 'num_query_terms' * 'num_results' large to fit everything, in case all docIDs are unique).
//                   then after merging and eliminating duplicate docIDs, sort by score.
//
// TODO: Find percentage of queries that can early terminate in each category of number of query terms.
// TODO: Find out how much work is A_2 x B_2 vs A_1 x B_2 && B_1 x A_2. Do we traverse significantly less elements (when we are able to early terminate)?
//       (And also for queries with more than 2 terms).
//       If the answer is yes, we traverse less elements --- then it would be good to keep the A_2, B_2 lists decompressed in main memory, with the BM25 score
//       precomputed. Otherwise, the costs of decompression and BM25 computation are too large overheads.
//
// Idea: Traverse all (or maybe some?) intersections in an interleaved manner. Then check threshold every N documents processed (after we get k unique docs)
//       if we can early terminate on one of the intersections.  We can also use the threshold info to skip chunks/blocks
//       if we store threshold info within the index or load it into main memory).  This is because not all intersections are equal...some are gonna have lower
//       max scores, the latter intersections due to IDF.
//
// TODO: Investigate different result for the query 'cam glacier national park'.
// TODO: Implement:
//       Non overlapping index; keep an unresolved docID pool and an upperbound score so we can eliminate documents.
int QueryProcessor::ProcessLayeredQuery(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results) {
  const int kMaxLayers = MAX_LIST_LAYERS;  // Assume our lists can contain this many layers.
  const int kMaxNumResults = *num_results;

  ListData* list_data_pointers[num_query_terms][kMaxLayers];  // Using a variable length array here.
  bool single_term_query;
  int single_layer_list_idx;
  int total_num_layers;
  OpenListLayers(query_term_data, num_query_terms, kMaxLayers, list_data_pointers, &single_term_query, &single_layer_list_idx, &total_num_layers);

  // Run the appropriate intersections.
  ListData* curr_intersection_list_data_pointers[num_query_terms];  // Using a variable length array here.
  int total_num_results = 0;
  bool run_standard_intersection = false;
  if (single_layer_list_idx == -1) { // We have 2 layers for each term in the query.
    // For only 2 query terms, the other method is better.
    if (query_algorithm_ == kDualLayeredOverlappingMergeDaat && num_query_terms > 2) {
      // Here, we merge all the first layers together (while removing duplicate docIDs) and then treat it as one virtual list and intersect with all the 2nd layers.
      // We do the merge in an interleaved manner with the intersections to improve processing speed.
      // This allows us to avoid allocating an in-memory list (previous attempt to do this resulted in significantly slower running times).
      // This method will wind up traversing and scoring more documents, but it also a sort of way to do "bulk lookups".
      // I think this method, combined with docID reordering could provide even larger gains.

      ListData* merge_list_data_pointers[num_query_terms];  // Using a variable length array here.

      // Now do the intersection using the virtual list to drive which documents we're looking up.
      // Note that the virtual list could be larger than one of the 2nd layers.
      for (int i = 0; i < num_query_terms; ++i) {
        // Use only the first layer for each term.
        merge_list_data_pointers[i] = list_data_pointers[i][0];
        curr_intersection_list_data_pointers[i] = list_data_pointers[i][1];
      }

      sort(curr_intersection_list_data_pointers, curr_intersection_list_data_pointers + num_query_terms, ListCompare());
      total_num_results = IntersectLists(merge_list_data_pointers, num_query_terms, curr_intersection_list_data_pointers, num_query_terms, results, kMaxNumResults);
      *num_results = min(total_num_results, kMaxNumResults);
    } else {
      // TODO: It's not enough for each intersection to return just k results because there might be duplicate docIDs that we'll be filtering...
      //       This should probably be solved by writing a new function for intersect lists --- that will also combine the same docIDs from different list
      //       intersections right away so we do a merge of all the results in one step.

      Result all_results[num_query_terms][kMaxNumResults]; // Using a variable length array here.
      int num_intersection_results[num_query_terms]; // Using a variable length array here.
      for (int i = 0; i < num_query_terms; ++i) {
        // Build the intersection list.
        // We always intersect with the first layer of each list.
        curr_intersection_list_data_pointers[i] = list_data_pointers[i][0];

        // We also intersect with all the second layers of all the other lists.
        for (int j = 0; j < num_query_terms; ++j) {
          if (j != i) {
            curr_intersection_list_data_pointers[j] = list_data_pointers[j][1];
          }
        }

        // List intersections must be arranged in order from shortest list to longest list.
        sort(curr_intersection_list_data_pointers, curr_intersection_list_data_pointers + num_query_terms, ListCompare());
        int curr_total_num_results = IntersectLists(curr_intersection_list_data_pointers, num_query_terms, all_results[i], kMaxNumResults);
        num_intersection_results[i] = min(curr_total_num_results, kMaxNumResults);
        total_num_results += curr_total_num_results;

        for (int j = 0; j < num_query_terms; ++j) {
          // Need to reset the 2nd layers after running the query since we'll be using them again in the next iteration.
          // In our current setup of 2 layers, we really need to only reset each 2nd layer once, and the 2nd time, it doesn't particularly matter.
          // But this is pretty cheap.
          if (curr_intersection_list_data_pointers[j]->layer_num() == 1) {
            curr_intersection_list_data_pointers[j]->ResetList(single_term_query);
          }
        }

        // Print results of individual intersections for debugging.
        if (!silent_mode_) {
          for (int j = 0; j < num_intersection_results[i]; ++j) {
            cout << all_results[i][j].second << ", score: " << all_results[i][j].first << endl;
          }
          cout << endl;
        }
      }

      // Merge the results from all the previous intersection(s) using a heap.

      // The 'pair<int, int>' is for keeping track of the index of the intersection as well as the index of the current Result entry within the intersection.
      pair<Result, pair<int, int> > result_heap[num_query_terms]; // Using a variable length array here.
      int result_heap_size = 0;
      for (int i = 0; i < num_query_terms; ++i) {
        if (num_intersection_results[i] > 0) {
          result_heap[i] = make_pair(all_results[i][0], make_pair(i, 1));
          ++result_heap_size;
          --num_intersection_results[i];
        }
      }

      make_heap(result_heap, result_heap + result_heap_size); // Default is max heap, which is what we want.
      int curr_result = 0;
      while (result_heap_size && curr_result < kMaxNumResults) {
        pop_heap(result_heap, result_heap + result_heap_size);

        Result& curr_top_result = result_heap[result_heap_size - 1].first;
        pair<int, int>& curr_top_result_idx = result_heap[result_heap_size - 1].second;

        // If the previous result we stored is the same as the current, we don't need to insert it.
        // We only compare the docIDs because the scores could be different when the order of the addition of the partial BM25 sums is different.
        // This is due to floating point rounding errors.
        if (curr_result == 0 || results[curr_result - 1].second != curr_top_result.second) {
          results[curr_result++] = curr_top_result;
        }

        int top_intersection_index = curr_top_result_idx.first;
        if (num_intersection_results[top_intersection_index] > 0) {
          --num_intersection_results[top_intersection_index];
          result_heap[result_heap_size - 1] = make_pair(all_results[top_intersection_index][curr_top_result_idx.second], make_pair(top_intersection_index,
                                                                                                                                   curr_top_result_idx.second
                                                                                                                                       + 1));
          push_heap(result_heap, result_heap + result_heap_size);
        } else {
          --result_heap_size;
        }
      }

      *num_results = curr_result;
    }

    // Need to satisfy the early termination conditions.

    // Check if we have enough results first.
    if (*num_results >= kMaxNumResults) {
      // We have enough results to possibly early terminate.
      // Check whether we meet the early termination requirements.
      Result& min_result = results[min(kMaxNumResults - 1, *num_results - 1)];
      float remaining_document_score_upperbound = 0;
      for (int i = 0; i < num_query_terms; ++i) {
        float bm25_partial_score = query_term_data[i]->layer_score_threshold(query_term_data[i]->num_layers() - 1);
        assert(!isnan(bm25_partial_score));
        remaining_document_score_upperbound += bm25_partial_score;
      }

      if (min_result.first > remaining_document_score_upperbound) {
        ////////////TODO: print the properly early terminated query.
        //        if(num_query_terms == 1) {
        //          static int QUERY_COUNT = 0;
        //          cout << QUERY_COUNT++ << ":" << query << endl;
        //        }
        ////////////////

        ++num_queries_kth_result_meeting_threshold_;
        if (!silent_mode_)
          cout << "Early termination possible!" << endl;

        if (!warm_up_mode_)
          ++num_early_terminated_queries_;
      } else {
        ++num_queries_kth_result_not_meeting_threshold_;
        if (!silent_mode_)
          cout << "Cannot early terminate due to score thresholds." << endl;

        run_standard_intersection = true;
      }

    } else {
      // Don't have enough results from the first layers, execute query on the 2nd layer.
      if (!warm_up_mode_ && *num_results < kMaxNumResults) {
        if (total_num_results < kMaxNumResults) {
          ++not_enough_results_definitely_;
          if (!silent_mode_)
            cout << "Definitely don't have enough results." << endl;
        } else {
          ++not_enough_results_possibly_;
          if (!silent_mode_)
            cout << "Potentially don't have enough results." << endl;
        }
      }

      run_standard_intersection = true;
    }
  } else {
    // If we have at least one term in the query that has only a single layer,
    // we can get away with doing only on intersection on the last layers of each inverted list.
    ++num_queries_containing_single_layered_terms_;
    if (!silent_mode_)
      cout << "Query includes term with only a single layer." << endl;

    run_standard_intersection = true;

    // We count this as an early terminated query.
    if (!warm_up_mode_)
      ++num_early_terminated_queries_;
  }

  if (run_standard_intersection) {
    // Need to re-run the query on the last layers for each list (this is actually the standard DAAT approach).
    for (int i = 0; i < num_query_terms; ++i) {
      // Before we rerun the query, we need to reset the list information so we start from the beginning.
      list_data_pointers[i][query_term_data[i]->num_layers() - 1]->ResetList(single_term_query);
      curr_intersection_list_data_pointers[i] = list_data_pointers[i][query_term_data[i]->num_layers() - 1];
    }

    sort(curr_intersection_list_data_pointers, curr_intersection_list_data_pointers + num_query_terms, ListCompare());
    total_num_results = IntersectLists(curr_intersection_list_data_pointers, num_query_terms, results, kMaxNumResults);
    *num_results = min(total_num_results, kMaxNumResults);
  }

  CloseListLayers(num_query_terms, kMaxLayers, list_data_pointers);

  // TODO: This is incorrect for some queries where we don't actually open and traverse the lower layers (such as one word queries).
  return total_num_results;
}

// Merges the lists into an in-memory list that only contains docIDs; it also removes duplicate docIDs that might be present in multiple lists.
// We do not score any documents here.
// TODO: We can also potentially score documents here and keep track of which lists the score came from, then we'd have to do less work scoring
//       when we intersect with the 2nd layers --- but the logic here would be more complicated.
//       Potentially we can also set up some thresholds...since we're doing OR mode processing --- look at the Efficient Query Processing in Main Memory paper...
int QueryProcessor::MergeLists(ListData** lists, int num_lists, uint32_t* merged_doc_ids, int max_merged_doc_ids) {
  pair<uint32_t, int> heap[num_lists];  // Using a variable length array here.
  int heap_size = 0;

  // Initialize the heap.
  for (int i = 0; i < num_lists; ++i) {
    uint32_t curr_doc_id;
    if ((curr_doc_id = lists[i]->NextGEQ(0)) < ListData::kNoMoreDocs) {
      heap[heap_size++] = make_pair(curr_doc_id, i);
    }
  }

  // We use the default comparison --- which is fine, but the comparison for a pair checks both values, and we really only need to check the docID part
  // so it could be more efficient to write your own simple comparator.
  make_heap(heap, heap + heap_size, greater<pair<uint32_t, int> >());

  int i = 0;
  while (heap_size) {
    pair<uint32_t, int> top = heap[0];

    // Don't insert duplicate docIDs.
    assert(i < max_merged_doc_ids);
    if (i == 0 || merged_doc_ids[i - 1] != top.first) {
      merged_doc_ids[i++] = top.first;
    }

    // Need to pop and push to make sure heap property is maintained.
    pop_heap(heap, heap + heap_size, greater<pair<uint32_t, int> >());

    uint32_t curr_doc_id;
    if ((curr_doc_id = lists[top.second]->NextGEQ(top.first + 1)) < ListData::kNoMoreDocs) {
      heap[heap_size - 1] = make_pair(curr_doc_id, top.second);
      // TODO: OR Instead of making a new pair, can just update the pair, with the correct docID, and (possibly the list idx?, might depend on whether the heap size decreased previously).
      //       or maybe use the 'top' we have created.
      push_heap(heap, heap + heap_size, greater<pair<uint32_t, int> >());
    } else {
      --heap_size;
    }
  }

  return i;
}

// Standard DAAT OR mode processing for comparison purposes.
int QueryProcessor::MergeLists(ListData** lists, int num_lists, Result* results, int num_results) {
  // Setting this option to 'true' makes a considerable difference in average query latency (> 100ms).
  // When we score the complete doc, we first find the lowest docID in the array, and then scan the array for that docID, and completely score it.
  // All lists from which the docID was scored have their list pointers moved forward.
  // When we don't score the complete doc, at each turn of the while loop, we find a partial score of the lowest docID posting.
  // We add these together for a particular docID to get the complete score -- but it requires several iterations of the main while loop.
  // This is less efficient, since we have to do a complete linear search through the array for every posting.
  // On the other hand, when we score the complete doc right away, we only have to do one more linear search through all postings to score all the lists.
  // Clearly, if the majority of the docIDs are present in more than one list, we'll be getting a speedup.
  const bool kScoreCompleteDoc = true;

  // Use an array instead of a heap for selecting the list with the lowest docID at each step.
  // Using a heap for picking the list with the lowest docID is only implemented for when 'kScoreCompleteDoc' is false.
  // For compatibility with 'kScoreCompleteDoc' equal to true, you'd need to use the heap to choose the next list to score, instead of iterating through the array, which is what's done now.
  // Array based method is faster than the heap based method for choosing the lowest docID from all the lists, so this option should be set to 'true'.
  // TODO: Try another array based strategy: keep a sorted array of docIDs. When updating, only need to find the spot for the new docID and re-sort the array up to that spot.
  // TODO: Can also use a linked list for this. Then can just find the right spot, and do pointer changes. The locality here wouldn't be too good though.
  const bool kUseArrayInsteadOfHeapList = true;

  int total_num_results = 0;

  // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
  const float kBm25K1 =  2.0;  // k1
  const float kBm25B = 0.75;   // b

  // We can precompute a few of the BM25 values here.
  const float kBm25NumeratorMul = kBm25K1 + 1;
  const float kBm25DenominatorAdd = kBm25K1 * (1 - kBm25B);
  const float kBm25DenominatorDocLenMul = kBm25K1 * kBm25B / collection_average_doc_len_;

  // BM25 components.
  float bm25_sum = 0;  // The BM25 sum for the current document we're processing in the intersection.
  float partial_bm25_sum;
  int doc_len;
  uint32_t f_d_t;

  // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for each list.
  float idf_t[num_lists];  // Using a variable length array here.
  int num_docs_t;
  for (int i = 0; i < num_lists; ++i) {
    num_docs_t = lists[i]->num_docs_complete_list();
    idf_t[i] = log10(1 + (collection_total_num_docs_ - num_docs_t + 0.5) / (num_docs_t + 0.5));
  }

  // We use this to get the next lowest docID from all the lists.
  pair<uint32_t, int> lists_curr_postings[num_lists];  // Using a variable length array here.
  int num_lists_remaining = 0;  // The number of lists with postings remaining.
  for (int i = 0; i < num_lists; ++i) {
    uint32_t curr_doc_id;
    if ((curr_doc_id = lists[i]->NextGEQ(0)) < ListData::kNoMoreDocs) {
      lists_curr_postings[num_lists_remaining++] = make_pair(curr_doc_id, i);
    }
  }

  if (num_lists_remaining > 0) {
    if (!kUseArrayInsteadOfHeapList) {
      // We use our own comparator, that only checks the docID part.
      make_heap(lists_curr_postings, lists_curr_postings + num_lists_remaining, ListMaxDocIdCompare());
    }
  } else {
    return total_num_results;
  }

  // For the heap based method, the lowest element will always be the first element in the array.
  // So we can keep 'top' constant since it's just a pointer to the first element and just push/pop the heap.
  // For the array based method, we need to initialize it to the first element in the array, and then find the lowest value at the top of the while loop.
  // We have to find the lowest element here as well, since we need to initialize 'curr_doc_id' to the right value before we start the loop.
  pair<uint32_t, int>* top = &lists_curr_postings[0];
  if (kUseArrayInsteadOfHeapList) {
    for (int i = 1; i < num_lists_remaining; ++i) {
      if (lists_curr_postings[i].first < top->first) {
        top = &lists_curr_postings[i];
      }
    }
  }

  int i;
  uint32_t curr_doc_id = top->first;  // Current docID we're processing the score for.

  while (num_lists_remaining) {
    if (kUseArrayInsteadOfHeapList) {
      top = &lists_curr_postings[0];
      for (i = 1; i < num_lists_remaining; ++i) {
        if (lists_curr_postings[i].first < top->first) {
          top = &lists_curr_postings[i];
        }
      }
    }

    if (kScoreCompleteDoc) {
      curr_doc_id = top->first;
      bm25_sum = 0;
      // Can start searching from the position of 'top' since it'll be the first lowest element in the array.
      while (top != &lists_curr_postings[num_lists_remaining]) {
        if (top->first == curr_doc_id) {
          // Compute BM25 score from frequencies.
          f_d_t = lists[top->second]->GetFreq();
          doc_len = index_reader_.document_map().GetDocumentLength(top->first);
          bm25_sum += idf_t[top->second] * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);

          ++num_postings_scored_;

          if ((top->first = lists[top->second]->NextGEQ(top->first + 1)) == ListData::kNoMoreDocs) {
            // Need to compact the array by one.
            // Just copy over the last value in the array and overwrite the top value, since we'll be removing it.
            // Now, we can declare our list one shorter.
            // If top happens to already point to the last value in the array, this step is superfluous.
          --num_lists_remaining;
            *top = lists_curr_postings[num_lists_remaining];
            --top;
          }
        }
        ++top;
      }

      // Need to keep track of the top-k documents.
      if (total_num_results < num_results) {
        // We insert a document if we don't have k documents yet.
        results[total_num_results] = make_pair(bm25_sum, curr_doc_id);
        push_heap(results, results + total_num_results + 1, ResultCompare());
      } else {
        if (bm25_sum > results->first) {
          // We insert a document only if it's score is greater than the minimum scoring document in the heap.
          pop_heap(results, results + num_results, ResultCompare());
          results[num_results - 1].first = bm25_sum;
          results[num_results - 1].second = curr_doc_id;
          push_heap(results, results + num_results, ResultCompare());
        }
      }
      ++total_num_results;
    } else {
      // Compute BM25 score from frequencies.
      f_d_t = lists[top->second]->GetFreq();
      doc_len = index_reader_.document_map().GetDocumentLength(top->first);
      partial_bm25_sum = idf_t[top->second] * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);

      ++num_postings_scored_;

#ifdef QUERY_PROCESSOR_DEBUG
      // Set 'kScoreCompleteDoc' to false to use this code path.
      cout << "doc_id: " << top->first << ", bm25: " << partial_bm25_sum << endl;
#endif

      // When we encounter the same docID as the current we'be been processing, we update it's score.
      // Otherwise, we know we're processing a new docID.
      if (top->first == curr_doc_id) {
        bm25_sum += partial_bm25_sum;
      } else if (top->first > curr_doc_id) {
        // Need to keep track of the top-k documents.
        if (total_num_results < num_results) {
          // We insert a document if we don't have k documents yet.
          results[total_num_results] = make_pair(bm25_sum, curr_doc_id);
          push_heap(results, results + total_num_results + 1, ResultCompare());
        } else {
          if (bm25_sum > results->first) {
            // We insert a document only if it's score is greater than the minimum scoring document in the heap.
            pop_heap(results, results + num_results, ResultCompare());
            results[num_results - 1].first = bm25_sum;
            results[num_results - 1].second = curr_doc_id;
            push_heap(results, results + num_results, ResultCompare());
          }
        }

        curr_doc_id = top->first;
        bm25_sum = partial_bm25_sum;
        ++total_num_results;
      } else {
        assert(false);
      }

      uint32_t next_doc_id;
      if ((next_doc_id = lists[top->second]->NextGEQ(top->first + 1)) < ListData::kNoMoreDocs) {
        if (kUseArrayInsteadOfHeapList) {
          top->first = next_doc_id;
        } else {
          // Need to pop and push to make sure heap property is maintained.
          pop_heap(lists_curr_postings, lists_curr_postings + num_lists_remaining, ListMaxDocIdCompare());
          lists_curr_postings[num_lists_remaining - 1].first = next_doc_id;
          push_heap(lists_curr_postings, lists_curr_postings + num_lists_remaining, ListMaxDocIdCompare());
        }
      } else {
        if (kUseArrayInsteadOfHeapList) {
          // Need to compact the array by one.
          // Just copy over the last value in the array and overwrite the top value, since we'll be removing it.
          // Now, we can declare our list one shorter.
          // If top happens to already point to the last value in the array, this step is superfluous.
          *top = lists_curr_postings[num_lists_remaining - 1];
        } else {
          pop_heap(lists_curr_postings, lists_curr_postings + num_lists_remaining, ListMaxDocIdCompare());
        }

        --num_lists_remaining;
      }
    }
  }

  if (!kScoreCompleteDoc) {
    // We always have a leftover result that we need to insert.
    // Note that there is no need to push the heap since we'll just be sorting all the results by their score next.
    if (total_num_results < num_results) {
      // We insert a document if we don't have k documents yet.
      results[total_num_results] = make_pair(bm25_sum, curr_doc_id);
    } else {
      if (bm25_sum > results->first) {
        // We insert a document only if it's score is greater than the minimum scoring document in the heap.
        pop_heap(results, results + num_results, ResultCompare());
        results[num_results - 1].first = bm25_sum;
        results[num_results - 1].second = curr_doc_id;
      }
    }
    ++total_num_results;
  }

  // Sort top-k results in descending order by document score.
  sort(results, results + min(num_results, total_num_results), ResultCompare());

  return total_num_results;
}

// The two tiered WAND first merges (OR mode) and scores the top docs lists, so that we know the k-th threshold (a better approximation of the lower bound).
// TODO: It seems to me that a two tiered WAND doesn't save us any computation. We'll be evaluating the top docs lists and then we'll be able to skip some docIDs from the 2nd layers.
//       NOTE: It WOULD save computation if the number of crappy, low scoring docIDs we'll be able to skip (due to an initially high threshold)
//             exceeds the extra number of top docs docIDs we had to compute scores for.
//       It would be really beneficial if you could decrease the upperbounds on the term lists for the 2nd layers
//       (which you can't unless you don't discard the top docs and store them in accumulators).
// In standard WAND, the k-th threshold is initialized to 0.
int QueryProcessor::MergeListsWand(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results, bool two_tiered) {
  // Constraints on the type of index we expect.
  assert(index_layered_);
  assert(index_overlapping_layers_);
  assert(index_num_layers_ == 2);

  const int kMaxNumResults = *num_results;

  // Holds a pointer to the list for each corresponding query term.
  ListData* list_data_pointers[num_query_terms];  // Using a variable length array here.

  // For WAND to work correctly, need term upperbounds on the whole list.
  float list_thresholds[num_query_terms];  // Using a variable length array here.

  bool single_term_query = false;
  if (num_query_terms == 1) {
    single_term_query = true;
  }

  for (int i = 0; i < num_query_terms; ++i) {
    // Open the first layer (the top docs).
    list_data_pointers[i] = index_reader_.OpenList(*query_term_data[i], 0, single_term_query);
    list_thresholds[i] = list_data_pointers[i]->score_threshold();
#ifdef IRTK_DEBUG
    cout << "Top Docs Layer for '" << string(query_term_data[i]->term(), query_term_data[i]->term_len())
        << "', Layer Num: 0, Score Threshold: " << list_data_pointers[i]->score_threshold()
        << ", Num Docs: " << list_data_pointers[i]->num_docs()
        << ", Num Blocks: " << list_data_pointers[i]->num_blocks()
        << ", Num Chunks: " << list_data_pointers[i]->num_chunks() << endl;
#endif
  }

  int total_num_results = 0;
  if (num_query_terms == 1) {
    // Do standard DAAT OR mode processing, since WAND won't help.
    if (index_layered_ && query_term_data[0]->num_layers() == 2) {
      // We have two layers, so let's run the standard DAAT OR on the first layer only.
      // If there are k results, we can stop; otherwise rerun the query on the second layer.
      total_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
      if (total_num_results < kMaxNumResults) {
        index_reader_.CloseList(list_data_pointers[0]);
        list_data_pointers[0] = index_reader_.OpenList(*query_term_data[0], query_term_data[0]->num_layers() - 1, single_term_query);
        total_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
      }
    } else {
      // There is only one layer, run the query on it.
      total_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
    }
  } else {
    /*
     * We can estimate the threshold after processing the top docs lists in OR mode, but we can't decrease the upperbounds on the 2nd layers
     * because this will result in many of our high scoring documents to be skipped from the 2nd layers (including the ones from the top docs lists).
     *
     * TODO: What are some ways of decreasing the upperbound on the 2nd layers...?
     */

    float threshold = 0;
    if (two_tiered) {
      // It's possible that after processing the top docs, there is an unresolved docID (only present in some of the top docs lists, but not in others)
      // that could have a score higher than the top-k threshold we derive here.
      // For this reason, we can't early terminate here if we get k results.
      int top_docs_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
#ifdef IRTK_DEBUG
      cout << "Num results from top docs lists: " << top_docs_num_results << endl;
#endif

      // The k-th score in the heap we get from the union of the top docs layers is our starting threshold.
      // It is a lower bound for the score necessary for a new docID to make it into our top-k.
      // The threshold is 0 if we didn't get k results from the top docs layers, meaning any docID can make it into the top-k.
      threshold = (top_docs_num_results >= kMaxNumResults) ? results[kMaxNumResults - 1].first : 0;
#ifdef IRTK_DEBUG
      cout << "Threshold from top docs lists: " << threshold << endl;
#endif
    }

    // We have to make sure that the layers are overlapping. So we'll be traversing the top-docs twice (in the second overlapping layer).
    // This is necessary because we're not using accumulators for the top-docs lists. It's only an approximate lower bound score on the top docIDs, since
    // the docID may be present in other lists, that did not make it into the top-docs.
    for (int i = 0; i < num_query_terms; ++i) {
      if (query_term_data[i]->num_layers() == 1) {
        // For a single layered list, we'll have to traverse it again.
        list_data_pointers[i]->ResetList(single_term_query);
      } else {
        // For a dual layered list, we close the first layer and open the second layer.
        index_reader_.CloseList(list_data_pointers[i]);
        list_data_pointers[i] = index_reader_.OpenList(*query_term_data[i], query_term_data[i]->num_layers() - 1, single_term_query);
      }

#ifdef IRTK_DEBUG
      cout << "Overlapping Layer for '" << string(query_term_data[i]->term(), query_term_data[i]->term_len())
          << "', Layer Num: " << (query_term_data[i]->num_layers() - 1)
          << ", Score Threshold: " << list_data_pointers[i]->score_threshold()
          << ", Num Docs: " << list_data_pointers[i]->num_docs()
          << ", Num Blocks: " << list_data_pointers[i]->num_blocks()
          << ", Num Chunks: " << list_data_pointers[i]->num_chunks() << endl;
#endif
    }

    const bool kMWand = true;

    // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
    const float kBm25K1 =  2.0;  // k1
    const float kBm25B = 0.75;  // b

    // We can precompute a few of the BM25 values here.
    const float kBm25NumeratorMul = kBm25K1 + 1;
    const float kBm25DenominatorAdd = kBm25K1 * (1 - kBm25B);
    const float kBm25DenominatorDocLenMul = kBm25K1 * kBm25B / collection_average_doc_len_;

    // BM25 components.
    float bm25_sum;  // The BM25 sum for the current document we're processing in the intersection.
    int doc_len;
    uint32_t f_d_t;

    // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for each list.
    float idf_t[num_query_terms];  // Using a variable length array here.
    int num_docs_t;
    for (int i = 0; i < num_query_terms; ++i) {
      num_docs_t = list_data_pointers[i]->num_docs_complete_list();
      idf_t[i] = log10(1 + (collection_total_num_docs_ - num_docs_t + 0.5) / (num_docs_t + 0.5));
    }

    // We use this to get the next lowest docID from all the lists.
    pair<uint32_t, int> lists_curr_postings[num_query_terms]; // Using a variable length array here.
    int num_lists_remaining = 0; // The number of lists with postings remaining.
    uint32_t curr_doc_id;
    for (int i = 0; i < num_query_terms; ++i) {
      if ((curr_doc_id = list_data_pointers[i]->NextGEQ(0)) < ListData::kNoMoreDocs) {
        lists_curr_postings[num_lists_remaining++] = make_pair(curr_doc_id, i);
      }
    }

    int i, j;
    pair<uint32_t, int> pivot = make_pair(0, -1);  // The pivot can't be a pointer to the 'lists_curr_postings'
                                                   // since those values will change when we advance list pointers after scoring a docID.
    float pivot_weight;                            // The upperbound score on the pivot docID.

    /*
     * Two implementation choices here:
     * * Keep track of the number of lists remaining; requires an if statement after each nextGEQ() to check if we reached the max docID sentinel value (implemented here).
     * * Don't keep track of the number of lists remaining. Don't need if statement after each nextGEQ(), but need to sort all list postings at every turn.
     */
    while (num_lists_remaining) {
      // Sort current postings in non-descending order.
      // Can also sort all entries less than or equal to the pivot docID and merge with all higher docIDs.
      // Although probably won't be faster unless we have a significant number of terms in the query.
      sort(lists_curr_postings, lists_curr_postings + num_lists_remaining, ListDocIdCompare());

      // Select a pivot.
      pivot_weight = 0;
      pivot.second = -1;
      for (i = 0; i < num_lists_remaining; ++i) {
        pivot_weight += list_thresholds[lists_curr_postings[i].second];
        if (pivot_weight >= threshold) {
          pivot = lists_curr_postings[i];
          break;
        }
      }

      /*
      // If using this, change the while condition to true. Don't need to check for sentinel value after NextGEQ(),
      // but need to sort all the list postings at each step.
      if(pivot.first == ListData::kNoMoreDocs) {
        break;
      }
      */

      // If we don't have a pivot (the pivot list is -1), or if the pivot docID is the sentinel value for no more docs,
      // it means that no newly encountered docID can make it into the top-k and we can quit.
      if (pivot.second == -1) {
        break;
      }

      if (pivot.first == lists_curr_postings[0].first) {
        // We have enough weight on the pivot, so score all docIDs equal to the pivot (these can be beyond the pivot as well).
        // We know we have enough weight when the docID at the pivot list equals the docID at the first list.
        bm25_sum = 0;
        for(i = 0; i < num_lists_remaining && pivot.first == lists_curr_postings[i].first; ++i) {
          // Compute the BM25 score from frequencies.
          f_d_t = list_data_pointers[lists_curr_postings[i].second]->GetFreq();
          doc_len = index_reader_.document_map().GetDocumentLength(lists_curr_postings[i].first);
          bm25_sum += idf_t[lists_curr_postings[i].second] * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);

          ++num_postings_scored_;

          // Advance list pointer.
          if ((lists_curr_postings[i].first = list_data_pointers[lists_curr_postings[i].second]->NextGEQ(lists_curr_postings[i].first + 1)) == ListData::kNoMoreDocs) {
            // Compact the array. Move the current posting to the end.
            --num_lists_remaining;
            pair<uint32_t, int> curr = lists_curr_postings[i];
            for(j = i; j < num_lists_remaining; ++j) {
              lists_curr_postings[j] = lists_curr_postings[j+1];
            }
            lists_curr_postings[num_lists_remaining] = curr;
            --i;
          }
        }

        // Decide whether docID makes it into the top-k.
        if (total_num_results < kMaxNumResults) {
          // We insert a document if we don't have k documents yet.
          results[total_num_results] = make_pair(bm25_sum, pivot.first);
          push_heap(results, results + total_num_results + 1, ResultCompare());
        } else {
          if (bm25_sum > results->first) {
            // We insert a document only if it's score is greater than the minimum scoring document in the heap.
            pop_heap(results, results + kMaxNumResults, ResultCompare());
            results[kMaxNumResults - 1].first = bm25_sum;
            results[kMaxNumResults - 1].second = pivot.first;
            push_heap(results, results + kMaxNumResults, ResultCompare());

            // Update the threshold.
            threshold = results->first;
          }
        }
        ++total_num_results;
      } else {
        // We don't have enough weight on the pivot yet. We know this is true when the docID from the first list != docID at the pivot.
        // There are two simple strategies that we can employ:
        // * Advance any one list before the pivot (just choose the first list). This is the original WAND algorithm.
        // * Advance all lists before the pivot (saves a few sorting operations at the cost of less list skipping). This is the mWAND algorithm.
        //   Main point is that index accesses are cheaper when the index is in main memory, so we try to do less list pointer sorting operations instead.
        // In both strategies, we advance the list pointer(s) at least to the pivot docID.
        if (kMWand) {
          for (i = 0; i < num_lists_remaining; ++i) {
            // Advance list pointer.
            if ((lists_curr_postings[i].first = list_data_pointers[lists_curr_postings[i].second]->NextGEQ(pivot.first)) == ListData::kNoMoreDocs) {
              // Compact the array. Move the current posting to the end.
              --num_lists_remaining;
              pair<uint32_t, int> curr = lists_curr_postings[i];
              for (j = i; j < num_lists_remaining; ++j) {
                lists_curr_postings[j] = lists_curr_postings[j + 1];
              }
              lists_curr_postings[num_lists_remaining] = curr;
              --i;
            }
          }
        } else {
          if ((lists_curr_postings[0].first = list_data_pointers[lists_curr_postings[0].second]->NextGEQ(pivot.first)) == ListData::kNoMoreDocs) {
            // Just swap the current posting with the one at the end of the array.
            // We'll be sorting at the start of the loop, so we don't need to compact and keep the order of the postings.
            --num_lists_remaining;
            pair<uint32_t, int> curr = lists_curr_postings[0];
            lists_curr_postings[0] = lists_curr_postings[num_lists_remaining];
            lists_curr_postings[num_lists_remaining] = curr;
          }
        }
      }
    }
  }

  // Sort top-k results in descending order by document score.
  sort(results, results + min(kMaxNumResults, total_num_results), ResultCompare());

  *num_results = min(total_num_results, kMaxNumResults);
  for (int i = 0; i < num_query_terms; ++i) {
    index_reader_.CloseList(list_data_pointers[i]);
  }
  return total_num_results;
}

// TODO:
// Difference between MaxScore and WAND is that once the threshold is sufficient enough, MaxScore will ignore the rest of the new docIDs in lists
// whose upperbounds indicate that they can't make it into the top-k.
//
// WAND is more akin to AND mode, since we move all list pointers to a common docID before scoring a document (unless we skip it); the difference being that we don't require the query terms to
// appear in all docIDs. Here, we skip scoring whole docIDs.
//
// MaxScore is more akin to OR mode, since we score a posting as soon as we reach it in the postings list (with the exception that we are able to skip scoring some postings).
// Here, we skip scoring individual postings.
//
// Use the MaxScore and Two Level MaxScore algorithms.
int QueryProcessor::MergeListsMaxScore(LexiconData** query_term_data, int num_query_terms, Result* results, int* num_results, bool two_tiered) {
  // Constraints on the type of index we expect.
  assert(index_layered_);
  assert(index_overlapping_layers_);
  assert(index_num_layers_ == 2);

  const int kMaxNumResults = *num_results;

  // Holds a pointer to the list for each corresponding query term.
  ListData* list_data_pointers[num_query_terms];  // Using a variable length array here.

  // For MaxScore to work correctly, need term upperbounds on the whole list.
  float list_thresholds[num_query_terms];  // Using a variable length array here.

  bool single_term_query = false;
  if (num_query_terms == 1) {
    single_term_query = true;
  }

  for (int i = 0; i < num_query_terms; ++i) {
    // Open the first layer (the top docs).
    list_data_pointers[i] = index_reader_.OpenList(*query_term_data[i], 0, single_term_query);
    list_thresholds[i] = list_data_pointers[i]->score_threshold();
#ifdef IRTK_DEBUG
    cout << "Top Docs Layer for '" << string(query_term_data[i]->term(), query_term_data[i]->term_len())
        << "', Layer Num: 0, Score Threshold: " << list_data_pointers[i]->score_threshold()
        << ", Num Docs: " << list_data_pointers[i]->num_docs()
        << ", Num Blocks: " << list_data_pointers[i]->num_blocks()
        << ", Num Chunks: " << list_data_pointers[i]->num_chunks() << endl;
#endif
  }

  int total_num_results = 0;
  if (num_query_terms == 1) {
    // Do standard DAAT OR mode processing, since Max Score won't help.
    if (index_layered_ && query_term_data[0]->num_layers() == 2) {
      // We have two layers, so let's run the standard DAAT OR on the first layer only.
      // If there are k results, we can stop; otherwise rerun the query on the second layer.
      total_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
      if (total_num_results < kMaxNumResults) {
        index_reader_.CloseList(list_data_pointers[0]);
        list_data_pointers[0] = index_reader_.OpenList(*query_term_data[0], query_term_data[0]->num_layers() - 1, single_term_query);
        total_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
      }
    } else {
      // There is only one layer, run the query on it.
      total_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
    }
  } else {
    /*
     * We can estimate the threshold after processing the top docs lists in OR mode, but we can't decrease the upperbounds on the 2nd layers
     * because this will result in many of our high scoring documents to be skipped from the 2nd layers (including the ones from the top docs lists).
     *
     * TODO: What are some ways of decreasing the upperbound on the 2nd layers...?
     */

    float threshold = 0;
    if (two_tiered) {
      // It's possible that after processing the top docs, there is an unresolved docID (only present in some of the top docs lists, but not in others)
      // that could have a score higher than the top-k threshold we derive here.
      // For this reason, we can't early terminate here if we get k results.
      int top_docs_num_results = MergeLists(list_data_pointers, num_query_terms, results, kMaxNumResults);
#ifdef IRTK_DEBUG
      cout << "Num results from top docs lists: " << top_docs_num_results << endl;
#endif

      // The k-th score in the heap we get from the union of the top docs layers is our starting threshold.
      // It is a lower bound for the score necessary for a new docID to make it into our top-k.
      // The threshold is 0 if we didn't get k results from the top docs layers, meaning any docID can make it into the top-k.
      threshold = (top_docs_num_results >= kMaxNumResults) ? results[kMaxNumResults - 1].first : 0;
#ifdef IRTK_DEBUG
      cout << "Threshold from top docs lists: " << threshold << endl;
#endif
    }

    // We have to make sure that the layers are overlapping. So we'll be traversing the top-docs twice (in the second overlapping layer).
    // This is necessary because we're not using accumulators for the top-docs lists. It's only an approximate lower bound score on the top docIDs, since
    // the docID may be present in other lists, that did not make it into the top-docs.
    for (int i = 0; i < num_query_terms; ++i) {
      if (query_term_data[i]->num_layers() == 1) {
        // For a single layered list, we'll have to traverse it again.
        list_data_pointers[i]->ResetList(single_term_query);
      } else {
        // For a dual layered list, we close the first layer and open the second layer.
        index_reader_.CloseList(list_data_pointers[i]);
        list_data_pointers[i] = index_reader_.OpenList(*query_term_data[i], query_term_data[i]->num_layers() - 1, single_term_query);
      }

#ifdef IRTK_DEBUG
      cout << "Overlapping Layer for '" << string(query_term_data[i]->term(), query_term_data[i]->term_len())
          << "', Layer Num: " << (query_term_data[i]->num_layers() - 1)
          << ", Score Threshold: " << list_data_pointers[i]->score_threshold()
          << ", Num Docs: " << list_data_pointers[i]->num_docs()
          << ", Num Blocks: " << list_data_pointers[i]->num_blocks()
          << ", Num Chunks: " << list_data_pointers[i]->num_chunks() << endl;
#endif
    }

    // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
    const float kBm25K1 =  2.0;  // k1
    const float kBm25B = 0.75;   // b

    // We can precompute a few of the BM25 values here.
    const float kBm25NumeratorMul = kBm25K1 + 1;
    const float kBm25DenominatorAdd = kBm25K1 * (1 - kBm25B);
    const float kBm25DenominatorDocLenMul = kBm25K1 * kBm25B / collection_average_doc_len_;

    // BM25 components.
    float bm25_sum;  // The BM25 sum for the current document we're processing in the intersection.
    int doc_len;
    uint32_t f_d_t;

    // For use with score skipping.
    float remaining_upperbound;

    // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for each list.
    float idf_t[num_query_terms];  // Using a variable length array here.
    int num_docs_t;
    for (int i = 0; i < num_query_terms; ++i) {
      num_docs_t = list_data_pointers[i]->num_docs_complete_list();
      idf_t[i] = log10(1 + (collection_total_num_docs_ - num_docs_t + 0.5) / (num_docs_t + 0.5));
    }

    // We use this to get the next lowest docID from all the lists.
    uint32_t lists_curr_postings[num_query_terms];  // Using a variable length array here.
    for (int i = 0; i < num_query_terms; ++i) {
      lists_curr_postings[i] = list_data_pointers[i]->NextGEQ(0);
    }

    pair<float, int> list_upperbounds[num_query_terms];  // Using a variable length array here.
    int num_lists_remaining = 0;  // The number of lists with postings remaining.
    for (int i = 0; i < num_query_terms; ++i) {
      if (lists_curr_postings[i] != ListData::kNoMoreDocs) {
        list_upperbounds[num_lists_remaining++] = make_pair(list_thresholds[i], i);
      }
    }

    sort(list_upperbounds, list_upperbounds + num_lists_remaining, greater<pair<float, int> > ());

    // Precalculate the upperbounds for all possibilities.
    for (int i = num_lists_remaining - 2; i >= 0; --i) {
      list_upperbounds[i].first += list_upperbounds[i + 1].first;
    }

    /*// When a list has no more postings remaining, we can remove it right away, or wait until we iterated through the rest of the lists,
    // and remove any that have no more postings remaining. Removing them after iterating through all lists required an additional if statement.
    // What's odd is that when we remove the threshold checks (so that we can no longer early terminate), setting this option to 'false'
    // performs about 2ms faster (we wouldn't expect it to because of the extra if statement). However, when the threshold checks are in place,
    // setting this option to 'true' performs slightly faster (1-2ms). As far as I can tell, both do the same thing.
    const bool kCompactArrayRightAway = false;*/

    // When 'true', enables the use of embedded list score information to provide further efficiency gains
    // through better list skipping and less scoring computations.
    const bool kScoreSkipping = false;

    // Defines the score skipping mode to use.
    // '0' means use block score upperbounds.
    // '1' means use chunk score upperbounds.
#define SCORE_SKIPPING_MODE 1

    int i, j;
    int curr_list_idx;
    pair<float, int>* top;
    uint32_t curr_doc_id;  // Current docID we're processing the score for.
    /*bool compact_upperbounds = false;*/

    while (num_lists_remaining) {
      top = &list_upperbounds[0];
      if (kScoreSkipping && threshold > list_upperbounds[1].first) {
#ifdef MAX_SCORE_DEBUG
        cout << "Current threshold: " << threshold << endl;
        cout << "Remaining upperbound: " << list_upperbounds[1].first << endl;
#endif

        // Only the first (highest scoring) list can contain a docID that can still make it into the top-k,
        // so we move the first list to the first docID that has an upperbound that will allow it to make it into the top-k.
#if SCORE_SKIPPING_MODE == 0
        if ((lists_curr_postings[0] = list_data_pointers[top->second]->NextGreaterBlockScore(threshold - list_upperbounds[1].first)) == ListData::kNoMoreDocs) {
#elif SCORE_SKIPPING_MODE == 1
        if ((lists_curr_postings[0] = list_data_pointers[top->second]->NextGreaterChunkScore(threshold - list_upperbounds[1].first)) == ListData::kNoMoreDocs) {
#endif
          // Can early terminate at this point.
          break;
        }
      } else {
        // Find the lowest docID that can still possibly make it into the top-k (while being able to make it into the top-k).
        for (i = 1; i < num_lists_remaining; ++i) {
          curr_list_idx = list_upperbounds[i].second;
          if (threshold > list_upperbounds[i].first) {
            break;
          }

          if (lists_curr_postings[curr_list_idx] < lists_curr_postings[top->second]) {
            top = &list_upperbounds[i];
          }
        }
      }

      // Check if we can early terminate. This might happen only after we have finished traversing at least one list.
      // This is because our upperbounds don't decrease unless we are totally finished traversing one list.
      // Must check this since we initialize top to point to the first element in the list upperbounds array by default.
      if (threshold > list_upperbounds[0].first) {
        break;
      }

      // At this point, 'curr_doc_id' can either not be able to exceed the threshold score, or it can be the max possible docID sentinel value.
      curr_doc_id = lists_curr_postings[top->second];

      // We score a docID fully here, making any necessary lookups right away into other lists.
      // Disadvantage with this approach is that you'll be doing a NextGEQ() more than once for some lists on the same docID.
      bm25_sum = 0;
      for (i = 0; i < num_lists_remaining; ++i) {
        curr_list_idx = list_upperbounds[i].second;

        // Check if we can early terminate the scoring of this particular docID.
        if (threshold > bm25_sum + list_upperbounds[i].first) {
          break;
        }

        // Move to the curr docID we're scoring.
        lists_curr_postings[curr_list_idx] = list_data_pointers[curr_list_idx]->NextGEQ(curr_doc_id);

        if (lists_curr_postings[curr_list_idx] == curr_doc_id) {
          // Use the tighter score bound we have on the current list to see if we can early terminate the scoring of this particular docID.
          if (kScoreSkipping) {
            // TODO: To avoid the (i == num_lists_remaining - 1) test, can insert a dummy list with upperbound 0.
            remaining_upperbound = (i == num_lists_remaining - 1) ? 0 : list_upperbounds[i + 1].first;
#if SCORE_SKIPPING_MODE == 0
            if (threshold > bm25_sum + list_data_pointers[curr_list_idx]->GetBlockScoreBound() + remaining_upperbound) {
#elif SCORE_SKIPPING_MODE == 1
            if (threshold > bm25_sum + list_data_pointers[curr_list_idx]->GetChunkScoreBound() + remaining_upperbound) {
#endif
#ifdef MAX_SCORE_DEBUG
              cout << "Short circuiting evaluation of docID: " << curr_doc_id << " from list with " << list_data_pointers[curr_list_idx]->num_docs()
                  << " postings" << endl;
              cout << "Current BM25 sum: " << bm25_sum << endl;
              cout << "Current chunk bound for docID " << curr_doc_id << " is: " << list_data_pointers[curr_list_idx]->GetChunkScoreBound() << endl;
              cout << "Current threshold: " << threshold << endl;
              cout << "Remaining upperbound: " << remaining_upperbound << endl;
#endif

              // Can now move the list pointer further.
              lists_curr_postings[curr_list_idx] = list_data_pointers[curr_list_idx]->NextGEQ(lists_curr_postings[curr_list_idx] + 1);
              if (lists_curr_postings[curr_list_idx] == ListData::kNoMoreDocs) {
                /*if (kCompactArrayRightAway) {*/
                  --num_lists_remaining;
                  float curr_list_upperbound = list_thresholds[curr_list_idx];

                  // Compact the list upperbounds array.
                  for (j = i; j < num_lists_remaining; ++j) {
                    list_upperbounds[j] = list_upperbounds[j + 1];
                  }

                  // Recalculate the list upperbounds. Note that we only need to recalculate those entries less than i.
                  for (j = 0; j < i; ++j) {
                    list_upperbounds[j].first -= curr_list_upperbound;
                  }
                  --i;
                /*} else {
                  compact_upperbounds = true;
                }*/
              }

              break;
            }
          }

          // Compute BM25 score from frequencies.
          f_d_t = list_data_pointers[curr_list_idx]->GetFreq();
          doc_len = index_reader_.document_map().GetDocumentLength(lists_curr_postings[curr_list_idx]);
          bm25_sum += idf_t[curr_list_idx] * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);

          ++num_postings_scored_;

          // Can now move the list pointer further.
          lists_curr_postings[curr_list_idx] = list_data_pointers[curr_list_idx]->NextGEQ(lists_curr_postings[curr_list_idx] + 1);
        }

        if (lists_curr_postings[curr_list_idx] == ListData::kNoMoreDocs) {
          /*if (kCompactArrayRightAway) {*/
            --num_lists_remaining;
            float curr_list_upperbound = list_thresholds[curr_list_idx];

            // Compact the list upperbounds array.
            for (j = i; j < num_lists_remaining; ++j) {
              list_upperbounds[j] = list_upperbounds[j + 1];
            }

            // Recalculate the list upperbounds. Note that we only need to recalculate those entries less than i.
            for (j = 0; j < i; ++j) {
              list_upperbounds[j].first -= curr_list_upperbound;
            }
            --i;
          /*} else {
            compact_upperbounds = true;
          }*/
        }
      }

      // Need to keep track of the top-k documents.
      if (total_num_results < kMaxNumResults) {
        // We insert a document if we don't have k documents yet.
        results[total_num_results] = make_pair(bm25_sum, curr_doc_id);
        push_heap(results, results + total_num_results + 1, ResultCompare());
      } else {
        if (bm25_sum > results->first) {
          // We insert a document only if it's score is greater than the minimum scoring document in the heap.
          pop_heap(results, results + kMaxNumResults, ResultCompare());
          results[kMaxNumResults - 1].first = bm25_sum;
          results[kMaxNumResults - 1].second = curr_doc_id;
          push_heap(results, results + kMaxNumResults, ResultCompare());

          // Update the threshold.
          threshold = results->first;
        }
      }
      ++total_num_results;

      /*if (!kCompactArrayRightAway) {
        if (compact_upperbounds) {
          int num_lists = num_lists_remaining;
          num_lists_remaining = 0;
          for (i = 0; i < num_lists; ++i) {
            curr_list_idx = list_upperbounds[i].second;
            if (lists_curr_postings[curr_list_idx] != ListData::kNoMoreDocs) {
              list_upperbounds[num_lists_remaining++] = make_pair(list_thresholds[curr_list_idx], curr_list_idx);
            }
          }

          sort(list_upperbounds, list_upperbounds + num_lists_remaining, greater<pair<float, int> > ());

          // Precalculate the upperbounds for all possibilities.
          for (i = num_lists_remaining - 2; i >= 0; --i) {
            list_upperbounds[i].first += list_upperbounds[i + 1].first;
          }

          compact_upperbounds = false;
        }
      }*/
    }
  }

  // Sort top-k results in descending order by document score.
  sort(results, results + min(kMaxNumResults, total_num_results), ResultCompare());

  *num_results = min(total_num_results, kMaxNumResults);
  for (int i = 0; i < num_query_terms; ++i) {
    index_reader_.CloseList(list_data_pointers[i]);
  }
  return total_num_results;
}

int QueryProcessor::IntersectLists(ListData** lists, int num_lists, Result* results, int num_results) {
  return IntersectLists(NULL, 0, lists, num_lists, results, num_results);
}

// Returns the total number of document results found in the intersection.
// Note that there is not a guaranteed order of same scoring docIDs.
int QueryProcessor::IntersectLists(ListData** merge_lists, int num_merge_lists, ListData** lists, int num_lists, Result* results, int num_results) {
  // We have a choice of whether to use a heap (push() / pop() an array) or just search through an array to replace low scoring results
  // and finally sorting it before returning the top-k results in sorted order.
  // For k = 10 results, an array performs only slightly better than a heap. As k increases above 10, heap should be faster.
  // In the general case, a heap should be used (unless k is less than 10), so this option should be 'false'.
  const bool kUseArrayInsteadOfHeap = false;

  int total_num_results = 0;

  // For the array instead of heap top-k technique.
  float curr_min_doc_score;
  Result* min_scoring_result = NULL;

  // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
  const float kBm25K1 =  2.0;  // k1
  const float kBm25B = 0.75;   // b

  // We can precompute a few of the BM25 values here.
  const float kBm25NumeratorMul = kBm25K1 + 1;
  const float kBm25DenominatorAdd = kBm25K1 * (1 - kBm25B);
  const float kBm25DenominatorDocLenMul = kBm25K1 * kBm25B / collection_average_doc_len_;

  // BM25 components.
  float bm25_sum;  // The BM25 sum for the current document we're processing in the intersection.
  int doc_len;
  uint32_t f_d_t;

  uint32_t did = 0;
  uint32_t d;
  int i;  // Index for various loops.

  // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for each list.
  float idf_t[num_lists];  // Using a variable length array here.
  int num_docs_t;
  for (i = 0; i < num_lists; ++i) {
    num_docs_t = lists[i]->num_docs_complete_list();
    idf_t[i] = log10(1 + (collection_total_num_docs_ - num_docs_t + 0.5) / (num_docs_t + 0.5));
  }

  // Necessary for the merge lists.
  // TODO: Can also try the heap based method here. Can select between heap and array method based on 'num_merge_lists'.
  uint32_t min_doc_id;

  while (did < ListData::kNoMoreDocs) {
    if (merge_lists != NULL) { // For the lists which we are merging.
      // This will select the lowest docID (ignoring duplicates among the merge lists and any docIDs we have skipped past through AND mode operation).
      min_doc_id = ListData::kNoMoreDocs;
      for (i = 0; i < num_merge_lists; ++i) {
        if ((d = merge_lists[i]->NextGEQ(did)) < min_doc_id) {
          min_doc_id = d;
        }
      }

      assert(min_doc_id >= did);

      did = min_doc_id;
      i = 0;
    } else {
      // Get next element from shortest list.
      did = lists[0]->NextGEQ(did);
      i = 1;
    }

    if (did == ListData::kNoMoreDocs)
      break;

    d = did;

    // Try to find entries with same docID in other lists.
    for (; (i < num_lists) && ((d = lists[i]->NextGEQ(did)) == did); ++i) {
      continue;
    }

    if (d > did) {
      // Not in intersection.
      did = d;
    } else {
      assert(d == did);

      // Compute BM25 score from frequencies.
      bm25_sum = 0;
      for (i = 0; i < num_lists; ++i) {
        f_d_t = lists[i]->GetFreq();
        doc_len = index_reader_.document_map().GetDocumentLength(did);
        bm25_sum += idf_t[i] * (f_d_t * kBm25NumeratorMul) / (f_d_t + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len);
      }

      if (kUseArrayInsteadOfHeap) {
        // Use an array to maintain the top-k documents.
        if (total_num_results < num_results) {
          results[total_num_results] = make_pair(bm25_sum, did);
          if (min_scoring_result == NULL || bm25_sum < min_scoring_result->first)
            min_scoring_result = results + total_num_results;
        } else {
          if (bm25_sum > min_scoring_result->first) {
            // Replace the min scoring result with the current (higher scoring) result.
            min_scoring_result->first = bm25_sum;
            min_scoring_result->second = did;

            // Find the new min scoring document.
            curr_min_doc_score = numeric_limits<float>::max();
            for (i = 0; i < num_results; ++i) {
              if (results[i].first < curr_min_doc_score) {
                curr_min_doc_score = results[i].first;
                min_scoring_result = results + i;
              }
            }
          }
        }
      } else {
        // Use a heap to maintain the top-k documents. This has to be a min heap,
        // where the lowest scoring document is on top, so that we can easily pop it,
        // and push a higher scoring document if need be.
        if (total_num_results < num_results) {
          // We insert a document if we don't have k documents yet.
          results[total_num_results] = make_pair(bm25_sum, did);
          push_heap(results, results + total_num_results + 1, ResultCompare());
        } else {
          if (bm25_sum > results->first) {
            // We insert a document only if it's score is greater than the minimum scoring document in the heap.
            pop_heap(results, results + num_results, ResultCompare());
            results[num_results - 1].first = bm25_sum;
            results[num_results - 1].second = did;
            push_heap(results, results + num_results, ResultCompare());
          }
        }
      }

      ++total_num_results;
      ++did;  // Search for next docID.
    }
  }

  // Sort top-k results in descending order by document score.
  sort(results, results + min(num_results, total_num_results), ResultCompare());

  return total_num_results;
}

// Processes queries in AND mode. Utilizes position data for the top scoring docIDs.
// The top docIDs (the number is configured within the function, by 'kNumTopPositionsToScore') are ranked according to BM25,
// and their position data is stored as well; these top scoring docIDs are then ranked along with position information,
// finally storing the new top 'num_results' docIDs into 'results'.
// Returns the total number of document results found in the intersection.
int QueryProcessor::IntersectListsTopPositions(ListData** lists, int num_lists, Result* results, int num_results) {
  assert(use_positions_ == true);

  // Maintain the top docIDs in a heap of this size, scored using standard BM25.
  // Then, utilize the position information for these results and keep only the top 'num_results'.
  const int kNumTopPositionsToScore = max(200, num_results);

  const int kNumLists = num_lists;                              // The number of lists we traverse.
  const int kMaxNumResults = num_results;                       // The maximum number of results we have to return.
  const int kMaxPositions = ChunkDecoder::kMaxProperties;       // The maximum number of positions for a docID in any list.
  const int kResultPositionStride = kMaxPositions + 1;          // For each result, per list, we store all the positions, plus an integer
                                                                // indicating the number of positions stored.
  const int kResultStride = kNumLists * kResultPositionStride;  // For each result, we have 'num_lists' worth of position information.

  // We will store position information for the top candidates in this array. The first 'kNumTopPositionsToScore' results will be stored sequentially,
  // but afterwards any result that gets pushed out of the top candidates heap, will have its positions replaced.
  // We always store a pointer to the start of the positions for each candidate document.
  uint32_t* position_pool = new uint32_t[kResultStride * kNumTopPositionsToScore];

  // The k temporary docID, score, and position pointer tuples, with a score comparator to maintain the top-k results.
  ResultPositionTuple* result_position_tuples = new ResultPositionTuple[kNumTopPositionsToScore];

  int total_num_results = 0;

  // BM25 parameters: see 'http://en.wikipedia.org/wiki/Okapi_BM25'.
  const float kBm25K1 =  2.0;  // k1
  const float kBm25B = 0.75;   // b

  // We can precompute a few of the BM25 values here.
  const float kBm25NumeratorMul = kBm25K1 + 1;
  const float kBm25DenominatorAdd = kBm25K1 * (1 - kBm25B);
  const float kBm25DenominatorDocLenMul = kBm25K1 * kBm25B / collection_average_doc_len_;

  // BM25 components.
  float bm25_sum;  // The BM25 sum for the current document we're processing in the intersection.
  int doc_len_d;   // The length for the current document we're processing in the intersection.
  // Using variable length arrays here.
  uint32_t f_d_t[kNumLists];                 // The document term frequencies, one per list.
  const uint32_t* positions_d_t[kNumLists];  // The document position pointers, one per list.
  float acc_d_t[kNumLists];                  // The term proximity accumulators, one per list.
  float idf_t[kNumLists];                    // The inverse document frequencies, one per list.

  uint32_t did = 0;
  uint32_t d;

  uint32_t num_positions;

  int i, j;

  // Compute the inverse document frequency component. It is not document dependent, so we can compute it just once for each list.
  int num_docs_t;
  for (i = 0; i < kNumLists; ++i) {
    num_docs_t = lists[i]->num_docs_complete_list();
    idf_t[i] = log10(1 + (collection_total_num_docs_ - num_docs_t + 0.5) / (num_docs_t + 0.5));
  }

  while (did < ListData::kNoMoreDocs) {
    // Get next element from shortest list.
    if ((did = lists[0]->NextGEQ(did)) == ListData::kNoMoreDocs)
      break;

    d = did;

    // Try to find entries with same docID in other lists.
    for (i = 1; (i < kNumLists) && ((d = lists[i]->NextGEQ(did)) == did); ++i) {
      continue;
    }

    if (d > did) {
      // Not in intersection.
      did = d;
    } else {
      assert(d == did);

      // Compute BM25 score from frequencies.
      bm25_sum = 0;
      doc_len_d = index_reader_.document_map().GetDocumentLength(did);
      for (i = 0; i < kNumLists; ++i) {
        f_d_t[i] = lists[i]->GetFreq();
        positions_d_t[i] = lists[i]->curr_chunk_decoder().current_positions();
        bm25_sum += idf_t[i] * (f_d_t[i] * kBm25NumeratorMul) / (f_d_t[i] + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * doc_len_d);
      }

      // Use a heap to maintain the top-k documents. This has to be a min heap,
      // where the lowest scoring document is on top, so that we can easily pop it,
      // and push a higher scoring document if need be.
      if (total_num_results < kNumTopPositionsToScore) {
        // We insert a document if we don't have k documents yet.
        result_position_tuples[total_num_results].doc_id = did;
        result_position_tuples[total_num_results].doc_len = doc_len_d;
        result_position_tuples[total_num_results].score = bm25_sum;
        result_position_tuples[total_num_results].positions = &position_pool[total_num_results * kResultStride];
        for (i = 0; i < kNumLists; ++i) {
          num_positions = min(f_d_t[i], static_cast<uint32_t>(kMaxPositions));
          result_position_tuples[total_num_results].positions[i * kResultPositionStride] = num_positions;
          memcpy(&result_position_tuples[total_num_results].positions[(i * kResultPositionStride) + 1], positions_d_t[i], num_positions * sizeof(*positions_d_t[i]));
        }
        push_heap(result_position_tuples, result_position_tuples + total_num_results + 1);
      } else {
        if (bm25_sum > result_position_tuples[0].score) {
          // We insert a document only if it's score is greater than the minimum scoring document in the heap.
          pop_heap(result_position_tuples, result_position_tuples + kNumTopPositionsToScore);
          result_position_tuples[kNumTopPositionsToScore - 1].doc_id = did;
          result_position_tuples[kNumTopPositionsToScore - 1].doc_len = doc_len_d;
          result_position_tuples[kNumTopPositionsToScore - 1].score = bm25_sum;
          // Replace the positions.
          for (i = 0; i < kNumLists; ++i) {
            num_positions = min(f_d_t[i], static_cast<uint32_t>(kMaxPositions));
            result_position_tuples[kNumTopPositionsToScore - 1].positions[i * kResultPositionStride] = num_positions;
            memcpy(&result_position_tuples[kNumTopPositionsToScore - 1].positions[(i * kResultPositionStride) + 1], positions_d_t[i], num_positions * sizeof(*positions_d_t[i]));
          }
          push_heap(result_position_tuples, result_position_tuples + num_results);
        }
      }

      ++total_num_results;
      ++did;  // Search for next docID.
    }
  }

  // Utilize positions and prepare final result set.
  // Note that positions are stored in gap coded form.
  // We use a formula that rewards proximity of the query terms. It's too slow to run on all possible candidates.
  const int kNumReturnedResults = min(kNumTopPositionsToScore, total_num_results);

  for (i = 0; i < kNumLists; ++i) {
    acc_d_t[i] = 0;
  }

  // Term proximity components.
  int r;
  uint32_t k, l;
  uint32_t num_positions_top, num_positions_bottom;
  const uint32_t* positions_top, *positions_bottom;
  uint32_t positions_top_actual, positions_bottom_actual;
  int dist;
  float ids;

  for (r = 0; r < kNumReturnedResults; ++r) {
    for (i = 0; i < kNumLists; ++i) {
      num_positions_top = result_position_tuples[r].positions[i * kResultPositionStride];
      positions_top = &result_position_tuples[r].positions[i * kResultPositionStride + 1];

      for (j = i + 1; j < kNumLists; ++j) {
        num_positions_bottom = result_position_tuples[r].positions[j * kResultPositionStride];
        positions_bottom = &result_position_tuples[r].positions[j * kResultPositionStride + 1];

        positions_top_actual = 0;  // Positions are stored gap coded for each document and we need to decode the gaps on the fly.
        for (k = 0; k < num_positions_top; ++k) {
          positions_top_actual += positions_top[k];

          positions_bottom_actual = 0;  // Positions are stored gap coded for each document and we need to decode the gaps on the fly.
          for (l = 0; l < num_positions_bottom; ++l) {
            positions_bottom_actual += positions_bottom[l];

            dist = positions_top_actual - positions_bottom_actual;
            assert(dist != 0);  // This is an indication of a bug in the program.

            ids = 1.0 / (dist * dist);

            acc_d_t[i] += idf_t[i] * ids;
            acc_d_t[j] += idf_t[j] * ids;
          }
        }
      }

      // Include the normalized proximity score.
      result_position_tuples[r].score += min(1.0f, idf_t[i]) * (acc_d_t[i] * kBm25NumeratorMul) / (acc_d_t[i] + kBm25DenominatorAdd + kBm25DenominatorDocLenMul * result_position_tuples[r].doc_len);
    }
  }

  // This just iterates through all the positions for each list of each result.
  /*for (i = 0; i < kNumReturnedResults; ++i) {
    cout << "docID: " << result_position_tuples[i].doc_id << endl;
    for (j = 0; j < kNumLists; ++j) {
      cout << "Positions for list: " << j << endl;
      const uint32_t* positions = &result_position_tuples[i].positions[j * kResultPositionStride];
      uint32_t num_positions = positions[0];
      ++positions;
      for (k = 0; k < num_positions; ++k) {
        cout << positions[k] << endl;
      }
    }
  }*/

  // Sort top results in descending order by document score.
  sort(result_position_tuples, result_position_tuples + kNumReturnedResults, ResultPositionTuple());

  // Copy the top scoring documents into the final result set.
  const int kNumFinalResultSet = min(kMaxNumResults, kNumReturnedResults);
  for (i = 0; i < kNumFinalResultSet; ++i) {
    results[i].first = result_position_tuples[i].score;
    results[i].second = result_position_tuples[i].doc_id;
  }

  delete[] result_position_tuples;
  delete[] position_pool;

  return total_num_results;
}

// In case of AND queries, we only count queries for which all terms are in the lexicon as part of the number of queries executed and the total elapsed querying
// time. A query that contains terms which are not in the lexicon will just terminate with 0 results and 0 running time, so we ignore these for our benchmarking
// purposes.
void QueryProcessor::ExecuteQuery(string query_line, int qid) {
  // All the words in the lexicon are lower case, so queries must be too, convert them to lower case.
  for (size_t i = 0; i < query_line.size(); i++) {
    if (isupper(query_line[i]))
      query_line[i] = tolower(query_line[i]);

    // We need to remove punctuation from the queries, since we only index alphanumeric characters and anything separated by a non-alphanumeric
    // character is considered a token separator by our parser. Not removing punctuation will result in the token not being found in the lexicon.
    int int_val = query_line[i];
    if (!((int_val >= 48 && int_val < 58) || (int_val >= 65 && int_val < 91) || (int_val >= 97 && int_val < 123) || (int_val == 32))) {
      query_line[i] = ' ';  // Replace it with a space.
    }
  }

  if (query_mode_ == kBatch) {
    if (!silent_mode_)
      cout << "\nSearch: " << query_line << endl;
  }

  istringstream qss(query_line);
  vector<string> words;
  string term;
  while (qss >> term) {
    // Apply query time word stop list.
    // Remove words that appear in our stop list.
    if (!stop_words_.empty()) {
      if (stop_words_.find(term) == stop_words_.end()) {
        words.push_back(term);
      }
    } else {
      words.push_back(term);
    }
  }

  if (words.size() == 0) {
    if (!silent_mode_)
      cout << "Please enter a query.\n" << endl;
    return;
  }

  // Remove duplicate words, since there is no point in traversing lists for the same word multiple times.
  sort(words.begin(), words.end());
  words.erase(unique(words.begin(), words.end()), words.end());

  int num_query_terms = words.size();
  LexiconData* query_term_data[num_query_terms];  // Using a variable length array here.

  // For AND semantics, all query terms must exist in the lexicon for query processing to proceed.
  // For OR semantics, any of the query terms can be in the lexicon.
  enum ProcessingSemantics {
    kAnd, kOr, kUndefined
  };
  ProcessingSemantics processing_semantics;
  switch (query_algorithm_) {
    case kDaatAnd:
    case kDaatAndTopPositions:
    case kDualLayeredOverlappingDaat:
    case kDualLayeredOverlappingMergeDaat:
      processing_semantics = kAnd;
      break;
    case kDaatOr:
    case kMultiLayeredDaatOr:
    case kLayeredTaatOrEarlyTerminated:
    case kWand:
    case kDualLayeredWand:
    case kMaxScore:
    case kDualLayeredMaxScore:
      processing_semantics = kOr;
      break;
    default:
      processing_semantics = kUndefined;
      assert(false);
  }

  if (result_format_ == kCompare) {
    // Print the query.
    for (int i = 0; i < num_query_terms; ++i) {
      cout << words[i] << ((i != num_query_terms - 1) ? ' ' : '\n');
    }
  }

  int curr_query_term_num = 0;
  for (int i = 0; i < num_query_terms; ++i) {
    LexiconData* lex_data = index_reader_.lexicon().GetEntry(words[i].c_str(), words[i].length());
    if (lex_data != NULL)
      query_term_data[curr_query_term_num++] = lex_data;
  }

  if (processing_semantics == kOr) {
    num_query_terms = curr_query_term_num;
  }

  int results_size;
  int total_num_results;
  double query_elapsed_time;

  if (curr_query_term_num == num_query_terms) {
    results_size = max_num_results_;

    // These results are ranked from highest BM25 score to lowest.
    Result ranked_results[max_num_results_];  // Using a variable length array here.

    Timer query_time;  // Time how long it takes to answer a query.
    switch (query_algorithm_) {
      case kDaatAnd:
      case kDaatOr:
      case kDaatAndTopPositions:
        total_num_results = ProcessQuery(query_term_data, num_query_terms, ranked_results, &results_size);
        break;
      case kDualLayeredOverlappingDaat:
      case kDualLayeredOverlappingMergeDaat:
        total_num_results = ProcessLayeredQuery(query_term_data, num_query_terms, ranked_results, &results_size);
        break;
      case kMultiLayeredDaatOr:
        total_num_results = ProcessMultiLayeredDaatOrQuery(query_term_data, num_query_terms, ranked_results, &results_size);
        break;
      case kLayeredTaatOrEarlyTerminated:
        total_num_results = ProcessLayeredTaatPrunedEarlyTerminatedQuery(query_term_data, num_query_terms, ranked_results, &results_size);
        break;
      case kWand:
        total_num_results = MergeListsWand(query_term_data, num_query_terms, ranked_results, &results_size, false);
        break;
      case kDualLayeredWand:
        total_num_results = MergeListsWand(query_term_data, num_query_terms, ranked_results, &results_size, true);
        break;
      case kMaxScore:
        total_num_results = MergeListsMaxScore(query_term_data, num_query_terms, ranked_results, &results_size, false);
        break;
      case kDualLayeredMaxScore:
        total_num_results = MergeListsMaxScore(query_term_data, num_query_terms, ranked_results, &results_size, true);
        break;
      default:
        total_num_results = 0;
        assert(false);
    }
    query_elapsed_time = query_time.GetElapsedTime();

    if (!warm_up_mode_) {
      total_querying_time_ += query_elapsed_time;
      ++total_num_queries_;
    }

    cout.setf(ios::fixed, ios::floatfield);
    cout.setf(ios::showpoint);

    if (result_format_ == kCompare) {
      cout << "num results: " << results_size << endl;
    }

    for (int i = 0; i < results_size; ++i) {
      switch (result_format_) {
        case kNormal:
          if (!silent_mode_)
            cout << setprecision(2) << setw(2) << "Score: " << ranked_results[i].first << "\tDocID: " << ranked_results[i].second << "\tURL: "
                << index_reader_.document_map().GetDocumentUrl(ranked_results[i].second) << setprecision(6) << "\n";
          break;
        case kTrec:
          cout << qid << '\t' << "Q0" << '\t' << index_reader_.document_map().GetDocumentNumber(ranked_results[i].second) << '\t' << i << '\t'
              << ranked_results[i].first << '\t' << "PolyIRTK" << "\n";
          break;
        case kCompare:
          cout << setprecision(2) << setw(2) << ranked_results[i].first << "\t" << ranked_results[i].second << setprecision(6) << "\n";
          break;
        case kDiscard:
          break;
        default:
          assert(false);
      }
    }
  } else {
    // One of the query terms did not exist in the lexicon.
    results_size = 0;
    total_num_results = 0;
    query_elapsed_time = 0;
  }

  if (result_format_ == kNormal)
    if (!silent_mode_)
      cout << "\nShowing " << results_size << " results out of " << total_num_results << ". (" << setprecision(1) << (query_elapsed_time * 1000)
          << setprecision(6) << " ms)\n";
}

void QueryProcessor::RunBatchQueries(const string& input_source, bool warmup, int num_timed_runs) {
  ifstream batch_query_file_stream;
  if (!(input_source.empty() || input_source == "stdin" || input_source == "cin")) {
    batch_query_file_stream.open(input_source.c_str());
    if (!batch_query_file_stream) {
      GetErrorLogger().Log("Could not open batch query file '" + input_source + "'.", true);
    }
  }

  istream& is = batch_query_file_stream.is_open() ? batch_query_file_stream : cin;

  vector<pair<int, string> > queries;
  string query_line;
  while (getline(is, query_line)) {
    size_t colon_pos = query_line.find(':');
    if (colon_pos != string::npos && colon_pos < (query_line.size() - 1)) {
      queries.push_back(make_pair(atoi(query_line.substr(0, colon_pos).c_str()), query_line.substr(colon_pos + 1)));
    } else {
      queries.push_back(make_pair(0, query_line));
    }
  }

  if (warmup) {
    warm_up_mode_ = true;
    for (int i = 0; i < static_cast<int> (queries.size()); ++i) {
#ifdef IRTK_DEBUG
      cout << queries[i].first << ":" << queries[i].second << endl;
#endif
      ExecuteQuery(queries[i].second, queries[i].first);
    }

    index_reader_.ResetStats();
  }

  warm_up_mode_ = false;
  while (num_timed_runs-- > 0) {
    for (int i = 0; i < static_cast<int> (queries.size()); ++i) {
#ifdef IRTK_DEBUG
      cout << queries[i].first << ":" << queries[i].second << endl;
#endif
      ExecuteQuery(queries[i].second, queries[i].first);
    }
  }
}

void QueryProcessor::LoadIndexProperties() {
  collection_total_num_docs_ = atol(index_reader_.meta_info().GetValue(meta_properties::kTotalNumDocs).c_str());
  if (collection_total_num_docs_ <= 0) {
    GetErrorLogger().Log("The '" + string(meta_properties::kTotalNumDocs) + "' value in the loaded index meta file seems to be incorrect.", false);
  }

  uint64_t collection_total_document_lengths = atol(index_reader_.meta_info().GetValue(meta_properties::kTotalDocumentLengths).c_str());
  if (collection_total_document_lengths <= 0) {
    GetErrorLogger().Log("The '" + string(meta_properties::kTotalDocumentLengths) + "' value in the loaded index meta file seems to be incorrect.", false);
  }

  if (collection_total_num_docs_ <= 0 || collection_total_document_lengths <= 0) {
    collection_average_doc_len_ = 1;
  } else {
    collection_average_doc_len_ = collection_total_document_lengths / collection_total_num_docs_;
  }

  if (!index_reader_.includes_positions()) {
    use_positions_ = false;
  }

  // Determine whether this index is layered and whether the index layers are overlapping.
  // From this info, we can determine the query processing mode.
  KeyValueStore::KeyValueResult<long int> layered_index_res = index_reader_.meta_info().GetNumericalValue(meta_properties::kLayeredIndex);
  KeyValueStore::KeyValueResult<long int> overlapping_layers_res = index_reader_.meta_info().GetNumericalValue(meta_properties::kOverlappingLayers);
  KeyValueStore::KeyValueResult<long int> num_layers_res = index_reader_.meta_info().GetNumericalValue(meta_properties::kNumLayers);

  // TODO:
  // If there are errors reading the values for these keys (most likely missing value), we assume they're false
  // (because that would require updating the index meta file generation in some places, which should be done eventually).
  index_layered_ = layered_index_res.error() ? false : layered_index_res.value_t();
  index_overlapping_layers_ = overlapping_layers_res.error() ? false : overlapping_layers_res.value_t();
  index_num_layers_ = num_layers_res.error() ? 1 : num_layers_res.value_t();

  bool inappropriate_algorithm = false;
  switch (query_algorithm_) {
    case kDefault:  // Choose a conservative algorithm based on the index properties.
      // Note that for a layered index with overlapping layers, we can do non-layered processing
      // by just opening the last layer from each list (which contains all the docIDs in the entire list).
      if (!index_layered_ || index_overlapping_layers_) {
        query_algorithm_ = kDaatAnd;  // TODO: Default should probably be an OR mode algorithm.
        break;
      }
      if (index_layered_ && !index_overlapping_layers_) {
        query_algorithm_ = kLayeredTaatOrEarlyTerminated;
        break;
      }
      break;
    case kDaatAnd:
    case kDaatOr:
    case kWand:  // TODO: For WAND, only need a single layered index, but need term upperbounds, which is not yet supported.
    case kDualLayeredWand:
    case kMaxScore:  // TODO: For MaxScore, only need a single layered index, but need term upperbounds, which is not yet supported.
    case kDualLayeredMaxScore:
    case kDaatAndTopPositions:
      if (index_layered_ && !index_overlapping_layers_) {
        inappropriate_algorithm = true;
      }
      break;
    case kDualLayeredOverlappingDaat:
    case kDualLayeredOverlappingMergeDaat:
      if (!index_layered_ || !index_overlapping_layers_ || index_num_layers_ != 2) {
        inappropriate_algorithm = true;
      }
      break;
    case kLayeredTaatOrEarlyTerminated:
    case kMultiLayeredDaatOr:
      if (!index_layered_ || index_overlapping_layers_) {
        inappropriate_algorithm = true;
      }
      break;
    case kTaatOr:
      GetErrorLogger().Log("The selected query algorithm is not yet supported.", true);
      break;
    default:
      assert(false);
  }

  if (inappropriate_algorithm) {
    GetErrorLogger().Log("The selected query algorithm is not appropriate for this index type.", true);
  }
}

void QueryProcessor::PrintQueryingParameters() {
  cout << "collection_total_num_docs_: " << collection_total_num_docs_ << endl;
  cout << "collection_average_doc_len_: " << collection_average_doc_len_ << endl;
  cout << "Using positions: " << use_positions_ << endl;
  cout << endl;
}

CacheManager* QueryProcessor::GetCacheManager(const char* index_filename) const {
  bool memory_mapped_index = IndexConfiguration::GetResultValue(Configuration::GetConfiguration().GetBooleanValue(config_properties::kMemoryMappedIndex), false);
  bool memory_resident_index = IndexConfiguration::GetResultValue(Configuration::GetConfiguration().GetBooleanValue(config_properties::kMemoryResidentIndex), false);

  if (memory_mapped_index) {
    return new MemoryMappedCachePolicy(index_filename);
  } else if (memory_resident_index) {
    return new FullContiguousCachePolicy(index_filename);
  } else {
    return new LruCachePolicy(index_filename);
  }
}

const ExternalIndexReader* QueryProcessor::GetExternalIndexReader(QueryAlgorithm query_algorithm, const char* external_index_filename) const {
  switch (query_algorithm) {
    case kMaxScore:
    case kDualLayeredMaxScore:
      return new ExternalIndexReader(external_index_filename);
    default:
      return NULL;
  }
}
