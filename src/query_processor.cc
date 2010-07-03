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
// TODO: Term proximity (positions) calculation takes too long, especially when query contains 2 or more words with lengthy lists.
// TODO: Support OR mode querying.
// TODO: Check BM25 score calculation. If BM25 scores are negative, it should be sorted from lowest to highest.
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

#include "config_file_properties.h"
#include "configuration.h"
#include "globals.h"
#include "logger.h"
#include "meta_file_properties.h"
#include "timer.h"
using namespace std;

/**************************************************************************************************************************************************************
 * QueryProcessor
 *
 **************************************************************************************************************************************************************/
QueryProcessor::QueryProcessor(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename,
                               QueryFormat query_format) :
      kMaxNumberResults(atol(Configuration::GetConfiguration().GetValue(config_properties::kMaxNumberResults).c_str())),
      result_format_(kNormal), query_format_(query_format),
      cache_policy_(((Configuration::GetConfiguration().GetValue(config_properties::kMemoryResidentIndex) == "true") ? static_cast<CacheManager*>(new FullContiguousCachePolicy(index_filename))
                                                                                                                     : static_cast<CacheManager*>(new LruCachePolicy(index_filename)))),
      index_reader_(IndexReader::kRandomQuery, IndexReader::kSortedGapCoded, *cache_policy_, lexicon_filename, doc_map_filename, meta_info_filename),
      collection_average_doc_len_(0), collection_total_num_docs_(0), use_positions_(false), silent_mode_(false), warm_up_mode_(false), total_querying_time_(0),
      total_num_queries_(0), kInitialQuerySize(32), query_size_(kInitialQuerySize), f_d_t_(new uint32_t[query_size_]) {
  if (kMaxNumberResults <= 0) {
    GetErrorLogger().Log("Incorrect configuration value for '" + string(config_properties::kMaxNumberResults) + "'", true);
  }

  LoadIndexProperties();
  PrintQueryingParameters();

  switch (query_format_) {
    case kInteractive:
    case kInteractiveSingle:
      AcceptQuery();
      break;
    case kBatch:
      RunBatchQueries(cin);
      break;
    default:
      assert(false);
      break;
  }

  // Output some querying statistics.
  double total_cached_bytes_read = index_reader_.total_cached_bytes_read();
  double total_disk_bytes_read = index_reader_.total_disk_bytes_read();
  double total_num_queries_issued = total_num_queries_;

  cout << "Number of queries executed: " << total_num_queries_issued << endl;
  cout << "Total querying time: " << total_querying_time_ << " seconds\n";

  cout << "\n";
  cout << "Per Query Statistics:\n";
  cout << "  Average data read from cache: " << (total_cached_bytes_read / total_num_queries_issued / (1 << 20)) << " MiB\n";
  cout << "  Average data read from disk: " << (total_disk_bytes_read / total_num_queries_issued / (1 << 20)) << " MiB\n";

  cout << "  Average query running time (latency): " << (total_querying_time_ / total_num_queries_issued * (1000)) << " ms\n";
}

QueryProcessor::~QueryProcessor() {
  delete cache_policy_;
}

void QueryProcessor::AcceptQuery() {
  while (true) {
    cout << "Search: ";
    string queryLine;
    getline(cin, queryLine);

    if (cin.eof())
      break;

    ExecuteQuery(queryLine, 0);

    if (query_format_ != kInteractive)
      break;
  }
}

// TODO: Improvement would be to assume a max query length of some sort, and stop making dynamic allocations based on query length
// (or use variable length arrays).
// TODO: Calculate stuff faster: http://graphics.stanford.edu/~seander/bithacks.html (especially logarithms).
int QueryProcessor::ProcessQuery(vector<string>& words, Results& results) {
  int total_num_results = 0;

  // Remove duplicate words, since there is no point in traversing lists for the same word multiple times.
  sort(words.begin(), words.end());
  words.erase(unique(words.begin(), words.end()), words.end());

  vector<LexiconData*> q;  // TODO: Dynamic array is not a good choice for performance reasons.

  // Make sure all the query terms exist in the lexicon, otherwise things go crazy.
  // If a term isn't in the lexicon, then it means we have no matching results, since we assume AND semantics.
  for (size_t i = 0; i < words.size(); ++i) {
    LexiconData* lex_data = index_reader_.lexicon().GetEntry(words[i].c_str(), words[i].length());
    if (lex_data != NULL)
      q.push_back(lex_data);
    else
      return total_num_results;
  }

  // Query terms must be arranged in order from shortest list to longest list.
  sort(q.begin(), q.end(), ListCompare());

  ListData** lp = new ListData*[q.size()];
  for (size_t i = 0; i < q.size(); ++i) {
    lp[i] = index_reader_.OpenList(*q[i]);
  }

  uint32_t* f_d_t = new uint32_t[q.size()];

  float min_doc_score = numeric_limits<float>::max();  // TODO: For using an array instead of a heap.

  uint32_t did = 0;
  while (did < numeric_limits<uint32_t>::max()) {
    // Get next element from shortest list.
    did = index_reader_.NextGEQ(lp[0], did);
    if (did == numeric_limits<uint32_t>::max())
      break;

    uint32_t d = did;
    // Try to find entries with same doc id in other lists.
    for (size_t i = 1; (i < q.size()) && ((d = index_reader_.NextGEQ(lp[i], did)) == did); ++i) {
      continue;
    }

    if (d > did) {
      // Not in intersection.
      did = d;
    } else {
      assert(d == did);

      // Doc id is in intersection, now get all frequencies.
      for (size_t i = 0; i < q.size(); ++i) {
        f_d_t[i] = index_reader_.GetFreq(lp[i], did);  // This is the frequency that a term appeared in the document.
      }

/*      // Compute contribution from proximity info.
      float* acc_t = new float[q.size()];
      for (size_t i = 0; i < q.size(); ++i) {
        acc_t[i] = 0.0;
      }

      if (use_positions_) {
        for (size_t i = 0; i < q.size(); ++i) {
          ChunkDecoder* decoded_chunk_top = lp[i]->curr_block_decoder()->curr_chunk_decoder();
          const uint32_t* positions_top = decoded_chunk_top->current_positions();

          for (size_t j = i + 1; j < q.size(); ++j) {
            ChunkDecoder* decoded_chunk_bottom = lp[j]->curr_block_decoder()->curr_chunk_decoder();
            const uint32_t* positions_bottom = decoded_chunk_bottom->current_positions();

            uint32_t positions_top_actual = 0; // Positions are stored gap coded for each document and we need to decode the gaps on the fly.
            for (uint32_t k = 0; k < decoded_chunk_top->current_frequency(); ++k) {
              positions_top_actual += positions_top[k];

              uint32_t positions_bottom_actual = 0; // Positions are stored gap coded for each document and we need to decode the gaps on the fly.
              for (size_t l = 0; l < decoded_chunk_bottom->current_frequency(); ++l) {
                positions_bottom_actual += positions_bottom[l];

                int dist = positions_top_actual - positions_bottom_actual;
                assert(dist != 0); // This is an indication of a bug in the program.

                float ids = 1.0 / (dist * dist);

                int f_t_i = q[i]->num_docs();
                int f_t_j = q[j]->num_docs();

                acc_t[i] += log10((collection_total_num_docs_ - f_t_i + 0.5) / (f_t_i + 0.5)) * ids;
                acc_t[j] += log10((collection_total_num_docs_ - f_t_j + 0.5) / (f_t_j + 0.5)) * ids;
              }
            }
          }
        }
      }
*/
      // Compute BM25 score from frequencies.
      float bm_25_sum = 0;

      for (size_t i = 0; i < q.size(); ++i) {
        int f_t = q[i]->num_docs();
        float K = 1.2 * ((1 - 0.75) + 0.75 * (index_reader_.GetDocLen(did) / collection_average_doc_len_));

        float w_t = log10((collection_total_num_docs_ - f_t + 0.5) / (f_t + 0.5));
        bm_25_sum += w_t * (((1.2 + 1) * f_d_t[i]) / (K + f_d_t[i]));

        // Add the proximity stuff.
//        bm_25_sum += min(1.0f, w_t) * (acc_t[i] * (1.2 + 1) / (acc_t[i] + K));
      }

//      bm_25_sum = 0; // TODO: This tests what contribution to the running time the heap makes...as this will cause most stuff to skip the heap.

      // TODO: Try using an array instead of a heap. Have to search it, but good cache performance.
      if (total_num_results < kMaxNumberResults) {
        results[total_num_results] = make_pair(bm_25_sum, did);
        if (bm_25_sum < min_doc_score) {
          min_doc_score = bm_25_sum;
        }
      } else if (bm_25_sum > min_doc_score) {
        bool replaced = false;
        bool update_min_score = true;
        float curr_min_score = numeric_limits<float>::max();

        for (int i = 0; i < kMaxNumberResults; ++i) {
          if (!replaced && bm_25_sum < results[i].first) {
            results[i] = make_pair(bm_25_sum, did);
            update_min_score = false;  // We haven't changed the min score.
            break;
          }

          if(!replaced && min_doc_score == results[i].first) {
            results[i] = make_pair(bm_25_sum, did);
            replaced = true;
          }

          if(results[i].first < curr_min_score) {
            curr_min_score = results[i].first;
          }
        }

        if(update_min_score) {
          min_doc_score = curr_min_score;
        }
      }

      // TODO: Only insert if the score is greater than the minimum element in the heap.
      // TODO: Should be OR instead of AND.
//      if (results.size() < static_cast<size_t> (kMaxNumberResults) || bm_25_sum > results.top().first) {
//        results.push(make_pair(bm_25_sum, did));
//        if (results.size() > static_cast<size_t> (kMaxNumberResults)) {
//          results.pop();
//        }
//      }

      ++total_num_results;
      ++did;  // Increase did to search for next doc id.
    }
  }

  delete[] f_d_t;
//  delete[] acc_t;

  for (size_t i = 0; i < q.size(); ++i) {
    index_reader_.CloseList(lp[i]);
  }
  delete[] lp;
  return total_num_results;
}

void QueryProcessor::ExecuteQuery(string query_line, int qid) {
  // All the words in the lexicon are lower case, so queries must be too, convert them to lower case.
  for (size_t i = 0; i < query_line.size(); i++) {
    if (isupper(query_line[i]))
      query_line[i] = tolower(query_line[i]);
  }

  if(query_format_ == kBatch) {
    if(!silent_mode_)
      cout << "\nSearch: " << query_line << endl;
  }

  istringstream qss(query_line);
  vector<string> words;
  string term;
  while (qss >> term) {
    words.push_back(term);
  }

  if (words.size() == 0) {
    if(!silent_mode_)
      cout << "Please enter a query.\n" << endl;
    return;
  }

  Timer query_time;  // Time how long it takes to answer a query.

  // These results are ranked from lowest BM25 score to highest.
//  Results ranked_results;
  Results ranked_results = new Result[kMaxNumberResults];
  int total_num_results = ProcessQuery(words, ranked_results);

  double query_elapsed_time = query_time.GetElapsedTime();

  if(!warm_up_mode_) {
    total_querying_time_ += query_elapsed_time;
    ++total_num_queries_;
  }

  // TODO: For the array based method:
  size_t results_size = kMaxNumberResults;
  Result* top_results = ranked_results;


  // TODO: For the heap based method.
//  size_t results_size = ranked_results.size();
//  Result* top_results = new Result[results_size];

//  for (size_t i = 0; i < results_size; ++i) {
//    top_results[results_size - i - 1] = ranked_results.top();
//    ranked_results.pop();
//  }

  for (size_t i = 0; i < results_size; ++i) {
    switch (result_format_) {
      case kNormal:
        if(!silent_mode_)
          cout << setprecision(4) << setw(4) << "Score: " << top_results[i].first << "\tDocID: " << top_results[i].second << "\tURL: "
              << index_reader_.GetDocUrl(top_results[i].second) << "\n";
        break;
      case kTrec:
        if(!silent_mode_)
          cout << qid << '\t' << "Q0" << '\t' << index_reader_.GetDocUrl(top_results[i].second) << '\t' << i << '\t' << top_results[i].first << '\t' << "STANDARD" << "\n";
        break;
      default:
        assert(false);
    }
  }
  delete[] top_results;

  if (result_format_ == kNormal)
    if(!silent_mode_)
      cout << "\nShowing " << results_size << " results out of " << total_num_results << ". (" << query_elapsed_time << " seconds)\n";
}

void QueryProcessor::RunBatchQueries(istream& is) {
  // The percentage of total batch queries to use to generate statistics.
  // The rest of the queries will be used to warm up the cache.
  const float kPercentageTestQueries = 0.01f;

  vector<string> queries;
  string query_line;
  while (getline(is, query_line)) {
    size_t colon_pos = query_line.find(':');
    if (colon_pos != string::npos && colon_pos < (query_line.size() - 1)) {
      queries.push_back(query_line.substr(colon_pos + 1));
    } else {
      queries.push_back(query_line);
    }
  }

  random_shuffle(queries.begin(), queries.end());

  int num_test_queries = ceil(kPercentageTestQueries * queries.size());
  int num_warm_up_queries = queries.size() - num_test_queries;

  silent_mode_ = true;
  warm_up_mode_ = true;
  for (int i = 0; i < num_warm_up_queries; ++i) {
    ExecuteQuery(queries[i], 0);
  }

  index_reader_.ResetStats();

  silent_mode_ = false;
  warm_up_mode_ = false;
  for (int i = num_warm_up_queries; i < static_cast<int> (queries.size()); ++i) {
    ExecuteQuery(queries[i], 0);
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

  // Default is not to use positions.
  if (index_reader_.includes_positions()) {
    string use_positions = Configuration::GetConfiguration().GetValue(config_properties::kUsePositions);
    if (use_positions == "true") {
      use_positions_ = true;
    }
  }
}

void QueryProcessor::PrintQueryingParameters() {
  cout << "collection_total_num_docs_: " << collection_total_num_docs_ << endl;
  cout << "collection_average_doc_len_: " << collection_average_doc_len_ << endl;
  cout << "Using positions: " << use_positions_ << endl;
  cout << endl;
}
