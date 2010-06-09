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
// The query processor. Implements block caching, skipping over chunks, decoding frequencies and positions on demand.
//==============================================================================================================================================================

#ifndef QUERY_PROCESSOR_H_
#define QUERY_PROCESSOR_H_

#include <cassert>
#include <stdint.h>

#include <iostream>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "cache_manager.h"
#include "index_reader.h"

/**************************************************************************************************************************************************************
 * QueryProcessor
 *
 * Responsible for query processing. Contains useful methods for traversing inverted lists.
 **************************************************************************************************************************************************************/
class QueryProcessor {
public:
  typedef std::pair<float, unsigned int> Result;
  typedef std::priority_queue<Result, std::vector<Result>, std::greater<Result> > Results;

  enum ResultFormat {
    kTrec, kNormal
  };
  enum QueryFormat {
    kInteractive, kInteractiveSingle, kBatch
  };
  enum DocumentOrder {
    kSorted, kSortedGapCoded
  };

  QueryProcessor(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename,
                 QueryFormat query_format);

  void AcceptQuery();
  int ProcessQuery(std::vector<std::string>& words, Results& results);
  void ExecuteQuery(std::string query_line, int qid);
  void RunBatchQueries(std::istream& is);
  void LoadIndexProperties();
  void PrintQueryingParameters();

private:
  // The max number of results to display.
  const int kMaxNumberResults;

  // The result format we'll be using for the output.
  ResultFormat result_format_;
  // The way we'll be accepting queries.
  QueryFormat query_format_;

  LruCachePolicy cache_policy_;
  IndexReader index_reader_;

  // The average document length of a document in the indexed collection.
  // This plays a role in the ranking function.
  uint32_t collection_average_doc_len_;
  // The total number of documents in the indexed collection.
  uint32_t collection_total_num_docs_;

  // Whether positions will be utilized during ranking (requires index built with positions).
  bool use_positions_;
};

#endif /* QUERY_PROCESSOR_H_ */
