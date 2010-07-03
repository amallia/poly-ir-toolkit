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

#include "index_cat.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "index_reader.h"
#include "cache_manager.h"
#include "index_util.h"
using namespace std;

/**************************************************************************************************************************************************************
 * IndexCat
 *
 **************************************************************************************************************************************************************/
IndexCat::IndexCat(const IndexFiles& index_files) :
  index_(NULL), includes_contexts_(true), includes_positions_(true) {
  CacheManager* cache_policy = new MergingCachePolicy(index_files.index_filename().c_str());
  IndexReader* index_reader = new IndexReader(IndexReader::kMerge, IndexReader::kSortedGapCoded, *cache_policy, index_files.lexicon_filename().c_str(),
                                              index_files.document_map_filename().c_str(), index_files.meta_info_filename().c_str());

  if (!index_reader->includes_contexts())
    includes_contexts_ = false;
  if (!index_reader->includes_positions())
    includes_positions_ = false;

  index_ = new Index(cache_policy, index_reader);
}

// If 'term' is NULL and 'term_len' is 0, it will go through and output all the lists in the index into a readable format.
// If 'term' is not NULL and 'term_len' is greater than 0, it will find the corresponding list and output this list into a readable format.
void IndexCat::Cat(const char* term, int term_len) {
  assert((term == NULL && term_len == 0) || (term != NULL && term_len > 0));

  while (index_->NextTerm()) {
    if ((term == NULL && term_len == 0) || (index_->curr_term_len() == term_len && strncmp(index_->curr_term(), term, min(index_->curr_term_len(), term_len))
        == 0)) {
      for (int i = 0; i < index_->curr_term_len(); ++i) {
        printf("%c", index_->curr_term()[i]);
      }
      printf(":\n");

      while (index_->NextDocId()) {
        uint32_t curr_frequency = index_->index_reader()->GetFreq(index_->curr_list_data(), index_->curr_doc_id());
        printf("(%u, %u, <", index_->curr_doc_id(), curr_frequency);

        if (includes_positions_) {
          const uint32_t* curr_positions = index_->curr_list_data()->curr_block_decoder()->curr_chunk_decoder()->current_positions();
          for (size_t i = 0; i < curr_frequency; ++i) {
            printf("%u", curr_positions[i]);
            if (i != (curr_frequency - 1))
              printf(", ");
          }
        }

        printf(">)\n");
      }

      printf("\n");
    }
  }
}

IndexCat::~IndexCat() {
  delete index_;
}
