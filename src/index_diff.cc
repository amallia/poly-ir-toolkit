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

#include "index_diff.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "index_reader.h"
#include "cache_manager.h"
#include "index_util.h"
using namespace std;

/**************************************************************************************************************************************************************
 * IndexDiff
 *
 **************************************************************************************************************************************************************/
IndexDiff::IndexDiff(const IndexFiles& index_files1, const IndexFiles& index_files2) :
  index1_(NULL), index2_(NULL) {
  CacheManager* cache_policy1 = new MergingCachePolicy(index_files1.index_filename().c_str());
  IndexReader* index_reader1 = new IndexReader(IndexReader::kMerge, IndexReader::kSortedGapCoded, *cache_policy1, index_files1.lexicon_filename().c_str(),
                                               index_files1.document_map_filename().c_str(), index_files1.meta_info_filename().c_str());

  CacheManager* cache_policy2 = new MergingCachePolicy(index_files2.index_filename().c_str());
  IndexReader* index_reader2 = new IndexReader(IndexReader::kMerge, IndexReader::kSortedGapCoded, *cache_policy2, index_files2.lexicon_filename().c_str(),
                                               index_files2.document_map_filename().c_str(), index_files2.meta_info_filename().c_str());

  index1_ = new Index(cache_policy1, index_reader1);
  index2_ = new Index(cache_policy2, index_reader2);

  // Seed the indices.
  index1_->Next();
  index2_->Next();
}

// If 'term' is NULL and 'term_len' is 0, it will go through and output all the lists in the index into a readable format.
// If 'term' is not NULL and 'term_len' is greater than 0, it will find the corresponding list and output this list into a readable format.
void IndexDiff::Diff(const char* term, int term_len) {
  assert((term == NULL && term_len == 0) || (term != NULL && term_len > 0));

  bool index1_no_more = false;
  bool index2_no_more = false;

  while (true) {
    bool index1_greater = IndexComparison().operator()(index1_, index2_);
    bool index2_greater = IndexComparison().operator()(index2_, index1_);

    // Several cases to consider:
    // 'index1_greater' == 'index2_greater' == true: Their current postings are equal.
    // 'index1_greater' == 'index2_greater' == false: Impossible.
    // 'index1_greater' == true, 'index2_greater' == false: Posting in 'index1_' is greater.
    // 'index1_greater' == false, 'index2_greater' == true: Posting in 'index2_' is greater.

    if (index1_greater == index2_greater) {
      assert(index1_greater == true && index2_greater == true);

      // The terms for both index1 and index2 should be the same.
      assert(index1_->curr_term_len() == index2_->curr_term_len() && strncmp(index1_->curr_term(), index2_->curr_term(), index1_->curr_term_len()) == 0);
      if ((term == NULL && term_len == 0) || (index1_->curr_term_len() == term_len && strncmp(index1_->curr_term(), term, term_len) == 0)) {
        // Check the frequencies and positions for any differences.
        uint32_t curr_frequency1 = index1_->index_reader()->GetFreq(index1_->curr_list_data(), index1_->curr_doc_id());
        const uint32_t* curr_positions1 = index1_->curr_list_data()->curr_block()->GetCurrChunk()->GetCurrentPositions();

        uint32_t curr_frequency2 = index2_->index_reader()->GetFreq(index2_->curr_list_data(), index2_->curr_doc_id());
        const uint32_t* curr_positions2 = index2_->curr_list_data()->curr_block()->GetCurrChunk()->GetCurrentPositions();

        if (curr_frequency1 != curr_frequency2) {
          printf("Frequencies differ: index1: %u, index2: %u (Postings from index1 and index2 shown below)\n", curr_frequency1, curr_frequency2);
          Print(index1_, term, term_len);
          Print(index2_, term, term_len);
          printf("\n");
        }

        // This is similar to doing a merge on the positions, since they are in sorted order.
        size_t i1 = 0;
        size_t i2 = 0;
        while (i1 < curr_frequency1 && i2 < curr_frequency2) {
          if (curr_positions1[i1] == curr_positions2[i2]) {
            ++i1;
            ++i2;
          } else {
            printf("(%d, '", WhichIndex(((curr_positions1[i1] < curr_positions2[i2]) ? index1_ : index2_)));
            for (int i = 0; i < index1_->curr_term_len(); ++i) {
              printf("%c", index1_->curr_term()[i]);
            }
            printf("', ");

            if (curr_positions1[i1] < curr_positions2[i2]) {
              // Index2 is missing this position from index1.
              printf("%u, {%u})\n", index1_->curr_doc_id(), curr_positions1[i1]);
              ++i1;
            } else if (curr_positions2[i2] < curr_positions1[i1]) {
              // Index1 is missing this position from index2.
              printf("%u, {%u})\n", index2_->curr_doc_id(), curr_positions2[i2]);
              ++i2;
            }
          }
        }

        // Get any remaining positions in index1, which are missing from index2.
        if (i1 < curr_frequency1) {
          while (i1 < curr_frequency1) {
            printf("(%d, '", WhichIndex(index1_));
            for (int i = 0; i < index1_->curr_term_len(); ++i) {
              printf("%c", index1_->curr_term()[i]);
            }
            printf("', ");

            printf("%u, {%u})\n", index1_->curr_doc_id(), curr_positions1[i1]);
            ++i1;
          }
        }

        // Get any remaining positions in index2, which are missing in index1.
        if (i2 < curr_frequency2) {
          while (i2 < curr_frequency2) {
            printf("(%d, '", WhichIndex(index2_));
            for (int i = 0; i < index2_->curr_term_len(); ++i) {
              printf("%c", index2_->curr_term()[i]);
            }
            printf("', ");

            printf("%u, {%u})\n", index2_->curr_doc_id(), curr_positions2[i2]);
            ++i2;
          }
        }
      }

      if (!index1_->Next())
        index1_no_more = true;
      if (!index2_->Next())
        index2_no_more = true;
      if (index1_no_more || index2_no_more)
        break;
    } else {
      // Need to print the smaller of the postings.
      if (index1_greater) {
        Print(index2_, term, term_len);
        if (!index2_->Next()) {
          index2_no_more = true;
          break;
        }
      } else if (index2_greater) {
        Print(index1_, term, term_len);
        if (!index1_->Next()) {
          index1_no_more = true;
          break;
        }
      }
    }
  }

  // At this point, at least one of the indices is exhausted of postings,
  // so we need to output them since the other index does not contain them.
  if (!index1_no_more) {
    do {
      Print(index1_, term, term_len);
    } while (index1_->Next());
  }

  if (!index2_no_more) {
    do {
      Print(index2_, term, term_len);
    } while (index2_->Next());
  }
}

void IndexDiff::Print(Index* index, const char* term, int term_len) {
  if ((term == NULL && term_len == 0) || (index->curr_term_len() == term_len && strncmp(index->curr_term(), term, term_len) == 0)) {
    printf("(%d, '", WhichIndex(index));
    for (int i = 0; i < index->curr_term_len(); ++i) {
      printf("%c", index->curr_term()[i]);
    }
    printf("', ");

    uint32_t curr_frequency = index->index_reader()->GetFreq(index->curr_list_data(), index->curr_doc_id());
    const uint32_t* curr_positions = index->curr_list_data()->curr_block()->GetCurrChunk()->GetCurrentPositions();

    printf("%u, %u, <", index->curr_doc_id(), curr_frequency);

    for (size_t i = 0; i < curr_frequency; ++i) {
      printf("%u", curr_positions[i]);
      if (i != (curr_frequency - 1))
        printf(", ");
    }

    printf(">)\n");
  }
}

int IndexDiff::WhichIndex(Index* index) {
  if (index == index1_)
    return 1;
  else if (index == index2_)
    return 2;
  else
    assert(false);
}

IndexDiff::~IndexDiff() {
  delete index1_;
  delete index2_;
}
