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

#ifndef INDEX_UTIL_H_
#define INDEX_UTIL_H_

#include <cassert>
#include <cstring>
#include <stdint.h>

#include <string>

/**************************************************************************************************************************************************************
 * IndexFiles
 *
 **************************************************************************************************************************************************************/
class IndexFiles {
public:
  IndexFiles();
  IndexFiles(const std::string& prefix);
  IndexFiles(int group_num, int file_num);
  IndexFiles(const std::string& prefix, int group_num, int file_num);

  void UpdateNums(int group_num, int file_num);

  void SetDirectory(const std::string& dir);

  const std::string& prefix() const {
    return prefix_;
  }

  const std::string& index_filename() const {
    return index_filename_;
  }

  const std::string& lexicon_filename() const {
    return lexicon_filename_;
  }

  const std::string& document_map_basic_filename() const {
    return document_map_basic_filename_;
  }

  const std::string& document_map_extended_filename() const {
    return document_map_extended_filename_;
  }

  const std::string& meta_info_filename() const {
    return meta_info_filename_;
  }

  const std::string& external_index_filename() const {
    return external_index_filename_;
  }

private:
  void InitIndexFiles(const std::string& prefix, int group_num, int file_num);

  std::string prefix_;
  std::string index_filename_;
  std::string lexicon_filename_;
  std::string document_map_basic_filename_;
  std::string document_map_extended_filename_;
  std::string meta_info_filename_;
  std::string external_index_filename_;
};

/**************************************************************************************************************************************************************
 * Index
 *
 * Currently, this class assumes that all accesses to the lexicon will be sequential
 * (useful and efficient when doing things such as merging or catting indices).
 **************************************************************************************************************************************************************/
class CacheManager;
class IndexReader;
class ListData;
class LexiconData;

class Index {
public:
  Index(CacheManager* cache_policy, IndexReader* index_reader);
  ~Index();

  bool Next();
  bool NextTerm();
  bool NextDocId();

  IndexReader* index_reader() const {
    return index_reader_;
  }

  ListData* curr_list_data() const {
    return curr_list_data_;
  }

  uint32_t curr_doc_id() const {
    return curr_doc_id_;
  }

  const char* curr_term() const {
    return curr_term_;
  }

  int curr_term_len() const {
    return curr_term_len_;
  }

private:
  CacheManager* cache_policy_;
  IndexReader* index_reader_;

  LexiconData* curr_lex_data_;
  ListData* curr_list_data_;
  uint32_t curr_doc_id_;
  uint32_t next_doc_id_;
  const char* curr_term_;
  int curr_term_len_;
};

/**************************************************************************************************************************************************************
 * IndexComparison
 *
 * Compares two indices based on their current term and current docID being processed.
 * Returns true if 'lhs' >= 'rhs', false otherwise.
 **************************************************************************************************************************************************************/
struct IndexComparison {
  bool operator()(const Index* lhs, const Index* rhs) {
    // Compare the terms based on the minimum number of characters in both.
    int cmp = strncmp(lhs->curr_term(), rhs->curr_term(), std::min(lhs->curr_term_len(), rhs->curr_term_len()));
    if (cmp == 0) {
      // If the terms are the same, the doc id is the deciding factor, otherwise the term length is.
      return ((lhs->curr_term_len() == rhs->curr_term_len()) ? lhs->curr_doc_id() >= rhs->curr_doc_id() : lhs->curr_term_len() > rhs->curr_term_len());
    }
    return (cmp > 0) ? true : false;
  }
};

/**************************************************************************************************************************************************************
 * IndexDocIdComparison
 *
 **************************************************************************************************************************************************************/
struct IndexDocIdComparison {
  bool operator()(const Index* lhs, const Index* rhs) const {
    return lhs->curr_doc_id() >= rhs->curr_doc_id();
  }
};

/**************************************************************************************************************************************************************
 * IndexTermComparison
 *
 **************************************************************************************************************************************************************/
struct IndexTermComparison {
  bool operator()(const Index* lhs, const Index* rhs) {
    int cmp = strncmp(lhs->curr_term(), rhs->curr_term(), std::min(lhs->curr_term_len(), rhs->curr_term_len()));
    if (cmp == 0)
      return lhs->curr_term_len() > rhs->curr_term_len();
    return (cmp > 0) ? true : false;
  }
};

/**************************************************************************************************************************************************************
 * PositionsPool
 *
 **************************************************************************************************************************************************************/
class PositionsPool {
public:
  PositionsPool() :
    size_(0), curr_offset_(0), positions_(NULL) {
  }

  PositionsPool(int size) :
    size_(size), curr_offset_(0), positions_(new uint32_t[size_]) {
  }

  ~PositionsPool() {
    delete[] positions_;
  }

  uint32_t* StorePositions(const uint32_t* positions, int num_positions) {
    assert(positions_ != NULL);

    if (curr_offset_ + num_positions < size_) {
      for (int i = 0; i < num_positions; ++i) {
        positions_[curr_offset_ + i] = positions[i];
      }
      curr_offset_ += num_positions;
      return positions_ + (curr_offset_ - num_positions);
    }
    return NULL;
  }

  void Reset() {
    curr_offset_ = 0;
  }

private:
  int size_;
  int curr_offset_;
  uint32_t* positions_;
};

/**************************************************************************************************************************************************************
 * IndexEntry
 *
 **************************************************************************************************************************************************************/
struct IndexEntry {
  uint32_t doc_id;
  uint32_t frequency;
  uint32_t* positions;
};

/**************************************************************************************************************************************************************
 * IndexEntryDocIdComparison
 *
 **************************************************************************************************************************************************************/
struct IndexEntryDocIdComparison {
  bool operator()(const IndexEntry& lhs, const IndexEntry& rhs) const {
    return lhs.doc_id < rhs.doc_id;
  }
};

#endif /* INDEX_UTIL_H_ */
