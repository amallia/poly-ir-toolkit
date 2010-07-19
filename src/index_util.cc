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

#include "index_util.h"

#include <limits>
#include <sstream>

#include "cache_manager.h"
#include "index_reader.h"
using namespace std;

/**************************************************************************************************************************************************************
 * IndexFiles
 *
 **************************************************************************************************************************************************************/
IndexFiles::IndexFiles(const string& index_filename, const string& lexicon_filename, const string& document_map_filename, const string& meta_info_filename) :
  index_filename_(index_filename), lexicon_filename_(lexicon_filename), document_map_filename_(document_map_filename), meta_info_filename_(meta_info_filename) {
}

IndexFiles::IndexFiles() :
  index_filename_("index.idx"), lexicon_filename_("index.lex"), document_map_filename_("index.dmap"), meta_info_filename_("index.meta") {
}

IndexFiles::IndexFiles(int group_num, int file_num) {
  ostringstream index_filename;
  index_filename << "index.idx." << group_num << "." << file_num;

  ostringstream lexicon_filename;
  lexicon_filename << "index.lex." << group_num << "." << file_num;

  ostringstream document_map_filename;
  document_map_filename << "index.dmap." << group_num << "." << file_num;

  ostringstream meta_info_filename;
  meta_info_filename << "index.meta." << group_num << "." << file_num;

  index_filename_ = index_filename.str();
  lexicon_filename_ = lexicon_filename.str();
  document_map_filename_ = document_map_filename.str();
  meta_info_filename_ = meta_info_filename.str();
}

// 'dir' is expected to be a directory path that does not end with a "/",
// except in the case where it is the root directory.
void IndexFiles::SetDirectory(const string& dir) {
  string separator = (dir == "/") ? "" : "/";
  index_filename_ = dir + separator + index_filename_;
  lexicon_filename_ = dir + separator + lexicon_filename_;
  document_map_filename_ = dir + separator + document_map_filename_;
  meta_info_filename_ = dir + separator + meta_info_filename_;
}

/**************************************************************************************************************************************************************
 * Index
 *
 **************************************************************************************************************************************************************/
Index::Index(CacheManager* cache_policy, IndexReader* index_reader) :
  cache_policy_(cache_policy), index_reader_(index_reader), curr_lex_data_(NULL), curr_list_data_(NULL), curr_doc_id_(0), next_doc_id_(0), curr_term_(NULL),
      curr_term_len_(0) {
}

Index::~Index() {
  delete cache_policy_;
  delete index_reader_;
}

bool Index::Next() {
  return NextDocId() || (NextTerm() && NextDocId());
}

// Closes the list for the current term.
// Then returns true if there is a next term (lexicographically) and changes state so as to open the corresponding list for this term.
// Otherwise, if there is no next term, changes state so all relevant term and list pointers are NULL, and returns false.
// Note: After calling this method, NextDocId() should be called prior to doing anything with the docIDs and any other data associated with them.
bool Index::NextTerm() {
  // Close the previous list before moving on to the next one.
  if (curr_list_data_ != NULL) {
    index_reader_->CloseList(curr_list_data_);
    curr_list_data_ = NULL;
    delete curr_lex_data_;
    curr_lex_data_ = NULL;
  }

  if ((curr_lex_data_ = index_reader_->lexicon().GetNextEntry()) != NULL) {
    curr_list_data_ = index_reader_->OpenList(*curr_lex_data_);
    curr_term_ = curr_lex_data_->term();
    curr_term_len_ = curr_lex_data_->term_len();
    return true;
  }

  curr_term_ = NULL;
  curr_term_len_ = 0;
  curr_list_data_ = NULL;
  return false;
}

bool Index::NextDocId() {
  if (curr_list_data_ != NULL && (curr_doc_id_ = index_reader_->NextGEQ(curr_list_data_, next_doc_id_)) < numeric_limits<uint32_t>::max()) {
    next_doc_id_ = curr_doc_id_ + 1;
    return true;
  }

  curr_doc_id_ = 0;
  next_doc_id_ = 0;
  return false;
}
