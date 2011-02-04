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
// Implements the callback functions for the parser.
//==============================================================================================================================================================

#ifndef PARSER_CALLBACK_H_
#define PARSER_CALLBACK_H_

// Enables debugging output for this module.
//#define PARSER_CALLBACK_DEBUG

#include <iostream>
#include <string>
#include <utility>

#include <strings.h>

#include "globals.h"
#include "logger.h"
#include "posting_collection.h"

/**************************************************************************************************************************************************************
 * ParserCallback
 *
 **************************************************************************************************************************************************************/
class ParserCallback {
public:
  void ProcessTerm(const char* term, int term_len, uint32_t doc_id, uint32_t position, unsigned char context) {
  }

  // URLs could begin with "http://".
  // Might want to strip "#" from URLs.
  void ProcessUrl(const char* url, int url_len, uint32_t doc_id) {
  }

  // Indicates the start of a new document.
  // The TREC DOCNO specifies the document bundle folder, bundle file, and the document's byte offset within the uncompressed bundle file.
  void ProcessDocno(const char* docno, int docno_len, uint32_t doc_id) {
  }

  void ProcessDocLength(int doc_length, uint32_t doc_id) {
  }

  // Out links might need to be concatenated with the base URL (if they're relative URLs).
  void ProcessLink(const char* url, int url_len, uint32_t doc_id) {
  }
};

/**************************************************************************************************************************************************************
 * IndexingParserCallback
 *
 **************************************************************************************************************************************************************/
class IndexingParserCallback : public ParserCallback {
public:
  IndexingParserCallback(PostingCollectionController* posting_collection_controller);

  void ProcessTerm(const char* term, int term_len, uint32_t doc_id, uint32_t position, unsigned char context);

  void ProcessDocLength(int doc_length, uint32_t doc_id);

  void ProcessUrl(const char* url, int url_len, uint32_t doc_id);

  void ProcessDocno(const char* docno, int docno_len, uint32_t doc_id);

private:
  PostingCollectionController* posting_collection_controller_;
};

inline IndexingParserCallback::IndexingParserCallback(PostingCollectionController* posting_collection_controller) :
  posting_collection_controller_(posting_collection_controller) {
}

inline void IndexingParserCallback::ProcessTerm(const char* term, int term_len, uint32_t doc_id, uint32_t position, unsigned char context) {
#ifdef PARSER_CALLBACK_DEBUG
  // Detects skips in docIDs. Assumes docIDs assigned sequentially. For catching potential parser bugs.
  static int lost_doc_id_count = 0;
  static uint32_t prev_doc_id = 0;
  if (doc_id > prev_doc_id) {
    if (doc_id > (prev_doc_id + 1)) {
      GetErrorLogger().Log("No postings for docID: " + Stringify(prev_doc_id + 1) + " and " + Stringify(doc_id - prev_doc_id - 2) + " more docs.", false);
      lost_doc_id_count += (doc_id - prev_doc_id - 1);
    }
    prev_doc_id = doc_id;
  }
#endif

  Posting posting(term, term_len, doc_id, position, context);
  posting_collection_controller_->InsertPosting(posting);
}

inline void IndexingParserCallback::ProcessDocLength(int doc_length, uint32_t doc_id) {
  posting_collection_controller_->SaveDocLength(doc_length, doc_id);
}

inline void IndexingParserCallback::ProcessUrl(const char* url, int url_len, uint32_t doc_id) {
  assert(url_len > 7);  // We strip away the 'http://' part.
  posting_collection_controller_->SaveDocUrl(url + 7, url_len - 7, doc_id);
}

inline void IndexingParserCallback::ProcessDocno(const char* docno, int docno_len, uint32_t doc_id) {
  posting_collection_controller_->SaveDocno(docno, docno_len, doc_id);
}

/**************************************************************************************************************************************************************
 * DocUrlRetrievalParserCallback
 *
 **************************************************************************************************************************************************************/
class DocUrlRetrievalParserCallback : public ParserCallback {
public:
  void ProcessUrl(const char* url, int url_len, uint32_t doc_id);

  std::vector<std::pair<std::string, uint32_t> >& document_urls() {
    return document_urls_;
  }

private:
  std::vector<std::pair<std::string, uint32_t> > document_urls_;
};

inline void DocUrlRetrievalParserCallback::ProcessUrl(const char* url, int url_len, uint32_t doc_id) {
  const char kCommonUrlPrefix[] = "http://";
  const int kCommonUrlPrefixLen = sizeof(kCommonUrlPrefix) - 1;
  document_urls_.push_back(make_pair(std::string(url + kCommonUrlPrefixLen, url_len - kCommonUrlPrefixLen), doc_id));
}

#endif /* PARSER_CALLBACK_H_ */
