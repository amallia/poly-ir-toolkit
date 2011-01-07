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
// TODO: Need to decide at what level we want metadata such as time crawled and language. Document level, Document collection level, Index collection level?
//==============================================================================================================================================================

#ifndef DOCUMENT_COLLECTION_H_
#define DOCUMENT_COLLECTION_H_

#include <stdint.h>

#include <string>
#include <vector>

#include "parser.h"
#include "parser_callback.h"

/**************************************************************************************************************************************************************
 * Document
 *
 * Container for information about a single document.
 * TODO: Might want to store TREC number.
 **************************************************************************************************************************************************************/
class Document {
public:
  Document(const char* doc_buf, int doc_len, const char* url_buf, int url_len, uint32_t doc_id);

  const char* doc_buf() const {
    return doc_buf_;
  }

  int doc_len() const {
    return doc_len_;
  }

  const char* url_buf() const {
    return url_buf_;
  }

  int url_len() const {
    return url_len_;
  }

  uint32_t doc_id() const {
    return doc_id_;
  }

private:
  const char* doc_buf_;
  int doc_len_;

  const char* url_buf_;
  int url_len_;

  uint32_t doc_id_;
};

/**************************************************************************************************************************************************************
 * DocumentCollection
 *
 * Holds information about multiple documents lumped into a single file.
 * Contains metadata for all documents in the current collection, such as the language and date crawled.
 **************************************************************************************************************************************************************/
class DocumentCollection {
public:
  enum Language {
    ENGLISH
  };

  DocumentCollection(const std::string& file_path);

  int Fill(char** document_collection_buf, int* document_collection_buf_size);

  bool processed() const {
    return processed_;
  }

  void set_processed(bool processed) {
    processed_ = processed;
  }

  std::string file_path() const {
    return file_path_;
  }

  uint32_t initial_doc_id() const {
    return initial_doc_id_;
  }

  uint32_t final_doc_id() const {
    return final_doc_id_;
  }

  void set_initial_doc_id(uint32_t initial_doc_id) {
    initial_doc_id_ = initial_doc_id;
  }

  void set_final_doc_id(uint32_t final_doc_id) {
    final_doc_id_ = final_doc_id;
  }

private:
  bool processed_;  // True when this document collection has been processed (parsed).
  std::string file_path_;
  Language lang_;

  uint32_t initial_doc_id_;
  uint32_t final_doc_id_;
};

/**************************************************************************************************************************************************************
 * IndexCollection
 *
 * Responsible for all the DocumentCollection objects that are part of the index we'll be operating on.
 **************************************************************************************************************************************************************/
class IndexCollection {
public:
  void AddDocumentCollection(const std::string& path);
  void ProcessDocumentCollections(std::istream& is);

protected:
  std::vector<DocumentCollection> doc_collections_;
};

/**************************************************************************************************************************************************************
 * CollectionIndexer
 *
 * Responsible for indexing the document collection.
 **************************************************************************************************************************************************************/
class CollectionIndexer : public IndexCollection {
public:
  CollectionIndexer();
  ~CollectionIndexer();

  Parser<IndexingParserCallback>::DocType GetAndVerifyDocType();
  void ParseTrec();
  void OutputDocumentCollectionDocIdRanges(const char* filename);

  // Returns the current running docID.
  uint32_t doc_id() const {
    return doc_id_;
  }

private:
  int document_collection_buffer_size_;
  char* document_collection_buffer_;

  IndexingParserCallback parser_callback_;
  Parser<IndexingParserCallback> parser_;

  uint32_t doc_id_;  // Keeps the running docID, which will either be updated by the parser or externally, depending on what mode the parser is in.
  int avg_doc_length_;
};

/**************************************************************************************************************************************************************
 * CollectionUrlExtractor
 *
 **************************************************************************************************************************************************************/
class CollectionUrlExtractor : public IndexCollection {
public:
  CollectionUrlExtractor();
  ~CollectionUrlExtractor();

  Parser<DocUrlRetrievalParserCallback>::DocType GetAndVerifyDocType();
  void ParseTrec(const char* document_urls_filename);

private:
  int document_collection_buffer_size_;
  char* document_collection_buffer_;

  DocUrlRetrievalParserCallback parser_callback_;
  Parser<DocUrlRetrievalParserCallback> parser_;

  uint32_t doc_id_;  // Keeps the running docID, which will either be updated by the parser or externally, depending on what mode the parser is in.
  int avg_doc_length_;
};

#endif /* DOCUMENT_COLLECTION_H_ */
