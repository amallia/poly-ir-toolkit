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
// Code originally based on parser from search engines course.
//==============================================================================================================================================================

#ifndef PARSER_H_
#define PARSER_H_

#include <cassert>
#include <cctype>
#include <stdint.h>

#include <strings.h>

template<class Callback>
  class Parser {
  public:
    enum ParsingMode {
      kSingleDoc, kManyDoc
    };

    enum DocType {
      kStandard, kTrec, kWarc, kNoSuchDocType
    };

    enum Context {
      kContextP = 0, // Plain.
      kContextB = 1, // Bold.
      kContextH = 2, // Heading.
      kContextI = 4, // Italic.
      kContextT = 8, // Title.
      kContextU = 16 // URL.
    };

    enum Tag {
      kTagNot, kTagB, kTagI, kTagH, kTagTitle, kTagScript, kTagDoc, kTagDocno, kTagDochdr
    };

    struct WarcHeader {
      int content_length;

      const char* url;
      int url_len;

      const char* docno;
      int docno_len;
    };

    Parser(const ParsingMode& parsing_mode, const DocType& doc_type, Callback* callback);

    int ParseDocumentCollection(const char* buf, int buf_len, uint32_t& doc_id, int& avg_doc_length);

    // TODO: Can make private.
    int ParseBuffer(const char* buf, int buf_len, uint32_t& doc_id, int& avg_doc_length, const char*& curr_p);

    Tag ProcessTag(const char* tag, int tag_len, bool& in_closing_tag, uint32_t doc_id);

    bool IsValidTag(const char* curr_tag_p, const char* tag, int tag_len, const char curr_tag_name[]);

    bool IsWithinBounds(const char* curr, const char* start, int len);

    // Handles the case where you have a tag that starts with the same characters, but the ending characters could be anything within a range.
    bool AreValidTags(const char* curr_tag_p, const char* tag, int tag_len, const char curr_tag_base[], const char start_range[], const char end_range[],
                      int range_len);

    // Processes the WARC header. Returns the number of bytes read from the header.
    int ProcessWarcHeader(const char* buf, int buf_len, const char* curr_p, WarcHeader* header);

    bool IsIndexable(char c) {
      return isalnum(static_cast<unsigned char> (c));
    }

    void BitSet(unsigned char& b1, unsigned char b2) {
      b1 |= b2;
    }

    void BitUnset(unsigned char& b1, unsigned char b2) {
      b1 &= ~b2;
    }

    bool BitCheck(unsigned char b1, unsigned char b2) {
      return b1 & b2;
    }

    void UpdateContext(unsigned char& context, bool closing_tag, unsigned char set_context) {
      if (!closing_tag)
        BitSet(context, set_context);
      else
        BitUnset(context, set_context);
    }

    static DocType GetDocumentCollectionFormat(const char* doc_type_str) {
      if (strcasecmp("trec", doc_type_str) == 0) {
        return kTrec;
      } else if (strcasecmp("warc", doc_type_str) == 0) {
        return kWarc;
      } else {
        return kNoSuchDocType;
      }
    }

  private:
    ParsingMode parsing_mode_;
    DocType doc_type_;
    Callback* callback_;
  };

#include "parser-inl.h"

#endif /* PARSER_H_ */
