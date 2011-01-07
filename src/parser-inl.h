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
// TODO: DOCNO can be determined algorithmically from the docID (instead of storing as a string)? We also have to consider docID remapping.
// TODO: Need to implement modes 'kSingleDoc' and 'kStandard'.
//
// Some WARC records in ClueWeb09 are malformed. See 'http://www.umiacs.umd.edu/~jimmylin/cloud9/docs/content/clue.html' for more information. Here, we handle the
// common problem of having an extra new line in the WARC header. A few of the records also happen to have garbled URLs.
//==============================================================================================================================================================

#ifndef PARSERINL_H_
#define PARSERINL_H_

#include <cstring>

///////TODO
#include <string>
#include <iostream>
#include <cstdlib>
/////////

template<class Callback>
  Parser<Callback>::Parser(const Parser<Callback>::ParsingMode& parsing_mode, const Parser<Callback>::DocType& doc_type, Callback* callback) :
    parsing_mode_(parsing_mode), doc_type_(doc_type), callback_(callback) {
    assert(callback_ != NULL);
  }

// Returns the number of documents parsed if parsing mode is set to 'MANY_DOC', otherwise 0.
// Notes: The base URL can be set by a <base> tag within the page and by the Content-Location field in the web server's HTTP response header.
// These cases are not currently covered.
template<class Callback>
  int Parser<Callback>::ParseDocumentCollection(const char* buf, int buf_len, uint32_t& doc_id, int& avg_doc_length) {
    assert(buf != NULL);
    assert(buf_len > 0);

    int num_docs_parsed = 0;

    const char* curr_p = buf; // Tracks the current point in the buffer.

    if (doc_type_ == kWarc) {
      // The WARC format starts each bundle with some info lines, which we skip here.

      WarcHeader warc_header;

      curr_p += ProcessWarcHeader(buf, buf_len, curr_p, &warc_header);
      curr_p += warc_header.content_length;

      while (true) {
        // If using the WARC format, need to process the header first for each document..
        int header_bytes = ProcessWarcHeader(buf, buf_len, curr_p, &warc_header);
        if (header_bytes == 0)
          break;

        curr_p += header_bytes;
        const char* content_start = curr_p;
//        curr_p += warc_header.content_length;

        num_docs_parsed += ParseBuffer(curr_p, warc_header.content_length, doc_id, avg_doc_length, curr_p);
        std::cout << "num_docs_parsed: " << num_docs_parsed << std::endl;

        // Since we're parsing one document at a time, we need to update the docID count, average doc length here.
        ++doc_id;
        ++num_docs_parsed;

        std::cout << "curr_p - content_start" << (curr_p - content_start) << ", warc_header.content_length: " << warc_header.content_length << std::endl;
        assert(curr_p - content_start == warc_header.content_length);

        std::cout << "NEXT HEADER" << std::endl;
      }
    } else {
      num_docs_parsed += ParseBuffer(buf, buf_len, doc_id, avg_doc_length, curr_p);
    }

    return num_docs_parsed;
  }

template<class Callback>
  int Parser<Callback>::ParseBuffer(const char* buf, int buf_len, uint32_t& doc_id, int& avg_doc_length, const char*& curr_p) {
  assert(buf != NULL);
  assert(buf_len > 0);

  uint32_t initial_doc_id = doc_id;

  Tag tag_ret;  // The special type of tag we encountered.

  unsigned char context = '\0';  // Bit array for the context.
  uint32_t position = 0;         // Tracks position of each word, final position for a document is it's size in words.

  // For parsing HTML.
  bool in_closing_tag = false;  // True when we're parsing a closing tag.
  bool in_script = false;       // True when we're parsing contents of script tag.

  // For TREC documents.
  bool in_doc = false;     // True when we're parsing contents of doc tag.
  bool in_docno = false;   // True when we're parsing contents of docno tag.
  bool in_dochdr = false;  // True when we're parsing contents of dochdr tag.


  // Track the starting point of various things we want to parse out.
  const char* word_p;        // Standalone word.
  const char* url_p;         // TREC document URL.
  const char* docno_p;       // TREC document number.
  const char* tag_p = NULL;  // Tracks the starting point of a tag; doubles as a flag as to whether we're currently in a tag.
//  const char* curr_p = buf;  // Tracks the current point in the buffer.



  while (IsWithinBounds(curr_p, buf, buf_len)) {
    if (!IsIndexable(*curr_p)) {
      if (*curr_p != '>') {
        if (*curr_p == '<') {
          tag_p = curr_p;
        }
        ++curr_p;
        continue;
      }

      if (!tag_p) {
        ++curr_p;
        continue;
      }

      // At this point, we must have just seen the end of a closing tag, '>'.
      ++curr_p;
      tag_ret = ProcessTag(tag_p, curr_p - tag_p, in_closing_tag, doc_id);

      switch (tag_ret) {
        case kTagNot:
          break;

        case kTagB:
          UpdateContext(context, in_closing_tag, kContextB);
          break;

        case kTagI:
          UpdateContext(context, in_closing_tag, kContextI);
          break;

        case kTagH:
          UpdateContext(context, in_closing_tag, kContextH);
          break;

        case kTagTitle:
          UpdateContext(context, in_closing_tag, kContextT);
          break;

        case kTagScript:
          in_script = in_closing_tag ? false : true;
          break;

        case kTagDoc:
          if (doc_type_ != kTrec)
            break;

          if (in_closing_tag) {
            in_doc = false;

            // The position at this time is actually the document length.
            avg_doc_length += position;
            callback_->ProcessDocLength(position, doc_id);

            // This only applies when we're parsing multiple documents in one go.
            if (parsing_mode_ == kManyDoc) {
              context = 0;
              position = 0;
              ++doc_id;

              // Need to reset certain properties before moving on to the next document.
              in_script = false;
            }
          } else {
            in_doc = true;
          }
          break;

        case kTagDocno:
          if (doc_type_ != kTrec)
            break;

          in_docno = in_closing_tag ? false : true;
          break;

        case kTagDochdr:
          if (doc_type_ != kTrec)
            break;

          in_dochdr = in_closing_tag ? false : true;
          break;

        default:
          break;
      }

      tag_p = NULL;
      continue;
    }

    // Ignore everything between <script></script> tags and ignore inner contents of tags.
    if (in_script || tag_p) {
      ++curr_p;
      continue;
    }

    if (doc_type_ == kTrec) {
      if (in_docno) {
        docno_p = curr_p;
        while (IsWithinBounds(curr_p, buf, buf_len) && *curr_p != '<') {
          ++curr_p;
        }
        callback_->ProcessDocno(docno_p, curr_p - docno_p, doc_id);

        continue;
      } else if (in_dochdr) {
        BitSet(context, kContextU);

        url_p = curr_p;
        while (IsWithinBounds(url_p, buf, buf_len) && *url_p != '\n') {
          if (!IsIndexable(*url_p)) {
            url_p++;
            continue;
          }

          word_p = url_p;
          while (IsWithinBounds(url_p, buf, buf_len) && IsIndexable(*url_p)) {
            url_p++;
          }

          callback_->ProcessTerm(word_p, url_p - word_p, doc_id, position++, context);
        }

        BitUnset(context, kContextU);

        callback_->ProcessUrl(curr_p, url_p - curr_p, doc_id);

        curr_p = url_p + 1;
        // Skip the rest of the dochdr contents (making sure that we're at the end of the dochdr).
        while (IsWithinBounds(curr_p, buf, buf_len)) {
          if (*curr_p == '<') {
            const char kDocHdrClosingTag[] = "</DOCHDR>";
            // Make sure it's actually the closing tag.
            if (IsWithinBounds(curr_p + sizeof(kDocHdrClosingTag) - 1, buf, buf_len) && strncasecmp(curr_p, kDocHdrClosingTag, sizeof(kDocHdrClosingTag) - 1) == 0) {
              break;
            }
          }
          ++curr_p;
        }

        continue;
      }
    }

    word_p = curr_p;
    while (IsWithinBounds(curr_p, buf, buf_len) && IsIndexable(*curr_p)) {
      ++curr_p;
    }

    callback_->ProcessTerm(word_p, curr_p - word_p, doc_id, position++, context);
  }

  return doc_id - initial_doc_id;
}

template<class Callback>
  typename Parser<Callback>::Tag Parser<Callback>::ProcessTag(const char* tag, int tag_len, bool& in_closing_tag, uint32_t doc_id) {
    // Caller must ensure tag_len is always at least 2, for tag "<>".
    assert(tag_len >= 2);

    const char* curr_tag_p = tag + 1;

    // Check whether this is a closing tag.
    if (*curr_tag_p == '/') {
      in_closing_tag = true;
      ++curr_tag_p;
    } else {
      in_closing_tag = false;
    }

    switch (*curr_tag_p) {
      case 'a':
      case 'A': {
        const char* l_start = NULL;
        const char* l_end = NULL;

        for (++curr_tag_p; IsWithinBounds(curr_tag_p, tag, tag_len); ++curr_tag_p) {
          if (*curr_tag_p == '"' || *curr_tag_p == '\'') {
            if (!l_start) {
              l_start = curr_tag_p + 1;
            } else {
              l_end = curr_tag_p;
              break;
            }
          }
        }

        if (l_start && l_end) {
          callback_->ProcessLink(l_start, l_end - l_start, doc_id);
        }

        return Parser::kTagNot;
      }
      case 'b':
      case 'B': {
        return IsValidTag(curr_tag_p, tag, tag_len, "b") ? Parser::kTagB : Parser::kTagNot;
      }
      case 'i':
      case 'I': {
        return IsValidTag(curr_tag_p, tag, tag_len, "i") ? Parser::kTagI : Parser::kTagNot;
      }
      case 'e':
      case 'E': {
        return IsValidTag(curr_tag_p, tag, tag_len, "em") ? Parser::kTagI : Parser::kTagNot;
      }
      case 'h':
      case 'H': {
        return AreValidTags(curr_tag_p, tag, tag_len, "h", "1", "6", 2) ? Parser::kTagH : Parser::kTagNot;
      }
      case 't':
      case 'T': {
        return IsValidTag(curr_tag_p, tag, tag_len, "title") ? Parser::kTagTitle : Parser::kTagNot;
      }

      case 's':
      case 'S': {
        return IsValidTag(curr_tag_p, tag, tag_len, "strong") ? Parser::kTagB : IsValidTag(curr_tag_p, tag, tag_len, "script") ? Parser::kTagScript
                                                                                                                               : Parser::kTagNot;
      }
      case 'd':
      case 'D': {
        return IsValidTag(curr_tag_p, tag, tag_len, "dochdr") ? Parser::kTagDochdr
                                                              : IsValidTag(curr_tag_p, tag, tag_len, "docno") ? Parser::kTagDocno
                                                                                                              : IsValidTag(curr_tag_p, tag, tag_len, "doc") ? Parser::kTagDoc
                                                                                                                                                            : Parser::kTagNot;
      }
      default: {
        break;
      }
    }

    return Parser::kTagNot;
  }

template<class Callback>
  bool Parser<Callback>::IsValidTag(const char* curr_tag_p, const char* tag, int tag_len, const char curr_tag_name[]) {
    const size_t curr_tag_name_len = strlen(curr_tag_name);

    if (!strncasecmp(curr_tag_p, curr_tag_name, curr_tag_name_len)) {
      curr_tag_p += curr_tag_name_len;
      for (; IsWithinBounds(curr_tag_p, tag, tag_len); ++curr_tag_p) {
        if (*curr_tag_p != ' ') {
          if (*curr_tag_p == '>')
            return true;
          else
            break;
        }
      }
    }

    return false;
  }

template<class Callback>
  inline bool Parser<Callback>::IsWithinBounds(const char* curr, const char* start, int len) {
    return (curr - start) < len;
  }

template<class Callback>
  bool Parser<Callback>::AreValidTags(const char* curr_tag_p, const char* tag, int tag_len, const char curr_tag_base[], const char start_range[],
                                      const char end_range[], int range_len) {
    const size_t curr_tag_base_len = strlen(curr_tag_base);

    if (!strncasecmp(curr_tag_p, curr_tag_base, curr_tag_base_len)) {
      curr_tag_p += curr_tag_base_len;

      if (IsWithinBounds(curr_tag_p + range_len - 1, tag, tag_len)) {
        if (strncmp(curr_tag_p, start_range, range_len) >= 0 && strncmp(curr_tag_p, end_range, range_len) <= 0) {
          curr_tag_p += range_len;

          for (; IsWithinBounds(curr_tag_p, tag, tag_len); ++curr_tag_p) {
            if (*curr_tag_p != ' ') {
              if (*curr_tag_p == '>')
                return true;
              else
                break;
            }
          }
        }
      }
    }

    return false;
  }

template<class Callback>
  int Parser<Callback>::ProcessWarcHeader(const char* buf, int buf_len, const char* curr_p, WarcHeader* header) {
      const char* header_start = curr_p;

      while (IsWithinBounds(curr_p, buf, buf_len)) {
        if (*curr_p == '\n') {
          ++curr_p;

          // Double new line means we read the complete header.
          if (IsWithinBounds(curr_p, buf, buf_len) && *curr_p == '\n') {
            ++curr_p;
            break;
          }

          const char* key_start = curr_p;
          int key_length = 0;
          while (IsWithinBounds(curr_p, buf, buf_len) && *curr_p != ':') {
            ++curr_p;
            ++key_length;
          }

          std::string key = std::string(key_start, key_length);
          std::cout << "key: " << key << std::endl;

          ++curr_p;

          if (IsWithinBounds(curr_p, buf, buf_len) && *curr_p == ' ') {
            ++curr_p;
          }

          const char* value_start = curr_p;
          int value_length = 0;

          while (IsWithinBounds(curr_p, buf, buf_len) && *curr_p != '\n') {
            ++curr_p;
            ++value_length;
          }

          const char kContentLengthStr[] = "Content-Length";
          const int kContentLengthStrLen = sizeof(kContentLengthStr) - 1;

          std::string value = std::string(value_start, value_length);
          std::cout << "value: " << value << std::endl;

          // We need to know how long the document content is.
          if (key_length == kContentLengthStrLen && strncmp(key_start, kContentLengthStr, kContentLengthStrLen) == 0) {
            char content_length_buf[11]; // Has to fit a 4 byte integer and a terminating null character.
            assert(value_length < 11);
            memcpy(content_length_buf, value_start, value_length);
            content_length_buf[value_length] = '\0';
            header->content_length = atoi(content_length_buf);
          }

        } else {
          ++curr_p; // Skips past the 'WARC/0.18' part.
        }
      }

      return (curr_p - header_start);
  }

#endif /* PARSERINL_H_ */
