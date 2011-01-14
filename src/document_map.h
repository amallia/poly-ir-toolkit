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
//
/*
 * TODO: Index reorganization idea:
 *
 * Store all the block headers together at the front of the list.
 * We can also create a "layered" skipping method by indexing the block headers.
 * This way, we can first check the block header index, and skip ahead to the correct block header.
 * We then decompress the block header and find the proper chunk we need to get.
 *
 * Another idea to eliminate the "wasted space" at the end of each block. This means that a chunk can span block headers (assuming it can span a maximum of 2 blocks only).
 * Since we know the size of each chunk (stored in the block header), when we start decompressing a chunk, we know if it'll spill over to the next block.
 * The PROBLEM with these scheme is that when a chunk spans across a block, we need to get the complete chunk data in a single array. However, our caching scheme (LRU) prevents this
 * because consecutive blocks are not guaranteed to be placed consecutively in the cache (although it is likely they are). When using this caching scheme, we'd have to check whether
 * the blocks that the chunk spans are consecutively placed, and if not we need to copy the chunk data into a new array and use it as input to the compression function.
 * Note that this does not affect us when the index is completely in main memory, since the blocks are always consecutively placed in main memory.
 */
//
// TODO:
// Have methods for storing fixed length and variable length info.
// Format: URL_len, URL, Docno_len, Docno, DocLen, DocID (Docno is actually a fixed length TREC id)
//
// TODO: We're not doing reordering during initial indexing step...yet
// Docs are not necessarily collected in order, so the docIDs need to be stored.
//
// Dynamic fields can be allocated in another big buffer and we'll just store a pointer to it in the fixed buffer. Leads to less memory fragmentation! like PositionsPool
//
// The document map is loaded into memory as an array, indexed by the docID.
// Due to document reordering and possible gaps between the reorded docIDs, our document map may have dummy entries.
//
// Notes about the document map:
// During the initial indexing phase, there are two scenarios:
// In both scenarios, we don't need the document map in memory. We can simply buffer it and keep appending the data to a single file per index.
// We can also use integer compression for the integers and character compression for the URLs (check out vcdiff and similar...). This would require us to choose some block
// size in which to compress these entries.
// 1) We do document reordering:
//    This means the document info will not be collected in order (because we'll be remapping it right away to a different docID), so we'll have to store the
//    remapped docID along with the document info.
//    Here we can have an optimization: We sort the partial document map buffer by the remapped docIDs prior to dumping it to disk.
//    So, here we'll have several partial document map files per index, that are now sorted and just need to be merged.
//    We also have to insert dummy entries into any docID gaps. PROBLEM: We could have very large gaps since we don't control the remapping...might be ok though
//    Since there might be large gaps. We can elect to store the docIDs within the file, so when we load it, it'll be apparent which docIDs are missing OR
//    we can just put in dummy entries. Which is more space efficient?
// 2) We don't do document reordering:
//    This means that there will not be gaps in the docID and they will be assigned in order. The document map does not need to store the docID. We can just keep appending data
//    to a single file.
//
//
// TODO: Some ideas:
//     * When doing an index merge/docID reordering/index layering and similar, we access the document map sequentially per term. So for long lists, we can conserve memory by
//       reading the document map in blocks of some fixed size.
//     * We'll need to pre allocate an array for the new full document map in memory because we'll be accessing it all over the place (due to the reordered docs) and we'll just fill it in.
//==============================================================================================================================================================

#ifndef DOCUMENT_MAP_H_
#define DOCUMENT_MAP_H_

#include <cassert>
#include <stdint.h>
#include <cstdlib>

/**************************************************************************************************************************************************************
 * DocumentDynamicEntriesPool
 *
 **************************************************************************************************************************************************************/
class DocumentDynamicEntriesPool {
public:
  DocumentDynamicEntriesPool() :
    size_(0), curr_offset_(0), urls_(NULL) {
  }

  DocumentDynamicEntriesPool(int size) :
    size_(size), curr_offset_(0), urls_(new char[size_]) {
  }

  ~DocumentDynamicEntriesPool() {
    delete[] urls_;
  }

  char* StoreUrl(const char* url, int url_len) {
    assert(urls_ != NULL);

    if (curr_offset_ + url_len < size_) {
      // TODO: memcpy might be a better choice.
      for (int i = 0; i < url_len; ++i) {
        urls_[curr_offset_ + i] = url[i];
      }
      curr_offset_ += url_len;
      return urls_ + (curr_offset_ - url_len);
    }
    return NULL;
  }

  void Reset() {
    curr_offset_ = 0;
  }

private:
  int size_;
  int curr_offset_;
  char* urls_;
};


struct DocMapEntry {
  int doc_len;
//  int doc_url_len;
//  DocumentDynamicEntriesPool* doc_url;
};

/**************************************************************************************************************************************************************
 * DocumentMapReader
 *
 * This class reads the complete document map into an in memory buffer for fast access.
 **************************************************************************************************************************************************************/
class DocumentMapReader {
public:
  DocumentMapReader(const char* document_map_filename);
  ~DocumentMapReader();

  int size() const {
    return doc_map_buffer_size_;
  }

  int DocMapSize();

  int GetDocumentLen(uint32_t doc_id) const {
    return doc_map_buffer_[doc_id].doc_len;
  }

private:
  int doc_map_fd_;
  int doc_map_buffer_size_;
  DocMapEntry* doc_map_buffer_;
};



/**************************************************************************************************************************************************************
 * DocumentMapWriter
 *
 * This class buffers a portion of the document map before appending it to the document map file.
 *
 * In order to use this document map in the case of remapped docIDs, we must store them as well and later resort (do an I/O efficient sort) it by the docID.
 * Since there may be gaps in the docIDs, we can either store the remapped docIDs along with the document info, or store the docID gaps so that the
 * DocumentMapReader knows about the gaps. It's not apparent which technique is more space conservative.
 **************************************************************************************************************************************************************/
class DocumentMapWriter {
public:
  DocumentMapWriter(const char* document_map_filename_lengths, const char* document_map_filename_urls);
  ~DocumentMapWriter();

  void AddDocMapEntry(const DocMapEntry& doc_map_entry);
  void DumpBuffer();

  void AddUrl(const char* url, int url_len);
  void DumpBufferUrls();

private:
  const int kDocMapBufferSize; // TODO: get from config file
  int doc_map_fd_lengths_; // TODO might wanna make constant
  DocMapEntry* doc_map_buffer_;
  int doc_map_buffer_len_;

  const int kUrlsBufferSize;
  int doc_map_fd_urls_; // TODO might wanna make constant
  char* urls_buffer_;
  int urls_buffer_len_;
};

#endif /* DOCUMENT_MAP_H_ */
