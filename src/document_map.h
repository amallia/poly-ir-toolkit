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
// First, a note on reordered/remapped indices:
// The document reordering/remapping will only take place once we have a complete index ready. We will not be doing document reordering while indexing.
// Further, for reordered indices, we assume that there will be no gaps in the reordered docIDs because there will be a one to one correspondence between the
// original docIDs and the remapped docIDs. We also assume that the remapped docIDs will be in the same exact range as the original docIDs.
// Having said that, in order to implement the document map for the reordered indices, we'll be using the original file used to remap the docIDs, and remap the
// original document map in main memory.
//
// Notes on the document map implementation:
// The document map consists of two files. The basic (index.dmap_basic) and extended (index.dmap_extended) document maps. The basic document map only holds the
// document lengths and extended offsets; the extended offset is to the start of the document information in the extended document map.
// The extended document map holds the URLs and document numbers (for TREC experiments).
// The basic document map is designed to be fully in memory; the document lengths are necessary for BM25 score computation, so access must be fast. The extended
// information is used only for the final top-k documents and is not timed during query runs, so we can afford to go to disk k times.
// Currently, no compression is used on the extended document map, so it could take up a bit of space. Compression for the basic document map is generally not
// necessary, since it's already small.
//
// Future improvements to the document map:
// Sometimes, we would like to store everything in main memory (i.e. the document URLs). To conserve main memory, we can employ compression (i.e. gzip) on
// the URLs; however this is most effective when we compress several URLs ("bunches" of URLs) together to exploit their common attributes.
// For this, we'll load up the URLs and compress bunches of them, storing the compressed data into a large in memory array.
// In main memory, for each of n bunches, we'll store an offset/pointer to each compressed bunch in a table of size ceil(num_docs / n).
// From this info, any docID URL can be found by decompressing a bunch (size n) of the URLs and then offsetting to the correct one (doc_id % n).
//
// Constructing an in-memory compressed document map for the case of a reordered index takes more work, as roughly described below.
// In order to construct this compressed in-memory URL array, the URLs corresponding to the docIDs have to be in monotonically increasing docID order, which
// is not the case due to docID reordering. So, to avoid making many random seeks into the extended document map file (which would take VERY long for millions
// of docIDs) and not loading the complete file in main memory (it might not fit), we would sort the <docID, doc_len, offset> tuples from the basic document map
// by the offsets. Then, load up the extended document map file contiguously, getting tuples of <offset, URL>. By merging, we could create
// <docID, doc_len, offset, URL> tuples, which would be ordered by ascending offsets, but non-specified docID order. From here on, we'll do an I/O efficient
// merge-sort, so we can get a new extended document map file, now ordered by docIDs and we will also get a new basic document map, with the new offsets (might
// as well also remap the docIDs at this step). This file would be suitable for in-memory compression as described above.
//==============================================================================================================================================================

#ifndef DOCUMENT_MAP_H_
#define DOCUMENT_MAP_H_

// Enables debugging output for this module.
//#define DOCUMENT_MAP_DEBUG

#include <cassert>
#include <cstdlib>
#include <stdint.h>

#ifdef DOCUMENT_MAP_DEBUG
#include <iostream>
#endif
#include <string>

/**************************************************************************************************************************************************************
 * DocumentDynamicEntriesPool
 *
 * TODO: Currently not used. Might be useful if we want all the extended document data (like URLs) in main memory.
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

/**************************************************************************************************************************************************************
 * DocMapEntry
 *
 * This is the basic document map entry we keep for each docID in main memory at all times. It is meant to be array indexable by the docID. There is an also
 * an additional "extended" document map file that will keep document attributes such as the document URLs. This is only required for top-k lookups, and as
 * such, we will keep an offset into the extended document map file in main memory for each docID, and only fetch the extended information when required from
 * the disk to conserve main memory.
 **************************************************************************************************************************************************************/
struct DocMapEntry {
  int doc_len;                 // The length, in words, of the current docID.
  off_t extended_file_offset;  // The offset into the extended document map file for the current docID.
}__attribute__((__packed__));  // Without the packed attribute, g++ was adding an extra 4 bytes worth of padding. We want this as small as possible.

/**************************************************************************************************************************************************************
 * DocumentMapWriter
 *
 * This class buffers a portion of the document map before appending it to the document map files.
 **************************************************************************************************************************************************************/
class DocumentMapWriter {
public:
  DocumentMapWriter(const char* basic_document_map_filename, const char* extended_document_map_filename);
  ~DocumentMapWriter();

  void AddDocLen(int doc_len, uint32_t doc_id);

  void AddDocUrl(const char* url, int url_len, uint32_t doc_id);

  void AddDocNum(const char* docnum, int docnum_len, uint32_t doc_id);

  void DumpBasicDocMapBuffer();

  void DumpExtendedDocMapBuffer();

private:
  // TODO: Get these from the configuration file.
  //       Convert from MiB specified to take into account that these are specifying the number of array elements of objects of a certain size.
  const int kBasicDocMapBufferSize;
  const int kExtendedBufferSize;

  // File descriptors we will write to.
  int basic_doc_map_fd_;
  int extended_doc_map_fd_;

  // Buffers for data before dumping to document map files.
  DocMapEntry* basic_doc_map_buffer_;
  char* extended_doc_map_buffer_;

  // The current offsets into the buffers we'll be saving data to.
  int basic_doc_map_buffer_len_;
  int extended_doc_map_buffer_len_;

  // The current docID we're saving data for.
  uint32_t curr_doc_id_;

  // The current size of the extended document map file.
  off_t extended_doc_map_file_offset_;

  // The total size of the current document map entry.
  int extended_doc_map_curr_entry_size_;
};

/**************************************************************************************************************************************************************
 * DocumentMapReader
 *
 * This class reads the complete document map into an in memory buffer for fast access.
 **************************************************************************************************************************************************************/
class DocumentMapReader {
public:
  DocumentMapReader(const char* document_map_filename, const char* extended_document_map_filename);
  ~DocumentMapReader();

  int GetDocumentLength(uint32_t doc_id) const {
    return basic_doc_map_buffer_[doc_id].doc_len;
  }

  void LoadRemappingTranslationTable(const char* doc_id_map_filename);

  std::string GetDocumentUrl(uint32_t doc_id) const;

  std::string GetDocumentNumber(uint32_t doc_id) const;

private:
  enum ExtendedInfoComponent {
    kDocUrl, kDocNum
  };

  int BasicDocMapSize();
  std::string DecodeDocumentExtendedInfo(uint32_t doc_id, ExtendedInfoComponent component) const;

  int basic_doc_map_fd_;
  int extended_doc_map_fd_;

  int basic_doc_map_buffer_size_;
  DocMapEntry* basic_doc_map_buffer_;
};

#endif /* DOCUMENT_MAP_H_ */
