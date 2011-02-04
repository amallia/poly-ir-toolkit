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

#include "document_map.h"

#include <cerrno>
#include <cstring>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef DOCUMENT_MAP_DEBUG
#include <iostream>
#endif

#include <fstream>
#include <limits>

#include "globals.h"
#include "logger.h"
#include "meta_file_properties.h"
using namespace std;

/**************************************************************************************************************************************************************
 * DocumentMapWriter
 *
 **************************************************************************************************************************************************************/
DocumentMapWriter::DocumentMapWriter(const char* basic_document_map_filename, const char* extended_document_map_filename) :
    kBasicDocMapBufferSize(2 << 20),
    kExtendedBufferSize(32 << 20),
    basic_doc_map_fd_(open(basic_document_map_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)),
    extended_doc_map_fd_(open(extended_document_map_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)),
    basic_doc_map_buffer_(new DocMapEntry[kBasicDocMapBufferSize]),
    extended_doc_map_buffer_(new char[kExtendedBufferSize]),
    basic_doc_map_buffer_len_(0),
    extended_doc_map_buffer_len_(0),
    curr_doc_id_(0),
    extended_doc_map_file_offset_(0),
    extended_doc_map_curr_entry_size_(0) {
}

DocumentMapWriter::~DocumentMapWriter() {
  DumpBasicDocMapBuffer();
  DumpExtendedDocMapBuffer();
  delete[] basic_doc_map_buffer_;
  delete[] extended_doc_map_buffer_;
  int close_ret;
  close_ret = close(basic_doc_map_fd_);
  assert(close_ret != -1);
  close_ret = close(extended_doc_map_fd_);
  assert(close_ret != -1);
}

void DocumentMapWriter::AddDocLen(int doc_len, uint32_t doc_id) {
  assert(basic_doc_map_buffer_len_ < kBasicDocMapBufferSize);
  assert(doc_id == curr_doc_id_);  // The document length always comes last (after the document number and URL).

  DocMapEntry doc_map_entry = {doc_len, extended_doc_map_file_offset_};
  basic_doc_map_buffer_[basic_doc_map_buffer_len_++] = doc_map_entry;
  if (basic_doc_map_buffer_len_ == kBasicDocMapBufferSize) {
    DumpBasicDocMapBuffer();
    basic_doc_map_buffer_len_ = 0;
  }

  extended_doc_map_file_offset_ += extended_doc_map_curr_entry_size_;
  extended_doc_map_curr_entry_size_ = 0;
}

void DocumentMapWriter::AddDocUrl(const char* url, int url_len, uint32_t doc_id) {
  assert(doc_id >= curr_doc_id_);

  if (doc_id > curr_doc_id_) {
    curr_doc_id_ = doc_id;
  }

  if (extended_doc_map_buffer_len_ + static_cast<int> (sizeof(url_len)) + url_len > kExtendedBufferSize) {
    DumpExtendedDocMapBuffer();
    extended_doc_map_buffer_len_ = 0;
  }

  memcpy(extended_doc_map_buffer_ + extended_doc_map_buffer_len_, &url_len, sizeof(url_len));
  extended_doc_map_buffer_len_ += sizeof(url_len);

  memcpy(extended_doc_map_buffer_ + extended_doc_map_buffer_len_, url, url_len);
  extended_doc_map_buffer_len_ += url_len;

  extended_doc_map_curr_entry_size_ += sizeof(url_len) + url_len;
}

void DocumentMapWriter::AddDocNum(const char* docnum, int docnum_len, uint32_t doc_id) {
  assert(doc_id >= curr_doc_id_);

  if (doc_id > curr_doc_id_) {
    curr_doc_id_ = doc_id;
  }

  if (extended_doc_map_buffer_len_ + static_cast<int> (sizeof(docnum_len)) + docnum_len > kExtendedBufferSize) {
    DumpExtendedDocMapBuffer();
    extended_doc_map_buffer_len_ = 0;
  }

  memcpy(extended_doc_map_buffer_ + extended_doc_map_buffer_len_, &docnum_len, sizeof(docnum_len));
  extended_doc_map_buffer_len_ += sizeof(docnum_len);

  memcpy(extended_doc_map_buffer_ + extended_doc_map_buffer_len_, docnum, docnum_len);
  extended_doc_map_buffer_len_ += docnum_len;

  extended_doc_map_curr_entry_size_ += sizeof(docnum_len) + docnum_len;
}

void DocumentMapWriter::DumpBasicDocMapBuffer() {
  int write_bytes = sizeof(*basic_doc_map_buffer_) * basic_doc_map_buffer_len_;
  ssize_t write_ret = write(basic_doc_map_fd_, basic_doc_map_buffer_, write_bytes);
  if (write_ret < 0) {
    GetErrorLogger().LogErrno("write() in DocumentMapWriter::DumpBasicDocMapBuffer()", errno, true);
  } else if (write_ret != write_bytes) {
    GetErrorLogger().Log("write() in DocumentMapWriter::DumpBasicDocMapBuffer(): wrote " + Stringify(write_ret) + " bytes, but requested "
        + Stringify(write_bytes) + " bytes.", true);
  }
}

void DocumentMapWriter::DumpExtendedDocMapBuffer() {
  int write_bytes = sizeof(*extended_doc_map_buffer_) * extended_doc_map_buffer_len_;
  ssize_t write_ret = write(extended_doc_map_fd_, extended_doc_map_buffer_, write_bytes);
  if (write_ret < 0) {
    GetErrorLogger().LogErrno("write() in DocumentMapWriter::DumpExtendedDocMapBuffer()", errno, true);
  } else if (write_ret != write_bytes) {
    GetErrorLogger().Log("write() in DocumentMapWriter::DumpExtendedDocMapBuffer(): wrote " + Stringify(write_ret) + " bytes, but requested "
        + Stringify(write_bytes) + " bytes.", true);
  }
}

/**************************************************************************************************************************************************************
 * DocumentMapReader
 *
 **************************************************************************************************************************************************************/
DocumentMapReader::DocumentMapReader(const char* basic_document_map_filename, const char* extended_document_map_filename) :
  basic_doc_map_fd_(open(basic_document_map_filename, O_RDONLY)),
  extended_doc_map_fd_(open(extended_document_map_filename, O_RDONLY)),
  basic_doc_map_buffer_size_(BasicDocMapSize()),
  basic_doc_map_buffer_(new DocMapEntry[basic_doc_map_buffer_size_]),
  doc_id_map_(NULL),
  doc_id_map_size_(0) {
  int read_bytes = sizeof(*basic_doc_map_buffer_) * basic_doc_map_buffer_size_;
  ssize_t read_ret = read(basic_doc_map_fd_, basic_doc_map_buffer_, read_bytes);
  if (read_ret < 0) {
    GetErrorLogger().LogErrno("read() in DocumentMapReader::DocumentMapReader()", errno, true);
  } else if (read_ret != read_bytes) {
    GetErrorLogger().Log("read() in DocumentMapReader::DocumentMapReader(): read " + Stringify(read_ret) + " bytes, but requested " + Stringify(read_bytes)
        + " bytes.", true);
  }
}

DocumentMapReader::~DocumentMapReader() {
  delete[] basic_doc_map_buffer_;
  int close_ret;
  close_ret = close(basic_doc_map_fd_);
  assert(close_ret != -1);
  close_ret = close(extended_doc_map_fd_);
  assert(close_ret != -1);
}

int DocumentMapReader::BasicDocMapSize() {
  struct stat stat_buf;
  if (fstat(basic_doc_map_fd_, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in DocumentMapReader::BasicDocMapSize()", errno, true);
  }

  assert(stat_buf.st_size % sizeof(*basic_doc_map_buffer_) == 0);
  return stat_buf.st_size / sizeof(*basic_doc_map_buffer_);
}

string DocumentMapReader::DecodeDocumentExtendedInfo(uint32_t doc_id, ExtendedInfoComponent component) const {
  off_t seek_ret = lseek(extended_doc_map_fd_, basic_doc_map_buffer_[doc_id].extended_file_offset, SEEK_SET);
  if (seek_ret < 0) {
    GetErrorLogger().LogErrno("lseek() in DocumentMapReader::GetDocumentUrl()", errno, true);
  }
  assert(seek_ret == basic_doc_map_buffer_[doc_id].extended_file_offset);

  ssize_t read_ret;

  int docnum_len;
  read_ret = read(extended_doc_map_fd_, &docnum_len, sizeof(docnum_len));
  if (read_ret < 0) {
    GetErrorLogger().LogErrno("read() in DocumentMapReader::DecodeDocumentExtendedInfo()", errno, true);
  } else if (read_ret != sizeof(docnum_len)) {
    GetErrorLogger().Log("read() in DocumentMapReader::DecodeDocumentExtendedInfo(): read " + Stringify(read_ret) + " bytes, but requested "
        + Stringify(sizeof(docnum_len)) + " bytes.", true);
  }

#ifdef DOCUMENT_MAP_DEBUG
  cout << "docnum_len: " << docnum_len << endl;
#endif

  char* docnum_buffer = new char[docnum_len];
  read_ret = read(extended_doc_map_fd_, docnum_buffer, docnum_len);
  if (read_ret < 0) {
    GetErrorLogger().LogErrno("read() in DocumentMapReader::DecodeDocumentExtendedInfo()", errno, true);
  } else if (read_ret != docnum_len) {
    GetErrorLogger().Log("read() in DocumentMapReader::DecodeDocumentExtendedInfo(): read " + Stringify(read_ret) + " bytes, but requested "
        + Stringify(docnum_len) + " bytes.", true);
  }

#ifdef DOCUMENT_MAP_DEBUG
  cout << "docnum_buffer: " << string(docnum_buffer, docnum_len) << endl;
#endif

  string docnum_str = string(docnum_buffer, docnum_len);
  delete[] docnum_buffer;

  int url_len;
  read_ret = read(extended_doc_map_fd_, &url_len, sizeof(url_len));
  if (read_ret < 0) {
    GetErrorLogger().LogErrno("read() in DocumentMapReader::DecodeDocumentExtendedInfo()", errno, true);
  } else if (read_ret != sizeof(url_len)) {
    GetErrorLogger().Log("read() in DocumentMapReader::DecodeDocumentExtendedInfo(): read " + Stringify(read_ret) + " bytes, but requested "
        + Stringify(sizeof(url_len)) + " bytes.", true);
  }

#ifdef DOCUMENT_MAP_DEBUG
  cout << "url_len: " << url_len << endl;
#endif

  char* url_buffer = new char[url_len];
  read_ret = read(extended_doc_map_fd_, url_buffer, url_len);
  if (read_ret < 0) {
    GetErrorLogger().LogErrno("read() in DocumentMapReader::DecodeDocumentExtendedInfo()", errno, true);
  } else if (read_ret != url_len) {
    GetErrorLogger().Log("read() in DocumentMapReader::DecodeDocumentExtendedInfo(): read " + Stringify(read_ret) + " bytes, but requested "
        + Stringify(url_len) + " bytes.", true);
  }

#ifdef DOCUMENT_MAP_DEBUG
  cout << "url_buffer: " << string(url_buffer, url_len) << endl;
#endif

  string url_str = string(url_buffer, url_len);
  delete[] url_buffer;

  switch(component) {
    case kDocNum:
      return docnum_str;
    case kDocUrl:
      return url_str;
    default:
      assert(false);
      return url_str;
  }
}

void DocumentMapReader::LoadRemappingTranslationTable(const char* doc_id_map_filename) {
  // Scan docID remapping file, and find the largest docID that we have to deal with.
  ifstream doc_id_mapping_stream(doc_id_map_filename);
  if (!doc_id_mapping_stream) {
    GetErrorLogger().Log("Could not open docID remapping file '" + string(doc_id_map_filename) + "'.", true);
  }

  string curr_line;
  istringstream curr_line_stream;
  uint32_t curr_doc_id, remapped_doc_id;
  uint32_t min_curr_doc_id = numeric_limits<uint32_t>::max();
  uint32_t min_remapped_doc_id = numeric_limits<uint32_t>::max();
  uint32_t max_curr_doc_id = 0;
  uint32_t max_remapped_doc_id = 0;

  while (getline(doc_id_mapping_stream, curr_line)) {
    curr_line_stream.str(curr_line);
    curr_line_stream >> curr_doc_id >> remapped_doc_id;

    if (curr_doc_id < min_curr_doc_id)
      min_curr_doc_id = curr_doc_id;

    if (remapped_doc_id < min_remapped_doc_id)
      min_remapped_doc_id = remapped_doc_id;

    if (curr_doc_id > max_curr_doc_id)
      max_curr_doc_id = curr_doc_id;

    if (remapped_doc_id > max_remapped_doc_id)
      max_remapped_doc_id = remapped_doc_id;
  }

  // Reset stream to the beginning.
  doc_id_mapping_stream.clear();
  doc_id_mapping_stream.seekg(0, ios::beg);

  // Create one array, indexed by the old docID.
  doc_id_map_size_ = max_curr_doc_id;
  doc_id_map_ = new uint32_t[doc_id_map_size_];

  while (getline(doc_id_mapping_stream, curr_line)) {
    curr_line_stream.str(curr_line);
    curr_line_stream >> curr_doc_id >> remapped_doc_id;
    doc_id_map_[curr_doc_id] = remapped_doc_id;
  }
}

string DocumentMapReader::GetDocumentUrl(uint32_t doc_id) const {
  return DecodeDocumentExtendedInfo((doc_id_map_ != NULL) ? doc_id_map_[doc_id] : doc_id, kDocUrl);
}

string DocumentMapReader::GetDocumentNumber(uint32_t doc_id) const {
  return DecodeDocumentExtendedInfo((doc_id_map_ != NULL) ? doc_id_map_[doc_id] : doc_id, kDocNum);
}
