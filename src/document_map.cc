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

#include "globals.h"
#include "logger.h"
using namespace std;

/**************************************************************************************************************************************************************
 * DocumentMapWriter
 *
 **************************************************************************************************************************************************************/
DocumentMapWriter::DocumentMapWriter(const char* document_map_filename_lengths, const char* document_map_filename_urls) :
    kDocMapBufferSize(2097152),
    doc_map_fd_lengths_(open(document_map_filename_lengths, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)),
    doc_map_buffer_(new DocMapEntry[kDocMapBufferSize]), doc_map_buffer_len_(0),

    kUrlsBufferSize(33554432),
    doc_map_fd_urls_(open(document_map_filename_urls, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)),
    urls_buffer_(new char[kUrlsBufferSize]), urls_buffer_len_(0) {

}

DocumentMapWriter::~DocumentMapWriter() {
  DumpBuffer();
  DumpBufferUrls();
  delete[] doc_map_buffer_;
  delete[] urls_buffer_;
  close(doc_map_fd_lengths_);  //TODO: Check return value.
  close(doc_map_fd_urls_);  //TODO: Check return value.
}

void DocumentMapWriter::AddDocMapEntry(const DocMapEntry& doc_map_entry) {
  assert(doc_map_buffer_len_ < kDocMapBufferSize);

  doc_map_buffer_[doc_map_buffer_len_++] = doc_map_entry;
  if(doc_map_buffer_len_ == kDocMapBufferSize) {
    DumpBuffer();
    doc_map_buffer_len_ = 0;
  }
}

void DocumentMapWriter::DumpBuffer() {
  int write_ret = write(doc_map_fd_lengths_, doc_map_buffer_, sizeof(*doc_map_buffer_) * doc_map_buffer_len_);

  if(write_ret < 0) {
    GetErrorLogger().Log("write() oops", true);
  }
}

void DocumentMapWriter::AddUrl(const char* url, int url_len) {
  if (urls_buffer_len_ + static_cast<int> (sizeof(url_len)) + url_len > kUrlsBufferSize) {
    DumpBufferUrls();
    urls_buffer_len_ = 0;
  }

  memcpy(urls_buffer_ + urls_buffer_len_, &url_len, sizeof(url_len));
  urls_buffer_len_ += sizeof(url_len);

  memcpy(urls_buffer_ + urls_buffer_len_, url, url_len);
  urls_buffer_len_ += url_len;
}

void DocumentMapWriter::DumpBufferUrls() {
  int write_ret = write(doc_map_fd_urls_, urls_buffer_, sizeof(*urls_buffer_) * urls_buffer_len_);

  if(write_ret < 0) {
    GetErrorLogger().Log("write() oops", true);
  }
}

/**************************************************************************************************************************************************************
 * DocumentMapReader
 *
 **************************************************************************************************************************************************************/
DocumentMapReader::DocumentMapReader(const char* document_map_filename) :
  doc_map_fd_(open(document_map_filename, O_RDONLY)), doc_map_buffer_size_(DocMapSize()),
  doc_map_buffer_(new DocMapEntry[doc_map_buffer_size_]) {
  int read_ret = read(doc_map_fd_, doc_map_buffer_, doc_map_buffer_size_ * sizeof(*doc_map_buffer_));
  if (read_ret == -1)
    assert(false);
}

DocumentMapReader::~DocumentMapReader() {
  delete[] doc_map_buffer_;
  close(doc_map_fd_);
}

int DocumentMapReader::DocMapSize() {
  struct stat stat_buf;
  if (fstat(doc_map_fd_, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in DocumentMapReader::DocMapSize()", errno, true);
  }

//  cout << "stat_buf.st_size: " << stat_buf.st_size << endl;
//  cout << "sizeof(*doc_map_buffer_): " << sizeof(*doc_map_buffer_) << endl;

  assert(stat_buf.st_size % sizeof(*doc_map_buffer_) == 0);
  return stat_buf.st_size / sizeof(*doc_map_buffer_);
}
