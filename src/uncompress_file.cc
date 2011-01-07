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

#include "uncompress_file.h"

#include <cassert>
#include <cerrno>
#include <cstring>

#include <string>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <zlib.h>

#include "globals.h"
#include "logger.h"
using namespace std;

const char* zerr(int ret) {
  switch (ret) {
    case Z_STREAM_ERROR:
      return "invalid compression level";
      break;
    case Z_DATA_ERROR:
      return "invalid or incomplete deflate data";
      break;
    case Z_MEM_ERROR:
      return "out of memory";
      break;
    case Z_VERSION_ERROR:
      return "zlib version mismatch!";
    default:
      assert(false);
      return "";
  }
}

// Uses zlib to decompress files with either zlib or gzip headers.
int Uncompress(const unsigned char* src, int src_len, unsigned char** dest, int* dest_size, int* dest_len) {
  assert(src != NULL);
  assert(src_len > 0);
  assert(dest != NULL && *dest != NULL);
  assert(dest_size != NULL && *dest_size > 0);
  assert(dest_len != NULL);

  const unsigned int kWindowBits = 15;  // Maximum window bits.
  const unsigned int kHeaderType = 32;  // zlib and gzip decoding with automatic header detection.

  int err;
  z_stream stream;
  stream.next_in = const_cast<Bytef*> (src);
  stream.avail_in = static_cast<uInt> (src_len);
  stream.next_out = static_cast<Bytef*> (*dest);
  stream.avail_out = static_cast<uInt> (*dest_size);
  stream.zalloc = Z_NULL;
  stream.zfree = Z_NULL;

  err = inflateInit2(&stream, kWindowBits + kHeaderType);
  if (err != Z_OK)
    return err;

  // Double our destination buffer if not enough space.
  while ((err = inflate(&stream, Z_FINISH)) == Z_BUF_ERROR && stream.avail_in != 0) {
    stream.avail_out += static_cast<uInt> (*dest_size);
    unsigned char* new_dest = new unsigned char[*dest_size *= 2];
    memcpy(new_dest, *dest, stream.total_out);
    delete[] *dest;
    *dest = new_dest;
    stream.next_out = static_cast<Bytef*> (*dest + stream.total_out);
  }

  if (err != Z_STREAM_END) {
    inflateEnd(&stream);
    if (err == Z_NEED_DICT || (err == Z_BUF_ERROR && stream.avail_in == 0))
      return Z_DATA_ERROR;
    return err;
  }

  *dest_len = stream.total_out;
  err = inflateEnd(&stream);
  return err;
}

// Memory maps the input file for faster reading since we can read directly from the kernel buffer.
void UncompressFile(const char* file_path, char** dest, int* dest_size, int* dest_len) {
  assert(file_path != NULL);
  assert(dest != NULL && *dest != NULL);
  assert(dest_size != NULL && dest_size > 0);
  assert(dest_len != NULL);

  int fd;
  if ((fd = open(file_path, O_RDONLY)) < 0) {
    GetErrorLogger().LogErrno("open() in UncompressFile()", errno, true);
  }

  struct stat stat_buf;
  if (fstat(fd, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in UncompressFile()", errno, true);
  }

  void* src;
  if ((src = mmap(0, stat_buf.st_size, PROT_READ, MAP_SHARED, fd, 0)) == MAP_FAILED) {
    GetErrorLogger().LogErrno("mmap() in UncompressFile()", errno, true);
  }

  // Aliases are permitted for types that only differ by sign.
  int ret = Uncompress(reinterpret_cast<unsigned char*> (src), stat_buf.st_size, reinterpret_cast<unsigned char**> (dest), dest_size, dest_len);
  if (ret != Z_OK) {
    GetErrorLogger().Log("zlib error in UncompressFile(): " + string(zerr(ret)), true);
  }

  if (munmap(src, stat_buf.st_size) < 0) {
    GetErrorLogger().LogErrno("munmap() in UncompressFile()", errno, true);
  }

  if (close(fd) < 0) {
    GetErrorLogger().LogErrno("close() in UncompressFile()", errno, true);
  }
}
