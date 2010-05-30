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
// Contains 'main'. Starting point for exploring the program.
//
// High Priority:
// TODO: Abstract away compression/decompression interfaces, including for the block header.
// TODO: Create a 'NULL' compressor/decompressor interface; that is, it doesn't perform any compression.
// TODO: Implement "checkpointing"; if the indexer is killed or even crashes, it should be able to start again without re-indexing everything.
// TODO: Allow batch queries to be executed by reading from stdin.
// TODO: The index cat and diff utilities should fully decode positions (right now it's outputting gap coded ones).
// TODO: If we allow overlapping docIDs during merge (overlapping over several indices), then when merging positions, must fully decode them for each index
//       first and then gap code them back again.
// TODO: Investigate why zettair and IR Toolkit differ by 1 in some docIDs on certain queries.
// TODO: Need to make sure the term hash table is of appropriate size during querying. Do it based on number of unique words in the index.
// TODO: Consider outputting intermediate indices with crappier (but faster compression). In this case, compression speed is also important. This should result
//       in faster merging and index building. Final index generation should use good compression methods. (Make it configurable). Can also try benchmarking
//       indexing and merging speed against zettair, since comparison would be more fair (use varbyte coding for the index).
// TODO: Allow configuration for user to specify how much memory to use while merging. Then system can pick buffer sizes, and appropriate merge degree.
//       Choose merge degree so that we can merge in as few passes as possible, and so that every pass merges approximately the same amount of indices.
//       I suspect this would result in better processor cache usage, since heaps will be smaller, and less buffers.
// TODO: Ignore blank lines in list of document collection files to process, and don't stop when a file is missing, but output a warning message.
//
// Low Priority:
// TODO: Detect whether a document collection is gzipped or not and automatically uncompress it or just load it into memory.
// TODO: Might want to limit the number of index files per directory by placing them in numbered directories.
// TODO: Consider creating a zlib-esque interface for the compression toolkit.
// TODO: What about doing an in-place merge? Since we already use 64KB blocks, it might be helpful.
// TODO: Might be a good idea to build separate binaries for indexing, querying, merging, cat, diff, etc. This way is cleaner because we don't need to
//       initialize static variables that we won't use in a particular mode.
//==============================================================================================================================================================

#include "ir_toolkit.h"

#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <stdint.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <getopt.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <zlib.h>

#include "cache_manager.h"
#include "configuration.h"
#include "document_collection.h"
#include "globals.h"
#include "index_cat.h"
#include "index_diff.h"
#include "index_merge.h"
#include "key_value_store.h"
#include "logger.h"
#include "query_processor.h"
#include "test_compression.h"
#include "timer.h"
using namespace std;
using namespace logger;

IndexCollection& GetIndexCollection() {
  static IndexCollection index_collection;
  return index_collection;
}

// TODO: Proper cleanup needed, depending on what mode the program is running in. Delete incomplete indices, etc.
void SignalHandler(int sig) {
  GetDefaultLogger().Log("Received termination request. Cleaning up now...", false);

  IndexCollection& index_collection = GetIndexCollection();
  index_collection.OutputDocumentCollectionDocIdRanges(document_collections_doc_id_ranges_filename);

  PostingCollectionController& posting_collection_controller = GetPostingCollectionController();
  // FIXME: It's possible that the parser callback will call this simultaneously as we're cleaning up.
  posting_collection_controller.Finish();

  exit(0);
}

void InstallSignalHandler() {
  struct sigaction sig_action;
  sig_action.sa_flags = 0;
  // Mask SIGINT.
  sigemptyset(&sig_action.sa_mask);
  sigaddset(&sig_action.sa_mask, SIGINT);
  sig_action.sa_handler = SignalHandler;
  sigaction(SIGINT, &sig_action, 0);
}

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
  if ((fd = open(file_path, O_RDONLY)) == -1) {
    GetErrorLogger().LogErrno("open() in UncompressFile()", errno, true);
  }

  struct stat stat_buf;
  if (fstat(fd, &stat_buf) == -1) {
    GetErrorLogger().LogErrno("fstat() in UncompressFile()", errno, true);
  }

  void *src;
  if ((src = mmap(0, stat_buf.st_size, PROT_READ, MAP_SHARED, fd, 0)) == MAP_FAILED) {
    GetErrorLogger().LogErrno("mmap() in UncompressFile()", errno, true);
  }

  // Aliases are permitted for types that only differ by sign.
  int ret = Uncompress(reinterpret_cast<unsigned char*> (src), stat_buf.st_size, reinterpret_cast<unsigned char**> (dest), dest_size, dest_len);
  if (ret != Z_OK) {
    GetErrorLogger().Log("zlib error in UncompressFile(): " + string(zerr(ret)), true);
  }

  if (munmap(src, stat_buf.st_size) == -1) {
    GetErrorLogger().LogErrno("munmap() in UncompressFile()", errno, true);
  }

  if (close(fd) == -1) {
    GetErrorLogger().LogErrno("close() in UncompressFile()", errno, true);
  }
}

void Init() {
  InstallSignalHandler();

#ifndef NDEBUG
  cout << "Assertions are enabled." << endl;
#endif
}

void ProcessInputDocumentCollections(IndexCollection& index_collection) {
  index_collection.ProcessDocumentCollections(cin);
}

void Query(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename,
           QueryProcessor::QueryFormat query_mode) {
  GetDefaultLogger().Log("Starting query processor with index file '" + Stringify(index_filename) + "', " + "lexicon file '" + Stringify(lexicon_filename)
      + "', " + "document map file '" + Stringify(doc_map_filename) + "', and " + "meta file '" + Stringify(meta_info_filename) + "'.", false);

  QueryProcessor* query_processor = new QueryProcessor(index_filename, lexicon_filename, doc_map_filename, meta_info_filename, query_mode);
  delete query_processor;
}

void Index() {
  GetDefaultLogger().Log("Indexing document collection...", false);

  IndexCollection& index_collection = GetIndexCollection();
  // Input to the indexer is a list of document collection files we want to index in order.
  ProcessInputDocumentCollections(index_collection);

  // Start timing indexing process.
  Timer index_time;
  index_collection.ParseTrec();
  GetDefaultLogger().Log("Time Elapsed: " + Stringify(index_time.GetElapsedTime()), false);

  index_collection.OutputDocumentCollectionDocIdRanges(document_collections_doc_id_ranges_filename);

  uint64_t posting_count = GetPostingCollectionController().posting_count();

  cout << "Collection Statistics:\n";
  cout << "total posting count: " << posting_count << "\n";
  cout << "total number of documents indexed: " << index_collection.doc_id() << endl;
}

// TODO: Merger should have capability to read files to be used for merging from stdin (they should be sorted alphanumerically though
//       (this is assuming merger does not take into account index offsets)).
// TODO: User should specify how much memory to use for merging; the system can calculate an optimal merge degree.
void Merge(int merge_degree) {
  DIR* dir;
  if ((dir = opendir(".")) == NULL) {
    GetErrorLogger().Log("Could not open directory to access files to merge.", true);
    return;
  }

  int num_indices = 0;
  struct dirent* entry;
  while ((entry = readdir(dir)) != NULL) {
    const char initial_index_prefix[] = "index.idx.0";  // Just checks for the presence of the index files.
    int idx_file = strncmp(entry->d_name, initial_index_prefix, sizeof(initial_index_prefix) - 1);
    if (idx_file == 0) {
      ++num_indices;
    }
  }

  closedir(dir);

  const int kDefaultMergeDegree = 64;
  CollectionMerger merger(num_indices, (merge_degree <= 0 ? kDefaultMergeDegree : merge_degree));
}

// TODO: Just for testing.
void TestMerge() {
  vector<IndexFiles> input_index_files;
  for (int i = 0; i < 2; ++i) {
    IndexFiles index_files(1, i);
    input_index_files.push_back(index_files);
  }

  CollectionMerger merger(input_index_files, 64);
}

void Cat(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename, const char* term, int term_len) {
  IndexFiles index_files(index_filename, lexicon_filename, doc_map_filename, meta_info_filename);
  IndexCat index_cat(index_files);
  index_cat.Cat(term, term_len);
}

void Diff(const char* index1_filename, const char* lexicon1_filename, const char* doc_map1_filename, const char* meta_info1_filename,
          const char* index2_filename, const char* lexicon2_filename, const char* doc_map2_filename, const char* meta_info2_filename,
          const char* term, int term_len) {
  IndexFiles index_files1(index1_filename, lexicon1_filename, doc_map1_filename, meta_info1_filename);
  IndexFiles index_files2(index2_filename, lexicon2_filename, doc_map2_filename, meta_info2_filename);

  IndexDiff index_diff(index_files1, index_files2);
  index_diff.Diff(term, term_len);
}

// Displays usage information.
void Help() {
  cout << "To index: irtk --index\n";
  cout << "To merge: irtk --merge\n";
  cout << "  options:\n";
  cout << "    --merge-degree: specify the merge degree to use (default: 64)\n";
  cout << "To query: irtk --query [INDEX_FILENAME] [LEXICON_FILENAME] [DOCUMENT_MAP_FILENAME] [META_FILENAME]\n";
  cout << "To cat: irtk --cat [INDEX_FILENAME] [LEXICON_FILENAME] [DOCUMENT_MAP_FILENAME] [META_FILENAME]\n";
  cout << "To diff: irtk --diff [INDEX1_FILENAME] [LEXICON1_FILENAME] [DOCUMENT_MAP1_FILENAME] [META1_FILENAME] [INDEX2_FILENAME] [LEXICON2_FILENAME] [DOCUMENT2_MAP_FILENAME] [META2_FILENAME]\n";
  cout << "To run compression tests: irtk --test-compression\n";
  cout << "To test a particular coder: irtk --test-coder [rice, turbo-rice, pfor, s9, s16, vbyte]\n";
  cout << endl;
}

struct CommandLineArgs {
  CommandLineArgs() :
    mode(kNoIdea), index1_filename("index.idx"), lexicon1_filename("index.lex"), doc_map1_filename("index.dmap"), meta_info1_filename("index.meta"),
        index2_filename("index.idx"), lexicon2_filename("index.lex"), doc_map2_filename("index.dmap"), meta_info2_filename("index.meta"), merge_degree(0),
        cat_diff_term(NULL), cat_diff_term_len(0), query_mode(QueryProcessor::kInteractive) {
  }

  ~CommandLineArgs() {
    delete[] cat_diff_term;
  }

  enum Mode {
    kIndex, kMerge, kQuery, kCat, kDiff, kNoIdea
  };

  Mode mode;
  const char* index1_filename;
  const char* lexicon1_filename;
  const char* doc_map1_filename;
  const char* meta_info1_filename;

  const char* index2_filename;
  const char* lexicon2_filename;
  const char* doc_map2_filename;
  const char* meta_info2_filename;

  int merge_degree;

  char* cat_diff_term;
  int cat_diff_term_len;

  QueryProcessor::QueryFormat query_mode;
};

int main(int argc, char** argv) {
  CommandLineArgs command_line_args;

  const char* opt_string = "imqcdth";
  const struct option long_opts[] = { { "index", no_argument, NULL, 'i' },
                                      { "merge", no_argument, NULL, 'm' },
                                      { "merge-degree", required_argument, NULL, 0 },
                                      { "query", no_argument, NULL, 'q' },
                                      { "query-mode", required_argument, NULL, 0 },
                                      { "cat", no_argument, NULL, 'c' },
                                      { "cat-term", required_argument, NULL, 0 },
                                      { "diff", no_argument, NULL, 'd' },
                                      { "diff-term", required_argument, NULL, 0 },
                                      { "test-compression", no_argument, NULL, 't' },
                                      { "test-coder", required_argument, NULL, 0 },
                                      { "help", no_argument, NULL, 'h' },
                                      { NULL, no_argument, NULL, 0 } };

  int opt, long_index;
  while ((opt = getopt_long(argc, argv, opt_string, long_opts, &long_index)) != -1) {
    switch (opt) {
      case 'i':
        command_line_args.mode = CommandLineArgs::kIndex;
        break;

      case 'm':
        command_line_args.mode = CommandLineArgs::kMerge;
        break;

      case 'q':
        command_line_args.mode = CommandLineArgs::kQuery;
        break;

      case 'c':
        command_line_args.mode = CommandLineArgs::kCat;
        break;

      case 'd':
        command_line_args.mode = CommandLineArgs::kDiff;
        break;

      case 't':
        TestCompression();
        return EXIT_SUCCESS;

      case 'h':
        Help();
        return EXIT_SUCCESS;

      case 0:
        // Process options which do not have a short arg.
        if (strcmp("merge-degree", long_opts[long_index].name) == 0) {
          command_line_args.merge_degree = atoi(optarg);
        } else if (strcmp("query-mode", long_opts[long_index].name) == 0) {
          if (strcmp("interactive", optarg) == 0)
            command_line_args.query_mode = QueryProcessor::kInteractive;
          else if (strcmp("interactive-single", optarg) == 0)
            command_line_args.query_mode = QueryProcessor::kInteractiveSingle;
          else if (strcmp("batch", optarg) == 0)
            command_line_args.query_mode = QueryProcessor::kBatch;
        } else if (strcmp("cat-term", long_opts[long_index].name) == 0 || strcmp("diff-term", long_opts[long_index].name) == 0) {
          command_line_args.cat_diff_term_len = strlen(optarg);
          command_line_args.cat_diff_term = new char[command_line_args.cat_diff_term_len];
          memcpy(command_line_args.cat_diff_term, optarg, command_line_args.cat_diff_term_len);
        } else if (strcmp("test-coder", long_opts[long_index].name) == 0) {
          TestCoder(optarg);
          return EXIT_SUCCESS;
        }
        break;

      default:
        cout << "Run with '--help' for more information." << endl;
        return EXIT_SUCCESS;
    }
  }

  char** input_files = argv + optind;
  int num_input_files = argc - optind;

  if (command_line_args.mode == CommandLineArgs::kQuery || command_line_args.mode == CommandLineArgs::kCat || command_line_args.mode == CommandLineArgs::kDiff) {
    for (int i = 0; i < num_input_files; ++i) {
      switch (i) {
        case 0:
          command_line_args.index1_filename = input_files[i];
          break;
        case 1:
          command_line_args.lexicon1_filename = input_files[i];
          break;
        case 2:
          command_line_args.doc_map1_filename = input_files[i];
          break;
        case 3:
          command_line_args.meta_info1_filename = input_files[i];
          break;

        case 4:
          command_line_args.index2_filename = input_files[i];
          break;
        case 5:
          command_line_args.lexicon2_filename = input_files[i];
          break;
        case 6:
          command_line_args.doc_map2_filename = input_files[i];
          break;
        case 7:
          command_line_args.meta_info2_filename = input_files[i];
          break;
      }
    }
  }

  Init();
  srand(time(NULL));

  switch (command_line_args.mode) {
    case CommandLineArgs::kIndex:
      Index();
      break;
    case CommandLineArgs::kQuery:
      Query(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename, command_line_args.meta_info1_filename,
            command_line_args.query_mode);
      break;
    case CommandLineArgs::kMerge:
      Merge(command_line_args.merge_degree);
      break;
    case CommandLineArgs::kCat:
      Cat(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename, command_line_args.meta_info1_filename,
          command_line_args.cat_diff_term, command_line_args.cat_diff_term_len);
      break;
    case CommandLineArgs::kDiff:
      Diff(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename, command_line_args.meta_info1_filename,
           command_line_args.index2_filename, command_line_args.lexicon2_filename, command_line_args.doc_map2_filename, command_line_args.meta_info2_filename,
           command_line_args.cat_diff_term, command_line_args.cat_diff_term_len);
      break;
    default:
      Help();
      break;
  }

  return EXIT_SUCCESS;
}
