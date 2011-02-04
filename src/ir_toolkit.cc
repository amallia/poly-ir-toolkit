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
// TODO: Implement parallel indexing capability. Have command line options to specify the number of processes we want and fork off several irtk processes.
//       Each process should create a unique folder for outputting index files (perhaps using the pid for the folder name).
//       The parent that sets this up can split the files to index for each child process and pipe them into each child (through I/O redirection) and then die.
//       Things like merging can be done independently within each folder. The final merge would have to be done by specifying the indices in each folder.
//       Note that each process would need to record the docID offset for it's index. The final merge would have to take the offsets into account.
//
// Minor Improvements:
// TODO: While doing merging, set the optimized flag that it's a single term query.
// TODO: If you can't merge files, don't exit, but return an error code, and print a warning message.
// TODO: During query processing, if we request more blocks than there are in the index, an assertion should be activated. Might want to check this...
// TODO: Check what happens when passing an empty index (an empty file) to query, merge, etc...
// TODO: When the cache is full and you have a cache miss, it might be wise to evict a bunch of blocks (30% in one paper) to amortize the eviction cost.
// TODO: Need to make sure the term hash table is of appropriate size during querying. Do it based on number of unique words in the index.
//
// Low Priority:
// TODO: Implement "checkpointing"; if the indexer is killed or even crashes, it should be able to start again without re-indexing everything.
// TODO: The index cat and diff utilities should fully decode positions (right now it's outputting gap coded ones).
// TODO: If we allow overlapping docIDs during merge (overlapping over several indices), then when merging positions, must fully decode them for each index
//       first and then gap code them back again.
// TODO: Allow configuration for user to specify how much memory to use while merging. Then system can pick buffer sizes, and appropriate merge degree.
//       Choose merge degree so that we can merge in as few passes as possible, and so that every pass merges approximately the same amount of indices.
//       I suspect this would result in better processor cache usage, since heaps will be smaller, and less buffers.
// TODO: Detect whether a document collection is gzipped or not and automatically uncompress it or just load it into memory.
// TODO: Might want to limit the number of index files per directory by placing them in numbered directories.
// TODO: What about doing an in-place merge? Since we already use 64KB blocks, it might be helpful.
// TODO: Might be a good idea to build separate binaries for indexing, querying, merging, cat, diff, etc. This way is cleaner because we don't need to
//       initialize static variables that we won't use in a particular mode.
// TODO: It would be good to support layered indices in all our index operations (aside from just querying).
// TODO: Include the option to use a stop list during indexing (and query time).
// TODO: Would be neat to have an index split class, that would take an index and break it down into manageable pieces; these can then be used for other
//       I/O efficient operations --- such as merge, layer, etc.
// TODO: Allow use of document mapping file during indexing and/or merging stages.
//
// Notes:
// The BM25 formula used here modifies the IDF component to always be positive, by adding 1 to the log() function. I found that this is especially necessary
// during the implementation of the TAAT with early termination query processing mode.
//==============================================================================================================================================================

#include "ir_toolkit.h"

#include <cassert>
#include <cctype>
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
#include <getopt.h>
#include <libgen.h>
#include <signal.h>

//TODO: Play around with SIMD extensions.
#include <mmintrin.h>   // MMX
#include <xmmintrin.h>  // SSE
#include <emmintrin.h>  // SSE2
#include <pmmintrin.h>  // SSE3

#include "cache_manager.h"
#include "config_file_properties.h"
#include "configuration.h"
#include "document_collection.h"
#include "globals.h"
#include "index_cat.h"
#include "index_diff.h"
#include "index_layerify.h"
#include "index_merge.h"
#include "index_reader.h"
#include "index_remapper.h"
#include "key_value_store.h"
#include "logger.h"
#include "query_processor.h"
#include "test_compression.h"
#include "timer.h"
using namespace std;

struct CommandLineArgs {
  CommandLineArgs() :
    mode(kNoIdea),
    index1_filename("index.idx"),
    lexicon1_filename("index.lex"),
    doc_map1_filename("index.dmap"),
    meta_info1_filename("index.meta"),
    external_index1_filename("index.ext"),
    index2_filename("index.idx"),
    lexicon2_filename("index.lex"),
    doc_map2_filename("index.dmap"),
    meta_info2_filename("index.meta"),
    external_index2_filename("index.ext"),
    merge_degree(0),
    term(NULL),
    term_len(0),
    in_memory_index(false),
    doc_mapping_file(NULL),
    query_stop_words_list_file(NULL),
    query_algorithm(QueryProcessor::kDefault),
    query_mode(QueryProcessor::kInteractive),
    result_format(QueryProcessor::kNormal) {
  }

  ~CommandLineArgs() {
    delete[] term;
    delete[] doc_mapping_file;
    delete[] query_stop_words_list_file;
  }

  enum Mode {
    kIndex, kMergeInitial, kMergeInput, kQuery, kRemap, kLayerify, kCat, kDiff, kRetrieveIndexData, kLoopOverIndexData, kNoIdea
  };

  Mode mode;
  const char* index1_filename;
  const char* lexicon1_filename;
  const char* doc_map1_filename;
  const char* meta_info1_filename;
  const char* external_index1_filename;

  const char* index2_filename;
  const char* lexicon2_filename;
  const char* doc_map2_filename;
  const char* meta_info2_filename;
  const char* external_index2_filename;

  int merge_degree;

  char* term;
  int term_len;

  bool in_memory_index;
  bool memory_mapped_index;

  char* doc_mapping_file;

  char* query_stop_words_list_file;

  QueryProcessor::QueryAlgorithm query_algorithm;
  QueryProcessor::QueryMode query_mode;
  QueryProcessor::ResultFormat result_format;
};
static CommandLineArgs command_line_args;

static const char document_collections_doc_id_ranges_filename[] = "document_collections_doc_id_ranges";

CollectionIndexer& GetCollectionIndexer() {
  static CollectionIndexer collection_indexer;
  return collection_indexer;
}

void SignalHandlerIndex(int sig) {
/*  GetDefaultLogger().Log("Received termination request. Cleaning up now...", false);

  CollectionIndexer& collection_indexer = GetCollectionIndexer();
  collection_indexer.OutputDocumentCollectionDocIdRanges(document_collections_doc_id_ranges_filename);

  PostingCollectionController& posting_collection_controller = GetPostingCollectionController();
  // FIXME: It's possible that the parser callback will call this simultaneously as we're cleaning up.
  //        Set some special variable in class that's feeding the parser to indicate it to finish up.
  posting_collection_controller.Finish();*/

  exit(0);
}

// TODO: Proper cleanup needed, depending on what mode the program is running in. Delete incomplete indices, etc. Be careful about overwriting indices.
void InstallSignalHandler() {
  struct sigaction sig_action;
  sig_action.sa_flags = 0;
  // Mask SIGINT.
  sigemptyset(&sig_action.sa_mask);
  sigaddset(&sig_action.sa_mask, SIGINT);

  // Install the signal handler for the correct mode we were started in.
  switch (command_line_args.mode) {
    case CommandLineArgs::kIndex:
      sig_action.sa_handler = SignalHandlerIndex;
      break;
    default:
      sig_action.sa_handler = SIG_DFL;
      break;
  }

  sigaction(SIGINT, &sig_action, 0);
}

void Init() {
  InstallSignalHandler();

#ifndef NDEBUG
  cout << "Assertions are enabled." << endl;
#endif
}

void Query(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename,
           const char* external_index_filename, const char* stop_words_list_filename, QueryProcessor::QueryAlgorithm query_algorithm,
           QueryProcessor::QueryMode query_mode, QueryProcessor::ResultFormat result_format) {
  GetDefaultLogger().Log("Starting query processor with index file '" + Stringify(index_filename) + "', " + "lexicon file '" + Stringify(lexicon_filename)
      + "', " + "document map file '" + Stringify(doc_map_filename) + "', and " + "meta file '" + Stringify(meta_info_filename) + "'.", false);

  QueryProcessor query_processor(index_filename, lexicon_filename, doc_map_filename, meta_info_filename, external_index_filename, stop_words_list_filename,
                                 query_algorithm, query_mode, result_format);
}

void IndexCollection() {
  GetDefaultLogger().Log("Indexing document collection...", false);

  CollectionIndexer& collection_indexer = GetCollectionIndexer();
  // Input to the indexer is a list of document collection files we want to index in order.
  collection_indexer.ProcessDocumentCollections(cin);

  // Start timing indexing process.
  Timer index_time;
  collection_indexer.ParseTrec();
  GetDefaultLogger().Log("Time Elapsed: " + Stringify(index_time.GetElapsedTime()), false);

  collection_indexer.OutputDocumentCollectionDocIdRanges(document_collections_doc_id_ranges_filename);

  uint64_t posting_count = GetPostingCollectionController().posting_count();

  cout << "Collection Statistics:\n";
  cout << "total posting count: " << posting_count << "\n";
  cout << "total number of documents indexed: " << collection_indexer.doc_id() << endl;
}

// This performs the merge for the complete index starting from the initial 0.0 indices.
void MergeInitial(int merge_degree) {
  DIR* dir;
  if ((dir = opendir(".")) == NULL) {
    GetErrorLogger().LogErrno("opendir() in MergeInitial(), could not open directory to access files to merge", errno, true);
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
  const bool kDeleteMergedFiles = Configuration::GetResultValue(Configuration::GetConfiguration().GetBooleanValue(config_properties::kDeleteMergedFiles));
  CollectionMerger merger(num_indices, (merge_degree <= 0 ? kDefaultMergeDegree : merge_degree), kDeleteMergedFiles);
}

// Merges index files specified on the standard input stream.
// Each line specifies the path (relative to the working directory or absolute) to the index meta file whose associated index will be merged.
// The index, lexicon, and document map files are assumed to be similarly named (but with the correct extension) to the meta file.
// An empty line concludes the list of indices to be merged; but the line prior to the empty line specifies the output name of the meta file.
// The merged indices will be named in the same manner as the specified output meta file.
void MergeInput() {
  const bool kDeleteMergedFiles = Configuration::GetResultValue(Configuration::GetConfiguration().GetBooleanValue(config_properties::kDeleteMergedFiles));

  vector<string> input_lines;
  string curr_line;
  while (getline(cin, curr_line)) {
    input_lines.push_back(curr_line);
  }

  vector<IndexFiles> curr_group_input_index_files;
  for (vector<string>::iterator itr_i = input_lines.begin(); itr_i != input_lines.end(); ++itr_i) {
    if (itr_i->size() == 0) {
      if (curr_group_input_index_files.size() >= 2) {
        IndexFiles output_index_files = curr_group_input_index_files.back();
        curr_group_input_index_files.pop_back();

        cout << "The following files will be merged into index '" << output_index_files.meta_info_filename() << "'\n";
        for (vector<IndexFiles>::iterator itr_j = curr_group_input_index_files.begin(); itr_j != curr_group_input_index_files.end(); ++itr_j) {
          cout << itr_j->index_filename() << "\n";
        }
        cout << endl;

        CollectionMerger merger(curr_group_input_index_files, output_index_files, kDeleteMergedFiles);
      } else {
        GetErrorLogger().Log("Input must include index meta files to merge followed by the output index meta file. Skipping...", false);
      }
      curr_group_input_index_files.clear();
    } else {
      char* file = strdup(itr_i->c_str());
      char* dir = strdup(itr_i->c_str());

      string meta_filename = basename(file);
      string meta_directoryname = dirname(dir);

      bool proper_meta_filename = false;

      size_t first_dot = meta_filename.find('.');
      if (first_dot != string::npos && (first_dot + 1) < meta_filename.size()) {
        size_t second_dot = meta_filename.find('.', first_dot + 1);
        if (second_dot != string::npos && (second_dot + 1) < meta_filename.size()) {
          string group_num_str = meta_filename.substr(second_dot + 1);
          // Make sure it's an integer.
          bool proper_group_num = true;
          for (string::iterator itr_j = group_num_str.begin(); itr_j != group_num_str.end(); ++itr_j) {
            if (*itr_j == '.') {
              break;
            }
            if (!isdigit(*itr_j)) {
              proper_group_num = false;
              break;
            }
          }

          if (proper_group_num) {
            int group_num = atoi(group_num_str.c_str());

            size_t third_dot = meta_filename.find('.', second_dot + 1);
            if (third_dot != string::npos && (third_dot + 1) < meta_filename.size()) {
              string file_num_str = meta_filename.substr(third_dot + 1);
              // Make sure it's an integer.
              bool proper_file_num = true;
              for (string::iterator itr_j = file_num_str.begin(); itr_j != file_num_str.end(); ++itr_j) {
                if (*itr_j == '.') {
                  break;
                }
                if (!isdigit(*itr_j)) {
                  proper_file_num = false;
                  break;
                }
              }

              if (proper_file_num) {
                int file_num = atoi(file_num_str.c_str());

                IndexFiles curr_index_file(group_num, file_num);
                curr_index_file.SetDirectory(meta_directoryname);
                curr_group_input_index_files.push_back(curr_index_file);
                proper_meta_filename = true;
              }
            }
          }
        }
      }

      if (!proper_meta_filename) {
        GetErrorLogger().Log("Index meta filename '" + meta_filename + "' improperly named. Skipping...", false);
      }
      free(file);
      free(dir);
    }
  }

  // Merge whatever is left.
  if (curr_group_input_index_files.size() >= 2) {
    IndexFiles output_index_files = curr_group_input_index_files.back();
    curr_group_input_index_files.pop_back();

    cout << "The following files will be merged into index '" << output_index_files.meta_info_filename() << "'\n";
    for (vector<IndexFiles>::iterator itr_j = curr_group_input_index_files.begin(); itr_j != curr_group_input_index_files.end(); ++itr_j) {
      cout << itr_j->index_filename() << "\n";
    }
    cout << endl;

    CollectionMerger merger(curr_group_input_index_files, output_index_files, kDeleteMergedFiles);
  } else {
    GetErrorLogger().Log("Input must include index meta files to merge followed by the output index meta file. Skipping...", false);
  }
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

// Some sample code which allows user to retrieve index data (docIDs, frequencies, or positions) into an integer array.
void RetrieveIndexData(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename,
                       const char* term, int term_len) {
  CacheManager* cache_policy = new MergingCachePolicy(index_filename);  // Appropriate policy since we'll only be reading ahead into the index.
  IndexReader* index_reader = new IndexReader(IndexReader::kMerge, *cache_policy, lexicon_filename, doc_map_filename, meta_info_filename, true);

  // Need to read through the lexicon until we reach the term we want.
  LexiconData* lex_data;
  while ((lex_data = index_reader->lexicon().GetNextEntry()) != NULL
      && !(lex_data->term_len() == term_len && strncmp(lex_data->term(), term, min(lex_data->term_len(), term_len)) == 0)) {
    delete lex_data;
    continue;
  }

  if (lex_data == NULL) {
    cout << "No such term in index." << endl;
    return;
  }

  const int kInitialIndexDataSize = 4096;
  uint32_t index_data_chunk[kInitialIndexDataSize];

  int index_data_size = kInitialIndexDataSize;
  uint32_t* index_data = new uint32_t[index_data_size];

  ListData* list_data = index_reader->OpenList(*lex_data, 0);

  // What type of data we want to retrieve from the inverted list.
  ListData::RetrieveDataType data_type = ListData::kDocId;  // Retrieve the docIDs.

  int num_elements_stored;
  int element_offset = 0;
  int total_num_elements_stored = 0;

  Timer timer;

  // We keep looping, retrieving index data in chunks, and storing into one large array (which is resized as necessary).
  while ((num_elements_stored = list_data->GetList(data_type, index_data_chunk, kInitialIndexDataSize)) != 0) {
    assert(num_elements_stored != -1);

    total_num_elements_stored += num_elements_stored;

    if (total_num_elements_stored > index_data_size) {
      int index_data_larger_size = index_data_size * 2;
      uint32_t* index_data_larger = new uint32_t[index_data_larger_size];

      // Copy the elements from the old array into the newly resized array.
      for (int i = 0; i < element_offset; ++i) {
        index_data_larger[i] = index_data[i];
      }

      delete[] index_data;
      index_data = index_data_larger;
      index_data_size = index_data_larger_size;
    }

    // Copy the new elements into the large array.
    for (int i = 0; i < num_elements_stored; ++i) {
      index_data[element_offset + i] = index_data_chunk[i];
    }

    element_offset += num_elements_stored;
  }

  double time_elapsed = timer.GetElapsedTime();
  // Loop through and print the index data (or do whatever with it).
  for (int i = 0; i < total_num_elements_stored; ++i) {
    //cout << index_data[i] << "\n";
  }
  cout << "Index data elements retrieved: " << total_num_elements_stored << ", took " << time_elapsed << " seconds."<< endl;

  // Clean up.
  delete[] index_data;
  delete lex_data;
  delete index_reader;
  delete cache_policy;
}

// Loops over certain index data (docIDs, frequencies, or positions). Useful for testing decompression speed. Has option for loading the index completely into
// main memory before testing, or reading it from disk while traversing it.
void LoopOverIndexData(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename,
                       const char* term, int term_len, bool in_memory_index, bool memory_mapped_index) {
  CacheManager* cache_policy;
  if (memory_mapped_index) {
    cache_policy = new MemoryMappedCachePolicy(index_filename);  // Memory maps the index.
  } else if (in_memory_index) {
    cache_policy = new FullContiguousCachePolicy(index_filename);  // Loads the index fully into main memory.
  } else {
    cache_policy = new MergingCachePolicy(index_filename);  // Appropriate policy since we'll only be reading ahead into the index.
  }

  IndexReader* index_reader = new IndexReader(IndexReader::kMerge, *cache_policy, lexicon_filename, doc_map_filename, meta_info_filename, true);

  // Need to read through the lexicon until we reach the term we want.
  LexiconData* lex_data;
  while ((lex_data = index_reader->lexicon().GetNextEntry()) != NULL
      && !(lex_data->term_len() == term_len && strncmp(lex_data->term(), term, min(lex_data->term_len(), term_len)) == 0)) {
    delete lex_data;
    continue;
  }

  if (lex_data == NULL) {
    cout << "No such term in index." << endl;
    return;
  }

  ListData* list_data = index_reader->OpenList(*lex_data, 0);

  // What type of data we want to retrieve from the inverted list.
  ListData::RetrieveDataType data_type;
//  data_type = ListData::kDocId;      // Decodes only the docIDs.
  data_type = ListData::kFrequency;  // Decodes the docIDs and the frequencies.
//  data_type = ListData::kPosition;   // Decodes docIDs, frequencies, and positions (for positions, frequency must be decoded as well).

  Timer timer;
  int num_elements_retrieved = list_data->LoopOverList(data_type);
  double time_elapsed = timer.GetElapsedTime();

  cout << "Index data elements retrieved: " << num_elements_retrieved << "; took " << time_elapsed << " seconds."<< endl;
  cout << "Million integers per second: " << num_elements_retrieved / time_elapsed / 1000000 << endl;

  // Clean up.
  delete lex_data;
  delete index_reader;
  delete cache_policy;
}

// TODO: Name of map should be specified on command line with option.
//       Name of index should probably be specified as command option as well.
void RemapIndexDocIds(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename) {
  IndexRemapper index_remapper(IndexFiles(index_filename, lexicon_filename, doc_map_filename, meta_info_filename), "index_remapped");
  index_remapper.GenerateMap(command_line_args.doc_mapping_file);
  index_remapper.Remap();
}

void LayerifyIndex(const char* index_filename, const char* lexicon_filename, const char* doc_map_filename, const char* meta_info_filename) {
  LayeredIndexGenerator layered_index_generator = LayeredIndexGenerator(IndexFiles(index_filename, lexicon_filename, doc_map_filename, meta_info_filename),
                                                                        "index_layered");
  layered_index_generator.CreateLayeredIndex();
}

void GenerateUrlSortedDocIdMappingFile(const char* document_urls_filename) {
  GetDefaultLogger().Log("Generating URL sorted docID mapping file...", false);

  CollectionUrlExtractor collection_url_extractor;
  collection_url_extractor.ProcessDocumentCollections(cin);

  Timer url_extraction_time;
  collection_url_extractor.ParseTrec(document_urls_filename);
  GetDefaultLogger().Log("Time Elapsed: " + Stringify(url_extraction_time.GetElapsedTime()), false);
}

void SetConfigurationOption(string key_value) {
  size_t eq = key_value.find('=');
  if (eq != string::npos) {
    string key = key_value.substr(0, eq);
    string value = key_value.substr(eq + 1);
    bool override = Configuration::GetConfiguration().SetKeyValue(key, value);
    cout << key << " = " << value << (override ? " (override)" : " (add)") << endl;
  }
}

// Overrides the options set in the configuration file or adds new options to the configuration as specified on the command line.
// Syntax for 'options': key1=value1;key2=value2;
// Note that each key/value pair must end with a semicolon, except the last pair, which is optional for convenience.
// When entering on the command line, the semicolon char ';' is considered a special character by the shell and so
// must be escaped by prepending a '\' character in front.
// Example:
// $ ./irtk --index --config-options=document_collection_format=trec\;include_positions=false\;new_option=1
void OverrideConfigurationOptions(const string& options) {
  cout << "Overriding the following configuration file options: " << endl;

  size_t option_start = 0;
  size_t option_end = 0;
  size_t last_option_start = 0;

  while ((option_end = options.find(';', option_start)) != string::npos) {
    string key_value = options.substr(option_start, (option_end - option_start));
    ++option_end;
    option_start = option_end;
    last_option_start = option_start;
    SetConfigurationOption(key_value);
  }

  // The only option specified or the last option specified didn't end with a semicolon.
  if (option_start == 0 || option_start != options.size()) {
    string key_value = options.substr(last_option_start);
    SetConfigurationOption(key_value);
  }
}

// Displays common usage information. For more details, the project wiki should be consulted.
// TODO: Update the help information.
void Help() {
  cout << "To index: irtk --index\n";
  cout << "To merge: irtk --merge=[value]\n";
  cout << "                        value is 'initial', 'input'\n";
  cout << "  options:\n";
  cout << "    --merge-degree=[value]: specify the merge degree to use (default: 64)\n";
  cout << "To query: irtk --query [IndexFilename] [LexiconFilename] [DocumentMapFilename] [MetaFilename]\n";
  cout << "  options:\n";
  cout << "    --query-mode=[value]: sets the querying mode.\n";
  cout << "                          value is 'interactive', 'interactive-single', or 'batch'\n";
  cout << "To cat: irtk --cat [IndexFilename] [LexiconFilename] [DocumentMapFilename] [MetaFilename]\n";
  cout << "To diff: irtk --diff [IndexFilename1] [LexiconFilename1] [DocumentMapFilename1] [MetaFilename1] [IndexFilename2] [LexiconFilename2] [DocumentMapFilename2] [MetaFilename2]\n";
  cout << "To run compression tests: irtk --test-compression\n";
  cout << "To test a particular coder: irtk --test-coder [rice, turbo-rice, pfor, s9, s16, vbyte, null]\n";
  cout << endl;
}

void SeekHelp() {
  cout << "Run with '--help' for more information." << endl;
}

void UnrecognizedOptionValue(const char* option_name, const char* option_value) {
  cout << "Option '" << string(option_name) << "' has an unrecognized value of '" << string(option_value) << "'" << endl;
}

// Assume we can load the whole file into main memory.
void LoopThroughLexicon(const char* lexicon_filename) {
  int lexicon_fd = open(lexicon_filename, O_RDONLY);
  if (lexicon_fd < 0) {
    GetErrorLogger().LogErrno("open() in LoopThroughLexicon()", errno, true);
  }

  struct stat stat_buf;
  if (fstat(lexicon_fd, &stat_buf) < 0) {
    GetErrorLogger().LogErrno("fstat() in LoopThroughLexicon()", errno, true);
  }
  off_t lexicon_file_size = stat_buf.st_size;
  char* lexicon_buffer = new char[lexicon_file_size];

  ssize_t read_ret;
  off_t bytes_read = 0;
  while ((read_ret = read(lexicon_fd, lexicon_buffer + bytes_read, lexicon_file_size - bytes_read)) > 0) {
    bytes_read += read_ret;
  }
  if (read_ret == -1) {
    GetErrorLogger().LogErrno("read() in LoopThroughLexicon(), trying to read lexicon", errno, true);
  }
  assert(bytes_read == lexicon_file_size);

  off_t num_bytes_read = 0;
  char* lexicon_buffer_ptr_ = lexicon_buffer;
  while (num_bytes_read != lexicon_file_size) {
    assert(num_bytes_read <= lexicon_file_size);

    // num_layers
    int num_layers;
    int num_layers_bytes = sizeof(num_layers);
    memcpy(&num_layers, lexicon_buffer_ptr_, num_layers_bytes);
    lexicon_buffer_ptr_ += num_layers_bytes;
    num_bytes_read += num_layers_bytes;

    // term_len
    int term_len;
    int term_len_bytes = sizeof(term_len);
    memcpy(&term_len, lexicon_buffer_ptr_, term_len_bytes);
    lexicon_buffer_ptr_ += term_len_bytes;
    num_bytes_read += term_len_bytes;

    // term
    char* term = lexicon_buffer_ptr_;
    int term_bytes = term_len;
    lexicon_buffer_ptr_ += term_bytes;
    num_bytes_read += term_bytes;

    // num_docs
    int* num_docs = reinterpret_cast<int*> (lexicon_buffer_ptr_);
    int num_docs_bytes = num_layers * sizeof(*num_docs);
    lexicon_buffer_ptr_ += num_docs_bytes;
    num_bytes_read += num_docs_bytes;

    // num_chunks
    int* num_chunks = reinterpret_cast<int*> (lexicon_buffer_ptr_);
    int num_chunks_bytes = num_layers * sizeof(*num_chunks);
    lexicon_buffer_ptr_ += num_chunks_bytes;
    num_bytes_read += num_chunks_bytes;

    // num_chunks_last_block
    int* num_chunks_last_block = reinterpret_cast<int*> (lexicon_buffer_ptr_);
    int num_chunks_last_block_bytes = num_layers * sizeof(*num_chunks_last_block);
    lexicon_buffer_ptr_ += num_chunks_last_block_bytes;
    num_bytes_read += num_chunks_last_block_bytes;

    // num_blocks
    int* num_blocks = reinterpret_cast<int*> (lexicon_buffer_ptr_);
    int num_blocks_bytes = num_layers * sizeof(*num_blocks);
    lexicon_buffer_ptr_ += num_blocks_bytes;
    num_bytes_read += num_blocks_bytes;

    // block_number
    int* block_numbers = reinterpret_cast<int*> (lexicon_buffer_ptr_);
    int block_numbers_bytes = num_layers * sizeof(*block_numbers);
    lexicon_buffer_ptr_ += block_numbers_bytes;
    num_bytes_read += block_numbers_bytes;

    // chunk_number
    int* chunk_numbers = reinterpret_cast<int*> (lexicon_buffer_ptr_);
    int chunk_numbers_bytes = num_layers * sizeof(*chunk_numbers);
    lexicon_buffer_ptr_ += chunk_numbers_bytes;
    num_bytes_read += chunk_numbers_bytes;

    // score_thresholds
    float* score_thresholds = reinterpret_cast<float*> (lexicon_buffer_ptr_);
    int score_thresholds_bytes = num_layers * sizeof(*score_thresholds);
    lexicon_buffer_ptr_ += score_thresholds_bytes;
    num_bytes_read += score_thresholds_bytes;

    // external_index_offsets
    uint32_t* external_index_offsets = reinterpret_cast<uint32_t*> (lexicon_buffer_ptr_);
    int external_index_offsets_bytes = num_layers * sizeof(*external_index_offsets);
    lexicon_buffer_ptr_ += external_index_offsets_bytes;
    num_bytes_read += external_index_offsets_bytes;

    cout << string(term, term_len) << endl;
  }
}

void ReadDocumentUrls() {
  int fd = open("index.dmap_urls", O_RDONLY);

  int read_ret;
  int url_len;
  char* url;

  int count = 0;
  while (true) {
    read_ret = read(fd, &url_len, sizeof(int));
    if (read_ret <= 0) {
      cout << "read ret: " << read_ret << endl;
      break;
    }

    url = new char[url_len];
    read_ret = read(fd, url, url_len);
    assert(read_ret > 0);

    cout << string(url, url_len) << endl;
    ++count;

    delete[] url;
  }

  cout << "Total # urls: " << count << endl;
}

// Case that demonstrates than addition order matters in floating point ops (the assertion fails).
void FloatingPointTest() {
  float a, b, c;
  a = -0.420604676;
  b = -0.2024171948;
  c = -0.2698420882;
  float sum1 = a + b + c;
  float sum2 = c + b + a;
  float sum3 = b + c + a;

  if (sum1 != sum2 || sum1 != sum3 || sum2 != sum3) {
    assert(false);
  }
}

void SimdTest() {
  float test[2][4] = {{1,2,3,4},{5,6,7,8}};

  cout << "test: " << test << endl;
  cout << "TEST[0]: " << test[0] << ", " << *test[0]<<  endl;
  cout << "TEST[1]: " << test[1] << ", " << *test[1]<<  endl;

  float a = 1.5;
  float b = 2.1;
  float vec[] = { 1, 2, 3, 4 };

//  assert(__alignof__(vec) % 16 == 0);

  cout << "__alignof__(vec): " << __alignof__(vec) << endl;

  __m128 SSEa = _mm_load1_ps(&a);
  __m128 SSEb = _mm_load1_ps(&b);
  __m128 v = _mm_load_ps(vec);
  v = _mm_add_ps(_mm_mul_ps(v, SSEa), SSEb);
  _mm_store_ps(vec, v);

  cout << "vec alignment: " << (long int) vec % 16 << ", " << vec << endl;

  for (int i = 0; i < 4; ++i) {
    cout << "RESULT " << i << ": " << vec[i] << endl;
  }
}

int main(int argc, char** argv) {
  const char* opt_string = "iqcdh";
  const struct option long_opts[] = { { "index", no_argument, NULL, 'i' },                                // Index the document collection bundles.
                                      { "merge", required_argument, NULL, 0 },                            // Merge the indices (depends on argument).
                                      { "merge-degree", required_argument, NULL, 0 },                     // Override the default merge degree.
                                      { "query", no_argument, NULL, 'q' },                                // Query an index.
                                      { "query-algorithm", required_argument, NULL, 0 },                  // Set which query algorithm we want to use.
                                      { "query-mode", required_argument, NULL, 0 },                       // Set which query mode we want to use.
                                      { "query-stop-list-file", required_argument, NULL, 0 },             // Use the following stop word list at query time.
                                      { "result-format", required_argument, NULL, 0 },                    // Set which result format we want to use.
                                      { "cat", no_argument, NULL, 'c' },                                  // Outputs inverted list data in a human readable format.
                                      { "cat-term", required_argument, NULL, 0 },                         // Specify the inverted list (term) on which we want to run the cat procedure.
                                      { "diff", no_argument, NULL, 'd' },                                 // Outputs the differences between two inverted lists.
                                      { "diff-term", required_argument, NULL, 0 },                        // Specify the inverted list (term) on which we want to run the diff procedure.
                                      { "remap", required_argument, NULL, 0 },                            // Remaps an index. The argument specifies the document mapping file to use for the remap procedure.
                                      { "layerify", no_argument, NULL, 0 },                               // Creates a layered index.
                                      { "retrieve-index-data", required_argument, NULL, 0 },              // Retrieves index data for an inverted list into an in-memory array. See function 'RetrieveIndexData()'.
                                      { "loop-over-index-data", required_argument, NULL, 0 },             // Loops over an inverted list (decompresses but does not do any top-k). Useful for benchmarking decompression coders.
                                      { "in-memory-index", no_argument, NULL, 0 },                        // Loads the index into main memory.
                                      { "memory-map-index", no_argument, NULL, 0 },                       // Memory maps the index into our address space.
                                      { "block-level-index", no_argument, NULL, 0 },                      // Builds an in-memory block level index.
                                      { "generate-url-sorted-doc-mapping", required_argument, NULL, 0 },  // Generates a docID mapping file (docIDs are remapped by URL) that can be used as input to the remap procedure.
                                      { "config-options", required_argument, NULL, 0 },                   // Overrides/adds options defined in the configuration file.
                                      { "test-compression", no_argument, NULL, 0 },                       // Runs compression tests on some randomly generated data.
                                      { "test-coder", required_argument, NULL, 0 },                       // Tests a specific coder.
                                      { "help", no_argument, NULL, 'h' },                                 // Help.
                                      { NULL, no_argument, NULL, 0 } };

  int opt, long_index;
  while ((opt = getopt_long(argc, argv, opt_string, long_opts, &long_index)) != -1) {
    switch (opt) {
      case 'i':
        command_line_args.mode = CommandLineArgs::kIndex;
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
        if (strcmp("merge", long_opts[long_index].name) == 0) {
          if (strcmp("initial", optarg) == 0)
            command_line_args.mode = CommandLineArgs::kMergeInitial;
          else if (strcmp("input", optarg) == 0)
            command_line_args.mode = CommandLineArgs::kMergeInput;
          else
            UnrecognizedOptionValue(long_opts[long_index].name, optarg);
        } else if (strcmp("merge-degree", long_opts[long_index].name) == 0) {
          command_line_args.merge_degree = atoi(optarg);
        } else if (strcmp("query-algorithm", long_opts[long_index].name) == 0) {
          if (strcmp("default", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kDefault;
          else if (strcmp("daat-and", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kDaatAnd;
          else if (strcmp("daat-or", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kDaatOr;
          else if (strcmp("taat-or", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kTaatOr;
          else if (strcmp("dual-layered-overlapping-daat", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kDualLayeredOverlappingDaat;
          else if (strcmp("dual-layered-overlapping-merge-daat", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kDualLayeredOverlappingMergeDaat;
          else if (strcmp("layered-taat-or-early-terminated", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kLayeredTaatOrEarlyTerminated;
          else if (strcmp("wand", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kWand;
          else if (strcmp("dual-layered-wand", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kDualLayeredWand;
          else if (strcmp("max-score", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kMaxScore;
          else if (strcmp("dual-layered-max-score", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kDualLayeredMaxScore;
          else if (strcmp("daat-and-top-positions", optarg) == 0)
            command_line_args.query_algorithm = QueryProcessor::kDaatAndTopPositions;
          else
            UnrecognizedOptionValue(long_opts[long_index].name, optarg);
        } else if (strcmp("query-mode", long_opts[long_index].name) == 0) {
          if (strcmp("interactive", optarg) == 0)
            command_line_args.query_mode = QueryProcessor::kInteractive;
          else if (strcmp("interactive-single", optarg) == 0)
            command_line_args.query_mode = QueryProcessor::kInteractiveSingle;
          else if (strcmp("batch", optarg) == 0)
            command_line_args.query_mode = QueryProcessor::kBatch;
          else if (strcmp("batch-all", optarg) == 0)
            command_line_args.query_mode = QueryProcessor::kBatchAll;
          else
            UnrecognizedOptionValue(long_opts[long_index].name, optarg);
        } else if (strcmp("query-stop-list-file", long_opts[long_index].name) == 0) {
          command_line_args.query_stop_words_list_file = new char[strlen(optarg)];
          memcpy(command_line_args.query_stop_words_list_file, optarg, strlen(optarg));
        } else if (strcmp("result-format", long_opts[long_index].name) == 0) {
          if (strcmp("trec", optarg) == 0)
            command_line_args.result_format = QueryProcessor::kTrec;
          else if (strcmp("compare", optarg) == 0)
            command_line_args.result_format = QueryProcessor::kCompare;
          else if (strcmp("discard", optarg) == 0)
            command_line_args.result_format = QueryProcessor::kDiscard;
          else
            UnrecognizedOptionValue(long_opts[long_index].name, optarg);
        } else if (strcmp("remap", long_opts[long_index].name) == 0) {
          command_line_args.mode = CommandLineArgs::kRemap;
          command_line_args.doc_mapping_file = new char[strlen(optarg)];
          memcpy(command_line_args.doc_mapping_file, optarg, strlen(optarg));
        } else if (strcmp("layerify", long_opts[long_index].name) == 0) {
          command_line_args.mode = CommandLineArgs::kLayerify;
        } else if (strcmp("cat-term", long_opts[long_index].name) == 0 || strcmp("diff-term", long_opts[long_index].name) == 0) {
          command_line_args.term_len = strlen(optarg);
          command_line_args.term = new char[command_line_args.term_len];
          memcpy(command_line_args.term, optarg, command_line_args.term_len);
        } else if (strcmp("retrieve-index-data", long_opts[long_index].name) == 0) {
          command_line_args.mode = CommandLineArgs::kRetrieveIndexData;
          command_line_args.term_len = strlen(optarg);
          command_line_args.term = new char[command_line_args.term_len];
          memcpy(command_line_args.term, optarg, command_line_args.term_len);
        } else if (strcmp("loop-over-index-data", long_opts[long_index].name) == 0) {
          command_line_args.mode = CommandLineArgs::kLoopOverIndexData;
          command_line_args.term_len = strlen(optarg);
          command_line_args.term = new char[command_line_args.term_len];
          memcpy(command_line_args.term, optarg, command_line_args.term_len);
        } else if (strcmp("in-memory-index", long_opts[long_index].name) == 0) {
          command_line_args.in_memory_index = true;
          SetConfigurationOption(string(config_properties::kMemoryResidentIndex) + string("=true"));
        } else if (strcmp("memory-map-index", long_opts[long_index].name) == 0) {
          command_line_args.memory_mapped_index = true;
          SetConfigurationOption(string(config_properties::kMemoryMappedIndex) + string("=true"));
        } else if (strcmp("block-level-index", long_opts[long_index].name) == 0) {
          SetConfigurationOption(string(config_properties::kUseBlockLevelIndex) + string("=true"));
        } else if (strcmp("generate-url-sorted-doc-mapping", long_opts[long_index].name) == 0) {
          GenerateUrlSortedDocIdMappingFile(optarg);
          return EXIT_SUCCESS;
        } else if (strcmp("config-options", long_opts[long_index].name) == 0) {
          OverrideConfigurationOptions(optarg);
        } else if (strcmp("test-compression", long_opts[long_index].name) == 0) {
          TestCompression();
          return EXIT_SUCCESS;
        } else if (strcmp("test-coder", long_opts[long_index].name) == 0) {
          TestCoder(optarg);
          return EXIT_SUCCESS;
        }
        break;

      default:
        SeekHelp();
        return EXIT_SUCCESS;
    }
  }

  char** input_files = argv + optind;
  int num_input_files = argc - optind;

  switch (command_line_args.mode) {
    case CommandLineArgs::kQuery:
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
            command_line_args.external_index1_filename = input_files[i];
            break;
        }
      }
      break;

    case CommandLineArgs::kCat:
    case CommandLineArgs::kDiff:
    case CommandLineArgs::kRetrieveIndexData:
    case CommandLineArgs::kLoopOverIndexData:
    case CommandLineArgs::kLayerify:
    case CommandLineArgs::kRemap:
      for (int i = 0; i < num_input_files; ++i) {
        switch (i) {
          // Index files for the first index.
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
          // Index files for the seconds index (if any).
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
      break;
    // These don't take any arguments.
    case CommandLineArgs::kIndex:
    case CommandLineArgs::kMergeInitial:
    case CommandLineArgs::kMergeInput:
      break;
    default:
      assert(false);
  }

  Init();
  srand(time(NULL));

  switch (command_line_args.mode) {
    case CommandLineArgs::kIndex:
      IndexCollection();
      break;
    case CommandLineArgs::kQuery:
      Query(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename, command_line_args.meta_info1_filename,
            command_line_args.external_index1_filename, command_line_args.query_stop_words_list_file, command_line_args.query_algorithm,
            command_line_args.query_mode, command_line_args.result_format);
      break;
    case CommandLineArgs::kMergeInitial:
      MergeInitial(command_line_args.merge_degree);
      break;
    case CommandLineArgs::kMergeInput:
      MergeInput();
      break;
    case CommandLineArgs::kRemap:
      RemapIndexDocIds(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename,
                       command_line_args.meta_info1_filename);
      break;
    case CommandLineArgs::kLayerify:
      LayerifyIndex(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename,
                    command_line_args.meta_info1_filename);
      break;
    case CommandLineArgs::kCat:
      Cat(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename, command_line_args.meta_info1_filename,
          command_line_args.term, command_line_args.term_len);
      break;
    case CommandLineArgs::kDiff:
      Diff(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename, command_line_args.meta_info1_filename,
           command_line_args.index2_filename, command_line_args.lexicon2_filename, command_line_args.doc_map2_filename, command_line_args.meta_info2_filename,
           command_line_args.term, command_line_args.term_len);
      break;
    case CommandLineArgs::kRetrieveIndexData:
      RetrieveIndexData(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename,
                        command_line_args.meta_info1_filename, command_line_args.term, command_line_args.term_len);
      break;
    case CommandLineArgs::kLoopOverIndexData:
      LoopOverIndexData(command_line_args.index1_filename, command_line_args.lexicon1_filename, command_line_args.doc_map1_filename,
                        command_line_args.meta_info1_filename, command_line_args.term, command_line_args.term_len, command_line_args.in_memory_index,
                        command_line_args.memory_mapped_index);
      break;
    default:
      Help();
      break;
  }

  return EXIT_SUCCESS;
}
