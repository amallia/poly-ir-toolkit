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
// Defines standard properties that are used by config file readers.
//==============================================================================================================================================================

#ifndef CONFIG_FILE_PROPERTIES_H_
#define CONFIG_FILE_PROPERTIES_H_

namespace config_properties {

/**************************************************************************************************************************************************************
 * Indexing Parameters
 *
 **************************************************************************************************************************************************************/
// Size of the hash table used for posting accumulation.
static const char kHashTableSize[] = "hash_table_size";

// The initial buffer size to use for uncompressing document collection files.
static const char kDocumentCollectionBufferSize[] = "document_collection_buffer_size";

// Size of the memory pool for posting accumulation.
static const char kMemoryPoolSize[] = "memory_pool_size";

// Size of each memory pool block. Should be kept small to make the most use of memory. The smaller the size, the less memory we waste for very small lists,
// but the more overall pointer overhead we have since we have to store a pointer for each block allocated in the memory pool.
static const char kMemoryPoolBlockSize[] = "memory_pool_block_size";

// Controls whether positions will be indexed.
static const char kIncludePositions[] = "include_positions";

// Controls whether contexts will be indexed.
static const char kIncludeContexts[] = "include_contexts";

/**************************************************************************************************************************************************************
 * Merging Parameters
 *
 **************************************************************************************************************************************************************/
// Controls whether files that have already been merged should be deleted (Saves considerable disk space).
static const char kDeleteMergedFiles[] = "delete_merged_files";

/**************************************************************************************************************************************************************
 * Querying Parameters
 *
 **************************************************************************************************************************************************************/
// Sets whether the whole index will be read into main memory. If true, the 'block_cache_size' and 'read_ahead_blocks' options are ignored.
static const char kMemoryResidentIndex[] = "memory_resident_index";

// The number of blocks to cache in memory.
static const char kBlockCacheSize[] = "block_cache_size";

// The number of blocks to read ahead from a list into the cache.
static const char kReadAheadBlocks[] = "read_ahead_blocks";

// Size of the hash table used for the lexicon.
static const char kLexiconSize[] = "lexicon_size";

// The maximum number of results returned by the query processor.
static const char kMaxNumberResults[] = "max_number_results";

// Controls whether positions will be utilized if the index was built with positions.
static const char kUsePositions[] = "use_positions";

} // namespace config_properties

#endif /* CONFIG_FILE_PROPERTIES_H_ */
