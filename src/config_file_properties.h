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
// Document collection format.
static const char kDocumentCollectionFormat[] = "document_collection_format";

// The initial buffer size to use for uncompressing document collection files.
static const char kDocumentCollectionBufferSize[] = "document_collection_buffer_size";

// Size of the hash table used for posting accumulation.
static const char kHashTableSize[] = "hash_table_size";

// Size of the memory pool for posting accumulation.
static const char kMemoryPoolSize[] = "memory_pool_size";

// Size of each memory pool block. Should be kept small to make the most use of memory. The smaller the size, the less memory we waste for very small lists,
// but the more overall pointer overhead we have since we have to store a pointer for each block allocated in the memory pool.
static const char kMemoryPoolBlockSize[] = "memory_pool_block_size";

// Controls whether positions will be indexed.
static const char kIncludePositions[] = "include_positions";

// Controls whether contexts will be indexed.
static const char kIncludeContexts[] = "include_contexts";

// The coding policy to be used for compressing the docIDs.
static const char kIndexingDocIdCoding[] = "indexing_doc_id_coding";

// The coding policy to be used for compressing the frequencies.
static const char kIndexingFrequencyCoding[] = "indexing_frequency_coding";

// The coding policy to be used for compressing the positions, if they are to be included.
static const char kIndexingPositionCoding[] = "indexing_position_coding";

// The coding policy to be used for compressing the block header.
static const char kIndexingBlockHeaderCoding[] = "indexing_block_header_coding";

/**************************************************************************************************************************************************************
 * Merging Parameters
 *
 **************************************************************************************************************************************************************/
// Controls whether files that have already been merged should be deleted (Saves considerable disk space).
static const char kDeleteMergedFiles[] = "delete_merged_files";

// The coding policy to be used for compressing the docIDs.
static const char kMergingDocIdCoding[] = "merging_doc_id_coding";

// The coding policy to be used for compressing the frequencies.
static const char kMergingFrequencyCoding[] = "merging_frequency_coding";

// The coding policy to be used for compressing the positions, if they were included in the index.
static const char kMergingPositionCoding[] = "merging_position_coding";

// The coding policy to be used for compressing the block header.
static const char kMergingBlockHeaderCoding[] = "merging_block_header_coding";

/**************************************************************************************************************************************************************
 * Layering Parameters
 *
 **************************************************************************************************************************************************************/
// Controls whether the layers are overlapping or not. If they are, successive layers will always be a superset of the previous layers.
static const char kOverlappingLayers[] = "overlapping_layers";

// This is subject to 'MAX_LIST_LAYERS' defined in 'index_layout_parameters.h'. It controls the (max) number of layers we should have per list.
static const char kNumLayers[] = "num_layers";

/**************************************************************************************************************************************************************
 * Querying Parameters
 *
 **************************************************************************************************************************************************************/
// Sets whether the whole index will be read into main memory. If true, the 'block_cache_size' and 'read_ahead_blocks' options are ignored.
static const char kMemoryResidentIndex[] = "memory_resident_index";

// Sets whether the whole index will be memory mapped into the process address space. If true, the 'block_cache_size' and 'read_ahead_blocks' options are ignored.
// This option has precedence over the 'memory_resident_index' option when both are true.
static const char kMemoryMappedIndex[] = "memory_mapped_index";

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

// Controls whether an in-memory block level index will be built on startup and used to skip fetching/decoding unnecessary blocks.
static const char kUseBlockLevelIndex[] = "use_block_level_index";

// Controls from where batch query input will come from.
// Valid values are either 'stdin'/'cin' or the path to the batch query file.
static const char kBatchQueryInputFile[] = "batch_query_input_file";

/**************************************************************************************************************************************************************
 * Index DocID Remapping Parameters
 *
 **************************************************************************************************************************************************************/
// Controls whether intermediate remapped index files should be deleted (Saves considerable disk space).
static const char kDeleteIntermediateRemappedFiles[] = "delete_intermediate_remapped_files";

// Size of the buffer (in MiB) to hold index entries.
static const char kIndexEntryBufferSize[] = "index_entry_buffer_size";

// Size of the positions pool buffer (in MiB). Only allocated if the index to be remapped holds positions.
static const char kPositionsPoolBufferSize[] = "positions_pool_buffer_size";

} // namespace config_properties

#endif /* CONFIG_FILE_PROPERTIES_H_ */
