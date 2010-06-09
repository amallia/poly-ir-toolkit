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
// We don't accumulate the whole vocabulary in main memory during indexing, instead we write out runs, (partial indices) and then merge them at the end.
// We accumulate postings in memory for each term in compressed form (using variable byte encoding). We use var byte coding even for non gap coded docIDs and
// positions (gap coding would produce much smaller integers, so compression would work better) since most docIDs and positions will be smaller than a full
// integer, so var byte coding would help in this case too.
//
// TODO: Need to handle docID reordering. The reordering might come from a file (assume it can fit in memory). We can't do gap coding in this case. Might need
//       to do an I/O efficient sort on the postings.
//
// TODO: Need a way to handle encoding of "magic bytes" into our blocks. These can contain fixed width or variable width data. Can use contexts as a test case
//       of fixed width magic bytes.
//
// TODO: Might want to have per list compression (different compression algorithms for different lists, since they might have different characteristics). Also,
//       maybe per chunk compression (like OptPForDelta).
//
// TODO: Try new method of in-memory compression of postings. Collect n postings (or maybe until we run out of space in a block), then recompress them
//       (and sort them if they're not in order) so that we can compact the postings even further, with a good compression method.
//
// TODO: If building an index without positions, contexts, and other per docID information, can optimize usage of the memory pool as follows. For each
//       TermBlock, buffer the frequency count in memory for the last docID inserted for that particular term. Once we get a new docID (or we dump the run),
//       can write the buffered frequency count into the memory pool. This allows more efficient use of the memory pool since we write each docID only once
//       for a particular term, as opposed to multiple times, and then calculating the frequency later.
//==============================================================================================================================================================

#include "posting_collection.h"

#include <cassert>
#include <cctype>
#include <cstdlib>
#include <cstring>

#include <algorithm>
#include <iostream>
#include <limits>
#include <string>
#include <utility>

#include "config_file_properties.h"
#include "configuration.h"
#include "globals.h"
#include "index_build.h"
#include "logger.h"
#include "meta_file_properties.h"
using namespace std;
using namespace logger;

PostingCollectionController& GetPostingCollectionController() {
  static PostingCollectionController posting_collection_controller;
  return posting_collection_controller;
}

MemoryPoolManager& GetMemoryPoolManager() {
  static MemoryPoolManager memory_pool_manager;
  return memory_pool_manager;
}

/**************************************************************************************************************************************************************
 * TermBlock
 *
 **************************************************************************************************************************************************************/
// Encoding / decoding for postings into memory pool blocks.
unsigned char TermBlock::compressed_tmp_posting[11];  // TODO: Does being integer aligned have any performance benefit?
int TermBlock::compressed_tmp_posting_len;

TermBlock::TermBlock(const char* term, int term_len) :
  memory_pool_manager_(&GetMemoryPoolManager()), term_(new char[term_len]), term_len_(term_len), ordered_documents_(true), index_positions_(true),
      index_contexts_(false), prev_doc_id_(0), prev_position_(0), block_list_(NULL), last_block_(NULL), curr_block_position_(NULL), next_(NULL) {
  // Need to transform term to lower case.
  for (int i = 0; i < term_len; i++) {
    term_[i] = tolower(static_cast<unsigned char> (term[i]));
  }
}

void TermBlock::InitBlockList(unsigned char* block_start) {
  BlockList* new_block = new BlockList(block_start);
  last_block_ = new_block;
  block_list_ = new_block;
}

void TermBlock::AddBlockToList(unsigned char* block_start) {
  BlockList* new_block = new BlockList(block_start);
  last_block_->set_next_block(new_block);
  last_block_ = new_block;
}

TermBlock::~TermBlock() {
  delete[] term_;
  ClearBlockList();
}

void TermBlock::ClearBlockList() {
  BlockList* curr_block = block_list_;
  BlockList* next_block;

  while (curr_block != NULL) {
    next_block = curr_block->next_block();
    delete curr_block;
    curr_block = next_block;
  }

  block_list_ = NULL;
  last_block_ = NULL;
}

void TermBlock::Encode(uint32_t num, unsigned char* out, int* len) {
  unsigned char tmp[5];
  int i;
  for (i = 0; i < 5; ++i) {
    tmp[i] = (num & 0x7F) << 1;
    if ((num >>= 7) == 0)
      break;
  }
  *len = i + 1;

  // i is already set at the correct value, since we break above, before it gets incremented.
  // i == 0 is handled outside the loop.
  for (; i > 0; --i) {
    *out++ = tmp[i] | 0x01;  // Turn on least significant bit.
  }
  *out = tmp[0] & ~0x01;  // Turn off least significant bit.
}

uint32_t TermBlock::Decode(const unsigned char* in, int* len) {
  uint32_t num = (*in >> 1);
  *len = 1;
  if ((*in & 0x1) != 0) {
    in++;
    num = (num << 7) | (*in >> 1);
    *len = 2;
    if ((*in & 0x1) != 0) {
      in++;
      num = (num << 7) | (*in >> 1);
      *len = 3;
      if ((*in & 0x1) != 0) {
        in++;
        num = (num << 7) | (*in >> 1);
        *len = 4;
      }
    }
  }
  return num;
}

// Returns the next var byte encoded integer from the current position in our postings list.
// Upon reaching the end of the postings list, returns the max value of uint32_t.
uint32_t TermBlock::GetVarByteInt() {
  if (block_list_ == NULL)
    return numeric_limits<uint32_t>::max();

  const int kMaxVarByteDataLen = 5; // 5 bytes for a 32 bit integer.
  unsigned char varbyte_data[kMaxVarByteDataLen];
  unsigned char* varbyte_data_ptr = varbyte_data;
  int varbyte_data_len = 0;

  bool no_more = false;
  while (true) {
    while (block_list_->IsWithinBlock(curr_block_position_)) {
      if (!VarByteHasMore(curr_block_position_)) {
        no_more = true;
      }

      assert((varbyte_data_ptr - varbyte_data) < 5);
      *(varbyte_data_ptr++) = *(curr_block_position_++);
      ++varbyte_data_len;

      if (no_more) {
        break;
      }
    }

    if (no_more) {
      break;
    } else {
      block_list_ = block_list_->next_block();
      if (block_list_ == NULL) {
        return numeric_limits<uint32_t>::max();  // Indicates we reached the end of the postings list.
      }

      curr_block_position_ = block_list_->block();
    }
  }

  int decoded_int_len;
  uint32_t decoded_int = Decode(varbyte_data, &decoded_int_len);
  assert(decoded_int != numeric_limits<uint32_t>::max());  // We reserved this value as an indicator of the end of the postings list.
  return decoded_int;
}

// Returns the value of the next byte from our postings list.
// Upon reaching the end of the postings list, returns the max value of uint32_t.
uint32_t TermBlock::GetByte() {
  if (block_list_ == NULL)
    return numeric_limits<uint32_t>::max();

  while (true) {
    while (block_list_->IsWithinBlock(curr_block_position_)) {
      return *(curr_block_position_++);
    }

    block_list_ = block_list_->next_block();
    if (block_list_ == NULL) {
      return numeric_limits<uint32_t>::max();
    }

    curr_block_position_ = block_list_->block();
  }
}

// Returns true upon decoding the next posting in the postings list for this term and stores the info into 'decoded_posting'.
// Returns false when there are no more postings in the postings lists for this term and 'decoded_posting' is not modified.
bool TermBlock::DecodePosting(DecodedPosting* decoded_posting) {
  // Decode doc_id.
  uint32_t doc_id = GetVarByteInt();
  if (doc_id == 0 || doc_id == numeric_limits<uint32_t>::max()) {
    // doc_id == 0:
    // A block of memory that contains zeroes for the docID means that we have no more postings encoded in the block,
    // so this is the end of the postings list for this term, even though we have extra unfilled space leftover in the block.
    // doc_id == UINT32_MAX:
    // We tried to get another posting, but we have no more in the block.
    return false;
  }
  // doc_id is decremented by one (one was added in the first place to maintain that a doc_id of zero means the end of the list).
  --doc_id;
  decoded_posting->set_doc_id(doc_id);

  // Decode position.
  uint32_t position;
  if (index_positions_) {
    position = GetVarByteInt();
    assert(position != numeric_limits<uint32_t>::max());  // End of list only makes sense at the start of the posting.
  } else {
    position = 0;
  }
  decoded_posting->set_position(position);

  // Decode context.
  uint32_t context;
  if (index_contexts_) {
    context = GetByte();
    assert(context != numeric_limits<uint32_t>::max());  // End of list only makes sense at the start of the posting.
  } else {
    context = 0;
  }
  decoded_posting->set_context(context);

  return true;
}

// This interface assumes the postings were accumulated in sorted order (which is true unless we're doing document reordering while indexing).
//
// '*num_docs' initially holds the size of the 'doc_ids' and 'frequencies' arrays.
// '*num_properties' initially holds the size of the 'positions' and 'contexts' arrays.
//
// The accumulated postings for this term will be decoded into the appropriate arrays,
// and '*num_docs' will hold the number of documents which were decoded,
// and '*num_properties' will hold the number of document properties (positions and contexts) which were decoded
// (this is the summation of all the frequency values).
//
// Returns true when some postings have been decoded and false when no postings were able to be decoded.
bool TermBlock::DecodePostings(uint32_t* doc_ids, uint32_t* frequencies, uint32_t* positions, unsigned char* contexts, int* num_docs, int* num_properties, DecodedPosting* prev_posting, bool* prev_posting_valid, Posting* overflow_postings, int* num_overflow_postings, uint32_t overflow_doc_id, uint32_t prev_chunk_last_doc_id) {
  assert(doc_ids != NULL);
  assert(frequencies != NULL);
  assert(positions != NULL);
  assert(contexts != NULL);
  assert(num_docs != NULL && *num_docs > 0);
  assert(num_properties != NULL && *num_properties > 0);
  assert(prev_posting != NULL);
  assert(prev_posting_valid != NULL);
  assert(ordered_documents_ == true);

  int num_docs_decoded = 0;
  int num_properties_decoded = 0;
  int num_properties_decoded_per_doc = 0;

  uint32_t curr_decoded_doc_id = prev_chunk_last_doc_id;
  uint32_t overflow_posting_position = 0;  // To decode the position gaps in the overflow posting.
  int overflow_postings_i = 0;

  if (!*prev_posting_valid) {
    if (DecodePosting(prev_posting) == false) {
      *num_docs = 0;
      *num_properties = 0;
      *num_overflow_postings = 0;
      return false;
    }
  } else {
    *prev_posting_valid = false;
  }

  // Decode the doc id from the gaps, and see if it's an overflow doc id.
  curr_decoded_doc_id += prev_posting->doc_id();

  if (*num_overflow_postings > 0 && curr_decoded_doc_id == overflow_doc_id) {
    assert(overflow_postings != NULL);
    assert(overflow_postings_i < *num_overflow_postings);

    overflow_posting_position += prev_posting->position();
    overflow_postings[overflow_postings_i++] = Posting(NULL, 0, curr_decoded_doc_id, overflow_posting_position, prev_posting->context());
  }

  doc_ids[num_docs_decoded] = prev_posting->doc_id();
  frequencies[num_docs_decoded] = 1;
  ++num_docs_decoded;

  positions[num_properties_decoded] = prev_posting->position();
  contexts[num_properties_decoded] = prev_posting->context();
  ++num_properties_decoded;
  ++num_properties_decoded_per_doc;

  // We read ahead one more document than we were allocated for to check whether the next document is a continuation of the previous one.
  // If it wasn't, since we can't "put back" the posting into the block, we'll store it into prev_posting and set prev_posting_valid to true, otherwise set it to false.
  DecodedPosting curr_posting;
  while ((num_docs_decoded < (*num_docs + 1)) && DecodePosting(&curr_posting)) {
//    if (prev_posting.doc_id() == curr_posting.doc_id()) {  // TODO: For when we don't do docID gaps (we always do docID gaps in such a case).
    if (curr_posting.doc_id() == 0) {
      // A continuation of the same document.
      ++frequencies[num_docs_decoded - 1];
    } else if(num_docs_decoded == *num_docs) {
      // The posting we decoded was not a continuation of the same doc id
      // and we have no more room to put it into the output array.
      *prev_posting = curr_posting;
      *prev_posting_valid = true;
      break;
    }
    else {
      // Found a new document.
      prev_posting->set_doc_id(curr_posting.doc_id());

      doc_ids[num_docs_decoded] = curr_posting.doc_id();
      frequencies[num_docs_decoded] = 1;
      ++num_docs_decoded;

      num_properties_decoded_per_doc = 0;
    }

    // We're truncating the number of per document properties.
    if (num_properties_decoded_per_doc < Chunk::kMaxProperties) {
      assert(num_properties_decoded < *num_properties);
      positions[num_properties_decoded] = curr_posting.position();
      contexts[num_properties_decoded] = curr_posting.context();
      ++num_properties_decoded;
      ++num_properties_decoded_per_doc;
    } else {
      // If we're truncating, we need to adjust the frequency to be less.
      --frequencies[num_docs_decoded - 1];
    }

    // Decode the doc id from the gaps, and see if it's an overflow doc id.
    curr_decoded_doc_id += curr_posting.doc_id();

    if (*num_overflow_postings > 0 && curr_decoded_doc_id == overflow_doc_id) {
      assert(overflow_postings != NULL);
      assert(overflow_postings_i < *num_overflow_postings);

      overflow_posting_position += curr_posting.position();
      overflow_postings[overflow_postings_i++] = Posting(NULL, 0, curr_decoded_doc_id, overflow_posting_position, curr_posting.context());
    }
  }

  *num_docs = num_docs_decoded;
  *num_properties = num_properties_decoded;
  *num_overflow_postings = overflow_postings_i;
  return true;
}

// TODO: Document reordering:
//       Sort a single list in main memory and take d-gaps (and also position gaps). Then build the index as usual.
// TODO: This interface should be used for when the postings are not accumulated in sorted order.
// So we'll have to assume the whole list either fits in memory, and get all the postings here and sort them in memory
// or if we can't assume it to fit in memory (remember we're using a large memory pool for accumulating and it's not completely free'd at this point),
// we need to dump them to disk and do an external merge sort.

// Fills the supplied array with decoded postings and stores the length actually decoded back into 'decoded_postings_len'.
// If upon returning, 'decoded_postings_len' is less than the value supplied, then all the postings in this TermBlock have been decoded.
void TermBlock::DecodePostings(DecodedPosting* decoded_postings, int* decoded_postings_len, Posting* overflow_postings, int* num_overflow_postings,
                               uint32_t overflow_doc_id) {
  int overflow_postings_i = 0;
  int i;
  for (i = 0; i < *decoded_postings_len; ++i) {
    if (DecodePosting(decoded_postings + i) == false)
      break;

    if (decoded_postings[i].doc_id() == overflow_doc_id) {
      assert(overflow_postings_i < *num_overflow_postings);
      overflow_postings[overflow_postings_i++] = Posting(NULL, 0, decoded_postings[i].doc_id(), decoded_postings[i].position(), decoded_postings[i].context());
    }
  }

  *decoded_postings_len = i;
  *num_overflow_postings = overflow_postings_i;
}

// Compresses posting into a static buffer and records it's length.
void TermBlock::EncodePosting(const Posting& posting) {
  compressed_tmp_posting_len = 0;
  int encoding_offset;

  // If we're continuing to process the same document, can always take position deltas.
  int position_gap = posting.position();
  if (prev_doc_id_ == posting.doc_id()) {
    position_gap -= prev_position_;
  }
  prev_position_ = posting.position();

  // If the docIDs are assigned such that they are monotonically increasing, we can take docID deltas.
  int doc_id_gap = posting.doc_id();
  if (ordered_documents_) {
    doc_id_gap -= prev_doc_id_;
  }
  prev_doc_id_ = posting.doc_id();

  // We increment all docID gaps by 1 so they're never 0. Remember to decrement by 1 when decoding!
  Encode(doc_id_gap + 1, compressed_tmp_posting, &encoding_offset);
  compressed_tmp_posting_len += encoding_offset;

  if (index_positions_) {
    Encode(position_gap, compressed_tmp_posting + compressed_tmp_posting_len, &encoding_offset);
    compressed_tmp_posting_len += encoding_offset;
  }

  if (index_contexts_) {
    *(compressed_tmp_posting + compressed_tmp_posting_len) = posting.context();
    ++compressed_tmp_posting_len;
  }
}

// Returns true when the compressed posting was successfully inserted into the TermBlock, and false
// when the memory pool either ran out of blocks or the last block remaining did not have enough space to
// fit the complete compressed posting.
bool TermBlock::AddCompressedPosting() {
  // We don't init it in the constructor because it's possible we have initialized a TermBlock
  // but we actually don't have any space in the memory pool.
  if (curr_block_position_ == NULL) {
    curr_block_position_ = memory_pool_manager_->AllocateBlock();
    if (curr_block_position_ == NULL) {
      return false;
    } else {
      // Only do this if it's the first posting we're adding to this term block.
      if (block_list_ == NULL) {
        InitBlockList(curr_block_position_);
      } else {
        AddBlockToList(curr_block_position_);
      }
    }
  }

  // If we need to dump run, set flag that we have a compressed posting in the buffer that needs to be put into a new TermBlock.
  if (!memory_pool_manager_->HaveSpace(curr_block_position_, compressed_tmp_posting_len)) {
    return false;
  }

  int remaining_posting_bytes = compressed_tmp_posting_len;

  while (remaining_posting_bytes > 0) {
    unsigned char* next_block_start = memory_pool_manager_->GetNextBlockStart(curr_block_position_);

    int space_left_curr_block = next_block_start - curr_block_position_;

    int curr_write = min(space_left_curr_block, remaining_posting_bytes);

    int offset = compressed_tmp_posting_len - remaining_posting_bytes;

    memcpy(curr_block_position_, compressed_tmp_posting + offset, curr_write);

    remaining_posting_bytes -= curr_write;
    curr_block_position_ += curr_write;

    // If we require an additional block.
    if (remaining_posting_bytes > 0) {
      // Allocate a new block.
      curr_block_position_ = memory_pool_manager_->AllocateBlock();

      assert(curr_block_position_ != NULL);  // We DumpRun() prior to avoid this exact situation.

      AddBlockToList(curr_block_position_);
    } else if (remaining_posting_bytes == 0 && curr_block_position_ == next_block_start) {
      // We fully filled this block.
      // A new block will be allocated if we encounter another posting for this term
      // with code at the beginning of this function.
      curr_block_position_ = NULL;
    }
  }

  // Compressed posting was successfully added.
  return true;
}

bool TermBlock::AddPosting(const Posting& posting) {
  EncodePosting(posting);
  // Our only assumption now is that the maximum compressed size of a single posting never exceeds the size of the memory pool.
  assert(compressed_tmp_posting_len <= GetMemoryPoolManager().kMemoryPoolSize);
  return AddCompressedPosting();
}

/**************************************************************************************************************************************************************
 * MemoryPoolManager
 *
 **************************************************************************************************************************************************************/
MemoryPoolManager::MemoryPoolManager() :
  kMemoryPoolSize(atol(Configuration::GetConfiguration().GetValue(config_properties::kMemoryPoolSize).c_str())),
      kBlockSize(atol(Configuration::GetConfiguration().GetValue(config_properties::kMemoryPoolBlockSize).c_str())), memory_pool_(new unsigned char[kMemoryPoolSize]),
      curr_allocated_block_(memory_pool_) {
  if (kMemoryPoolSize == 0) {
    GetErrorLogger().Log("Incorrect configuration value for 'memory_pool_size'", true);
  }

  if (kBlockSize == 0) {
    GetErrorLogger().Log("Incorrect configuration value for 'memory_pool_block_size'", true);
  }

  if (kMemoryPoolSize % kBlockSize != 0) {
    GetErrorLogger().Log("Incorrect configuration: 'memory_pool_size' must be a multiple of 'term_block_size'", true);
  }

  Init();
}

MemoryPoolManager::~MemoryPoolManager() {
  delete[] memory_pool_;
}

void MemoryPoolManager::Init() {
  memset(memory_pool_, 0, kMemoryPoolSize);
}

// Answers the question:
// Can we allocate enough blocks to be able to write out the posting with 'posting_len' into the current memory pool,
// starting from position 'curr_block_pos' in the currently allocated block?
bool MemoryPoolManager::HaveSpace(unsigned char* curr_block_pos, int posting_len) {
  unsigned char* next_block_start = GetNextBlockStart(curr_block_pos);

  posting_len -= (next_block_start - curr_block_pos);
  curr_block_pos = curr_allocated_block_;

  while (posting_len > 0) {
    curr_block_pos += kBlockSize;
    posting_len -= kBlockSize;
  }
  return (curr_block_pos >= (memory_pool_ + kMemoryPoolSize)) ? false : true;
}

unsigned char* MemoryPoolManager::AllocateBlock() {
  unsigned char* curr = curr_allocated_block_;
  curr_allocated_block_ += kBlockSize;

  if (curr != (memory_pool_ + kMemoryPoolSize)) {
    return curr;
  } else {
    return NULL;
  }
}

void MemoryPoolManager::Reset() {
  // Can start allocating from the beginning of the memory pool.
  curr_allocated_block_ = memory_pool_;
  Init();
}

/**************************************************************************************************************************************************************
 * PostingCollectionController
 *
 * TODO: When doing doc reordering, need to initialize starting docID in index to something other than 0. This is the parameter to PostingCollection().
 **************************************************************************************************************************************************************/
PostingCollectionController::PostingCollectionController() :
  index_count_(0), posting_collection_(new PostingCollection(index_count_, 0)), posting_count_(0) {
}

PostingCollectionController::~PostingCollectionController() {
  delete posting_collection_;
}

void PostingCollectionController::Finish() {
  posting_collection_->DumpRun(false);
  posting_count_ += posting_collection_->posting_count();
  delete posting_collection_;
  posting_collection_ = NULL;
}

void PostingCollectionController::InsertPosting(const Posting& posting) {
  if (!posting_collection_->InsertPosting(posting)) {
    posting_collection_->DumpRun(true);
    posting_count_ += posting_collection_->posting_count();

    GetMemoryPoolManager().Reset();
    PostingCollection* new_posting_collection = new PostingCollection(++index_count_, posting.doc_id());

    PostingCollection::OverflowPostings overflow_postings = posting_collection_->GetOverflowPostings();
    if (overflow_postings.postings() != NULL && overflow_postings.num_postings() > 0) {
      for (int i = 0; i < overflow_postings.num_postings(); ++i) {
        bool status = new_posting_collection->InsertPosting(overflow_postings.postings()[i]);
        assert(status == true);
      }
    }

    // Add the leftover posting to the new index last, since it was inserted after the overflow postings.
    new_posting_collection->InsertPosting(posting);

    delete posting_collection_;
    posting_collection_ = new_posting_collection;
  }
}

/**************************************************************************************************************************************************************
 * PostingCollection
 *
 **************************************************************************************************************************************************************/
PostingCollection::PostingCollection(int index_count, uint32_t starting_doc_id) :
  kHashTableSize(atol(Configuration::GetConfiguration().GetValue(config_properties::kHashTableSize).c_str())), term_block_table_(kHashTableSize), overflow_postings_(NULL),
      first_doc_id_in_index_(starting_doc_id), last_doc_id_in_index_(0), num_overflow_postings_(-1), prev_doc_id_(numeric_limits<uint32_t>::max()),
      prev_doc_length_(0), total_document_lengths_(0), total_num_docs_(0), total_unique_num_docs_(0), index_count_(index_count), posting_count_(0),
      kOrderedDocuments(true),
      kIndexPositions((Configuration::GetConfiguration().GetValue(config_properties::kIncludePositions) == "true") ? true : false),
      kIndexContexts((Configuration::GetConfiguration().GetValue(config_properties::kIncludeContexts) == "true") ? true : false) {
  if (kHashTableSize <= 0) {
    GetErrorLogger().Log("Incorrect configuration value for '" + string(config_properties::kHashTableSize) + "'", true);
  }
}

PostingCollection::~PostingCollection() {
  delete[] overflow_postings_;
}

bool PostingCollection::InsertPosting(const Posting& posting) {
  if (posting.doc_id() != last_doc_id_in_index_) {
    num_overflow_postings_ = -1;
    last_doc_id_in_index_ = posting.doc_id();
  }

  ++num_overflow_postings_;

  TermBlock* curr_tb = term_block_table_.Insert(posting.term(), posting.term_len());
  // Set these properties only on a newly created term block.
  if (curr_tb->block_list() == NULL) {
    curr_tb->set_ordered_documents(kOrderedDocuments);
    curr_tb->set_index_positions(kIndexPositions);
    curr_tb->set_index_contexts(kIndexContexts);
  }

  bool status = curr_tb->AddPosting(posting);
  if (status) {
    // Find the length of each document based on the position of the last posting from a particular docID.
    // Also tracks the unique number of documents we have processed.
    if (posting.doc_id() != prev_doc_id_) {
      total_document_lengths_ += prev_doc_length_;
      prev_doc_id_ = posting.doc_id();
      ++total_unique_num_docs_;
    } else {
      prev_doc_length_ = posting.position();
    }
    ++posting_count_;
  } else {
    // This is the leftover posting, so we won't have any same docIDs as the leftover posting in this index.
    // TODO: This assumes that the postings inserted into the index all have monotonically increasing docIDs.
    // All postings with the leftover posting docID will be saved and inserted into the index for the next run.
    last_doc_id_in_index_ = posting.doc_id() - 1;
    if (posting.doc_id() == prev_doc_id_) {
      --total_unique_num_docs_;
    }
  }

  return status;
}

// If 'out_of_memory_dump' is true, then we need to look for overflow postings, otherwise we don't.
// The leftover posting (there is only one), is the posting that we tried inserting, but didn't have enough room in the
// memory pool to insert, so it will have to go into the next index we write out.  The overflow postings are those postings
// that have the same docID as the leftover posting.  These postings will also have to be inserted into the next index we
// write out. The reason for this is to make sure that docIDs don't overlap between indices; this helps to make merging them easier.
void PostingCollection::DumpRun(bool out_of_memory_dump) {
  GetDefaultLogger().Log("Dumping Run # " + Stringify(index_count_), false);

  // Set to the range of docIDs in this index.
  total_num_docs_ = last_doc_id_in_index_ - first_doc_id_in_index_ + 1;

  if (!out_of_memory_dump) {
    // The total document lengths must be adjusted since we never see a new docID.
    total_document_lengths_ += prev_doc_length_;
  }

  int num_term_blocks = term_block_table_.num_elements();
  TermBlock** term_blocks = new TermBlock*[num_term_blocks];

  int i = 0;
  for (MoveToFrontHashTable<TermBlock>::Iterator it = term_block_table_.begin(); it != term_block_table_.end(); ++it) {
    TermBlock* curr_term_block = *it;

    // We don't include the TermBlock with the leftover posting if it has an empty block list (dereferencing it will cause a SEG fault).
    // Note that only the TermBlock that contains the leftover posting could possibly have a NULL block list
    // (since the TermBlock was initialized, but there was no room in the memory pool to insert the posting, that is, if this is the only posting for this term).
    if (curr_term_block->block_list() == NULL) {
      --num_term_blocks;
    } else {
      term_blocks[i++] = curr_term_block;
    }
  }

  sort(&term_blocks[0], &term_blocks[num_term_blocks], TermBlockCompare());

  ostringstream index_filename;
  index_filename << "index.idx.0." << index_count_;

  ostringstream lexicon_filename;
  lexicon_filename << "index.lex.0." << index_count_;

  IndexBuilder* index_builder = new IndexBuilder(lexicon_filename.str().c_str(), index_filename.str().c_str());

  uint32_t doc_ids[Chunk::kChunkSize];
  uint32_t frequencies[Chunk::kChunkSize];
  uint32_t positions[Chunk::kChunkSize * Chunk::kMaxProperties];
  unsigned char contexts[Chunk::kChunkSize * Chunk::kMaxProperties];

  overflow_postings_ = NULL;
  int overflow_postings_offset = 0;

  if (!out_of_memory_dump)
    num_overflow_postings_ = 0;

  if (num_overflow_postings_ > 0)
    overflow_postings_ = new Posting[num_overflow_postings_];

  int num_overflow_postings_remaining = num_overflow_postings_;

  // Go through each term in lexicographical order.
  for (i = 0; i < num_term_blocks; ++i) {
    TermBlock* curr_term_block = term_blocks[i];

    // Save the block list since we're gonna be modifying it.
    // We need to restore it before deleting the term block to avoid a memory leak.
    BlockList* start_of_list = curr_term_block->block_list();

    // Set our position in the term block to the beginning.
    curr_term_block->ResetCurrBlockPosition();

    DecodedPosting prev_posting;
    bool prev_posting_valid = false;

    uint32_t prev_chunk_last_doc_id = 0;
    int num_docs;
    int num_properties;
    int num_overflow_postings;

    do {
      // Collect all chunks of the current term.
      num_docs = Chunk::kChunkSize;
      num_properties = Chunk::kChunkSize * Chunk::kMaxProperties;
      num_overflow_postings = num_overflow_postings_remaining;

      bool have_chunk = curr_term_block->DecodePostings(doc_ids, frequencies, positions, contexts, &num_docs, &num_properties, &prev_posting,
                                                        &prev_posting_valid, overflow_postings_ + overflow_postings_offset, &num_overflow_postings,
                                                        last_doc_id_in_index_ + 1, prev_chunk_last_doc_id);

      if (num_overflow_postings > 0) {
        num_properties -= frequencies[num_docs - 1];
        num_docs -= 1;

        for (int j = overflow_postings_offset; j < (overflow_postings_offset + num_overflow_postings); ++j) {
          overflow_postings_[j].set_term(curr_term_block->term());
          overflow_postings_[j].set_term_len(curr_term_block->term_len());
        }

        num_overflow_postings_remaining -= num_overflow_postings;
        overflow_postings_offset += num_overflow_postings;
      }

      if (have_chunk && num_docs > 0) {
        assert(num_properties > 0);
        Chunk* chunk = new Chunk(doc_ids, frequencies, (kIndexPositions ? positions : NULL), (kIndexContexts ? contexts : NULL), num_docs, num_properties,
                                 prev_chunk_last_doc_id);
        prev_chunk_last_doc_id = chunk->last_doc_id();
        index_builder->Add(*chunk, curr_term_block->term(), curr_term_block->term_len());
      }
    } while (num_docs == Chunk::kChunkSize);

    // Restore block list.
    curr_term_block->set_block_list(start_of_list);
  }

  index_builder->Finalize();

  WriteMetaFile(index_builder);

  ++index_count_;

  delete[] term_blocks;
  delete index_builder;
}

void PostingCollection::WriteMetaFile(IndexBuilder* index_builder) {
  KeyValueStore index_metafile;
  ostringstream metafile_values;

  // TODO: Need to write the document offset to be used for the true docIDs in the index
  //       (and all docIDs would need to have the 'first_doc_id_in_index_' subtracted before insertion to the memory pool).
  //       This will allow us to store smaller docIDs (for the non-gap-coded ones, anyway) resulting in slightly better compression.

  metafile_values << kIndexPositions;
  index_metafile.AddKeyValuePair(meta_properties::kIncludesPositions, metafile_values.str());
  metafile_values.str("");

  metafile_values << kIndexContexts;
  index_metafile.AddKeyValuePair(meta_properties::kIncludesContexts, metafile_values.str());
  metafile_values.str("");

  metafile_values << total_document_lengths_;
  index_metafile.AddKeyValuePair(meta_properties::kTotalDocumentLengths, metafile_values.str());
  metafile_values.str("");

  metafile_values << total_num_docs_;
  index_metafile.AddKeyValuePair(meta_properties::kTotalNumDocs, metafile_values.str());
  metafile_values.str("");

  metafile_values << total_unique_num_docs_;
  index_metafile.AddKeyValuePair(meta_properties::kTotalUniqueNumDocs, metafile_values.str());
  metafile_values.str("");

  metafile_values << first_doc_id_in_index_;
  index_metafile.AddKeyValuePair(meta_properties::kFirstDocId, metafile_values.str());
  metafile_values.str("");

  metafile_values << last_doc_id_in_index_;
  index_metafile.AddKeyValuePair(meta_properties::kLastDocId, metafile_values.str());
  metafile_values.str("");

  metafile_values << posting_count_;
  index_metafile.AddKeyValuePair(meta_properties::kDocumentPostingCount, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder->posting_count();
  index_metafile.AddKeyValuePair(meta_properties::kIndexPostingCount, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder->num_unique_terms();
  index_metafile.AddKeyValuePair(meta_properties::kNumUniqueTerms, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder->total_num_block_header_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalHeaderBytes, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder->total_num_doc_ids_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalDocIdBytes, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder->total_num_frequency_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalFrequencyBytes, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder->total_num_positions_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalPositionBytes, metafile_values.str());
  metafile_values.str("");

  metafile_values << index_builder->total_num_wasted_space_bytes();
  index_metafile.AddKeyValuePair(meta_properties::kTotalWastedBytes, metafile_values.str());
  metafile_values.str("");

  ostringstream meta_filename;
  meta_filename << "index.meta.0." << index_count_;
  index_metafile.WriteKeyValueStore(meta_filename.str().c_str());
}

// TODO: If the load factor in the hash table is too high, we can dump the run to disk.
// A high load factor means we might have to look through a long chain of TermBlocks to find the right one, adversely affecting performance.
// Although, this is mitigated by the move-to-front extension to the hash table, so the load factor can get fairly high while still achieving good performance.
bool PostingCollection::ReachedThreshold() const {
  const int kLoadFactor = 20;  // Picked arbitrarily; could experiment with reasonable value if used.
  double load_factor = static_cast<double> (term_block_table_.num_elements()) / static_cast<double> (kHashTableSize);
  if (load_factor > kLoadFactor)
    return false;
  return true;
}
