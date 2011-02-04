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

#ifndef POSTING_COLLECTION_H_
#define POSTING_COLLECTION_H_

// Enables debugging output for this module.
//#define POSTING_COLLECTION_DEBUG

#include <cassert>
#include <stdint.h>

#include "coding_policy.h"
#include "document_map.h"
#include "term_hash_table.h"

class PostingCollectionController;
PostingCollectionController& GetPostingCollectionController();

class MemoryPoolManager;
MemoryPoolManager& GetMemoryPoolManager();

/**************************************************************************************************************************************************************
 * Posting
 *
 * TODO: Should provide variable and fixed byte fields that can be used to easily store additional information in the index. This will be per document info.
 * Contexts can be the test case. The variable length byte fields should also hold a variable indicating the size of the field.
 **************************************************************************************************************************************************************/
class Posting {
public:
  Posting() :
    term_(NULL), term_len_(0), doc_id_(0), position_(0), context_('\0') {
  }

  Posting(const char* term, int term_len, uint32_t doc_id, uint32_t position, unsigned char context) :
    term_(term), term_len_(term_len), doc_id_(doc_id), position_(position), context_(context) {
  }

  const char* term() const {
    return term_;
  }

  void set_term(const char* term) {
    term_ = term;
  }

  int term_len() const {
    return term_len_;
  }

  void set_term_len(int term_len) {
    term_len_ = term_len;
  }

  uint32_t doc_id() const {
    return doc_id_;
  }

  uint32_t position() const {
    return position_;
  }

  unsigned char context() const {
    return context_;
  }

private:
  const char* term_;
  int term_len_;
  uint32_t doc_id_;
  uint32_t position_;
  unsigned char context_;
};

/**************************************************************************************************************************************************************
 * DecodedPosting
 *
 **************************************************************************************************************************************************************/
class DecodedPosting {
public:
  DecodedPosting() {
  }

  DecodedPosting(uint32_t doc_id, uint32_t position, unsigned char context) :
    doc_id_(doc_id), position_(position), context_(context) {
  }

  uint32_t doc_id() const {
    return doc_id_;
  }

  void set_doc_id(uint32_t doc_id) {
    doc_id_ = doc_id;
  }

  uint32_t position() const {
    return position_;
  }

  void set_position(uint32_t position) {
    position_ = position;
  }

  unsigned char context() const {
    return context_;
  }

  void set_context(unsigned char context) {
    context_ = context;
  }

private:
  uint32_t doc_id_;
  uint32_t position_;
  unsigned char context_;
};

class PostingCollection;

/**************************************************************************************************************************************************************
 * MemoryPoolManager
 *
 **************************************************************************************************************************************************************/
class MemoryPoolManager
{
public:
  MemoryPoolManager();
  ~MemoryPoolManager();

  void Init();

  unsigned char* AllocateBlock();

  unsigned char* GetNextBlockStart(unsigned char* curr_block_pos) {
    unsigned char* next_block_start = memory_pool_ + ((((curr_block_pos - memory_pool_) / kBlockSize) + 1) * kBlockSize);
    return next_block_start;
  }

  bool HaveSpace(unsigned char* curr_block_pos, int posting_len);

  void Reset();

  unsigned char* memory_pool() const {
    return memory_pool_;
  }

  unsigned char* curr_allocated_block() const {
    return curr_allocated_block_;
  }

  const long int kMemoryPoolSize;
  const long int kBlockSize;

private:
  PostingCollection* posting_collection_;

  unsigned char* memory_pool_;
  unsigned char* curr_allocated_block_;
};


/**************************************************************************************************************************************************************
 * BlockList
 *
 * Linked list of block pointers into the memory pool for a certain TermBlock.
 **************************************************************************************************************************************************************/
class BlockList {
public:
  BlockList(unsigned char* block) :
      block_(block), next_block_(NULL) {
    assert(block_ != NULL);
  }

  bool IsWithinBlock(const unsigned char* pos) const {
    if (pos - block_ < GetMemoryPoolManager().kBlockSize)
      return true;
    return false;
  }

  unsigned char* block() const {
    return block_;
  }

  BlockList* next_block() const {
    return next_block_;
  }

  void set_block(unsigned char* block) {
    block_ = block;
    assert(block_ != NULL);
  }

  void set_next_block(BlockList* next_block) {
    next_block_ = next_block;
  }

private:
  unsigned char* block_;
  BlockList* next_block_;
};

/**************************************************************************************************************************************************************
 * TermBlock
 *
 **************************************************************************************************************************************************************/
class TermBlock {
public:
  TermBlock(const char* term, int term_len);

  ~TermBlock();

  void Encode(uint32_t num, unsigned char* out, int* len);

  uint32_t Decode(const unsigned char* in, int* len);

  bool DecodePosting(DecodedPosting* decoded_posting);

  bool DecodePostings(uint32_t* doc_ids, uint32_t* frequencies, uint32_t* positions, unsigned char* contexts, int* num_docs, int* num_properties, DecodedPosting* prev_posting, bool* prev_posting_valid, Posting* overflow_postings, int* num_overflow_postings, uint32_t overflow_doc_id, uint32_t prev_chunk_last_doc_id);
  void DecodePostings(DecodedPosting* decoded_postings, int* decoded_postings_len, Posting* overflow_postings, int* num_overflow_postings, uint32_t overflow_doc_id);

  bool VarByteHasMore(const unsigned char* byte) {
    return ((*byte & 0x1) != 0) ? true : false;
  }

  uint32_t GetVarByteInt();

  uint32_t GetByte();

  void EncodePosting(const Posting& posting);

  bool AddCompressedPosting();

  bool AddPosting(const Posting& posting);

  const char* term() const {
    return term_;
  }

  const int term_len() const {
    return term_len_;
  }

  TermBlock* next() const {
    return next_;
  }

  void set_next(TermBlock* ntb) {
    next_ = ntb;
  }

  BlockList* block_list() const {
    return block_list_;
  }

  void set_block_list(BlockList* block_list) {
    block_list_ = block_list;
  }

  BlockList* last_block() const {
    return last_block_;
  }

  void ResetCurrBlockPosition() {
    curr_block_position_ = block_list_->block();
  }

  void ClearBlockList();

  void InitBlockList(unsigned char* block_start);

  void AddBlockToList(unsigned char* block_start);

  bool index_positions() const {
    return index_positions_;
  }

  void set_index_positions(bool index_positions) {
    index_positions_ = index_positions;
  }

  bool index_contexts() const {
    return index_contexts_;
  }

  void set_index_contexts(bool index_contexts) {
    index_contexts_ = index_contexts_;
  }

private:
  // Encode it into here first so that we know it fits in one run
  static unsigned char compressed_tmp_posting[11];  // Max 5 bytes for doc_id and position and 1 byte for context.
  static int compressed_tmp_posting_len;

  MemoryPoolManager* memory_pool_manager_;

  char* term_;
  int term_len_;

  // These are properties of the index to be built. They should be set before adding any postings.
  bool index_positions_;    // Determines whether the position information will be collected.
  bool index_contexts_;     // Determines whether context information will be collected.

  uint32_t prev_doc_id_;    // Necessary for taking docID deltas.
  uint32_t prev_position_;  // Necessary for taking position deltas within a document.

  BlockList* block_list_;
  BlockList* last_block_;  // For fast insertion at the end of the list.
  unsigned char* curr_block_position_;
  TermBlock* next_;
};

/**************************************************************************************************************************************************************
 * TermBlockCompare
 *
 **************************************************************************************************************************************************************/
struct TermBlockCompare {
  bool operator()(const TermBlock* lhs, const TermBlock* rhs) {
    int cmp = strncmp(lhs->term(), rhs->term(), std::min(lhs->term_len(), rhs->term_len()));
    if (cmp == 0)
      return lhs->term_len() <= rhs->term_len();
    return (cmp < 0) ? true : false;
  }
};

/**************************************************************************************************************************************************************
 * PostingCollectionController
 *
 **************************************************************************************************************************************************************/
class PostingCollectionController {
public:
  PostingCollectionController();
  ~PostingCollectionController();
  void Finish();
  void InsertPosting(const Posting& posting);
  void SaveDocLength(int doc_length, uint32_t doc_id);
  void SaveDocUrl(const char* url, int url_len, uint32_t doc_id);
  void SaveDocno(const char* docno, int docno_len, uint32_t doc_id);

  uint64_t posting_count() const {
    return posting_count_;
  }

private:
  int index_count_;  // The current mini index we're working on building.
  PostingCollection* posting_collection_;  // The posting collection for the current mini index.

  // TODO: Currently this will write the document map for the complete index.
  //       Ideally, we want a separate document map for each index slice. This would be especially useful when doing parallel indexing.
  //       We can have a separate method in the current PostingCollection that will be used to insert the document map entries.
  //       Before dumping the current index slice, we can get the overflow document map entry and insert it into the new index slice instead.
  //       Note: if we dump an index before we insert all the postings for it, we know the document map entry will go into the next index slice.
  DocumentMapWriter document_map_writer_;

  uint64_t posting_count_;  // For statistics purposes. Counts the total number of postings in the collection.
};

/**************************************************************************************************************************************************************
 * PostingCollection
 *
 * Class in charge of accumulating postings in memory and dumping them to "mini", fully self contained and usable indexes.
 **************************************************************************************************************************************************************/
class IndexBuilder;
class PostingCollection {
public:
  class OverflowPostings {
  public:
    OverflowPostings(Posting* overflow_postings, int num_overflow_postings) :
      postings_(overflow_postings), num_postings_(num_overflow_postings) {
    }

    Posting* postings() const {
      return postings_;
    }

    int num_postings() const {
      return num_postings_;
    }

  private:
    Posting* postings_;
    int num_postings_;
  };

  PostingCollection(int index_count, uint32_t starting_doc_id);
  ~PostingCollection();

  bool InsertPosting(const Posting& posting);
  void AddLeftOverPosting(const char* term, int term_len);
  void DumpRun(bool out_of_memory_dump);
  void WriteMetaFile(const IndexBuilder* index_builder, const std::string& meta_filename);
  bool ReachedThreshold() const;

  OverflowPostings GetOverflowPostings() {
    return OverflowPostings(overflow_postings_, num_overflow_postings_);
  }

  uint64_t posting_count() const {
    return posting_count_;
  }

  uint32_t last_doc_id_in_index() const {
    return last_doc_id_in_index_;
  }

  void set_last_doc_id_in_index(uint32_t doc_id) {
    last_doc_id_in_index_ = doc_id;
  }

private:
  const long int kHashTableSize;
  MoveToFrontHashTable<TermBlock> term_block_table_;

  // Postings that should go into the next index are referred to as "overflow postings".
  // We want each index to have a unique subset of the docIDs in the complete document collection (ie, no overlapping of docIDs between indices).
  // But it so happens that the memory pool might be full at a point when we did not finish processing all the terms from a document.
  // Thus, to handle this problem, we have to mark a doc id beyond which this index will not have any more greater docIDs.
  // Lastly, any docIDs that were inserted into this posting collection that are beyond the 'last_doc_id_in_index_' will not be written to the index,
  // but instead inserted into the index for the next run. Yes, it's a bit messy.
  Posting* overflow_postings_;
  uint32_t first_doc_id_in_index_;
  uint32_t last_doc_id_in_index_;
  int num_overflow_postings_;

  uint32_t prev_doc_id_;  // Used to detect postings associated with a new (never before seen) docID.
  int prev_doc_length_;   // Used to keep track of the lengths of documents.

  // The following members can be used together to calculate the average document length in this index.
  uint64_t total_document_lengths_;  // The sum of all document lengths in this index.
  uint32_t total_num_docs_;          // The range of docIDs in the index.

  uint32_t total_unique_num_docs_;  // The total number of unique documents in this index (where each document has at least one posting).
                                    // This can be different from 'total_num_docs_' when some documents don't contribute any postings to the index.

  int index_count_;  // The current mini index we're working on building.

  uint64_t posting_count_;  // For statistics purposes. Counts the number of postings in the current index we're building.

  // Properties of the index to be built.
  const bool kIndexPositions;  // Whether positions are indexed.
  const bool kIndexContexts;   // Whether contexts are indexed.

  // Compressors to be used for various parts of the index.
  CodingPolicy doc_id_compressor_;
  CodingPolicy frequency_compressor_;
  CodingPolicy position_compressor_;
  CodingPolicy block_header_compressor_;
};

#endif /* POSTING_COLLECTION_H_ */
