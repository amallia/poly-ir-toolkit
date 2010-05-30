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
// Chained hash table with move to front for terms. A new term will be inserted at the end of the chain for its hash bucket. On every access, a term will be
// moved to the front of the chain for its hash bucket.
// See Zobel, J., Heinz, S. & Williams, H. E. (2001), 'In-memory hash tables for accumulating text vocabularies' for info about advantages of move to front.
//==============================================================================================================================================================

#ifndef TERM_HASH_TABLE_H_
#define TERM_HASH_TABLE_H_

#include <cstring>
#include <algorithm>

template<class RecordT>
  class MoveToFrontHashTable {
  public:
    // Forward iterator into the hash table. Iterates over non NULL values.
    class Iterator : public std::iterator<std::forward_iterator_tag, RecordT> {
    public:
      Iterator(MoveToFrontHashTable<RecordT>& hash_table, int i) :
        hash_table_(hash_table), i_(i), curr_(NULL) {
        while (curr_ == NULL && ((i_ = i++) < hash_table_.kHashTableSize_)) {
          curr_ = hash_table_.htable_[i_];
        }
      }

      Iterator& operator=(const Iterator& rhs) {
        i_ = rhs.i_;
        curr_ = rhs.curr_;
        return *this;
      }

      bool operator==(const Iterator& rhs) {
        return (i_ == rhs.i_) && (curr_ == rhs.curr_);
      }

      bool operator!=(const Iterator& rhs) {
        return !(*this == rhs);
      }

      Iterator& operator++() {
        if (curr_ != NULL)
          curr_ = curr_->next();

        while (curr_ == NULL && ++i_ < hash_table_.kHashTableSize_) {
          curr_ = hash_table_.htable_[i_];
        }

        return *this;
      }

      Iterator& operator++(int) {
        Iterator tmp(*this);
        ++(*this);
        return tmp;
      }

      RecordT* operator*() {
        return curr_;
      }

      RecordT** operator->() {
        MoveToFrontHashTable<RecordT>::Iterator it = *this;
        RecordT* record = *it;
        return &record;
      }

    private:
      MoveToFrontHashTable<RecordT>& hash_table_;
      int i_;
      RecordT* curr_;
    };

    MoveToFrontHashTable(int hash_table_size);
    ~MoveToFrontHashTable();

    void Clear();

    // Inserts 'term' with length 'term_len' into the hash table if it doesn't already exist, and returns a pointer to the newly inserted RecordT.
    // If 'term' is already in the hash table, it returns a pointer to the existing RecordT.
    RecordT* Insert(const char* term, int term_len);

    RecordT* Find(const char* term, int term_len) const;

    int kHashTableSize() const {
      return kHashTableSize_;
    }

    int num_elements() const {
      return num_elements_;
    }

    Iterator begin() {
      return Iterator(*this, 0);
    }

    Iterator end() {
      return Iterator(*this, kHashTableSize_);
    }

  private:
    unsigned int HashTerm(const char* term, int term_len) const;

    const int kHashTableSize_;
    RecordT** htable_;
    int num_elements_;
  };

template<class RecordT>
  MoveToFrontHashTable<RecordT>::MoveToFrontHashTable(int hash_table_size) :
    kHashTableSize_(hash_table_size), htable_(new RecordT*[kHashTableSize_]), num_elements_(0) {
    for (int i = 0; i < kHashTableSize_; i++) {
      htable_[i] = NULL;
    }
  }

template<class RecordT>
  MoveToFrontHashTable<RecordT>::~MoveToFrontHashTable() {
    Clear();
    delete[] htable_;
  }

template<class RecordT>
  void MoveToFrontHashTable<RecordT>::Clear() {
    for (int i = 0; i < kHashTableSize_; i++) {
      RecordT* curr = htable_[i];
      RecordT* next;

      while (curr != NULL) {
        next = curr->next();
        delete curr;
        curr = next;
      }
      htable_[i] = NULL;
    }
    num_elements_ = 0;
  }

template<class RecordT>
  RecordT* MoveToFrontHashTable<RecordT>::Insert(const char* term, int term_len) {
    unsigned int hslot = HashTerm(term, term_len);
    RecordT* curr = htable_[hslot];
    RecordT* prev = NULL;

    while (curr != NULL && (curr->term_len() != term_len || strncasecmp(curr->term(), term, curr->term_len()) != 0)) {
      prev = curr;
      curr = curr->next();
    }

    if (curr != NULL) {
      if (prev != NULL) {
        // Move to front.
        prev->set_next(curr->next());
        curr->set_next(htable_[hslot]);
        htable_[hslot] = curr;
      }
    } else {
      curr = new RecordT(term, term_len);
      ++num_elements_;
      if (prev != NULL) {
        prev->set_next(curr);
      } else {
        htable_[hslot] = curr;
      }
    }

    return curr;
  }

template<class RecordT>
  RecordT* MoveToFrontHashTable<RecordT>::Find(const char* term, int term_len) const {
    unsigned int hslot = HashTerm(term, term_len);
    RecordT* curr = htable_[hslot];
    RecordT* prev = NULL;

    while (curr != NULL && (curr->term_len() != term_len || strncasecmp(curr->term(), term, curr->term_len()) != 0)) {
      prev = curr;
      curr = curr->next();
    }

    if (curr != NULL) {
      if (prev != NULL) {
        // Move to front.
        prev->set_next(curr->next());
        curr->set_next(htable_[hslot]);
        htable_[hslot] = curr;
      }
      return curr;
    }
    return NULL;
  }

// Note: Hash function is case insensitive, 'term' will be hashed as if it was completely lower case.
// Characters of 'term' that range from [A-Z] will be hashed as if they were in the range [a-z].
template<class RecordT>
  unsigned int MoveToFrontHashTable<RecordT>::HashTerm(const char* term, int term_len) const {
    // Author J. Zobel, April 2001.
    const unsigned int kLargePrime = 1159241;
    unsigned int h = kLargePrime;
    const char* term_end = term + term_len;
    char curr_term_char;
    for (; term != term_end; ++term) {
      curr_term_char = *term;
      h ^= ((h << 5) + (((curr_term_char >= 'A') && (curr_term_char <= 'Z')) ? (curr_term_char + ('a' - 'A')) : (curr_term_char)) + (h >> 2));
    }
    return h % kHashTableSize_;
  }

#endif /* TERM_HASH_TABLE_H_ */
