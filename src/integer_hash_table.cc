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
#include "integer_hash_table.h"

#include <cstdio>
#include <cstring>

#include <limits>
using namespace std;

/**************************************************************************************************************************************************************
 * OpenAddressedIntegerHashTable
 *
 **************************************************************************************************************************************************************/
const uint32_t OpenAddressedIntegerHashTable::kFreeValue = numeric_limits<uint32_t>::max();
const uint32_t OpenAddressedIntegerHashTable::kDeletedValue = numeric_limits<uint32_t>::max() - 1;

const float OpenAddressedIntegerHashTable::kInverseLoadFactor;  // Initialized in the class definition.

OpenAddressedIntegerHashTable::OpenAddressedIntegerHashTable(int max_num_items) :
  num_keys_(0),
  pow_two_size_(GetPowTwoSize(max_num_items)),
  num_slots_(1 << pow_two_size_),
  table_(new uint32_t[num_slots_]) {
  memset(table_, kFreeValue, num_slots_ * sizeof(*table_));
}

OpenAddressedIntegerHashTable::~OpenAddressedIntegerHashTable() {
  delete[] table_;
}

bool OpenAddressedIntegerHashTable::Exists(uint32_t key) const {
  int slot_num = SlotNum(HashFunction(key));
  int initial_slot_num = slot_num;
  assert(slot_num < num_slots_);

  int first_deleted_slot = -1;
  while (table_[slot_num] != key) {
    if (table_[slot_num] == kFreeValue)
      return false;

    if (table_[slot_num] == kDeletedValue && first_deleted_slot == -1) {
      first_deleted_slot = slot_num;
    }

    ++slot_num;
    if (slot_num == num_slots_)
      slot_num = 0;

    // Make sure we don't infinite loop in the case that the hash table is full and the key doesn't exist.
    if (slot_num == initial_slot_num)
      return false;

    assert(slot_num < num_slots_);
  }

  if (first_deleted_slot != -1) {
    table_[first_deleted_slot] = table_[slot_num];
    table_[slot_num] = kDeletedValue;
  }

  return true;
}

void OpenAddressedIntegerHashTable::Insert(uint32_t key) {
  assert(num_keys_ < num_slots_);
  assert(Exists(key) == false);

  int slot_num = SlotNum(HashFunction(key));
  assert(slot_num < num_slots_);

  while (table_[slot_num] != kFreeValue && table_[slot_num] != kDeletedValue) {
    ++slot_num;
    if (slot_num == num_slots_)
      slot_num = 0;

    assert(slot_num < num_slots_);
  }

  table_[slot_num] = key;
  ++num_keys_;
}

bool OpenAddressedIntegerHashTable::Remove(uint32_t key) {
  int slot_num = SlotNum(HashFunction(key));
  int initial_slot_num = slot_num;
  assert(slot_num < num_slots_);

  while (table_[slot_num] != key) {
    if (table_[slot_num] == kFreeValue) {
      return false;
    }

    ++slot_num;
    if (slot_num == num_slots_)
      slot_num = 0;

    if (slot_num == initial_slot_num)
      return false;

    assert(slot_num < num_slots_);
  }

  table_[slot_num] = kDeletedValue;
  --num_keys_;
  return true;
}

void OpenAddressedIntegerHashTable::PrintTable() const {
  for (int i = 0; i < num_slots_; ++i) {
    if (table_[i] == kFreeValue) {
      printf("Slot %d: %c\n", i, '-');
    } else if (table_[i] == kDeletedValue) {
      printf("Slot %d: %c\n", i, 'x');
    } else {
      printf("Slot %d: %u\n", i, table_[i]);
    }
  }
}

int OpenAddressedIntegerHashTable::GetPowTwoSize(int n) const {
  int slots = n * kInverseLoadFactor;
  int b = 1;  // We always round up to the next power of two.
  while (slots >>= 1) {
    ++b;
  }
  return b;
}

/**************************************************************************************************************************************************************
 * ChainedIntegerHashTable
 *
 **************************************************************************************************************************************************************/
const uint32_t ChainedIntegerHashTable::kFreeValue = numeric_limits<uint32_t>::max();

const float ChainedIntegerHashTable::kInverseLoadFactor;  // Initialized in the class definition.

ChainedIntegerHashTable::ChainedIntegerHashTable(int max_num_items) :
  num_keys_(0),
  pow_two_size_(GetPowTwoSize(max_num_items)),
  num_slots_(1 << pow_two_size_),
  table_(new Int[num_slots_]) {
  for (int i = 0; i < num_slots_; ++i) {
    table_[i].num = kFreeValue;
    table_[i].next = NULL;
  }
}

ChainedIntegerHashTable::~ChainedIntegerHashTable() {
  delete[] table_;
}

bool ChainedIntegerHashTable::Exists(uint32_t key) const {
  int slot_num = SlotNum(HashFunction(key));
  assert(slot_num < num_slots_);

  if (table_[slot_num].num != kFreeValue) {
    Int* curr = &table_[slot_num];
    while (curr != NULL) {
      if (curr->num == key)
        return true;
      curr = curr->next;
    }
  }

  return false;
}

void ChainedIntegerHashTable::Insert(uint32_t key) {
  assert(Exists(key) == false);

  int slot_num = SlotNum(HashFunction(key));
  assert(slot_num < num_slots_);

  if (table_[slot_num].num != kFreeValue) {
    // Walk the chain until we get to the end.
    Int* prev = &table_[slot_num];
    Int* curr = table_[slot_num].next;
    while (curr != NULL) {
      prev = curr;
      curr = curr->next;
    }

    // Create the new node.
    curr = new Int;
    curr->num = key;
    curr->next = NULL;

    // Insert the new node.
    prev->next = curr;
  } else {
    table_[slot_num].num = key;
    table_[slot_num].next = NULL;
  }

  ++num_keys_;
}

bool ChainedIntegerHashTable::Remove(uint32_t key) {
  int slot_num = SlotNum(HashFunction(key));
  assert(slot_num < num_slots_);

  if (table_[slot_num].num != kFreeValue) {
    if (table_[slot_num].num == key) {
      Int* next = table_[slot_num].next;
      if (next != NULL) {
        table_[slot_num].num = next->num;
        table_[slot_num].next = next->next;
        delete next;
      } else {
        table_[slot_num].num = kFreeValue;
      }
    } else {
      // Walk the chain until we find the key.
      Int* prev = &table_[slot_num];
      Int* curr = table_[slot_num].next;
      while (curr != NULL && curr->num != key) {
        prev = curr;
        curr = curr->next;
      }

      if (curr != NULL) {
        assert(curr->num == key);
        prev->next = curr->next;
        delete curr;
      } else {
        return false;
      }
    }
  } else {
    assert(table_[slot_num].next == NULL);
    return false;
  }

  --num_keys_;
  return true;
}

void ChainedIntegerHashTable::PrintTable() const {
  for (int i = 0; i < num_slots_; ++i) {
    if (table_[i].num == kFreeValue) {
      assert(table_[i].next == NULL);
      printf("Slot %d: %c\n", i, '-');
    } else {
      printf("Slot %d: %u", i, table_[i].num);
      // Walk the chain.
      Int* next = table_[i].next;
      while (next != NULL) {
        printf(" -> %u", next->num);
        next = next->next;
      }
      printf("\n");
    }
  }
}

int ChainedIntegerHashTable::GetPowTwoSize(int n) const {
  int slots = n * kInverseLoadFactor;
  int b = 1;  // We always round up to the next power of two.
  while (slots >>= 1) {
    ++b;
  }
  return b;
}
