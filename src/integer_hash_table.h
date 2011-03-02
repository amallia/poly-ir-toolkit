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
#ifndef INTEGER_HASH_TABLE_H_
#define INTEGER_HASH_TABLE_H_

#include <cassert>
#include <stdint.h>

#include <utility>
/**************************************************************************************************************************************************************
 * OpenAddressedIntegerHashTable
 *
 * An open addressed hash table with linear probing that allows the insertion of integers as keys. This acts like a set, allowing only to find the key, and not
 * storing a value along with a key.
 * Design goal is to have good cache locality (so, linear probing with a step size of one and open addressing so, there are no pointer overheads).
 * The hash table will be sized up to a power of two so that we could find the hash slot pretty fast (use &(size - 1) trick instead of modulo).
 * The user specifies the maximum number of entries that will be hashed at any given time. We will then size the table so that the load factor does not increase
 * above (1/5). We do not currently resize the table when the load factor gets too high; therefore, the user should not insert more items than specified in the
 * constructor. The hash table uses a lazy delete scheme; deleting an item is done by simply marking it deleted. During a find operation, the item that's found
 * is moved up to the first available deleted slot in the probe path, in order to reclaim deleted space, and make future find operations faster.
 **************************************************************************************************************************************************************/
class OpenAddressedIntegerHashTable {
public:
  OpenAddressedIntegerHashTable(int max_num_items);
  ~OpenAddressedIntegerHashTable();

  bool Exists(uint32_t key) const;

  // We don't allow for duplicate keys, external code should make sure of that.
  void Insert(uint32_t key);

  bool Remove(uint32_t key);

  void PrintTable() const;

private:
  // See 'http://burtleburtle.net/bob/hash/integer.html' for more info than you can stomach on integer hash functions.
  uint32_t HashFunction(uint32_t n) const {
    n = (n ^ 61) ^ (n >> 16);
    n = n + (n << 3);
    n = n ^ (n >> 4);
    n = n * 0x27d4eb2d;
    n = n ^ (n >> 15);
    return n;
  }

  // Returns an index into the hash table.
  int SlotNum(uint32_t hash) const {
    // 'num_slots' should be a power of two.
    return hash & (num_slots_ - 1);
  }

  int GetPowTwoSize(int n) const;

  static const uint32_t kFreeValue;     // Default value for free table slots (should never occur as a key).
  static const uint32_t kDeletedValue;  // Default value for deleted table slots (should never occur as a key).

  static const float kInverseLoadFactor = 5.0;  // Inverse load factor for this hash table.

  int num_keys_;      // Number of keys inserted into this hash table.
  int pow_two_size_;  // Our hash table size must be a power of two.
  int num_slots_;     // The number of slots in our hash table.
  uint32_t* table_;   // The hash table.
};

/**************************************************************************************************************************************************************
 * ChainedIntegerHashTable
 *
 * A chained hash table with the first (integer, pointer) pair stored directly in the table for better locality. This hash table implements a set (i.e. no value
 * is stored along with the integer).
 **************************************************************************************************************************************************************/
class ChainedIntegerHashTable {
public:
  ChainedIntegerHashTable(int max_num_items);
  ~ChainedIntegerHashTable();

  bool Exists(uint32_t key) const;

  // We don't allow for duplicate keys, external code should make sure of that.
  void Insert(uint32_t key);

  bool Remove(uint32_t key);

  void PrintTable() const;

private:
  struct Int {
    uint32_t num;
    Int* next;
  };

  // See 'http://burtleburtle.net/bob/hash/integer.html' for more info than you can stomach on integer hash functions.
  uint32_t HashFunction(uint32_t n) const {
    n = (n ^ 61) ^ (n >> 16);
    n = n + (n << 3);
    n = n ^ (n >> 4);
    n = n * 0x27d4eb2d;
    n = n ^ (n >> 15);
    return n;
  }

  // Returns an index into the hash table.
  int SlotNum(uint32_t hash) const {
    // 'num_slots' should be a power of two.
    return hash & (num_slots_ - 1);
  }

  int GetPowTwoSize(int n) const;

  static const uint32_t kFreeValue;  // Default value for empty table slots (should never occur as a key).

  static const float kInverseLoadFactor = 5.0;  // Inverse load factor for this hash table.

  int num_keys_;      // Number of keys inserted into this hash table.
  int pow_two_size_;  // Our hash table size must be a power of two.
  int num_slots_;     // The number of slots in our hash table.
  Int* table_;        // The hash table.
};

#endif /* INTEGER_HASH_TABLE_H_ */
