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
// TODO: Replace defines with static const ints.
// TODO: Would be good to have a class that defines what a chunk consists of.
//==============================================================================================================================================================

#ifndef INDEX_LAYOUT_PARAMETERS_H_
#define INDEX_LAYOUT_PARAMETERS_H_

#include <stdint.h>

// Maximum number of documents in a chunk.
#define CHUNK_SIZE 128

// Fixed size in bytes of a block (It's likely that the last few bytes in a block are garbage, or rather they are zeroed out).
#define BLOCK_SIZE 65536

// Instead of storing the position and context for every frequency (which are sometimes in the thousands)
// Store a maximum of MAX_FREQUENCY_PROPERTIES positions/contexts for every document in a list, regardless of actual frequency
// This solves the problem of having very large chunks, that are greater than the BLOCK_SIZE, which is not allowed
// and minimizes empty space at the end of a block since chunks are small.
// The actual frequency stored in the index is not limited by this parameter.
#define MAX_FREQUENCY_PROPERTIES 64

// The lower bound size of a single chunk in bytes (one integer per docID and frequency).
// Note that this does not include an integer for the positions because we don't always index positions,
// but we always have to include the docIDs and frequencies in an index.
// Note that we could have a tighter bound here if we configured this dynamically;
// we would then know if we could add another integer for the positions or not.
// All our coding methods use at least one word per integer encoded.
#define MIN_COMPRESSED_CHUNK_SIZE (2 * sizeof(uint32_t))

// Maximum number of layers that can be part of a single inverted list.
#define MAX_LIST_LAYERS 8

#endif /* INDEX_LAYOUT_PARAMETERS_H_ */
