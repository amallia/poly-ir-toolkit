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
// Defines standard properties that are used by index meta file readers and writers.
//==============================================================================================================================================================

#ifndef META_FILE_PROPERTIES_H_
#define META_FILE_PROPERTIES_H_

namespace meta_properties {

// Whether the index was indexed with position data.
static const char kIncludesPositions[] = "includes_positions";

// Whether the index was indexed with context data.
static const char kIncludesContexts[] = "includes_contexts";

// The sum of all document lengths in the index (length is based on the position of the last posting relating to a particular docID).
static const char kTotalDocumentLengths[] = "total_document_lengths";

// The total number of documents input to the indexer.
static const char kTotalNumDocs[] = "total_num_docs";

// The number of unique docIDs in the index. This may be different from 'kTotalNumDocs' when some documents don't produce any postings inserted into the index.
static const char kTotalUniqueNumDocs[] = "total_unique_num_docs";

// The first (lowest) docID that is contained in this index.
static const char kFirstDocId[] = "first_doc_id";

// The last (highest) docID that is contained in this index.
static const char kLastDocId[] = "last_doc_id";

// The number of unique terms contained in this index. This is the number of terms in the lexicon.
static const char kNumUniqueTerms[] = "num_unique_terms";

// The number of postings provided as input to the indexer (it may not index all of them; limits defined in 'index_layout_parameters.h').
static const char kDocumentPostingCount[] = "document_posting_count";

// The number of postings actually indexed.
static const char kIndexPostingCount[] = "index_posting_count";

// The total number of bytes used for the block headers in this index.
static const char kTotalHeaderBytes[] = "total_header_bytes";

// The total number of bytes used for the docIDs in this index.
static const char kTotalDocIdBytes[] = "total_doc_id_bytes";

// The total number of bytes used for the frequencies in this index.
static const char kTotalFrequencyBytes[] = "total_frequency_bytes";

// The total number of bytes used for the positions in this index.
static const char kTotalPositionBytes[] = "total_position_bytes";

// The total number of bytes used to pad the blocks to fill them to exactly block size.
static const char kTotalWastedBytes[] = "total_wasted_bytes";

} // namespace meta_properties

#endif /* META_FILE_PROPERTIES_H_ */
