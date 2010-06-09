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
// Prints differences between two indices in a human readable manner.  Useful for debugging.
// Takes two indices as inputs (index1 and index2).
//
// There are three types of output:
// 1) If there exists a term and doc id in one index, but completely missing from the other index, a tuple will be output of this form:
// (index_num, 'term', doc_id, frequency, <positions>), which indicates that index 'index_num' contains this tuple (and that the other index does NOT contain
// this tuple).
//
// 2) If there exists a term and a doc id contained in both indices, but some of the per document information differs (positions), tuples will be
// output of the form (index_num, 'term', doc_id, {position}), which indicates that index 'index_num' contains the position 'position' in the doc id 'doc_id',
// while the other index is missing this position.
//
// 3) If there exists a term and a doc id contained in both indices, but they have a different amount of per document information (positions), this will be
// noted by a message such as "Frequencies differ: index1: 10, index2: 11 (Postings from index1 and index2 shown below)", where 10 and 11, are the frequencies
// of index1 and index2, respectively.  This message will be followed by 2 lines, of the form in case 1) above, which simply prints all the positions associated
// with each index of the term/doc id tuple (it does not indicate any missing tuples).  This will be followed by any position tuples that differ and/or are
// missing, in the format of case 2) above.
//
// This class can also output the differences in the indices limited to only a specified term, and not for the complete indices.
//==============================================================================================================================================================

#ifndef INDEX_DIFF_H_
#define INDEX_DIFF_H_

/**************************************************************************************************************************************************************
 * IndexDiff
 *
 **************************************************************************************************************************************************************/
class IndexFiles;
class Index;

class IndexDiff {
public:
  IndexDiff(const IndexFiles& index_files1, const IndexFiles& index_files2);
  ~IndexDiff();

  void Diff(const char* term, int term_len);

  void Print(Index* index, const char* term, int term_len);
private:
  int WhichIndex(Index* index);

  Index* index1_;
  Index* index2_;

  // Some index properties.
  bool includes_contexts_;
  bool includes_positions_;
};

#endif /* INDEX_DIFF_H_ */
